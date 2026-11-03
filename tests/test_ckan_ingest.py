"""
test_ckan_ingest.py

Unit tests for src/ingest/ckan_ingest.py. No network and no Databricks needed:
a fake HTTP session replaces requests, and pytest's tmp_path replaces the volume.

Run locally (from the repository root):
    pytest -v tests/

Run in a Databricks notebook (repository checked out as a Git folder):
    import sys, pytest
    sys.dont_write_bytecode = True
    pytest.main(["-v", "-p", "no:cacheprovider", "tests/"])

Optional live smoke test (real network, skipped by default):
    CKAN_LIVE_TEST=1 pytest -v -k live tests/
"""

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

import ckan_ingest as ci

ROOT = Path(__file__).resolve().parents[1]
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status=200, content=b"", content_type="application/json"):
        self.status_code = status
        self.content = content
        self.headers = {"content-type": content_type}

    @property
    def text(self):
        return self.content.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.content)


class FakeSession:
    """Routes URL -> FakeResponse or Exception, and records every requested URL."""

    def __init__(self, routes):
        self.routes = dict(routes)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(url)
        if url not in self.routes:
            raise AssertionError(f"unexpected URL requested: {url}")
        outcome = self.routes[url]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


GATEWAY_TIMEOUT = FakeResponse(
    504, b"<html><title>504 Gateway Time-out</title></html>", "text/html"
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def make_spec(key="ds_a", pattern=ci.NEW_FILE, **overrides):
    """A DatasetSpec with sensible test defaults."""
    values = dict(
        key=key,
        domain="immigration",
        ckan_id=f"{key}-id",
        update_pattern=pattern,
        status=ci.STATUS_ACTIVE,
        purpose="test purpose",
        base_url="https://ckan.example/api/action",
    )
    values.update(overrides)
    return ci.DatasetSpec(**values)


def make_entry(**overrides):
    """A valid raw registry entry (as parsed from YAML)."""
    entry = {"key": "a", "domain": "d", "ckan_id": "x", "update_pattern": ci.NEW_FILE,
             "status": "active", "purpose": "why"}
    entry.update(overrides)
    return entry


def make_resource(rid, lang="en", fmt="XLSX", name=None, created="2024-01-01",
                  last_modified=None, url=None):
    """A CKAN resource dict with only the fields the module reads."""
    res = {
        "id": rid,
        "language": [lang],
        "format": fmt,
        "name": name or f"Resource {rid}",
        "created": created,
        "url": url if url is not None else f"https://files.example/{rid}/data_{lang}.xlsx",
    }
    if last_modified:
        res["last_modified"] = last_modified
    return res


def metadata(resources, success=True):
    """A 200 JSON response shaped like CKAN package_show."""
    body = {"success": success, "result": {"resources": resources}}
    return FakeResponse(200, json.dumps(body).encode())


def api(spec):
    return spec.package_show_url


@pytest.fixture
def settings(tmp_path):
    """Landing and metadata roots in a fresh temp directory."""
    return ci.RunSettings(
        landing_root=str(tmp_path / "raw"),
        meta_root=str(tmp_path / "_meta"),
        timeout=(1, 1),
    )


# ---------------------------------------------------------------------------
# Registry parsing and validation
# ---------------------------------------------------------------------------


def test_parse_registry_applies_defaults_and_normalises_lists():
    raw = {
        "defaults": {"languages": ["en", "fr"], "source": "open_canada"},
        "datasets": [make_entry(domain="housing", formats=["csv", "xlsx"])],
    }
    (spec,) = ci.parse_registry(raw)
    assert spec.languages == ("en", "fr")
    assert spec.formats == ("CSV", "XLSX")  # upper-cased tuple
    assert spec.source == "open_canada"


def test_parse_registry_entry_overrides_defaults():
    raw = {
        "defaults": {"languages": ["en"]},
        "datasets": [make_entry(languages=["fr"])],
    }
    assert ci.parse_registry(raw)[0].languages == ("fr",)


@pytest.mark.parametrize(
    "entry, message",
    [
        (make_entry(ckan_id=""), "missing"),
        (make_entry(update_pattern="weekly"), "update_pattern"),
        (make_entry(colour="red"), "unknown keys"),
        (make_entry(key="Bad Key"), "must match"),
        (make_entry(name_regex="("), "invalid name_regex"),
        (make_entry(status=""), "missing"),
        (make_entry(status="paused"), "status must be one of"),
        (make_entry(purpose=""), "missing"),
        (make_entry(purpose="   "), "purpose must be a non-empty string"),
        (make_entry(purpose=42), "purpose must be a non-empty string"),
    ],
)
def test_parse_registry_rejects_invalid_entries(entry, message):
    with pytest.raises(ci.RegistryError, match=message):
        ci.parse_registry({"datasets": [entry]})


def test_parse_registry_requires_status_and_purpose_keys():
    for key in ("status", "purpose"):
        entry = make_entry()
        del entry[key]
        with pytest.raises(ci.RegistryError, match=f"missing required keys.*{key}"):
            ci.parse_registry({"datasets": [entry]})


@pytest.mark.parametrize("status", ci.STATUSES)
def test_parse_registry_accepts_every_status_and_trims_purpose(status):
    (spec,) = ci.parse_registry(
        {"datasets": [make_entry(status=status, purpose="  Why we need it.  ")]}
    )
    assert spec.status == status
    assert spec.purpose == "Why we need it."


def test_parse_registry_rejects_legacy_enabled_and_says_what_to_write():
    with pytest.raises(ci.RegistryError) as excinfo:
        ci.parse_registry({"datasets": [make_entry(enabled=False)]})
    message = str(excinfo.value)
    assert "'enabled' was replaced by 'status'" in message
    assert "'status: active' (or 'candidate' / 'deprecated')" in message


@pytest.mark.parametrize("purpose", ["TODO", "TODO: why this dataset", "  TODO: padded"])
def test_parse_registry_rejects_todo_purpose_on_active_entry(purpose):
    with pytest.raises(ci.RegistryError, match="active entry needs a real purpose"):
        ci.parse_registry(
            {"datasets": [make_entry(status=ci.STATUS_ACTIVE, purpose=purpose)]}
        )


@pytest.mark.parametrize("status", [ci.STATUS_CANDIDATE, ci.STATUS_DEPRECATED])
def test_parse_registry_allows_todo_purpose_on_other_statuses(status):
    (spec,) = ci.parse_registry(
        {"datasets": [make_entry(status=status, purpose="  TODO: fill in")]}
    )
    assert spec.purpose == "TODO: fill in"


def test_parse_registry_allows_active_purpose_that_only_mentions_todo():
    # Only a leading TODO marks a placeholder.
    (spec,) = ci.parse_registry(
        {"datasets": [make_entry(purpose="Replaces the old TODO list of employers")]}
    )
    assert spec.status == ci.STATUS_ACTIVE


def test_parse_registry_rejects_duplicate_keys():
    entries = [make_entry(), make_entry(ckan_id="y")]
    with pytest.raises(ci.RegistryError, match="duplicate key"):
        ci.parse_registry({"datasets": entries})


def test_parse_registry_rejects_duplicate_ckan_id_on_same_portal():
    entries = [make_entry(key="a"), make_entry(key="b")]
    with pytest.raises(ci.RegistryError, match="duplicate ckan_id 'x'"):
        ci.parse_registry({"datasets": entries})

    # A trailing slash is the same portal.
    entries = [
        make_entry(key="a", base_url="https://p.example/api/action"),
        make_entry(key="b", base_url="https://p.example/api/action/"),
    ]
    with pytest.raises(ci.RegistryError, match="duplicate ckan_id"):
        ci.parse_registry({"datasets": entries})


def test_parse_registry_allows_same_ckan_id_on_different_portals():
    entries = [
        make_entry(key="a", base_url="https://one.example/api/action"),
        make_entry(key="b", base_url="https://two.example/api/action"),
    ]
    assert [s.key for s in ci.parse_registry({"datasets": entries})] == ["a", "b"]


def test_shipped_registry_file_is_valid():
    # Guards config/datasets.yml against typos before they reach a job run.
    specs = ci.load_registry(str(ROOT / "config" / "datasets.yml"))
    keys = [s.key for s in specs]
    assert "lmia_positive_employers" in keys
    assert len(keys) == len(set(keys))
    assert all(s.purpose for s in specs)
    # Existing landing paths and lineage depend on this key staying unchanged.
    lmia = next(s for s in specs if s.key == "lmia_positive_employers")
    assert lmia.status == ci.STATUS_ACTIVE
    assert lmia.update_pattern == ci.NEW_FILE


def test_shipped_registry_ingests_only_active_entries():
    specs = ci.load_registry(str(ROOT / "config" / "datasets.yml"))
    selected = ci.select_specs(specs)
    assert selected and all(s.status == ci.STATUS_ACTIVE for s in selected)
    assert {s.status for s in specs} >= {ci.STATUS_ACTIVE, ci.STATUS_CANDIDATE}
    # Registered but not ingested: candidates are skipped.
    assert not {s.key for s in specs if s.status == ci.STATUS_CANDIDATE} & {
        s.key for s in selected
    }


def test_select_specs_runs_only_active_and_filters_domain_and_key():
    specs = [
        make_spec("a", domain="immigration"),
        make_spec("b", domain="housing"),
        make_spec("c", domain="immigration", status=ci.STATUS_CANDIDATE),
        make_spec("d", domain="immigration", status=ci.STATUS_DEPRECATED),
    ]
    assert [s.key for s in ci.select_specs(specs)] == ["a", "b"]
    assert [s.key for s in ci.select_specs(specs, keys=["c", "d"])] == []
    assert [s.key for s in ci.select_specs(specs, domains=["housing"])] == ["b"]
    assert [s.key for s in ci.select_specs(specs, keys=["a"])] == ["a"]


# ---------------------------------------------------------------------------
# Resource selection, versions and paths
# ---------------------------------------------------------------------------


def test_select_resources_applies_language_format_and_name_filters():
    spec = make_spec(formats=("XLSX",), name_regex="province")
    result = {"resources": [
        make_resource("keep", name="PR by Province/Territory"),
        make_resource("fr", lang="fr", name="PR by Province/Territory"),
        make_resource("csv", fmt="CSV", name="PR by Province/Territory"),
        make_resource("other", name="PR by Country of Citizenship"),
        make_resource("nourl", url="", name="PR by Province/Territory"),
    ]}
    assert [r["id"] for r in ci.select_resources(result, spec)] == ["keep"]


def test_select_resources_prefers_english_translated_name():
    spec = make_spec(name_regex="province")
    res = make_resource("a", name="Résidents par province")
    res["name"] = "unrelated"
    res["name_translated"] = {"en": "Residents by Province", "fr": "..."}
    assert len(ci.select_resources({"resources": [res]}, spec)) == 1


def test_select_resources_sorted_oldest_first():
    spec = make_spec()
    result = {"resources": [make_resource("new", created="2025"),
                            make_resource("old", created="2014")]}
    assert [r["id"] for r in ci.select_resources(result, spec)] == ["old", "new"]


def test_resource_version_normalises_timestamp():
    assert ci.resource_version({"last_modified": "2025-10-23T15:50:27.245365"}) == "20251023T155027"
    assert ci.resource_version({}) is None


def test_resource_version_ignores_metadata_modified():
    # metadata_modified can stay put when the file is replaced, so it must not
    # stand in for last_modified.
    assert ci.resource_version({"metadata_modified": "2024-01-02T03:04:05"}) is None
    both = {"last_modified": "2025-10-23T15:50:27", "metadata_modified": "2024-01-02T03:04:05"}
    assert ci.resource_version(both) == "20251023T155027"


@pytest.mark.parametrize("last_modified", [None, "", "-", "///"])
def test_resource_version_is_none_when_last_modified_is_unusable(last_modified):
    assert ci.resource_version({"last_modified": last_modified}) is None


def test_target_relpath_per_pattern():
    res = make_resource("rid", url="https://x/download/tfwp_2025q2_pos_en.xlsx")
    assert ci.target_relpath(res, make_spec(), None) == "rid_tfwp_2025q2_pos_en.xlsx"
    snap = make_spec(pattern=ci.OVERWRITE)
    assert ci.target_relpath(res, snap, "20251023T155027") == "rid/20251023T155027_tfwp_2025q2_pos_en.xlsx"
    with pytest.raises(ValueError):
        ci.target_relpath(res, snap, None)


# ---------------------------------------------------------------------------
# Metadata and cache fallback
# ---------------------------------------------------------------------------


def test_fetch_metadata_rejects_gateway_timeout_html():
    spec = make_spec()
    with pytest.raises(ci.UpstreamError, match="504"):
        ci.fetch_metadata(FakeSession({api(spec): GATEWAY_TIMEOUT}), spec, (1, 1))


def test_fetch_metadata_rejects_ckan_failure():
    spec = make_spec()
    with pytest.raises(ci.UpstreamError, match="CKAN reported failure"):
        ci.fetch_metadata(FakeSession({api(spec): metadata([], success=False)}), spec, (1, 1))


def test_get_metadata_caches_then_falls_back(settings):
    spec = make_spec()
    _, source = ci.get_metadata(FakeSession({api(spec): metadata([make_resource("a")])}), spec, settings)
    assert source == "fresh"

    result, source = ci.get_metadata(FakeSession({api(spec): GATEWAY_TIMEOUT}), spec, settings)
    assert source == "cache"
    assert result["resources"][0]["id"] == "a"


def test_get_metadata_without_cache_raises(settings):
    spec = make_spec()
    with pytest.raises(ci.UpstreamError, match="no cache"):
        ci.get_metadata(FakeSession({api(spec): requests.exceptions.ReadTimeout("slow")}), spec, settings)


def package_response(body, content_type="application/json"):
    """A 200 response with an arbitrary JSON body."""
    return FakeResponse(200, json.dumps(body).encode(), content_type)


def test_fetch_metadata_rejects_a_200_response_that_is_not_json():
    # Status 200 with an HTML page (a maintenance or login page) must not be parsed.
    spec = make_spec()
    html = FakeResponse(200, b"<html>maintenance</html>", "text/html; charset=utf-8")
    with pytest.raises(ci.UpstreamError, match="non-JSON content-type"):
        ci.fetch_metadata(FakeSession({api(spec): html}), spec, (1, 1))


@pytest.mark.parametrize(
    "result",
    [{}, {"resources": None}, {"resources": "oops"}, {"resources": {"id": "a"}}],
)
def test_fetch_metadata_rejects_a_result_without_a_resources_list(result):
    spec = make_spec()
    response = package_response({"success": True, "result": result})
    with pytest.raises(ci.UpstreamError, match="no 'resources' list"):
        ci.fetch_metadata(FakeSession({api(spec): response}), spec, (1, 1))


def test_get_metadata_keeps_the_cache_when_a_200_response_is_unusable(settings):
    spec = make_spec()
    ci.get_metadata(FakeSession({api(spec): metadata([make_resource("a")])}), spec, settings)
    cache = ci.dataset_meta_dir(settings, spec) / "package_show.json"
    good_copy = cache.read_bytes()

    bad_responses = [
        FakeResponse(200, b"<html>maintenance</html>", "text/html"),       # not JSON
        package_response({"success": True, "result": {"resources": None}}),  # malformed
        package_response({"success": False, "result": {}}),                  # CKAN failure
    ]
    for bad in bad_responses:
        result, source = ci.get_metadata(FakeSession({api(spec): bad}), spec, settings)
        assert source == "cache" and result["resources"][0]["id"] == "a"
        assert cache.read_bytes() == good_copy  # a bad answer never replaces the cache


def test_metadata_caches_are_isolated_per_dataset(settings):
    a, b = make_spec("a"), make_spec("b")
    ci.get_metadata(FakeSession({api(a): metadata([make_resource("from_a")])}), a, settings)
    with pytest.raises(ci.UpstreamError):  # b has no cache of its own
        ci.get_metadata(FakeSession({api(b): GATEWAY_TIMEOUT}), b, settings)


# ---------------------------------------------------------------------------
# new_file_per_period
# ---------------------------------------------------------------------------


def test_new_file_downloads_then_skips_without_refetching(settings):
    spec = make_spec()
    r1, r2 = make_resource("r1", created="2014"), make_resource("r2", created="2015")
    routes = {api(spec): metadata([r1, r2]),
              r1["url"]: FakeResponse(200, b"one", XLSX),
              r2["url"]: FakeResponse(200, b"two", XLSX)}

    first = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    assert first.ok and first.metadata_source == "fresh"
    assert first.downloaded == ["r1_data_en.xlsx", "r2_data_en.xlsx"]

    session = FakeSession(routes)
    second = ci.ingest_dataset(session, spec, settings, "run2")
    assert second.downloaded == []
    assert sorted(second.skipped) == sorted(first.downloaded)
    assert session.calls == [api(spec)]  # no file requested again

    landing = ci.dataset_landing_dir(settings, spec)
    assert (landing / "r1_data_en.xlsx").read_bytes() == b"one"


def test_new_file_single_failure_then_rerun_fills_gap(settings):
    spec = make_spec()
    good, bad = make_resource("good", created="2014"), make_resource("bad", created="2015")
    routes = {api(spec): metadata([good, bad]),
              good["url"]: FakeResponse(200, b"ok", XLSX),
              bad["url"]: requests.exceptions.ReadTimeout("slow")}

    report = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    assert not report.ok
    assert report.failed[0][0] == "bad" and "ReadTimeout" in report.failed[0][1]

    routes[bad["url"]] = FakeResponse(200, b"late", XLSX)
    rerun = ci.ingest_dataset(FakeSession(routes), spec, settings, "run2")
    assert rerun.ok
    assert rerun.downloaded == ["bad_data_en.xlsx"]
    assert rerun.skipped == ["good_data_en.xlsx"]


def test_html_payload_is_rejected_and_nothing_written(settings):
    spec = make_spec()
    res = make_resource("a")
    routes = {api(spec): metadata([res]),
              res["url"]: FakeResponse(200, b"<html>error</html>", "text/html")}
    report = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    assert "HTML payload" in report.failed[0][1]
    assert ci.completed_files(ci.dataset_landing_dir(settings, spec)) == set()


def test_empty_download_is_rejected_nothing_written_and_a_rerun_fills_it(settings):
    spec = make_spec()
    good, empty = make_resource("good", created="2014"), make_resource("empty", created="2015")
    routes = {api(spec): metadata([good, empty]),
              good["url"]: FakeResponse(200, b"ok", XLSX),
              empty["url"]: FakeResponse(200, b"", XLSX)}

    report = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")

    # The empty body is a failure for that file only; the other file still lands.
    assert not report.ok
    assert [rid for rid, _msg in report.failed] == ["empty"]
    assert "empty payload" in report.failed[0][1]
    assert report.downloaded == ["good_data_en.xlsx"]
    landing = ci.dataset_landing_dir(settings, spec)
    assert ci.completed_files(landing) == {"good_data_en.xlsx"}
    # No empty file and no temp file was left behind either.
    leftovers = [n for _d, _s, names in os.walk(landing) for n in names]
    assert leftovers == ["good_data_en.xlsx"]

    routes[empty["url"]] = FakeResponse(200, b"later", XLSX)
    rerun = ci.ingest_dataset(FakeSession(routes), spec, settings, "run2")
    assert rerun.ok and rerun.downloaded == ["empty_data_en.xlsx"]
    assert rerun.skipped == ["good_data_en.xlsx"]


# ---------------------------------------------------------------------------
# overwrite_in_place (snapshots)
# ---------------------------------------------------------------------------


def test_snapshot_new_version_creates_new_file_same_version_skips(settings):
    spec = make_spec(pattern=ci.OVERWRITE)
    v1 = make_resource("pr", last_modified="2026-09-15T10:00:00.1")
    routes = {api(spec): metadata([v1]), v1["url"]: FakeResponse(200, b"sept", XLSX)}
    first = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    assert first.downloaded == ["pr/20260915T100000_data_en.xlsx"]

    # Same version: skipped without downloading the file again.
    session = FakeSession(routes)
    same = ci.ingest_dataset(session, spec, settings, "run2")
    assert same.skipped == ["pr/20260915T100000_data_en.xlsx"]
    assert session.calls == [api(spec)]

    # Publisher replaces the file in place: a second snapshot is kept.
    v2 = dict(v1, last_modified="2026-10-15T10:00:00")
    routes = {api(spec): metadata([v2]), v2["url"]: FakeResponse(200, b"oct", XLSX)}
    newer = ci.ingest_dataset(FakeSession(routes), spec, settings, "run3")
    assert newer.downloaded == ["pr/20261015T100000_data_en.xlsx"]

    landing = ci.dataset_landing_dir(settings, spec)
    assert ci.completed_files(landing) == {
        "pr/20260915T100000_data_en.xlsx",
        "pr/20261015T100000_data_en.xlsx",
    }


def test_snapshot_without_last_modified_redownloads_and_dedupes_by_content_hash(settings):
    spec = make_spec(pattern=ci.OVERWRITE)
    res = make_resource("pr")  # no last_modified
    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"same", XLSX)}

    first = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    digest = hashlib.sha256(b"same").hexdigest()[:12]
    assert first.downloaded == [f"pr/sha256-{digest}_data_en.xlsx"]

    # Same content: the file is fetched again (the hash needs the bytes) but
    # nothing new is stored.
    session = FakeSession(routes)
    second = ci.ingest_dataset(session, spec, settings, "run2")
    assert second.downloaded == [] and len(second.skipped) == 1
    assert session.calls == [api(spec), res["url"]]

    routes[res["url"]] = FakeResponse(200, b"changed", XLSX)
    third = ci.ingest_dataset(FakeSession(routes), spec, settings, "run3")
    assert len(third.downloaded) == 1
    assert len(ci.completed_files(ci.dataset_landing_dir(settings, spec))) == 2


def test_snapshot_with_only_metadata_modified_still_catches_changed_content(settings):
    # The case this rule exists for: the publisher replaces the file but
    # metadata_modified does not move. It must not be treated as "unchanged".
    spec = make_spec(pattern=ci.OVERWRITE)
    res = make_resource("pr")
    res["metadata_modified"] = "2026-01-01T00:00:00"
    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"old", XLSX)}

    first = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")
    old_digest = hashlib.sha256(b"old").hexdigest()[:12]
    assert first.downloaded == [f"pr/sha256-{old_digest}_data_en.xlsx"]

    routes[res["url"]] = FakeResponse(200, b"new", XLSX)  # metadata_modified unchanged
    second = ci.ingest_dataset(FakeSession(routes), spec, settings, "run2")
    new_digest = hashlib.sha256(b"new").hexdigest()[:12]
    assert second.downloaded == [f"pr/sha256-{new_digest}_data_en.xlsx"]
    assert len(ci.completed_files(ci.dataset_landing_dir(settings, spec))) == 2


def test_snapshot_with_last_modified_still_skips_without_downloading(settings):
    # Unchanged behaviour: a known version is skipped from metadata alone, even
    # when metadata_modified differs.
    spec = make_spec(pattern=ci.OVERWRITE)
    res = make_resource("pr", last_modified="2026-09-15T10:00:00")
    res["metadata_modified"] = "2026-10-01T00:00:00"
    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"x", XLSX)}
    ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")

    session = FakeSession(routes)
    again = ci.ingest_dataset(session, spec, settings, "run2")
    assert again.skipped == ["pr/20260915T100000_data_en.xlsx"]
    assert session.calls == [api(spec)]


# ---------------------------------------------------------------------------
# Multi-dataset runs, layout and logs
# ---------------------------------------------------------------------------


def test_utc_run_id_has_a_fixed_format_that_sorts_by_time():
    def at(*args):
        return ci._utc_run_id(lambda: datetime(*args, tzinfo=timezone.utc))

    assert at(2026, 10, 29, 19, 53, 0) == "20261029T195300Z"
    moments = [(2025, 12, 31, 23, 59, 59), (2026, 1, 1, 0, 0, 0), (2026, 1, 1, 0, 0, 1),
               (2026, 1, 1, 9, 0, 0), (2026, 1, 1, 10, 0, 0), (2026, 10, 9, 9, 9, 9)]
    ids = [at(*m) for m in moments]
    assert ids == sorted(ids)  # the checks compare run ids as text to find "the previous run"


def test_utc_run_id_defaults_to_the_current_utc_time():
    before = datetime.now(timezone.utc).replace(microsecond=0)
    run_id = ci._utc_run_id()
    after = datetime.now(timezone.utc)
    assert re.fullmatch(r"\d{8}T\d{6}Z", run_id)
    parsed = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    assert before <= parsed <= after + timedelta(seconds=1)


def test_run_ingestion_names_the_run_itself_when_given_no_run_id(settings):
    assert re.fullmatch(r"\d{8}T\d{6}Z", ci.run_ingestion([], settings, FakeSession({})).run_id)
    assert ci.run_ingestion([], settings, FakeSession({}), run_id="custom1").run_id == "custom1"


def test_run_ids_are_accepted_by_the_checks_validator():
    import ingestion_checks  # the post-ingestion checks read run logs by run id

    assert ingestion_checks.validate_run_id(ci._utc_run_id()) is not None


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def test_build_session_identifies_itself_and_retries_throttling_and_server_errors():
    session = ci.build_session("mapledata test agent", total_retries=3, backoff_factor=0.5)
    assert session.headers["User-Agent"] == "mapledata test agent"

    retry = session.get_adapter("https://open.canada.ca/data/api/action").max_retries
    assert retry.total == 3 and retry.backoff_factor == 0.5
    assert set(retry.status_forcelist) == {429, 500, 502, 503, 504} == set(ci.RETRYABLE_STATUS)
    assert set(retry.allowed_methods) == {"GET"}
    # The last 504 must come back as a response, so our own check names the status
    # instead of urllib3 raising a MaxRetryError.
    assert retry.raise_on_status is False


def test_build_session_defaults_to_five_retries_with_a_five_second_backoff():
    retry = ci.build_session("agent").get_adapter("https://x.example").max_retries
    assert (retry.total, retry.backoff_factor) == (5, 5.0)


def test_run_isolates_datasets_and_continues_after_dataset_failure(settings):
    ok_spec = make_spec("ok_ds")
    # A distinct base_url, so the fake session can route it to a 504.
    broken = make_spec("broken_ds", base_url="https://down.example/api/action")
    res = make_resource("a")
    routes = {api(ok_spec): metadata([res]),
              res["url"]: FakeResponse(200, b"x", XLSX),
              api(broken): GATEWAY_TIMEOUT}

    run = ci.run_ingestion([broken, ok_spec], settings, FakeSession(routes), run_id="r1")
    by_key = {d.key: d for d in run.datasets}
    assert not run.ok
    assert "no cache" in by_key["broken_ds"].error
    assert by_key["ok_ds"].ok and by_key["ok_ds"].downloaded == ["a_data_en.xlsx"]
    assert "FAIL broken_ds" in run.summary() and "OK   ok_ds" in run.summary()


def test_files_land_under_source_and_key_and_log_is_written(settings):
    spec = make_spec("lmia", source="open_canada")
    res = make_resource("a")
    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"x", XLSX)}
    ci.run_ingestion([spec], settings, FakeSession(routes), run_id="20261029T000000Z")

    assert (Path(settings.landing_root) / "open_canada" / "lmia" / "a_data_en.xlsx").exists()
    log_path = Path(settings.meta_root) / "open_canada" / "lmia" / "runs" / "20261029T000000Z.json"
    log = json.loads(log_path.read_text())
    assert log["report"]["downloaded"] == ["a_data_en.xlsx"]
    assert log["spec"]["key"] == "lmia"


def test_no_temp_files_left_and_meta_kept_out_of_landing(settings):
    spec = make_spec()
    res = make_resource("a")
    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"x", XLSX)}
    ci.run_ingestion([spec], settings, FakeSession(routes), run_id="r1")

    for dirpath, _dirs, files in os.walk(settings.landing_root):
        assert not any(f.startswith(ci.TMP_PREFIX) for f in files)
        assert not any(f.endswith(".json") for f in files)  # metadata stays in meta_root


def test_a_stale_temp_file_is_not_counted_as_landed_and_the_download_replaces_it(settings):
    # A crash between writing the temp file and renaming it leaves "_tmp_..." behind.
    # It must not count as a completed file, or the real file would be skipped for good.
    spec = make_spec()
    res = make_resource("a")
    landing = ci.dataset_landing_dir(settings, spec)
    landing.mkdir(parents=True)
    stale = landing / f"{ci.TMP_PREFIX}a_data_en.xlsx"
    stale.write_bytes(b"partial")
    assert ci.completed_files(landing) == set()

    routes = {api(spec): metadata([res]), res["url"]: FakeResponse(200, b"full", XLSX)}
    report = ci.ingest_dataset(FakeSession(routes), spec, settings, "run1")

    assert report.ok and report.downloaded == ["a_data_en.xlsx"] and report.skipped == []
    assert (landing / "a_data_en.xlsx").read_bytes() == b"full"
    assert not stale.exists()  # the rename consumed the temp file
    assert ci.completed_files(landing) == {"a_data_en.xlsx"}


# ---------------------------------------------------------------------------
# Optional live smoke test
# ---------------------------------------------------------------------------


@pytest.mark.skipif(os.environ.get("CKAN_LIVE_TEST") != "1", reason="set CKAN_LIVE_TEST=1")
def test_live_lmia_metadata():
    specs = ci.load_registry(str(ROOT / "config" / "datasets.yml"))
    spec = next(s for s in specs if s.key == "lmia_positive_employers")
    session = ci.build_session("mapledata.io pipeline test", total_retries=3, backoff_factor=2)
    result = ci.fetch_metadata(session, spec, (10, 300))
    assert ci.select_resources(result, spec)
