"""
test_ingestion_checks.py

Unit tests for src/checks/ingestion_checks.py. No network and no Spark: the
file-head reader is a fake, and pytest's tmp_path replaces the volumes.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import catalog_setup as cs
import ckan_ingest as ci
import ingestion_checks as ic

NOW = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
RUN = "20261031T120000Z"
LANDING = Path("landing")


# ---------------------------------------------------------------------------
# Builders and fakes
# ---------------------------------------------------------------------------


def make_spec(key="ds", pattern=ci.NEW_FILE, **overrides):
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


def res(rid, name="Table", fmt="CSV", lang=("en",), last_modified=None,
        description=None, **extra):
    resource = {
        "id": rid,
        "name": name,
        "format": fmt,
        "language": list(lang),
        "url": f"https://files.example/{rid}/data.{fmt.lower()}",
        "created": "2020-01-01T00:00:00",
    }
    if last_modified is not None:
        resource["last_modified"] = last_modified
    if description is not None:
        resource["description"] = description
    resource.update(extra)
    return resource


def meta(*resources, **extra):
    return {"id": "pkg-1", "title": "A dataset", "resources": list(resources), **extra}


def log(downloaded=(), skipped=(), failed=(), error="", source="fresh"):
    return {
        "run_id": RUN,
        "report": {
            "key": "ds",
            "metadata_source": source,
            "downloaded": list(downloaded),
            "skipped": list(skipped),
            "failed": [list(f) for f in failed],  # JSON turns tuples into lists
            "error": error,
        },
    }


def ctx(spec, metadata, run_log=None):
    return ic.Context(RUN, NOW, spec, metadata, run_log or log())


def snapshot(spec, metadata, run_log=None):
    return ic.build_resource_snapshot(RUN, NOW, spec, metadata, run_log or log())


def names(findings):
    return [(f.check_name, f.severity, f.resource_id) for f in findings]


def details(finding):
    return json.loads(finding.details_json)


class FakeHead:
    """A read_head replacement: path -> bytes, or an exception, and a call log."""

    def __init__(self, files):
        self.files = files
        self.calls = []

    def __call__(self, path):
        self.calls.append(path)
        outcome = self.files[path]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def landed(name):
    return str(LANDING / name)


# ---------------------------------------------------------------------------
# Resource snapshot
# ---------------------------------------------------------------------------


def test_snapshot_flags_selected_resources_and_maps_landed_paths():
    spec = make_spec(formats=("CSV",))
    metadata = meta(
        res("a", "Alpha"),                    # selected, landed
        res("b", "Beta", fmt="XLSX"),         # wrong format: not selected
        res("c", "Gamma"),                    # selected, download failed
        res("d", "Delta", lang=("fr",)),      # wrong language: not selected
    )
    rows = snapshot(
        spec, metadata, log(downloaded=["a_data.csv"], failed=[("c", "HTTP 504")])
    )

    by_id = {r["resource_id"]: r for r in rows}
    assert [r["resource_id"] for r in rows] == ["a", "b", "c", "d"]
    assert {k: v["selected"] for k, v in by_id.items()} == {
        "a": True, "b": False, "c": True, "d": False,
    }
    assert by_id["a"]["landed_path"] == "a_data.csv"
    assert by_id["c"]["landed_path"] is None
    assert by_id["b"]["landed_path"] is None
    assert by_id["a"]["language"] == ["en"] and by_id["a"]["name"] == "Alpha"
    assert all(list(r) == [n for n, _ in ic.SNAPSHOT_COLUMNS] for r in rows)
    assert {r["run_id"] for r in rows} == {RUN} and {r["checked_at"] for r in rows} == {NOW}
    assert {r["dataset_key"] for r in rows} == {"ds"}


def test_snapshot_maps_overwrite_paths_and_counts_skipped_files():
    spec = make_spec(pattern=ci.OVERWRITE)
    metadata = meta(res("x"), res("y"))
    rows = snapshot(
        spec,
        metadata,
        log(downloaded=["x/sha256-abc123_data.csv"], skipped=["y/20260101T000000_data.csv"]),
    )
    assert [r["landed_path"] for r in rows] == [
        "x/sha256-abc123_data.csv",
        "y/20260101T000000_data.csv",
    ]


def test_snapshot_takes_ckan_id_from_metadata_and_falls_back_to_spec():
    spec = make_spec()
    assert snapshot(spec, meta(res("a")))[0]["ckan_id"] == "pkg-1"
    bare = {"resources": [res("a")]}
    assert snapshot(spec, bare)[0]["ckan_id"] == "ds-id"


def test_snapshot_of_a_dataset_without_resources_is_empty():
    assert snapshot(make_spec(), meta()) == []


# ---------------------------------------------------------------------------
# download_failed
# ---------------------------------------------------------------------------


def test_download_failed_reports_each_failed_resource_as_error():
    found = ic.check_download_failed(
        ctx(make_spec(), meta(), log(failed=[("a", "HTTP 504"), ("b", "timeout")]))
    )
    assert names(found) == [
        (ic.DOWNLOAD_FAILED, ic.ERROR, "a"),
        (ic.DOWNLOAD_FAILED, ic.ERROR, "b"),
    ]
    assert "HTTP 504" in found[0].message and details(found[0]) == {"error": "HTTP 504"}


def test_download_failed_reports_a_dataset_level_error_without_resource_id():
    found = ic.check_download_failed(
        ctx(make_spec(), meta(), log(error="metadata unavailable and no cache"))
    )
    assert names(found) == [(ic.DOWNLOAD_FAILED, ic.ERROR, None)]
    assert "metadata unavailable" in found[0].message


def test_download_failed_is_silent_for_a_clean_run():
    assert ic.check_download_failed(ctx(make_spec(), meta(), log())) == []


# ---------------------------------------------------------------------------
# metadata_stale
# ---------------------------------------------------------------------------


def test_metadata_stale_warns_when_the_cache_was_used():
    found = ic.check_metadata_stale(ctx(make_spec(), meta(), log(source="cache")))
    assert names(found) == [(ic.METADATA_STALE, ic.WARN, None)]
    assert details(found[0]) == {"metadata_source": "cache"}


@pytest.mark.parametrize("source", ["fresh", ""])
def test_metadata_stale_is_silent_otherwise(source):
    assert ic.check_metadata_stale(ctx(make_spec(), meta(), log(source=source))) == []


# ---------------------------------------------------------------------------
# resource_removed / resource_added
# ---------------------------------------------------------------------------


def previous_rows(*resources):
    old = ic.build_resource_snapshot(
        "20260901T000000Z", NOW, make_spec(formats=("CSV",)), meta(*resources), log()
    )
    return old


def test_removed_and_added_are_skipped_on_the_first_run():
    spec = make_spec()
    snap = snapshot(spec, meta(res("a")))
    assert ic.check_resource_removed(ctx(spec, meta()), snap, []) == []
    assert ic.check_resource_added(ctx(spec, meta()), snap, []) == []


def test_removed_warns_for_each_resource_missing_now():
    spec = make_spec(formats=("CSV",))
    previous = previous_rows(res("a", "Alpha"), res("b", "Beta"), res("c", "Gamma", fmt="XLSX"))
    snap = snapshot(spec, meta(res("a", "Alpha")))
    found = ic.check_resource_removed(ctx(spec, meta()), snap, previous)

    assert names(found) == [
        (ic.RESOURCE_REMOVED, ic.WARN, "b"),
        (ic.RESOURCE_REMOVED, ic.WARN, "c"),
    ]
    assert details(found[0]) == {
        "name": "Beta", "format": "CSV", "was_selected": True,
        "previous_run_id": "20260901T000000Z",
    }
    assert details(found[1])["was_selected"] is False


def test_added_reports_each_new_resource_as_info():
    spec = make_spec(formats=("CSV",))
    previous = previous_rows(res("a", "Alpha"))
    snap = snapshot(spec, meta(res("a", "Alpha"), res("n1", "New one"), res("n2", "New two", fmt="XLSX")))
    found = ic.check_resource_added(ctx(spec, meta()), snap, previous)

    assert names(found) == [
        (ic.RESOURCE_ADDED, ic.INFO, "n1"),
        (ic.RESOURCE_ADDED, ic.INFO, "n2"),
    ]
    assert details(found[0])["selected"] is True and details(found[1])["selected"] is False


def test_removed_and_added_are_silent_when_nothing_changed():
    spec = make_spec(formats=("CSV",))
    previous = previous_rows(res("a"), res("b"))
    snap = snapshot(spec, meta(res("b"), res("a")))
    assert ic.check_resource_removed(ctx(spec, meta()), snap, previous) == []
    assert ic.check_resource_added(ctx(spec, meta()), snap, previous) == []


def test_select_previous_rows_takes_the_latest_earlier_run_only():
    rows = [
        {"run_id": "20260801T000000Z", "resource_id": "old"},
        {"run_id": "20260901T000000Z", "resource_id": "prev1"},
        {"run_id": "20260901T000000Z", "resource_id": "prev2"},
        {"run_id": RUN, "resource_id": "same-run"},
        {"run_id": "20261101T000000Z", "resource_id": "later"},
    ]
    assert [r["resource_id"] for r in ic.select_previous_rows(rows, RUN)] == ["prev1", "prev2"]
    assert ic.select_previous_rows(rows, "20260801T000000Z") == []
    assert ic.select_previous_rows([], RUN) == []


# ---------------------------------------------------------------------------
# format_gap (a lower bound)
# ---------------------------------------------------------------------------


def many(prefix, count, fmt, lang=("en",), name=None):
    return [res(f"{prefix}{i}", name or f"{prefix} table {i}", fmt, lang) for i in range(count)]


def test_format_gap_reports_a_lower_bound_per_unselected_format():
    spec = make_spec(formats=("CSV",))
    metadata = meta(
        *many("c", 3, "CSV"),
        *many("x", 5, "XLSX"),
        *many("l", 1, "XLS"),
        *many("f", 2, "XLSX", lang=("fr",)),   # other language: not counted
        *many("d", 1, "DOCX"),                 # documentation: ignored
    )
    found = ic.check_format_gap(ctx(spec, metadata))

    assert names(found) == [(ic.FORMAT_GAP, ic.WARN, None)]  # XLS: 1 - 3 < 0
    body = details(found[0])
    assert body["format"] == "XLSX"
    assert body["unselected_format_resources"] == 5
    assert body["selected_format_resources"] == 3
    assert body["minimum_tables_only_in_format"] == 2
    assert body["resource_names"] == sorted(f"x table {i}" for i in range(5))
    assert "lower bound" in body["note"]
    assert "at least 2" in found[0].message


def test_format_gap_reports_every_unselected_format_with_a_positive_gap():
    spec = make_spec(formats=("CSV",))
    metadata = meta(*many("c", 1, "CSV"), *many("x", 3, "XLSX"), *many("l", 2, "XLS"))
    found = ic.check_format_gap(ctx(spec, metadata))
    assert [details(f)["format"] for f in found] == ["XLS", "XLSX"]
    assert [details(f)["minimum_tables_only_in_format"] for f in found] == [1, 2]


def test_format_gap_is_silent_when_selected_resources_are_at_least_as_many():
    spec = make_spec(formats=("CSV",))
    assert ic.check_format_gap(ctx(spec, meta(*many("c", 4, "CSV"), *many("x", 4, "XLSX")))) == []
    assert ic.check_format_gap(ctx(spec, meta(*many("c", 4, "CSV"), *many("x", 2, "XLSX")))) == []


def test_format_gap_is_silent_without_a_format_filter_or_resources():
    assert ic.check_format_gap(ctx(make_spec(), meta(*many("x", 5, "XLSX")))) == []
    assert ic.check_format_gap(ctx(make_spec(formats=("CSV",)), meta())) == []


def test_format_gap_applies_the_name_filter_to_both_sides():
    spec = make_spec(formats=("CSV",), name_regex="province")
    metadata = meta(
        res("c1", "By province", "CSV"),
        res("c2", "By country", "CSV"),           # filtered out by name_regex
        *many("x", 3, "XLSX", name="By province"),
        *many("y", 10, "XLSX", name="By country"),  # filtered out by name_regex
    )
    (finding,) = ic.check_format_gap(ctx(spec, metadata))
    body = details(finding)
    assert body["unselected_format_resources"] == 3
    assert body["selected_format_resources"] == 1
    assert body["minimum_tables_only_in_format"] == 2


def test_format_gap_docstring_says_it_is_a_lower_bound_and_pairing_is_unreliable():
    doc = ic.check_format_gap.__doc__
    assert "LOWER BOUND" in doc and "unreliable" in doc


# ---------------------------------------------------------------------------
# missing_last_modified
# ---------------------------------------------------------------------------


def test_missing_last_modified_is_one_aggregated_finding_per_dataset():
    spec = make_spec(pattern=ci.OVERWRITE)
    metadata = meta(
        res("a"),
        res("b", last_modified="2026-01-01T00:00:00"),
        res("c", last_modified="-"),  # unusable once cleaned
        res("d", last_modified=""),
    )
    (finding,) = ic.check_missing_last_modified(ctx(spec, metadata))
    assert (finding.check_name, finding.severity, finding.resource_id) == (
        ic.MISSING_LAST_MODIFIED, ic.INFO, None,
    )
    assert details(finding) == {
        "affected_resource_count": 3,
        "selected_resource_count": 4,
        "resource_ids": ["a", "c", "d"],
    }
    assert "ADR 0003" in finding.message


def test_missing_last_modified_ignores_unselected_resources():
    spec = make_spec(pattern=ci.OVERWRITE, formats=("CSV",))
    metadata = meta(res("a", fmt="XLSX"), res("b", last_modified="2026-01-01T00:00:00"))
    assert ic.check_missing_last_modified(ctx(spec, metadata)) == []


def test_missing_last_modified_only_applies_to_overwrite_in_place():
    assert ic.check_missing_last_modified(ctx(make_spec(), meta(res("a")))) == []


# ---------------------------------------------------------------------------
# archived
# ---------------------------------------------------------------------------


def test_archived_matches_dataset_and_resources_ignoring_case():
    spec = make_spec()
    metadata = meta(
        res("a", "Old table (ARCHIVED)"),
        res("b", "Current table", description="This file is Archived."),
        res("c", "Current table"),
        notes="Archived dataset; kept for reference.",
    )
    found = ic.check_archived(ctx(spec, metadata))

    assert names(found) == [
        (ic.ARCHIVED, ic.WARN, None),
        (ic.ARCHIVED, ic.WARN, "a"),
        (ic.ARCHIVED, ic.WARN, "b"),
    ]
    assert details(found[0]) == {"matched_in": ["notes"]}
    assert details(found[1]) == {"matched_in": ["name"]}
    assert details(found[2]) == {"matched_in": ["description"]}


def test_archived_reads_translated_names_and_descriptions():
    spec = make_spec()
    metadata = meta(
        res("a", "Plain", name_translated={"en": "Table (archived)", "fr": "Tableau"}),
        res("b", "Plain", description_translated={"en": "ok", "fr": "Archived copy"}),
        title_translated={"en": "A dataset", "fr": "Archived"},
    )
    found = ic.check_archived(ctx(spec, metadata))
    assert [(f.resource_id, details(f)["matched_in"]) for f in found] == [
        (None, ["title"]), ("a", ["name"]), ("b", ["description"]),
    ]


def test_archived_only_looks_at_selected_resources():
    spec = make_spec(formats=("CSV",))
    metadata = meta(res("a", "Old (archived)", fmt="XLSX"), res("b", "Fine"))
    assert ic.check_archived(ctx(spec, metadata)) == []


def test_archived_is_silent_when_nothing_matches():
    assert ic.check_archived(ctx(make_spec(), meta(res("a", "Fine")))) == []


# ---------------------------------------------------------------------------
# rounded_values
# ---------------------------------------------------------------------------


def rounded(spec, metadata, files=None, run_log=None):
    run_log = run_log or log(downloaded=[f"{r['id']}_data.csv" for r in metadata["resources"]])
    snap = snapshot(spec, metadata, run_log)
    reader = FakeHead(files or {})
    found = ic.check_rounded_values(ctx(spec, metadata, run_log), snap, reader, LANDING)
    return found, reader


def test_rounded_values_records_where_each_resource_matched():
    spec = make_spec()
    metadata = meta(
        res("a", "[rounded - not for calculations] Table A"),
        res("b", "Table B", description="Figures are Rounded."),
        res("c", "Table C"),
        res("d", "Table D"),
        res("e", "Table E"),
    )
    files = {
        landed("a_data.csv"): b"a,b\n1,2\n",
        landed("b_data.csv"): b"a,b\n1,2\n",
        landed("c_data.csv"): b"title\n[ROUNDED numbers]\n1,2\n",
        landed("d_data.csv"): b"a,b\n1,2\n",
        landed("e_data.csv"): b"x\n",
    }
    (finding,) = rounded(spec, metadata, files)[0]

    assert (finding.check_name, finding.severity, finding.resource_id) == (
        ic.ROUNDED_VALUES, ic.INFO, None,
    )
    body = details(finding)
    assert body["affected_resource_count"] == 3 and body["selected_resource_count"] == 5
    assert body["resource_ids"] == ["a", "b", "c"]
    assert body["matched_in"] == {"a": ["name"], "b": ["description"], "c": ["file_head"]}
    assert body["files_read"] == 5 and body["files_unreadable"] == 0


def test_rounded_values_lists_every_place_a_resource_matched():
    spec = make_spec()
    metadata = meta(res("a", "Table (rounded)", description="rounded too"))
    files = {landed("a_data.csv"): b"rounded"}
    (finding,) = rounded(spec, metadata, files)[0]
    assert details(finding)["matched_in"] == {"a": ["name", "description", "file_head"]}


def test_rounded_values_reads_only_landed_csv_files_at_the_landing_path():
    spec = make_spec()
    metadata = meta(res("a", fmt="CSV"), res("b", fmt="XLSX"), res("c", fmt="CSV"))
    # c failed to land; b is not a CSV.
    run_log = log(downloaded=["a_data.csv", "b_data.xlsx"], failed=[("c", "HTTP 504")])
    found, reader = rounded(spec, metadata, {landed("a_data.csv"): b"plain"}, run_log)
    assert found == [] and reader.calls == [landed("a_data.csv")]


def test_rounded_values_skips_unreadable_files_and_counts_them():
    spec = make_spec()
    metadata = meta(res("a", "[rounded] A"), res("b", "B"))
    files = {landed("a_data.csv"): b"x", landed("b_data.csv"): FileNotFoundError("gone")}
    (finding,) = rounded(spec, metadata, files)[0]
    body = details(finding)
    assert body["resource_ids"] == ["a"]
    assert body["files_read"] == 1 and body["files_unreadable"] == 1


def test_rounded_values_checks_name_and_description_without_a_reader():
    spec = make_spec()
    metadata = meta(res("a", "[rounded] A"), res("b", "B"))
    snap = snapshot(spec, metadata, log(downloaded=["a_data.csv", "b_data.csv"]))
    (finding,) = ic.check_rounded_values(ctx(spec, metadata), snap)
    assert details(finding)["matched_in"] == {"a": ["name"]}
    assert details(finding)["files_read"] == 0


def test_rounded_values_is_silent_when_nothing_is_rounded():
    spec = make_spec()
    metadata = meta(res("a", "Plain"))
    assert rounded(spec, metadata, {landed("a_data.csv"): b"a,b\n1,2\n"})[0] == []


def test_rounded_values_only_searches_the_file_head():
    spec = make_spec()
    metadata = meta(res("a"))
    # The injected reader decides how much to return; a short head hides the word.
    assert rounded(spec, metadata, {landed("a_data.csv"): b"a,b\n"})[0] == []


# ---------------------------------------------------------------------------
# run_checks
# ---------------------------------------------------------------------------


def test_run_checks_covers_datasets_with_a_log_and_skips_the_rest():
    a, b, c = make_spec("a"), make_spec("b"), make_spec("c")
    found = ic.run_checks(
        RUN,
        NOW,
        [a, b, c],
        metadata_by_key={"a": meta(res("r1", "Old (archived)")), "b": meta()},
        run_logs_by_key={
            "a": log(failed=[("r1", "boom")]),
            "b": log(source="cache"),
        },
    )
    assert [(f.dataset_key, f.check_name, f.severity) for f in found] == [
        ("a", ic.DOWNLOAD_FAILED, ic.ERROR),
        ("a", ic.ARCHIVED, ic.WARN),
        ("b", ic.METADATA_STALE, ic.WARN),
    ]
    assert {f.dataset_key for f in found} == {"a", "b"}  # c has no run log


def test_run_checks_runs_only_log_based_checks_when_metadata_is_missing():
    spec = make_spec()
    found = ic.run_checks(
        RUN, NOW, [spec], metadata_by_key={}, run_logs_by_key={"ds": log(error="no metadata")}
    )
    assert names(found) == [(ic.DOWNLOAD_FAILED, ic.ERROR, None)]


def test_run_checks_sorts_by_severity_dataset_check_and_resource():
    spec = make_spec(pattern=ci.OVERWRITE)
    found = ic.run_checks(
        RUN,
        NOW,
        [spec],
        metadata_by_key={"ds": meta(res("z", "Z (archived)"), res("a", "A (archived)"))},
        run_logs_by_key={"ds": log(failed=[("z", "e1"), ("a", "e2")])},
    )
    assert [(f.severity, f.check_name, f.resource_id) for f in found] == [
        (ic.ERROR, ic.DOWNLOAD_FAILED, "a"),
        (ic.ERROR, ic.DOWNLOAD_FAILED, "z"),
        (ic.WARN, ic.ARCHIVED, "a"),
        (ic.WARN, ic.ARCHIVED, "z"),
        (ic.INFO, ic.MISSING_LAST_MODIFIED, None),
    ]


def test_run_checks_compares_with_the_previous_snapshot_per_dataset():
    spec = make_spec(formats=("CSV",))
    previous = {"ds": previous_rows(res("gone", "Gone"), res("kept", "Kept"))}
    found = ic.run_checks(
        RUN,
        NOW,
        [spec],
        metadata_by_key={"ds": meta(res("kept", "Kept"), res("new", "New"))},
        run_logs_by_key={"ds": log()},
        previous_rows_by_key=previous,
    )
    assert names(found) == [
        (ic.RESOURCE_REMOVED, ic.WARN, "gone"),
        (ic.RESOURCE_ADDED, ic.INFO, "new"),
    ]
    # A dataset without previous rows is a first run: no added/removed findings.
    first = ic.run_checks(
        RUN, NOW, [spec],
        metadata_by_key={"ds": meta(res("new", "New"))},
        run_logs_by_key={"ds": log()},
    )
    assert first == []


def test_run_checks_reads_landed_files_under_the_landing_root(tmp_path):
    spec = make_spec()
    landing_dir = ci.dataset_landing_dir(ci.RunSettings(str(tmp_path), ""), spec)
    landing_dir.mkdir(parents=True)
    (landing_dir / "a_data.csv").write_bytes(b"title [rounded - not for calculations]\n")

    found = ic.run_checks(
        RUN, NOW, [spec],
        metadata_by_key={"ds": meta(res("a"))},
        run_logs_by_key={"ds": log(downloaded=["a_data.csv"])},
        read_head=ic.file_head_reader(),
        landing_root=str(tmp_path),
    )
    assert [(f.check_name, details(f)["matched_in"]) for f in found] == [
        (ic.ROUNDED_VALUES, {"a": ["file_head"]})
    ]


def test_run_checks_works_on_a_real_run_log(tmp_path):
    # Guards the coupling to ckan_ingest: the run log it writes must be readable.
    class Response:
        def __init__(self, status=200, body=b"", ctype="application/json"):
            self.status_code, self.content = status, body
            self.headers = {"content-type": ctype}
            self.text = body.decode("utf-8", errors="replace")

        def json(self):
            return json.loads(self.content)

    spec = make_spec(formats=("CSV",))
    good, bad = res("ok1", "Good table"), res("bad1", "Bad table")
    package = {"success": True, "result": meta(good, bad)}
    routes = {
        spec.package_show_url: Response(body=json.dumps(package).encode()),
        good["url"]: Response(body=b"a,b\n[rounded]\n", ctype="text/csv"),
        bad["url"]: Response(504, b"<html>504</html>", "text/html"),
    }

    class Session:
        def get(self, url, params=None, timeout=None):
            return routes[url]

    settings = ci.RunSettings(str(tmp_path / "raw"), str(tmp_path / "_meta"), timeout=(1, 1))
    ci.ingest_dataset(Session(), spec, settings, RUN)

    run_log = ic.load_run_log(settings.meta_root, spec, RUN)
    metadata = ic.load_metadata(settings.meta_root, spec)
    found = ic.run_checks(
        RUN, NOW, [spec], {"ds": metadata}, {"ds": run_log},
        read_head=ic.file_head_reader(), landing_root=settings.landing_root,
    )
    assert [(f.check_name, f.resource_id) for f in found] == [
        (ic.DOWNLOAD_FAILED, "bad1"),
        (ic.ROUNDED_VALUES, None),
    ]
    rows = snapshot(spec, metadata, run_log)
    assert {r["resource_id"]: r["landed_path"] for r in rows} == {
        "ok1": "ok1_data.csv", "bad1": None,
    }


# ---------------------------------------------------------------------------
# Findings, summary and report
# ---------------------------------------------------------------------------


def sample_findings():
    spec = make_spec("ds")
    c = ctx(spec, meta())
    return [
        c.finding(ic.DOWNLOAD_FAILED, ic.ERROR, "download failed: HTTP 504", "r1", error="HTTP 504"),
        c.finding(ic.METADATA_STALE, ic.WARN, "used the cache"),
        c.finding(ic.RESOURCE_ADDED, ic.INFO, "new resource: Beta | gamma", "r2"),
    ]


def test_finding_as_row_matches_the_table_columns_and_holds_valid_json():
    row = sample_findings()[0].as_row()
    assert list(row) == [n for n, _ in ic.FINDING_COLUMNS]
    assert json.loads(row["details_json"]) == {"error": "HTTP 504"}
    assert row["checked_at"] == NOW and row["resource_id"] == "r1"


def test_summarise_counts_every_severity_and_check():
    summary = ic.summarise(RUN, NOW, sample_findings(), ["ds", "other"], ["skipped"])
    assert summary["by_severity"] == {"ERROR": 1, "WARN": 1, "INFO": 1}
    assert summary["by_check"] == {
        ic.DOWNLOAD_FAILED: 1, ic.METADATA_STALE: 1, ic.RESOURCE_ADDED: 1,
    }
    assert summary["datasets_checked"] == ["ds", "other"]
    assert summary["datasets_skipped"] == ["skipped"]
    assert summary["checked_at"] == NOW.isoformat()


def test_summarise_has_zero_counts_when_there_are_no_findings():
    summary = ic.summarise(RUN, NOW, [], ["ds"])
    assert summary["by_severity"] == {"ERROR": 0, "WARN": 0, "INFO": 0}
    assert summary["by_check"] == {}


def test_summary_lines_show_severity_and_check_counts():
    lines = ic.summary_lines(ic.summarise(RUN, NOW, sample_findings(), ["ds"], ["x"]))
    text = "\n".join(lines)
    assert "ERROR=1, WARN=1, INFO=1" in text
    assert "download_failed: 1" in text and "1 without a run log" in text


def test_render_markdown_lists_findings_by_severity_and_always_carries_the_note():
    findings = sample_findings()
    report = ic.render_markdown(findings, ic.summarise(RUN, NOW, findings, ["ds"], ["gone"]))

    assert report.startswith(f"# Ingestion checks: run {RUN}")
    assert "## ERROR (1)" in report and "## WARN (1)" in report and "## INFO (1)" in report
    assert "- **download_failed** `ds` / `r1`: download failed: HTTP 504" in report
    assert "- **metadata_stale** `ds`: used the cache" in report
    assert "`gone`" in report  # dataset without a run log
    assert report.index("## ERROR") < report.index("## WARN") < report.index("## INFO")
    assert "format_gap is a lower bound; no finding does not mean full coverage." in report


def test_render_markdown_without_findings_says_none_and_keeps_the_note():
    report = ic.render_markdown([], ic.summarise(RUN, NOW, [], ["ds"]))
    assert report.count("None.") == 3
    assert "no finding does not mean full coverage" in report


# ---------------------------------------------------------------------------
# Names, SQL fragments and files
# ---------------------------------------------------------------------------


def test_replace_where_predicate_names_the_run_and_the_datasets():
    assert ic.replace_where_predicate(RUN, ["b", "a", "a"]) == (
        f"run_id = '{RUN}' AND dataset_key IN ('a', 'b')"
    )


@pytest.mark.parametrize("run_id", ["", "x'; DROP TABLE t; --", "a b", "../x"])
def test_invalid_run_ids_are_rejected(run_id):
    with pytest.raises(ValueError):
        ic.validate_run_id(run_id)
    with pytest.raises(ValueError):
        ic.replace_where_predicate(run_id, ["a"])
    with pytest.raises(ValueError):
        ic.report_path("/meta", run_id)


@pytest.mark.parametrize("keys", [[], ["Bad Key"], ["a'; --"], ["ok", "x y"]])
def test_replace_where_rejects_empty_or_unsafe_dataset_keys(keys):
    with pytest.raises(ValueError):
        ic.replace_where_predicate(RUN, keys)


def test_qualified_table_checks_both_names():
    assert ic.qualified_table("mapledata", "ingestion_findings") == (
        "`mapledata`.`metadata`.`ingestion_findings`"
    )
    for catalog, table in [("Map", "t"), ("c", "t; drop"), ("", "t"), ("c", "")]:
        with pytest.raises(ValueError):
            ic.qualified_table(catalog, table)


def test_report_path_lives_under_reports(tmp_path):
    assert ic.report_path(str(tmp_path), RUN) == tmp_path / "reports" / f"{RUN}.md"


def write_meta(meta_root, spec, run_ids=(), package=None):
    base = ci.dataset_meta_dir(ci.RunSettings("", str(meta_root)), spec)
    (base / "runs").mkdir(parents=True)
    for run_id in run_ids:
        (base / "runs" / f"{run_id}.json").write_text(json.dumps(log()), encoding="utf-8")
    if package is not None:
        (base / "package_show.json").write_text(package, encoding="utf-8")


def test_load_helpers_read_the_metadata_cache_and_run_logs(tmp_path):
    a, b = make_spec("a"), make_spec("b")
    write_meta(tmp_path, a, ["20260901T000000Z", RUN], json.dumps(meta(res("r"))))
    write_meta(tmp_path, b, ["20260801T000000Z"], "{ not json")

    assert ic.load_metadata(str(tmp_path), a)["id"] == "pkg-1"
    assert ic.load_metadata(str(tmp_path), b) is None        # unreadable cache
    assert ic.load_metadata(str(tmp_path), make_spec("c")) is None
    assert ic.list_run_ids(str(tmp_path), a) == ["20260901T000000Z", RUN]
    assert ic.list_run_ids(str(tmp_path), make_spec("c")) == []
    assert ic.latest_run_id(str(tmp_path), [a, b]) == RUN
    assert ic.latest_run_id(str(tmp_path), [make_spec("c")]) is None
    assert ic.load_run_log(str(tmp_path), a, RUN)["report"]["metadata_source"] == "fresh"
    assert ic.load_run_log(str(tmp_path), b, RUN) is None    # b was not in that run


def test_file_head_reader_returns_only_the_first_bytes(tmp_path):
    path = tmp_path / "x.csv"
    path.write_bytes(b"0123456789")
    assert ic.file_head_reader(4)(str(path)) == b"0123"
    assert ic.file_head_reader()(str(path)) == b"0123456789"
    with pytest.raises(OSError):
        ic.file_head_reader()(str(tmp_path / "missing.csv"))


# ---------------------------------------------------------------------------
# Agreement with the table definitions
# ---------------------------------------------------------------------------


def test_row_columns_match_the_tables_defined_in_catalog_setup():
    assert [(n, t) for n, t, _ in cs.RESOURCE_SNAPSHOT_COLUMNS] == list(ic.SNAPSHOT_COLUMNS)
    assert [(n, t) for n, t, _ in cs.FINDING_COLUMNS] == list(ic.FINDING_COLUMNS)
    assert ic.SNAPSHOT_SCHEMA_DDL.startswith("run_id STRING, checked_at TIMESTAMP")
    assert "language ARRAY<STRING>" in ic.SNAPSHOT_SCHEMA_DDL
    assert ic.FINDINGS_SCHEMA_DDL.endswith("details_json STRING")
