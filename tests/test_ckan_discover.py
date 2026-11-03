"""
test_ckan_discover.py

Unit tests for src/ingest/ckan_discover.py. No network: a fake session
replaces requests.
"""

import json
from datetime import date, datetime, timezone

import pytest
import yaml

import catalog_setup as cs
import ckan_discover as cd
import ckan_ingest as ci

BASE = "https://ckan.example/api/action"
SEARCH_URL = BASE + "/package_search"
NOW = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)
RUN_ID = "20261030T120000Z"


class FakeResponse:
    def __init__(self, status=200, payload=None, content_type="application/json"):
        self.status_code = status
        self.headers = {"content-type": content_type}
        self._body = json.dumps(payload if payload is not None else {}).encode()

    @property
    def text(self):
        return self._body.decode()

    def json(self):
        return json.loads(self._body)


class FakeSession:
    """Serves `catalog` in pages of at most `cap` rows, like a CKAN portal."""

    def __init__(self, catalog=(), cap=None, response=None):
        self.catalog = list(catalog)
        self.cap = cap
        self.response = response
        self.requests = []

    def get(self, url, params=None, timeout=None):
        self.requests.append((url, dict(params)))
        if self.response is not None:
            return self.response
        rows = min(params["rows"], self.cap or params["rows"])
        page = self.catalog[params["start"] : params["start"] + rows]
        return FakeResponse(
            payload={
                "success": True,
                "result": {"count": len(self.catalog), "results": page},
            }
        )


def make_package(n=1, **overrides):
    package = {
        "id": f"id-{n}",
        "name": f"dataset-{n}",
        "title_translated": {"en": "TFWP Positive LMIA Employers", "fr": "EMT"},
        "organization": {
            "name": "esdc-edsc",
            "title": "Employment and Social Development Canada | Emploi et Developpement social Canada",
        },
        "metadata_modified": "2025-10-23T15:50:27",
        "frequency": "P1Y",
        "time_period_coverage_start": "2015-01-01 00:00:00",
        "time_period_coverage_end": "2026-08-31 00:00:00",
        "resources": [
            {"format": "csv"},
            {"format": "XLSX"},
            {"format": "csv"},
            {"format": ""},
        ],
    }
    package.update(overrides)
    return package


def make_spec(ckan_id, base_url=BASE, status=ci.STATUS_ACTIVE):
    return ci.DatasetSpec(
        key="ds",
        domain="immigration",
        ckan_id=ckan_id,
        update_pattern=ci.NEW_FILE,
        status=status,
        purpose="test purpose",
        base_url=base_url,
    )


def no_sleep(_seconds):
    pass


# --- 1. pagination ----------------------------------------------------------


def test_search_fetches_every_page_and_sends_start_and_rows():
    packages = [make_package(n) for n in range(250)]
    session = FakeSession(packages)
    found = cd.search_packages(
        session, "lmia", base_url=BASE + "/", page_size=100, sleep=no_sleep
    )
    assert [p["id"] for p in found] == [p["id"] for p in packages]
    assert [(u, p["start"], p["rows"], p["q"]) for u, p in session.requests] == [
        (SEARCH_URL, 0, 100, "lmia"),
        (SEARCH_URL, 100, 100, "lmia"),
        (SEARCH_URL, 200, 100, "lmia"),
    ]


def test_every_paged_request_sends_sort_rows_and_facet_limit():
    session = FakeSession([make_package(n) for n in range(5)])
    cd.search_packages(session, "q", BASE, page_size=2, sleep=no_sleep)
    assert len(session.requests) == 3
    for _url, params in session.requests:
        assert params["sort"] == "name asc"
        assert params["facet.limit"] == -1
        assert params["rows"] == 2


@pytest.mark.parametrize("page_size", [0, -1, cd.MAX_PAGE_SIZE + 1])
def test_search_rejects_page_size_outside_the_documented_range(page_size):
    session = FakeSession([make_package(1)])
    with pytest.raises(ValueError, match="page_size"):
        cd.search_packages(session, "q", BASE, page_size=page_size, sleep=no_sleep)
    assert session.requests == []


def test_search_accepts_the_largest_documented_page_size():
    session = FakeSession([make_package(1)])
    cd.search_packages(session, "q", BASE, page_size=cd.MAX_PAGE_SIZE, sleep=no_sleep)
    assert session.requests[0][1]["rows"] == 1000


def test_search_stops_exactly_at_page_boundary():
    session = FakeSession([make_package(n) for n in range(200)])
    found = cd.search_packages(session, "q", BASE, page_size=100, sleep=no_sleep)
    assert len(found) == 200 and len(session.requests) == 2


def test_search_follows_a_server_that_caps_rows_below_page_size():
    session = FakeSession([make_package(n) for n in range(25)], cap=10)
    found = cd.search_packages(session, "q", BASE, page_size=100, sleep=no_sleep)
    assert len(found) == 25


def test_search_stops_on_empty_page_even_if_count_is_larger():
    class Shrinking(FakeSession):
        def get(self, url, params=None, timeout=None):
            self.requests.append((url, dict(params)))
            page = [make_package(0)] if params["start"] == 0 else []
            return FakeResponse(
                payload={"success": True, "result": {"count": 5, "results": page}}
            )

    session = Shrinking()
    assert len(cd.search_packages(session, "q", BASE, sleep=no_sleep)) == 1
    assert len(session.requests) == 2


def test_search_drops_packages_repeated_across_pages_and_handles_no_results():
    session = FakeSession([make_package(1), make_package(1), make_package(2)], cap=2)
    found = cd.search_packages(session, "q", BASE, sleep=no_sleep)
    assert [p["id"] for p in found] == ["id-1", "id-2"]
    assert cd.search_packages(FakeSession([]), "q", BASE, sleep=no_sleep) == []


def test_search_pauses_between_pages_only():
    pauses = []
    session = FakeSession([make_package(n) for n in range(5)])
    cd.search_packages(
        session, "q", BASE, page_size=2, pause_seconds=1.5, sleep=pauses.append
    )
    assert pauses == [1.5, 1.5]  # 3 pages, 2 gaps


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(504, content_type="text/html"),
        FakeResponse(200, {"success": True, "result": {}}, "text/html"),
        FakeResponse(200, {"success": False, "result": {"results": [], "count": 0}}),
        FakeResponse(200, {"success": True, "result": {}}),
        FakeResponse(200, {"success": True, "result": {"results": []}}),
    ],
)
def test_search_rejects_untrusted_responses(response):
    with pytest.raises(ci.UpstreamError):
        cd.search_packages(FakeSession(response=response), "q", BASE, sleep=no_sleep)


# --- 2. formats as list ------------------------------------------------------


def test_formats_are_a_sorted_unique_upper_case_list():
    formats = cd.package_formats(make_package())
    assert formats == ["CSV", "XLSX"] and isinstance(formats, list)
    assert cd.package_formats({"resources": [{"format": ""}]}) == []
    assert cd.package_formats({}) == []


# --- 3. organization ---------------------------------------------------------


def test_organization_keeps_code_and_english_title():
    assert cd.organization_fields(make_package()) == (
        "esdc-edsc",
        "Employment and Social Development Canada",
    )
    plain = {"organization": {"name": "x", "title": "Plain Title"}}
    assert cd.organization_fields(plain) == ("x", "Plain Title")
    assert cd.organization_fields({}) == ("", "")
    assert cd.organization_fields({"organization": None}) == ("", "")


# --- 4. frequency and coverage -----------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2015-01-01 00:00:00", date(2015, 1, 1)),
        ("2026-08-31", date(2026, 8, 31)),
        (None, None),
        ("not a date", None),
        ("2026-13-45 00:00:00", None),
        (20150101, None),
    ],
)
def test_parse_date(value, expected):
    assert cd.parse_date(value) == expected


def test_snapshot_row_has_frequency_and_coverage_or_none():
    (row,) = cd.build_snapshot_rows([make_package()], [], BASE, {}, NOW, RUN_ID)
    assert row["frequency"] == "P1Y"
    assert (row["coverage_start"], row["coverage_end"]) == (
        date(2015, 1, 1),
        date(2026, 8, 31),
    )

    bare = make_package(frequency=None)
    for key in ("time_period_coverage_start", "time_period_coverage_end"):
        del bare[key]
    (row,) = cd.build_snapshot_rows([bare], [], BASE, {}, NOW, RUN_ID)
    assert (row["frequency"], row["coverage_start"], row["coverage_end"]) == (
        None,
        None,
        None,
    )


# --- 5. is_registered --------------------------------------------------------


def test_registered_ids_cover_every_status_and_same_portal_only():
    specs = [
        make_spec("id-1"),
        make_spec("id-2", status=ci.STATUS_CANDIDATE),
        make_spec("id-3", status=ci.STATUS_DEPRECATED),
        make_spec("other-portal-id", base_url="https://other.example/api/action"),
    ]
    assert cd.registered_ids(specs, BASE + "/") == {"id-1", "id-2", "id-3"}


@pytest.mark.parametrize(
    "spec_url",
    ["https://ckan.example/api/action", "https://ckan.example/api/3/action"],
)
def test_registered_ids_match_the_same_portal_with_or_without_api_version(spec_url):
    specs = [make_spec("id-1", base_url=spec_url)]
    assert cd.registered_ids(specs, "https://ckan.example/api/3/action") == {"id-1"}
    assert cd.registered_ids(specs, "https://ckan.example/api/action") == {"id-1"}


@pytest.mark.parametrize("status", ci.STATUSES)
def test_snapshot_marks_registered_whatever_the_status(status):
    (row,) = cd.build_snapshot_rows(
        [make_package(1)], [make_spec("id-1", status=status)], BASE, {}, NOW, RUN_ID
    )
    assert row["is_registered"] is True


def test_is_registered_matches_id_or_name():
    package = make_package(1)
    assert cd.is_registered(package, {"id-1"})
    assert cd.is_registered(package, {"dataset-1"})
    assert not cd.is_registered(package, {"id-9"})
    assert not cd.is_registered(package, set())


def test_snapshot_rows_flag_registered_packages():
    specs = [make_spec("id-1"), make_spec("dataset-2")]
    rows = cd.build_snapshot_rows(
        [make_package(1), make_package(2), make_package(3)],
        specs,
        BASE,
        {},
        NOW,
        RUN_ID,
    )
    assert [r["is_registered"] for r in rows] == [True, True, False]


# --- recall check ------------------------------------------------------------


def named_spec(key, ckan_id, status=ci.STATUS_ACTIVE, base_url=BASE):
    return ci.DatasetSpec(
        key=key,
        domain="immigration",
        ckan_id=ckan_id,
        update_pattern=ci.NEW_FILE,
        status=status,
        purpose="test purpose",
        base_url=base_url,
    )


def test_missing_from_run_lists_active_and_candidate_entries_not_found():
    specs = [
        named_spec("found_by_id", "id-1"),
        named_spec("found_by_name", "dataset-2", status=ci.STATUS_CANDIDATE),
        named_spec("lost_active", "id-8"),
        named_spec("lost_candidate", "id-9", status=ci.STATUS_CANDIDATE),
    ]
    packages = [make_package(1), make_package(2)]
    missing = cd.missing_from_run(specs, packages, BASE)
    assert [s.key for s in missing] == ["lost_active", "lost_candidate"]


def test_missing_from_run_ignores_deprecated_excluded_and_other_portals():
    specs = [
        named_spec("old", "id-8", status=ci.STATUS_DEPRECATED),
        named_spec("rejected", "id-9", status=ci.STATUS_EXCLUDED),
        named_spec("elsewhere", "id-7", base_url="https://other.example/api/3/action"),
    ]
    assert cd.missing_from_run(specs, [], BASE) == []


def test_missing_from_run_matches_the_portal_with_or_without_api_version():
    specs = [named_spec("lost", "id-8", base_url="https://ckan.example/api/action")]
    versioned = "https://ckan.example/api/3/action"
    assert [s.key for s in cd.missing_from_run(specs, [], versioned)] == ["lost"]


def test_missing_from_run_is_empty_when_everything_was_found_or_registry_is_empty():
    assert cd.missing_from_run([named_spec("a", "id-1")], [make_package(1)], BASE) == []
    assert cd.missing_from_run([], [make_package(1)], BASE) == []


# --- 6. snapshot rows --------------------------------------------------------


def test_snapshot_rows_share_run_id_and_timestamp_and_match_schema():
    rows = cd.build_snapshot_rows(
        [make_package(1), make_package(2)],
        [],
        BASE,
        {"id-1": ["lmia"], "id-2": ["lmia", "employers"]},
        NOW,
        RUN_ID,
    )
    assert {r["run_id"] for r in rows} == {RUN_ID}
    assert {r["discovered_at"] for r in rows} == {NOW}
    columns = [name for name, _ in cd.SNAPSHOT_COLUMNS]
    assert all(list(r) == columns for r in rows)
    assert "search_query" not in columns and "search_queries" in columns
    assert cd.SNAPSHOT_SCHEMA_DDL.startswith("run_id STRING, discovered_at TIMESTAMP")
    assert "search_queries ARRAY<STRING>" in cd.SNAPSHOT_SCHEMA_DDL
    assert rows[0]["base_url"] == BASE
    assert rows[0]["resource_count"] == 4 and rows[0]["formats"] == ["CSV", "XLSX"]


# --- multiple keywords --------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("lmia", ["lmia"]),
        ("permanent residents, LMIA", ["permanent residents", "LMIA"]),
        ("  a ,, b ,  ", ["a", "b"]),
        ("LMIA, lmia, Lmia, study", ["LMIA", "study"]),
        ("", []),
        (" , ,", []),
    ],
)
def test_parse_queries(text, expected):
    assert cd.parse_queries(text) == expected


class KeywordSession:
    """Serves a separate result list per keyword; a keyword can fail instead."""

    def __init__(self, catalogs, failing=()):
        self.catalogs = catalogs
        self.failing = set(failing)
        self.requests = []

    def get(self, url, params=None, timeout=None):
        self.requests.append(params["q"])
        if params["q"] in self.failing:
            return FakeResponse(504, content_type="text/html")
        found = self.catalogs[params["q"]]
        page = found[params["start"] : params["start"] + params["rows"]]
        return FakeResponse(
            payload={"success": True, "result": {"count": len(found), "results": page}}
        )


def test_search_many_searches_every_keyword_and_pages_each():
    session = KeywordSession(
        {"a": [make_package(n) for n in range(3)], "b": [make_package(10)]}
    )
    packages, _ = cd.search_many(
        session, ["a", "b"], BASE, page_size=2, sleep=no_sleep
    )
    assert session.requests == ["a", "a", "b"]  # "a" needs two pages
    assert [p["id"] for p in packages] == ["id-0", "id-1", "id-2", "id-10"]


def test_search_many_dedupes_by_ckan_id_and_records_every_keyword():
    session = KeywordSession(
        {
            "a": [make_package(1), make_package(2)],
            "b": [make_package(2, title_translated={"en": "Changed"}), make_package(3)],
            "c": [make_package(2)],
        }
    )
    packages, matches = cd.search_many(session, ["a", "b", "c"], BASE, sleep=no_sleep)

    assert [p["id"] for p in packages] == ["id-1", "id-2", "id-3"]
    assert matches == {"id-1": ["a"], "id-2": ["a", "b", "c"], "id-3": ["b"]}
    # The first search that found a dataset supplies its content.
    assert cd.package_title(packages[1]) == "TFWP Positive LMIA Employers"


def test_search_many_pauses_between_keywords():
    pauses = []
    session = KeywordSession({"a": [make_package(1)], "b": [make_package(2)]})
    cd.search_many(session, ["a", "b"], BASE, pause_seconds=2.0, sleep=pauses.append)
    assert pauses == [2.0]  # one gap between two single-page searches


def test_search_many_fails_whole_run_when_any_keyword_fails():
    session = KeywordSession(
        {"a": [make_package(1)], "b": [], "c": [make_package(3)]}, failing=["b"]
    )
    with pytest.raises(ci.UpstreamError):
        cd.search_many(session, ["a", "b", "c"], BASE, sleep=no_sleep)
    assert "c" not in session.requests  # stopped at the failure


def test_search_many_requires_a_keyword_or_an_organization():
    with pytest.raises(ValueError, match="keyword or organization"):
        cd.search_many(KeywordSession({}), [], BASE, sleep=no_sleep)


# --- organization mode -------------------------------------------------------


class SearchSession:
    """Serves package_search by q (keyword) or fq (organization filter)."""

    def __init__(self, by_term, failing=()):
        self.by_term = by_term  # "a" for q=a, "organization:cic" for fq
        self.failing = set(failing)
        self.requests = []

    def get(self, url, params=None, timeout=None):
        self.requests.append(dict(params))
        term = params.get("q") or params.get("fq")
        if term in self.failing:
            return FakeResponse(504, content_type="text/html")
        found = self.by_term[term]
        page = found[params["start"] : params["start"] + params["rows"]]
        return FakeResponse(
            payload={"success": True, "result": {"count": len(found), "results": page}}
        )


def test_search_organization_filters_with_fq_and_sends_no_q():
    session = SearchSession({"organization:cic": [make_package(n) for n in range(5)]})
    found = cd.search_organization(session, "cic", BASE, page_size=2, sleep=no_sleep)
    assert len(found) == 5 and len(session.requests) == 3
    for params in session.requests:
        assert params["fq"] == "organization:cic"
        assert "q" not in params
        assert params["sort"] == "name asc" and params["facet.limit"] == -1


def test_search_organization_checks_page_size_and_name_before_any_request():
    session = SearchSession({})
    with pytest.raises(ValueError, match="page_size"):
        cd.search_organization(session, "cic", BASE, page_size=1001, sleep=no_sleep)
    with pytest.raises(ValueError, match="organization"):
        cd.search_organization(session, "x OR y", BASE, sleep=no_sleep)
    assert session.requests == []


@pytest.mark.parametrize(
    "text, expected",
    [
        ("cic", ["cic"]),
        (" cic , esdc-edsc,,irb_cisr ", ["cic", "esdc-edsc", "irb_cisr"]),
        ("cic,cic", ["cic"]),
        ("", []),
        (" , ", []),
    ],
)
def test_parse_organizations(text, expected):
    assert cd.parse_organizations(text) == expected


@pytest.mark.parametrize("text", ["CIC", "cic;", "esdc edsc", "cic:x", "-cic", "a OR b"])
def test_parse_organizations_rejects_names_that_could_alter_the_filter(text):
    with pytest.raises(ValueError, match="invalid organization name"):
        cd.parse_organizations(text)


def test_search_many_merges_keywords_and_organizations_by_ckan_id():
    session = SearchSession(
        {
            "lmia": [make_package(1), make_package(2)],
            "organization:cic": [make_package(2), make_package(3)],
            "organization:esdc-edsc": [make_package(1)],
        }
    )
    packages, matches = cd.search_many(
        session,
        ["lmia"],
        BASE,
        sleep=no_sleep,
        organizations=["cic", "esdc-edsc"],
    )
    assert [p["id"] for p in packages] == ["id-1", "id-2", "id-3"]
    assert matches == {
        "id-1": ["lmia", "org:esdc-edsc"],
        "id-2": ["lmia", "org:cic"],
        "id-3": ["org:cic"],
    }


def test_search_many_accepts_organizations_alone_and_pauses_between_searches():
    pauses = []
    session = SearchSession(
        {"organization:a": [make_package(1)], "organization:b": [make_package(2)]}
    )
    _, matches = cd.search_many(
        session, [], BASE, pause_seconds=3.0, sleep=pauses.append, organizations=["a", "b"]
    )
    assert matches == {"id-1": ["org:a"], "id-2": ["org:b"]}
    assert pauses == [3.0]


def test_search_many_fails_whole_run_when_an_organization_search_fails():
    session = SearchSession(
        {"a": [make_package(1)], "organization:cic": []},
        failing=["organization:cic"],
    )
    with pytest.raises(ci.UpstreamError):
        cd.search_many(session, ["a"], BASE, sleep=no_sleep, organizations=["cic"])


def test_organization_search_label_lands_in_snapshot_search_queries():
    session = SearchSession({"organization:cic": [make_package(1)]})
    packages, matches = cd.search_many(
        session, [], BASE, sleep=no_sleep, organizations=["cic"]
    )
    (row,) = cd.build_snapshot_rows(packages, [], BASE, matches, NOW, RUN_ID)
    assert row["search_queries"] == ["org:cic"]


def test_multi_keyword_run_yields_one_row_per_dataset_sharing_run_values():
    session = KeywordSession(
        {"a": [make_package(1), make_package(2)], "b": [make_package(2), make_package(3)]}
    )
    packages, matches = cd.search_many(session, ["a", "b"], BASE, sleep=no_sleep)
    rows = cd.build_snapshot_rows(packages, [], BASE, matches, NOW, RUN_ID)

    assert [r["ckan_id"] for r in rows] == ["id-1", "id-2", "id-3"]
    assert [r["search_queries"] for r in rows] == [["a"], ["a", "b"], ["b"]]
    assert {r["run_id"] for r in rows} == {RUN_ID}
    assert {r["discovered_at"] for r in rows} == {NOW}


def test_row_without_matches_gets_empty_search_queries():
    (row,) = cd.build_snapshot_rows([make_package(1)], [], BASE, {}, NOW, RUN_ID)
    assert row["search_queries"] == []


# --- drafts -------------------------------------------------------------------


def test_package_title_prefers_english_then_falls_back():
    assert cd.package_title(make_package()) == "TFWP Positive LMIA Employers"
    assert cd.package_title({"title": "Plain", "name": "n"}) == "Plain"
    assert cd.package_title({"name": "only-name"}) == "only-name"


def test_slugify_key_is_folder_safe_and_bounded():
    assert cd.slugify_key("TFWP Positive LMIA Employers!") == "tfwp_positive_lmia_employers"
    assert cd.slugify_key("***") == "dataset"
    long_key = cd.slugify_key("word " * 30)
    assert len(long_key) <= cd.MAX_KEY_LENGTH and not long_key.endswith("_")


def test_registry_entry_yaml_is_accepted_by_parse_registry():
    # Quotes in the title must not break the YAML.
    package = make_package(title_translated={"en": 'Employers: "LMIA" list'})
    draft = cd.registry_entry_yaml(package, domain="immigration")
    raw = {"datasets": yaml.safe_load("datasets:\n" + draft)["datasets"]}

    (spec,) = ci.parse_registry(raw)
    assert spec.domain == "immigration"
    assert spec.ckan_id == package["id"]
    assert spec.formats == ("CSV", "XLSX")
    assert spec.status == ci.STATUS_CANDIDATE
    assert spec.purpose.startswith("TODO")  # placeholder for the reviewer to replace
    assert spec.description == 'Employers: "LMIA" list'


def test_unregistered_drafts_skip_registered_and_respect_max_drafts():
    packages = [make_package(n) for n in range(1, 6)]
    registered = {"id-1", "dataset-2"}

    drafts, left_out = cd.unregistered_drafts(packages, registered, "immigration", 2)
    assert len(drafts) == 2 and left_out == 1
    assert "id-1" not in "".join(drafts) and "id-2" not in "".join(drafts)
    assert "ckan_id: id-3" in drafts[0]

    assert cd.unregistered_drafts(packages, registered, "immigration", 0) == ([], 3)
    assert cd.unregistered_drafts(packages, registered, "immigration", 50)[1] == 0


# --- 7. table definition matches the rows --------------------------------------


def test_catalog_setup_table_columns_match_snapshot_columns():
    assert tuple((n, t) for n, t, _ in cs.SNAPSHOT_COLUMNS) == cd.SNAPSHOT_COLUMNS
