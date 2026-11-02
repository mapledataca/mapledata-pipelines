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


def archived(spec, metadata, previous_metadata=None):
    """Run check_archived, with the previous snapshot built from older metadata."""
    previous = (
        ic.build_resource_snapshot("20260901T000000Z", NOW, spec, previous_metadata, log())
        if previous_metadata is not None
        else []
    )
    return ic.check_archived(ctx(spec, metadata), previous)


def test_archived_first_run_warns_for_every_archived_resource_and_the_dataset():
    spec = make_spec()
    metadata = meta(
        res("a", "Old table (ARCHIVED)"),
        res("b", "Current table", description="This file is Archived."),
        res("c", "Current table"),
        title="Archived dataset: kept for reference",
    )
    found = archived(spec, metadata)  # no previous snapshot

    assert names(found) == [
        (ic.ARCHIVED, ic.WARN, None),
        (ic.ARCHIVED, ic.WARN, "a"),
        (ic.ARCHIVED, ic.WARN, "b"),
    ]
    assert details(found[0]) == {"matched_in": ["title"], "first_run": True}
    assert details(found[1]) == {"matched_in": ["name"], "first_run": True}
    assert details(found[2]) == {"matched_in": ["description"], "first_run": True}
    assert "first run" in found[1].message


def test_archived_unchanged_is_one_aggregated_info_per_dataset():
    spec = make_spec()
    metadata = meta(
        res("a", "Old table [ARCHIVED]"),
        res("b", "Old table 2", description="archived"),
        res("c", "Current"),
        title="Old data (archived)",
    )
    found = archived(spec, metadata, previous_metadata=metadata)

    assert names(found) == [(ic.ARCHIVED, ic.INFO, None)]
    assert found[0].message == (
        "2 archived resources landed, unchanged; "
        "the dataset name or title also says archived, unchanged"
    )
    assert details(found[0]) == {
        "archived_resource_count": 2,
        "resource_ids": ["a", "b"],
        "dataset_title_archived": True,
    }


def test_archived_unchanged_without_a_dataset_level_match_says_only_resources():
    spec = make_spec()
    metadata = meta(res("a", "Old [archived]"), res("b", "Current"))
    (finding,) = archived(spec, metadata, previous_metadata=metadata)
    assert finding.message == "1 archived resources landed, unchanged"
    assert details(finding)["dataset_title_archived"] is False


def test_archived_newly_archived_resource_warns_and_the_rest_stay_info():
    spec = make_spec()
    before = meta(res("a", "Old [archived]"), res("b", "Table B"), res("gone", "Gone"))
    now = meta(
        res("a", "Old [archived]"),        # still archived: unchanged
        res("b", "Table B [ARCHIVED]"),    # became archived: newly archived
        res("c", "Brand new [archived]"),  # arrives archived: never seen before
    )
    found = archived(spec, now, previous_metadata=before)

    assert names(found) == [
        (ic.ARCHIVED, ic.WARN, "b"),
        (ic.ARCHIVED, ic.WARN, "c"),
        (ic.ARCHIVED, ic.INFO, None),
    ]
    assert details(found[0]) == {"matched_in": ["name"], "first_run": False}
    assert "newly" in found[0].message
    assert details(found[2])["resource_ids"] == ["a"]
    assert details(found[2])["archived_resource_count"] == 1


def test_archived_dataset_that_newly_says_archived_warns_once():
    spec = make_spec()
    before = meta(res("a", "Table"))
    now = meta(res("a", "Table"), title="Employers list (archived)")
    found = archived(spec, now, previous_metadata=before)
    assert names(found) == [(ic.ARCHIVED, ic.WARN, None)]
    assert details(found[0]) == {"matched_in": ["title"], "first_run": False}
    assert "newly" in found[0].message


def test_dataset_name_counts_as_archived_too():
    found = archived(make_spec(), meta(res("a", "Table"), name="old-data-archived"))
    assert [(f.resource_id, details(f)["matched_in"]) for f in found] == [(None, ["name"])]


ESDC_NOTES = (
    "Quarterly LMIA statistics. The tables were reworked (TEER based).\n"
    "As such, Table 5, 8, 17, and 24 will no longer be updated but will remain "
    "as archived tables.\n\nFrequency of Publication: quarterly."
)


def test_a_description_mentioning_archived_tables_is_not_the_dataset_archived():
    spec = make_spec()
    metadata = meta(res("a", "Table 14"), notes=ESDC_NOTES)
    # Not the dataset-level WARN, in either run.
    assert archived(spec, metadata) == []
    assert archived(spec, metadata, previous_metadata=metadata) == []
    assert [r["dataset_archived"] for r in snapshot(spec, metadata)] == [False]


def test_description_mentions_archived_reports_the_matching_sentence_as_info():
    found = ic.check_description_mentions_archived(
        ctx(make_spec(), meta(res("a", "Table 14"), notes=ESDC_NOTES))
    )
    assert names(found) == [(ic.DESCRIPTION_MENTIONS_ARCHIVED, ic.INFO, None)]
    assert details(found[0]) == {
        "field": "notes",
        "matching_sentences": [
            "As such, Table 5, 8, 17, and 24 will no longer be updated but will "
            "remain as archived tables."
        ],
    }
    assert "remain as archived tables" in found[0].message


def test_description_mentions_archived_reads_every_language_and_keeps_each_sentence_once():
    metadata = meta(
        res("a"),
        notes="Table 5 is Archived.",
        notes_translated={"en": "Table 5 is Archived.", "fr": "Le tableau 6 est archived. Autre phrase."},
    )
    (finding,) = ic.check_description_mentions_archived(ctx(make_spec(), metadata))
    assert details(finding)["matching_sentences"] == [
        "Table 5 is Archived.",
        "Le tableau 6 est archived.",
    ]


def test_description_mentions_archived_is_silent_without_a_mention_or_description():
    assert ic.check_description_mentions_archived(ctx(make_spec(), meta(res("a"), notes="All current."))) == []
    assert ic.check_description_mentions_archived(ctx(make_spec(), meta(res("a")))) == []


def test_matching_sentences_collapse_space_cap_the_count_and_the_length():
    import re
    pattern = re.compile("archived", re.I)
    text = "One archived.  Two\narchived   here! None. " + " ".join(f"s{i} archived." for i in range(10))
    found = ic.matching_sentences([text], pattern)
    # "Two" has no match; a line break ends a sentence; white space is collapsed.
    assert found == [
        "One archived.", "archived here!", "s0 archived.", "s1 archived.", "s2 archived.",
    ]
    long = ic.matching_sentences(["archived " + "x" * 600], pattern, max_length=50)
    assert len(long) == 1 and len(long[0]) == 50
    assert ic.matching_sentences([], pattern) == []


def test_run_checks_includes_the_description_check_and_keeps_the_dataset_silent():
    spec = make_spec()
    found = ic.run_checks(
        RUN, NOW, [spec],
        {"ds": meta(res("a", "Table 14"), notes=ESDC_NOTES)},
        {"ds": log()},
    )
    assert names(found) == [(ic.DESCRIPTION_MENTIONS_ARCHIVED, ic.INFO, None)]


def test_archived_state_is_remembered_even_when_only_the_description_says_it():
    # A name-only comparison would call this newly archived on every run.
    spec = make_spec()
    metadata = meta(res("a", "Plain name", description="This table is archived."))
    rows = snapshot(spec, metadata)
    assert [(r["archived"], r["dataset_archived"]) for r in rows] == [(True, False)]
    found = ic.check_archived(ctx(spec, metadata), rows)
    assert names(found) == [(ic.ARCHIVED, ic.INFO, None)]


def test_archived_is_silent_when_nothing_is_archived():
    spec = make_spec()
    metadata = meta(res("a", "Fine"))
    assert archived(spec, metadata) == []
    assert archived(spec, metadata, previous_metadata=metadata) == []


def test_archived_no_longer_archived_is_not_reported():
    spec = make_spec()
    before = meta(res("a", "Old [archived]"))
    now = meta(res("a", "Old"))
    assert archived(spec, now, previous_metadata=before) == []


def test_archived_reads_translated_names_and_descriptions():
    spec = make_spec()
    metadata = meta(
        res("a", "Plain", name_translated={"en": "Table (archived)", "fr": "Tableau"}),
        res("b", "Plain", description_translated={"en": "ok", "fr": "Archived copy"}),
        title_translated={"en": "A dataset", "fr": "Archived"},
    )
    found = archived(spec, metadata)
    assert [(f.resource_id, details(f)["matched_in"]) for f in found] == [
        (None, ["title"]), ("a", ["name"]), ("b", ["description"]),
    ]


def test_archived_only_looks_at_selected_resources():
    spec = make_spec(formats=("CSV",))
    metadata = meta(res("a", "Old (archived)", fmt="XLSX"), res("b", "Fine"))
    assert archived(spec, metadata) == []
    assert archived(spec, metadata, previous_metadata=metadata) == []


def test_snapshot_records_archived_state_per_resource_and_per_dataset():
    spec = make_spec()
    metadata = meta(res("a", "Old [archived]"), res("b", "Current"), title="Archived data")
    rows = snapshot(spec, metadata)
    assert [(r["resource_id"], r["archived"]) for r in rows] == [("a", True), ("b", False)]
    assert {r["dataset_archived"] for r in rows} == {True}


def test_run_checks_reports_archived_as_info_on_the_second_run():
    spec = make_spec()
    metadata = {"ds": meta(res("a", "Old [archived]"))}
    first = ic.run_checks(RUN, NOW, [spec], metadata, {"ds": log()})
    assert names(first) == [(ic.ARCHIVED, ic.WARN, "a")]

    previous = {"ds": ic.build_resource_snapshot("20260901T000000Z", NOW, spec, metadata["ds"], log())}
    second = ic.run_checks(
        RUN, NOW, [spec], metadata, {"ds": log()}, previous_rows_by_key=previous
    )
    assert names(second) == [(ic.ARCHIVED, ic.INFO, None)]


# ---------------------------------------------------------------------------
# rounded_values
# ---------------------------------------------------------------------------


def scan(spec, metadata, files=None, tails=None, run_log=None):
    """Scan the landed files with fake readers; return (scan, head reader, tail reader)."""
    run_log = run_log or log(downloaded=[f"{r['id']}_data.csv" for r in metadata["resources"]])
    snap = snapshot(spec, metadata, run_log)
    landed_paths = {r["resource_id"]: r["landed_path"] for r in snap if r["landed_path"]}
    head = FakeHead(files or {})
    tail = FakeHead(tails) if tails is not None else None
    result = ic.scan_rounded(ctx(spec, metadata, run_log), landed_paths, head, tail, LANDING)
    return result, head, tail


def rounded(spec, metadata, files=None, tails=None, run_log=None):
    """Run check_rounded_values; return (findings, head reader, tail reader)."""
    result, head, tail = scan(spec, metadata, files, tails, run_log)
    run_log = run_log or log(downloaded=[f"{r['id']}_data.csv" for r in metadata["resources"]])
    return ic.check_rounded_values(ctx(spec, metadata, run_log), result), head, tail


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
    found, reader, _ = rounded(
        spec, metadata, {landed("a_data.csv"): b"plain"}, run_log=run_log
    )
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
    result = ic.scan_rounded(ctx(spec, metadata), {"a": "a_data.csv", "b": "b_data.csv"})
    (finding,) = ic.check_rounded_values(ctx(spec, metadata), result)
    assert details(finding)["matched_in"] == {"a": ["name"]}
    assert details(finding)["files_read"] == 0 and details(finding)["files_unreadable"] == 0


def test_rounded_values_is_silent_when_nothing_is_rounded():
    spec = make_spec()
    metadata = meta(res("a", "Plain"))
    assert rounded(spec, metadata, {landed("a_data.csv"): b"a,b\n1,2\n"})[0] == []


def test_rounded_values_only_searches_what_the_readers_return():
    spec = make_spec()
    metadata = meta(res("a"))
    # The injected readers decide how much to return; a short head hides the word.
    assert rounded(spec, metadata, {landed("a_data.csv"): b"a,b\n"})[0] == []


def test_rounded_values_also_reads_the_file_tail_and_records_file_tail():
    spec = make_spec()
    metadata = meta(res("a"), res("b"), res("c"))
    heads = {landed(f"{r}_data.csv"): b"a,b\n1,2\n" for r in "abc"}
    tails = {
        landed("a_data.csv"): b"Note: figures are rounded to the nearest 5.",
        landed("b_data.csv"): b"1,2\n",
        landed("c_data.csv"): b"[ROUNDED]",
    }
    (finding,) = rounded(spec, metadata, heads, tails)[0]
    body = details(finding)
    assert body["matched_in"] == {"a": ["file_tail"], "c": ["file_tail"]}
    assert body["resource_ids"] == ["a", "c"] and body["files_read"] == 3


def test_rounded_values_lists_name_description_head_and_tail_in_that_order():
    spec = make_spec()
    metadata = meta(res("a", "Table (rounded)", description="rounded too"))
    files = {landed("a_data.csv"): b"rounded"}
    tails = {landed("a_data.csv"): b"rounded"}
    (finding,) = rounded(spec, metadata, files, tails)[0]
    assert details(finding)["matched_in"] == {
        "a": ["name", "description", "file_head", "file_tail"]
    }


def test_rounded_values_skips_the_tail_when_there_is_no_tail_reader():
    spec = make_spec()
    metadata = meta(res("a"))
    result, head, tail = scan(spec, metadata, {landed("a_data.csv"): b"plain"})
    assert tail is None and head.calls == [landed("a_data.csv")]
    assert result.matches == {} and result.files_read == 1


def test_rounded_values_counts_a_file_once_when_a_tail_read_fails():
    spec = make_spec()
    metadata = meta(res("a", "[rounded] A"), res("b"))
    heads = {landed("a_data.csv"): b"x", landed("b_data.csv"): b"x"}
    tails = {landed("a_data.csv"): b"x", landed("b_data.csv"): OSError("gone")}
    result, _head, tail = scan(spec, metadata, heads, tails)
    assert (result.files_read, result.files_unreadable) == (1, 1)
    assert result.matches == {"a": ["name"]}
    assert tail.calls == [landed("a_data.csv"), landed("b_data.csv")]


def test_rounded_values_keeps_a_head_match_when_only_the_tail_read_fails():
    spec = make_spec()
    metadata = meta(res("a"))
    result, _h, _t = scan(
        spec, metadata, {landed("a_data.csv"): b"rounded"}, {landed("a_data.csv"): OSError("x")}
    )
    assert result.matches == {"a": ["file_head"]} and result.files_unreadable == 1


def test_scan_rounded_searches_every_resource_by_name_but_reads_only_landed_csv():
    spec = make_spec(formats=("CSV",))
    metadata = meta(res("a"), res("x", "[rounded] XLSX table", fmt="XLSX"))
    result, head, _t = scan(spec, metadata, {landed("a_data.csv"): b"plain"})
    assert result.matches == {"x": ["name"]}      # not selected, but named rounded
    assert head.calls == [landed("a_data.csv")]   # only the landed CSV is read
    # The finding speaks only of the selected resources.
    assert ic.check_rounded_values(ctx(spec, metadata), result) == []


def test_file_tail_reader_returns_the_end_of_a_file_without_overlapping_the_head(tmp_path):
    path = tmp_path / "t.csv"
    path.write_bytes(b"H" * 10 + b"M" * 30 + b"T" * 5)       # 45 bytes
    assert ic.file_tail_reader(size=5, skip_head=10)(str(path)) == b"T" * 5
    # The tail never reaches back into the head region.
    assert ic.file_tail_reader(size=100, skip_head=10)(str(path)) == b"M" * 30 + b"T" * 5
    # A file no longer than the head has no tail.
    assert ic.file_tail_reader(size=5, skip_head=45)(str(path)) == b""
    assert ic.file_tail_reader(size=5, skip_head=100)(str(path)) == b""
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    assert ic.file_tail_reader()(str(empty)) == b""
    with pytest.raises(OSError):
        ic.file_tail_reader()(str(tmp_path / "missing.csv"))


def test_file_readers_split_a_real_file_into_head_and_tail(tmp_path):
    path = tmp_path / "big.csv"
    path.write_bytes(b"a" * ic.HEAD_BYTES + b"b" * 50 + b"rounded")
    assert b"rounded" not in ic.file_head_reader()(str(path))
    assert b"rounded" in ic.file_tail_reader()(str(path))


# --- rounded_marker in the snapshot -------------------------------------------


def test_snapshot_rounded_marker_comes_from_name_and_description_without_a_scan():
    spec = make_spec()
    rows = snapshot(
        spec,
        meta(res("a", "[rounded] A"), res("b", "B", description="Rounded figures"), res("c", "C")),
    )
    assert [(r["resource_id"], r["rounded_marker"]) for r in rows] == [
        ("a", True), ("b", True), ("c", False),
    ]


def test_snapshot_rounded_marker_uses_the_scan_when_given_one():
    spec = make_spec()
    metadata = meta(res("a"), res("b"))
    rows = ic.build_resource_snapshot(
        RUN, NOW, spec, metadata, log(), rounded={"b": ["file_tail"]}
    )
    assert [(r["resource_id"], r["rounded_marker"]) for r in rows] == [("a", False), ("b", True)]


def test_rounded_marker_false_does_not_mean_the_values_are_exact():
    snapshot_doc = " ".join(ic.build_resource_snapshot.__doc__.split())
    scan_doc = " ".join(ic.scan_rounded.__doc__.split())
    assert "NOT that the values are exact" in snapshot_doc
    assert "no marker was found" in scan_doc


def test_rounded_marker_is_the_last_column_so_a_migrated_table_keeps_the_order():
    assert ic.SNAPSHOT_COLUMNS[-1] == ("rounded_marker", "BOOLEAN")
    assert list(snapshot(make_spec(), meta(res("a")))[0])[-1] == "rounded_marker"


def test_run_all_returns_snapshot_rows_with_the_marker_found_in_the_files(tmp_path):
    spec = make_spec()
    landing_dir = ci.dataset_landing_dir(ci.RunSettings(str(tmp_path), ""), spec)
    landing_dir.mkdir(parents=True)
    (landing_dir / "a_data.csv").write_bytes(b"a,b\n" + b"x" * ic.HEAD_BYTES + b"\nrounded to 5\n")
    (landing_dir / "b_data.csv").write_bytes(b"a,b\n1,2\n")

    findings, rows = ic.run_all(
        RUN, NOW, [spec],
        metadata_by_key={"ds": meta(res("a"), res("b"))},
        run_logs_by_key={"ds": log(downloaded=["a_data.csv", "b_data.csv"])},
        read_head=ic.file_head_reader(),
        read_tail=ic.file_tail_reader(),
        landing_root=str(tmp_path),
    )
    assert [(r["resource_id"], r["rounded_marker"]) for r in rows] == [("a", True), ("b", False)]
    assert [(f.check_name, details(f)["matched_in"]) for f in findings] == [
        (ic.ROUNDED_VALUES, {"a": ["file_tail"]})
    ]


def test_run_all_builds_rows_only_for_datasets_with_metadata_and_a_log():
    a, b, c = make_spec("a"), make_spec("b"), make_spec("c")
    findings, rows = ic.run_all(
        RUN, NOW, [a, b, c],
        metadata_by_key={"a": meta(res("r1"))},   # b has a log but no metadata
        run_logs_by_key={"a": log(), "b": log(error="no metadata")},
    )
    assert {r["dataset_key"] for r in rows} == {"a"}
    assert [(f.dataset_key, f.check_name) for f in findings] == [("b", ic.DOWNLOAD_FAILED)]


def test_run_checks_is_the_findings_of_run_all():
    spec = make_spec()
    args = (RUN, NOW, [spec], {"ds": meta(res("a", "Old [archived]"))}, {"ds": log()})
    assert ic.run_checks(*args) == ic.run_all(*args)[0]


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
