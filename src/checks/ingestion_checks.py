"""
ingestion_checks.py

Post-ingestion checks. After an ingestion run, look at what CKAN publishes for
each registered dataset (the cached package_show.json), what the run did (the
run log) and the landed files, and report what needs a human's attention.

The checks only read: nothing here changes ingestion behaviour. Network access
is not needed; files are read from the metadata and landing volumes, and the
function that reads a file's first bytes is injected so tests can fake it.

Inputs, per dataset:
    <meta_root>/<source>/<key>/package_show.json   cached CKAN metadata
    <meta_root>/<source>/<key>/runs/<run_id>.json  run log written by ckan_ingest
    <landing_root>/<source>/<key>/...              landed files
    previous snapshot rows (optional)              for added/removed resources

Outputs:
    build_resource_snapshot -> rows for metadata.ckan_resource_snapshot
    run_checks              -> Finding objects for metadata.ingestion_findings
    render_markdown         -> a human-readable report

The cached metadata is overwritten by every successful ingestion run, so run
the checks right after the run they describe (for example as the next task of
the same job). Otherwise the metadata may belong to a later run.

Severities: ERROR (the run failed to do its job), WARN (needs a look),
INFO (good to know).
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import ckan_ingest as ci

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ERROR, WARN, INFO = "ERROR", "WARN", "INFO"
SEVERITIES = (ERROR, WARN, INFO)

DOWNLOAD_FAILED = "download_failed"
METADATA_STALE = "metadata_stale"
RESOURCE_REMOVED = "resource_removed"
RESOURCE_ADDED = "resource_added"
FORMAT_GAP = "format_gap"
MISSING_LAST_MODIFIED = "missing_last_modified"
ARCHIVED = "archived"
ROUNDED_VALUES = "rounded_values"

#: Formats that can hold a data table. Documentation files (DOCX, PDF, ...) are
#: ignored by format_gap.
TABULAR_FORMATS = ("CSV", "XLS", "XLSX")

#: How many bytes of a landed CSV are searched for "rounded".
HEAD_BYTES = 4096

#: Shown in every report; format_gap cannot prove full coverage.
FORMAT_GAP_NOTE = (
    "format_gap is a lower bound; no finding does not mean full coverage."
)

#: Columns of metadata.ckan_resource_snapshot. catalog_setup.py holds the same
#: list (with comments) for the table; a test checks that the two agree.
SNAPSHOT_COLUMNS = (
    ("run_id", "STRING"),
    ("checked_at", "TIMESTAMP"),
    ("dataset_key", "STRING"),
    ("ckan_id", "STRING"),
    ("resource_id", "STRING"),
    ("name", "STRING"),
    ("language", "ARRAY<STRING>"),
    ("format", "STRING"),
    ("url", "STRING"),
    ("last_modified", "STRING"),
    ("created", "STRING"),
    ("selected", "BOOLEAN"),
    ("landed_path", "STRING"),
)

#: Columns of metadata.ingestion_findings (same note as above).
FINDING_COLUMNS = (
    ("run_id", "STRING"),
    ("checked_at", "TIMESTAMP"),
    ("dataset_key", "STRING"),
    ("resource_id", "STRING"),
    ("check_name", "STRING"),
    ("severity", "STRING"),
    ("message", "STRING"),
    ("details_json", "STRING"),
)

#: The same columns as Spark DDL schema strings, for spark.createDataFrame.
SNAPSHOT_SCHEMA_DDL = ", ".join(f"{n} {t}" for n, t in SNAPSHOT_COLUMNS)
FINDINGS_SCHEMA_DDL = ", ".join(f"{n} {t}" for n, t in FINDING_COLUMNS)

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_ARCHIVED = re.compile("archived", re.IGNORECASE)
_ROUNDED = re.compile("rounded", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    """One thing the checks noticed.

    resource_id is None for a finding about the whole dataset. details_json is
    a JSON object with check-specific details.
    """

    run_id: str
    checked_at: datetime
    dataset_key: str
    resource_id: Optional[str]
    check_name: str
    severity: str
    message: str
    details_json: str

    def as_row(self) -> dict:
        """A dict keyed by FINDING_COLUMNS, ready for spark.createDataFrame."""
        return {name: getattr(self, name) for name, _ in FINDING_COLUMNS}


@dataclass(frozen=True)
class Context:
    """Everything one dataset's checks need."""

    run_id: str
    checked_at: datetime
    spec: ci.DatasetSpec
    metadata: dict
    run_log: dict

    def finding(
        self,
        check_name: str,
        severity: str,
        message: str,
        resource_id: Optional[str] = None,
        **details: Any,
    ) -> Finding:
        return Finding(
            run_id=self.run_id,
            checked_at=self.checked_at,
            dataset_key=self.spec.key,
            resource_id=resource_id,
            check_name=check_name,
            severity=severity,
            message=message,
            details_json=json.dumps(details, sort_keys=True, ensure_ascii=False),
        )

    @property
    def report(self) -> dict:
        return self.run_log.get("report") or {}

    @property
    def resources(self) -> list:
        return self.metadata.get("resources") or []

    @property
    def selected(self) -> list:
        """The resources the registry entry ingests (same rule as ingestion)."""
        return ci.select_resources(self.metadata, self.spec)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def _texts(item: dict, fields: Iterable) -> dict:
    """field -> all of its text, including every language of a *_translated field."""
    found = {}
    for field in fields:
        parts = []
        value = item.get(field)
        if isinstance(value, str):
            parts.append(value)
        translated = item.get(f"{field}_translated")
        if isinstance(translated, dict):
            parts.extend(v for v in translated.values() if isinstance(v, str))
        if parts:
            found[field] = " ".join(parts)
    return found


def _matched_fields(item: dict, fields: Iterable, pattern: re.Pattern) -> list:
    """Names of the fields whose text matches the pattern."""
    return [f for f, text in _texts(item, fields).items() if pattern.search(text)]


# ---------------------------------------------------------------------------
# Resource snapshot
# ---------------------------------------------------------------------------


def _landed_paths(run_log: dict, resource_ids: Iterable) -> dict:
    """resource_id -> relative path landed (or already present) in this run.

    Landed paths start with "<resource_id>_" (new_file_per_period) or
    "<resource_id>/" (overwrite_in_place), see ADR 0003.
    """
    report = run_log.get("report") or {}
    paths = [*(report.get("downloaded") or []), *(report.get("skipped") or [])]
    landed = {}
    for rid in resource_ids:
        for path in paths:
            if path.startswith(f"{rid}_") or path.startswith(f"{rid}/"):
                landed[rid] = path
    return landed


def build_resource_snapshot(
    run_id: str, checked_at: datetime, spec: ci.DatasetSpec, metadata: dict, run_log: dict
) -> list:
    """One row (dict keyed by SNAPSHOT_COLUMNS) per resource CKAN lists.

    selected is True for the resources this registry entry ingests;
    landed_path is the file this run wrote or found already present, or None
    (not selected, or the download failed).
    """
    resources = metadata.get("resources") or []
    selected_ids = {r.get("id") for r in ci.select_resources(metadata, spec)}
    landed = _landed_paths(run_log, [r.get("id") for r in resources])
    rows = []
    for res in resources:
        rid = res.get("id")
        rows.append(
            {
                "run_id": run_id,
                "checked_at": checked_at,
                "dataset_key": spec.key,
                "ckan_id": metadata.get("id") or spec.ckan_id,
                "resource_id": rid,
                "name": ci._resource_name(res),
                "language": list(res.get("language") or []),
                "format": res.get("format"),
                "url": res.get("url"),
                "last_modified": res.get("last_modified"),
                "created": res.get("created"),
                "selected": rid in selected_ids,
                "landed_path": landed.get(rid),
            }
        )
    return rows


def select_previous_rows(rows: Iterable, run_id: str) -> list:
    """The rows of the latest run before run_id (run ids sort by time), or [].

    rows are snapshot rows of one dataset from earlier runs. Rows of run_id
    itself and later runs are ignored, so re-checking a run compares it with
    the run that came before it.
    """
    earlier = [r for r in rows if r["run_id"] < run_id]
    if not earlier:
        return []
    latest = max(r["run_id"] for r in earlier)
    return [r for r in earlier if r["run_id"] == latest]


# ---------------------------------------------------------------------------
# Checks. One function each; each returns a list of Finding.
# ---------------------------------------------------------------------------


def check_download_failed(ctx: Context) -> list:
    """ERROR: failures recorded in the run log.

    One finding per failed resource, plus one for a dataset-level failure
    (for example metadata unavailable and no cache).
    """
    findings = []
    error = ctx.report.get("error")
    if error:
        findings.append(
            ctx.finding(
                DOWNLOAD_FAILED, ERROR, f"dataset-level failure: {error}", error=error
            )
        )
    for rid, message in ctx.report.get("failed") or []:
        findings.append(
            ctx.finding(
                DOWNLOAD_FAILED, ERROR, f"download failed: {message}", rid, error=message
            )
        )
    return findings


def check_metadata_stale(ctx: Context) -> list:
    """WARN: the run used the cached metadata because a fresh fetch failed."""
    if ctx.report.get("metadata_source") != "cache":
        return []
    return [
        ctx.finding(
            METADATA_STALE,
            WARN,
            "ingestion used the cached package_show metadata; CKAN may list "
            "resources this run did not see",
            metadata_source="cache",
        )
    ]


def check_resource_removed(ctx: Context, snapshot: list, previous: list) -> list:
    """WARN: a resource in the previous snapshot is no longer listed.

    Skipped (no findings) when there is no previous snapshot, i.e. on the first run.
    """
    if not previous:
        return []
    current_ids = {r["resource_id"] for r in snapshot}
    return [
        ctx.finding(
            RESOURCE_REMOVED,
            WARN,
            f"resource no longer listed: {row['name']}",
            row["resource_id"],
            name=row["name"],
            format=row["format"],
            was_selected=row["selected"],
            previous_run_id=row["run_id"],
        )
        for row in sorted(previous, key=lambda r: r["resource_id"])
        if row["resource_id"] not in current_ids
    ]


def check_resource_added(ctx: Context, snapshot: list, previous: list) -> list:
    """INFO: a resource is listed that the previous snapshot did not have.

    Skipped (no findings) when there is no previous snapshot, i.e. on the first run.
    """
    if not previous:
        return []
    previous_ids = {r["resource_id"] for r in previous}
    return [
        ctx.finding(
            RESOURCE_ADDED,
            INFO,
            f"new resource: {row['name']}",
            row["resource_id"],
            name=row["name"],
            format=row["format"],
            selected=row["selected"],
            previous_run_id=previous[0]["run_id"],
        )
        for row in sorted(snapshot, key=lambda r: r["resource_id"])
        if row["resource_id"] not in previous_ids
    ]


def check_format_gap(ctx: Context) -> list:
    """WARN: tables that exist only in a format the registry entry does not select.

    This is a LOWER BOUND, and a no-finding result does not mean full coverage.

    Pairing the same table across formats by name is unreliable. Checked on the
    live metadata of the active immigration datasets, CSV and XLSX versions of
    one table rarely share a name or a file name: for example permanent
    residents has 16 CSV and 18 XLSX resources and not one pair matches. A
    check that looked for names present in only one format would flag nearly
    every XLSX file.

    So no pairing is attempted. Among resources that pass the entry's language
    and name_regex filters, count those in the selected formats (n_selected)
    and those in each unselected tabular format (n_f). If a table has at most
    one resource per format, at least n_f - n_selected tables can only be
    found in format f. That is reported when it is positive; the true number
    may be higher. Documentation formats (DOCX, PDF, ...) are ignored.

    Nothing is reported when the entry has no format filter.
    """
    if not ctx.spec.formats:
        return []
    candidates = ci.select_resources(ctx.metadata, replace(ctx.spec, formats=None))
    wanted = set(ctx.spec.formats)
    by_format = Counter(str(r.get("format", "")).upper() for r in candidates)
    n_selected = sum(n for fmt, n in by_format.items() if fmt in wanted)

    findings = []
    for fmt in TABULAR_FORMATS:
        if fmt in wanted:
            continue
        gap = by_format.get(fmt, 0) - n_selected
        if gap <= 0:
            continue
        names = sorted(
            ci._resource_name(r)
            for r in candidates
            if str(r.get("format", "")).upper() == fmt
        )
        findings.append(
            ctx.finding(
                FORMAT_GAP,
                WARN,
                f"at least {gap} table(s) are available only as {fmt}, which "
                f"this dataset does not select",
                format=fmt,
                unselected_format_resources=by_format[fmt],
                selected_format_resources=n_selected,
                minimum_tables_only_in_format=gap,
                resource_names=names,
                note=FORMAT_GAP_NOTE,
            )
        )
    return findings


def check_missing_last_modified(ctx: Context) -> list:
    """INFO: overwrite_in_place resources without a usable last_modified.

    They are versioned by content hash and downloaded on every run (ADR 0003).
    One finding per dataset, listing the affected resources.
    """
    if ctx.spec.update_pattern != ci.OVERWRITE:
        return []
    selected = ctx.selected
    affected = [r["id"] for r in selected if ci.resource_version(r) is None]
    if not affected:
        return []
    return [
        ctx.finding(
            MISSING_LAST_MODIFIED,
            INFO,
            f"{len(affected)} of {len(selected)} selected resources have no usable "
            f"last_modified; they are versioned by content hash (ADR 0003)",
            affected_resource_count=len(affected),
            selected_resource_count=len(selected),
            resource_ids=sorted(affected),
        )
    ]


def check_archived(ctx: Context) -> list:
    """WARN: "archived" (any case) in a name or description.

    Looks at the dataset (name, title, description) and at each selected
    resource (name, description). One finding per match.
    """
    findings = []
    dataset_fields = _matched_fields(
        ctx.metadata, ("name", "title", "notes"), _ARCHIVED
    )
    if dataset_fields:
        findings.append(
            ctx.finding(
                ARCHIVED,
                WARN,
                "dataset name or description says it is archived",
                matched_in=dataset_fields,
            )
        )
    for res in ctx.selected:
        fields = _matched_fields(res, ("name", "description"), _ARCHIVED)
        if fields:
            findings.append(
                ctx.finding(
                    ARCHIVED,
                    WARN,
                    f"resource name or description says it is archived: "
                    f"{ci._resource_name(res)}",
                    res["id"],
                    matched_in=fields,
                )
            )
    return findings


def check_rounded_values(
    ctx: Context,
    snapshot: list,
    read_head: Optional[Callable[[str], bytes]] = None,
    landing_dir: Optional[Path] = None,
) -> list:
    """INFO: "rounded" (any case) in a resource name, description or file head.

    IRCC marks tables "[rounded - not for calculations]"; silver and gold must
    then use the published totals, not sums of cells. Matches are looked for in
    the resource name, the resource description and, for landed CSV files, the
    first bytes of the file (read_head). details_json records where each
    resource matched: "name", "description" or "file_head".

    One finding per dataset. A file that cannot be read is skipped and counted.
    Without read_head or landing_dir only the name and description are checked.
    """
    landed = {r["resource_id"]: r["landed_path"] for r in snapshot if r["landed_path"]}
    matches, unreadable, files_read = {}, 0, 0
    selected = ctx.selected
    for res in selected:
        where = _matched_fields(res, ("name", "description"), _ROUNDED)
        path = landed.get(res["id"])
        is_csv = str(res.get("format", "")).upper() == "CSV"
        if read_head and landing_dir is not None and path and is_csv:
            try:
                head = read_head(str(Path(landing_dir) / path))
            except (OSError, ValueError):
                unreadable += 1
            else:
                files_read += 1
                if _ROUNDED.search(head.decode("utf-8", errors="replace")):
                    where.append("file_head")
        if where:
            matches[res["id"]] = where
    if not matches:
        return []
    return [
        ctx.finding(
            ROUNDED_VALUES,
            INFO,
            f"{len(matches)} of {len(selected)} selected resources are marked "
            f"rounded; do not sum their cells",
            affected_resource_count=len(matches),
            selected_resource_count=len(selected),
            resource_ids=sorted(matches),
            matched_in={rid: matches[rid] for rid in sorted(matches)},
            files_read=files_read,
            files_unreadable=unreadable,
        )
    ]


# ---------------------------------------------------------------------------
# Running the checks
# ---------------------------------------------------------------------------


def run_checks(
    run_id: str,
    checked_at: datetime,
    specs: Iterable,
    metadata_by_key: dict,
    run_logs_by_key: dict,
    previous_rows_by_key: Optional[dict] = None,
    read_head: Optional[Callable[[str], bytes]] = None,
    landing_root: Optional[str] = None,
) -> list:
    """Run every check for every spec that has a run log; return the findings.

    A spec without a run log took no part in the run and is skipped. A spec
    whose metadata is missing (never fetched, no cache) gets only the checks
    that need the run log. previous_rows_by_key maps a dataset key to the rows
    of its previous snapshot; a key without rows is treated as a first run.
    The result is sorted, so the output is stable.
    """
    previous_rows_by_key = previous_rows_by_key or {}
    findings = []
    for spec in specs:
        run_log = run_logs_by_key.get(spec.key)
        if run_log is None:
            continue
        metadata = metadata_by_key.get(spec.key)
        ctx = Context(run_id, checked_at, spec, metadata or {}, run_log)
        findings += check_download_failed(ctx)
        findings += check_metadata_stale(ctx)
        if metadata is None:
            continue

        snapshot = build_resource_snapshot(run_id, checked_at, spec, metadata, run_log)
        previous = previous_rows_by_key.get(spec.key) or []
        landing_dir = (
            ci.dataset_landing_dir(ci.RunSettings(landing_root, ""), spec)
            if landing_root
            else None
        )
        findings += check_resource_removed(ctx, snapshot, previous)
        findings += check_resource_added(ctx, snapshot, previous)
        findings += check_format_gap(ctx)
        findings += check_missing_last_modified(ctx)
        findings += check_archived(ctx)
        findings += check_rounded_values(ctx, snapshot, read_head, landing_dir)

    order = {s: i for i, s in enumerate(SEVERITIES)}
    return sorted(
        findings,
        key=lambda f: (
            order[f.severity],
            f.dataset_key,
            f.check_name,
            f.resource_id or "",
        ),
    )


# ---------------------------------------------------------------------------
# Summary and report
# ---------------------------------------------------------------------------


def summarise(
    run_id: str,
    checked_at: datetime,
    findings: Iterable,
    datasets_checked: Iterable,
    datasets_skipped: Iterable = (),
) -> dict:
    """Counts by severity and by check, plus which datasets were or were not checked."""
    findings = list(findings)
    by_severity = {s: 0 for s in SEVERITIES}
    by_severity.update(Counter(f.severity for f in findings))
    return {
        "run_id": run_id,
        "checked_at": checked_at.isoformat(),
        "datasets_checked": sorted(datasets_checked),
        "datasets_skipped": sorted(datasets_skipped),
        "by_severity": by_severity,
        "by_check": dict(sorted(Counter(f.check_name for f in findings).items())),
    }


def summary_lines(summary: dict) -> list:
    """The summary as plain lines, for a job log."""
    sev = summary["by_severity"]
    lines = [
        f"Run {summary['run_id']}: {len(summary['datasets_checked'])} datasets checked, "
        f"{len(summary['datasets_skipped'])} without a run log",
        "Findings by severity: " + ", ".join(f"{s}={sev[s]}" for s in SEVERITIES),
    ]
    lines += [f"  {name}: {count}" for name, count in summary["by_check"].items()]
    return lines


def render_markdown(findings: Iterable, summary: dict) -> str:
    """A Markdown report: summary, then findings by severity, then notes."""
    findings = list(findings)
    sev = summary["by_severity"]
    out = [
        f"# Ingestion checks: run {summary['run_id']}",
        "",
        f"Checked at {summary['checked_at']}.",
        "",
        "## Summary",
        "",
        f"- Datasets checked: {len(summary['datasets_checked'])}",
    ]
    if summary["datasets_skipped"]:
        out.append(
            "- Datasets without a run log (not checked): "
            + ", ".join(f"`{k}`" for k in summary["datasets_skipped"])
        )
    out.append("- Findings: " + ", ".join(f"{s} {sev[s]}" for s in SEVERITIES))
    if summary["by_check"]:
        out.append("")
        out += [f"  - `{name}`: {count}" for name, count in summary["by_check"].items()]

    for severity in SEVERITIES:
        group = [f for f in findings if f.severity == severity]
        out += ["", f"## {severity} ({len(group)})", ""]
        if not group:
            out.append("None.")
            continue
        for f in group:
            where = f"`{f.dataset_key}`" + (f" / `{f.resource_id}`" if f.resource_id else "")
            out.append(f"- **{f.check_name}** {where}: {f.message}")

    out += ["", "## Notes", "", f"- {FORMAT_GAP_NOTE}", ""]
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Files, names and SQL fragments (used by the notebook)
# ---------------------------------------------------------------------------


def validate_run_id(run_id: str) -> str:
    """Reject anything that is not a plain run id (it ends up in SQL and paths)."""
    if not _RUN_ID.match(run_id or ""):
        raise ValueError(f"invalid run id {run_id!r}; must match {_RUN_ID.pattern}")
    return run_id


def replace_where_predicate(run_id: str, dataset_keys: Iterable) -> str:
    """Delta replaceWhere predicate for one run and some datasets.

    Writing with this predicate replaces exactly those rows, so checking the
    same run again does not duplicate them.
    """
    keys = sorted(set(dataset_keys))
    if not keys:
        raise ValueError("at least one dataset key is required")
    for key in keys:
        if not ci._SAFE_NAME.match(key):
            raise ValueError(f"invalid dataset key {key!r}")
    quoted = ", ".join(f"'{k}'" for k in keys)
    return f"run_id = '{validate_run_id(run_id)}' AND dataset_key IN ({quoted})"


def qualified_table(catalog: str, table: str) -> str:
    """`catalog`.`metadata`.`table`, after checking both names."""
    for name in (catalog, table):
        if not _SAFE_NAME.match(name or ""):
            raise ValueError(f"invalid name {name!r}; must match {_SAFE_NAME.pattern}")
    return f"`{catalog}`.`metadata`.`{table}`"


def _meta_dir(meta_root: str, spec: ci.DatasetSpec) -> Path:
    return ci.dataset_meta_dir(ci.RunSettings("", meta_root), spec)


def load_metadata(meta_root: str, spec: ci.DatasetSpec) -> Optional[dict]:
    """The cached package_show result of a dataset, or None if there is none."""
    path = _meta_dir(meta_root, spec) / "package_show.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return None


def list_run_ids(meta_root: str, spec: ci.DatasetSpec) -> list:
    """Run ids that have a run log for this dataset, oldest first."""
    runs = _meta_dir(meta_root, spec) / "runs"
    return sorted(p.stem for p in runs.glob("*.json")) if runs.exists() else []


def latest_run_id(meta_root: str, specs: Iterable) -> Optional[str]:
    """The newest run id across the specs' run logs, or None."""
    ids = [rid for spec in specs for rid in list_run_ids(meta_root, spec)]
    return max(ids) if ids else None


def load_run_log(meta_root: str, spec: ci.DatasetSpec, run_id: str) -> Optional[dict]:
    """The run log of one dataset and run, or None if the dataset was not in it."""
    path = _meta_dir(meta_root, spec) / "runs" / f"{validate_run_id(run_id)}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def report_path(meta_root: str, run_id: str) -> Path:
    """<meta_root>/reports/<run_id>.md"""
    return Path(meta_root) / "reports" / f"{validate_run_id(run_id)}.md"


def file_head_reader(size: int = HEAD_BYTES) -> Callable[[str], bytes]:
    """A function that returns the first `size` bytes of a file."""

    def read_head(path: str) -> bytes:
        with open(path, "rb") as fh:
            return fh.read(size)

    return read_head
