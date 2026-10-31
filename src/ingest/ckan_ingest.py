"""
ckan_ingest.py

Generic, registry-driven ingestion of files published on CKAN portals
(open.canada.ca and any other CKAN site) into a landing area, for example a
Unity Catalog volume such as /Volumes/mapledata/landing/raw.

This module generalises the earlier lmia_ingest.py. Adding a new dataset is a
configuration change (one entry in config/datasets.yml), not a code change.

Landing layout
--------------
    <landing_root>/<source>/<dataset_key>/...           raw data files
    <meta_root>/<source>/<dataset_key>/package_show.json cached metadata
    <meta_root>/<source>/<dataset_key>/runs/<run_id>.json run log

Keep meta_root OUTSIDE landing_root (e.g. separate volumes landing.raw and landing.meta) so Auto Loader never reads metadata.

Update patterns
---------------
new_file_per_period
    The publisher adds a new file each period (e.g. LMIA, quarterly).
    File name: <resource_id>_<basename>. A file that already exists is
    skipped without downloading.

overwrite_in_place
    The publisher replaces the same file each period, often revising history
    (e.g. IRCC monthly updates). Every distinct version is kept as a snapshot:
        <resource_id>/<version>_<basename>
    <version> comes from the resource's last_modified (or metadata_modified).
    If neither is present, the file is downloaded and a content hash
    ("sha256-<12 hex>") is used instead, so identical content is never stored
    twice. Downstream, the snapshots can feed AUTO CDC FROM SNAPSHOT.

Design goals (unchanged from lmia_ingest.py)
-------------------------------------------
Idempotent, resilient (retries + metadata cache fallback), safe (atomic
writes, HTML error pages rejected) and testable (network access goes through
an injected session object).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default CKAN Action API base URL for open.canada.ca.
DEFAULT_BASE_URL = "https://open.canada.ca/data/api/action"

#: Allowed values for DatasetSpec.update_pattern.
NEW_FILE = "new_file_per_period"
OVERWRITE = "overwrite_in_place"
UPDATE_PATTERNS = (NEW_FILE, OVERWRITE)

#: Allowed values for DatasetSpec.status. Only ACTIVE entries are ingested.
STATUS_CANDIDATE = "candidate"
STATUS_ACTIVE = "active"
STATUS_DEPRECATED = "deprecated"
STATUSES = (STATUS_CANDIDATE, STATUS_ACTIVE, STATUS_DEPRECATED)

#: Prefix for temporary files. Spark file sources (including Auto Loader)
#: ignore names starting with "_" or ".", so partial downloads stay invisible.
TMP_PREFIX = "_tmp_"

#: HTTP status codes worth retrying (throttling and server-side errors).
RETRYABLE_STATUS = (429, 500, 502, 503, 504)

#: Keys every registry entry must define.
REQUIRED_KEYS = ("key", "domain", "ckan_id", "update_pattern", "status", "purpose")

#: Registry keys become folder names, so restrict them to safe characters.
_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_]*$")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class RegistryError(ValueError):
    """The dataset registry (datasets.yml) is invalid."""


class UpstreamError(RuntimeError):
    """The upstream responded with something we must not trust or store
    (non-200 status, HTML error page, empty body, CKAN success=false, ...)."""


# ---------------------------------------------------------------------------
# Configuration objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetSpec:
    """One dataset entry from the registry.

    Attributes:
        key:            Unique, folder-safe name, e.g. "lmia_positive_employers".
        domain:         Business domain, e.g. "immigration", "housing".
        ckan_id:        CKAN dataset id (or name) on the portal.
        update_pattern: NEW_FILE or OVERWRITE (see module docstring).
        status:         STATUS_CANDIDATE (registered, not ingested),
                        STATUS_ACTIVE (ingested) or STATUS_DEPRECATED
                        (kept for the record, not ingested).
        purpose:        Why the dataset is wanted and where it will be used.
        source:        Folder-safe name of the portal, e.g. "open_canada".
        base_url:       CKAN Action API base URL (".../api/action").
        languages:      Keep resources tagged with any of these languages.
        formats:        Optional allow-list of CKAN formats (upper-case),
                        e.g. ("CSV", "XLSX"). None keeps every format.
        name_regex:     Optional regex; keep resources whose English name
                        matches it (case-insensitive search).
        description:    Free text for humans.
    """

    key: str
    domain: str
    ckan_id: str
    update_pattern: str
    status: str
    purpose: str
    source: str = "open_canada"
    base_url: str = DEFAULT_BASE_URL
    languages: tuple = ("en",)
    formats: Optional[tuple] = None
    name_regex: Optional[str] = None
    description: str = ""

    @property
    def package_show_url(self) -> str:
        """Full URL of the CKAN package_show action for this portal."""
        return self.base_url.rstrip("/") + "/package_show"


@dataclass(frozen=True)
class RunSettings:
    """Settings shared by every dataset in one run.

    Attributes:
        landing_root:  Root folder for raw files.
        meta_root:     Root folder for metadata cache and run logs.
        timeout:       (connect_timeout, read_timeout) in seconds.
        pause_seconds: Pause between file downloads, to be polite to the server.
    """

    landing_root: str
    meta_root: str
    timeout: tuple = (10, 300)
    pause_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


@dataclass
class DatasetReport:
    """Outcome for one dataset."""

    key: str
    metadata_source: str = ""  # "fresh", "cache", or "" if metadata failed
    downloaded: list = field(default_factory=list)  # relative paths written
    skipped: list = field(default_factory=list)  # relative paths already present
    failed: list = field(default_factory=list)  # (resource id, error message)
    error: str = ""  # dataset-level failure (e.g. metadata unavailable)

    @property
    def ok(self) -> bool:
        """True when neither the dataset nor any of its resources failed."""
        return not self.error and not self.failed


@dataclass
class RunReport:
    """Outcome for one run across all selected datasets."""

    run_id: str
    datasets: list = field(default_factory=list)  # list of DatasetReport

    @property
    def ok(self) -> bool:
        return all(d.ok for d in self.datasets)

    def summary(self) -> str:
        """One line per dataset, convenient for job logs."""
        lines = []
        for d in self.datasets:
            status = "OK  " if d.ok else "FAIL"
            lines.append(
                f"{status} {d.key}: meta={d.metadata_source or '-'} "
                f"downloaded={len(d.downloaded)} skipped={len(d.skipped)} "
                f"failed={len(d.failed)}" + (f" error={d.error}" if d.error else "")
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Registry loading and validation
# ---------------------------------------------------------------------------


def parse_registry(raw: dict) -> list:
    """Validate a registry dict (the parsed YAML) and return DatasetSpec objects.

    Expected shape:
        defaults: {source: ..., base_url: ..., languages: [...]}   # optional
        datasets: [ {key: ..., domain: ..., ckan_id: ..., ...}, ... ]

    Raises:
        RegistryError: on missing keys, unknown keys, bad values or duplicates.
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("datasets"), list):
        raise RegistryError("registry must contain a 'datasets' list")

    defaults = raw.get("defaults") or {}
    allowed = set(DatasetSpec.__dataclass_fields__)
    specs, seen, seen_ids = [], set(), set()

    for index, entry in enumerate(raw["datasets"]):
        if not isinstance(entry, dict):
            raise RegistryError(f"datasets[{index}] must be a mapping")

        merged = {**defaults, **entry}  # entry values override defaults
        where = f"datasets[{index}] ({merged.get('key', '?')})"

        # Say what to write instead of reporting a generic unknown key.
        if "enabled" in merged:
            raise RegistryError(
                f"{where}: 'enabled' was replaced by 'status'; use "
                f"'status: active' (or 'candidate' / 'deprecated')"
            )

        missing = [k for k in REQUIRED_KEYS if not merged.get(k)]
        if missing:
            raise RegistryError(f"{where}: missing required keys {missing}")

        unknown = set(merged) - allowed
        if unknown:
            raise RegistryError(f"{where}: unknown keys {sorted(unknown)}")

        for name_field in ("key", "source", "domain"):
            value = str(merged.get(name_field, "open_canada"))
            if not _SAFE_NAME.match(value):
                raise RegistryError(
                    f"{where}: {name_field} '{value}' must match {_SAFE_NAME.pattern}"
                )

        if merged["update_pattern"] not in UPDATE_PATTERNS:
            raise RegistryError(
                f"{where}: update_pattern must be one of {UPDATE_PATTERNS}"
            )

        if merged["status"] not in STATUSES:
            raise RegistryError(f"{where}: status must be one of {STATUSES}")

        if not isinstance(merged["purpose"], str) or not merged["purpose"].strip():
            raise RegistryError(f"{where}: purpose must be a non-empty string")
        merged["purpose"] = merged["purpose"].strip()
        # Discovery drafts carry a "TODO" purpose; it must be replaced before
        # an entry can be ingested. Candidates and deprecated entries may keep it.
        if merged["status"] == STATUS_ACTIVE and merged["purpose"].startswith("TODO"):
            raise RegistryError(
                f"{where}: an active entry needs a real purpose, not a TODO placeholder"
            )

        if merged["key"] in seen:
            raise RegistryError(f"{where}: duplicate key '{merged['key']}'")
        seen.add(merged["key"])

        # The same dataset registered twice would be ingested twice.
        portal_id = (
            str(merged.get("base_url", DEFAULT_BASE_URL)).rstrip("/"),
            merged["ckan_id"],
        )
        if portal_id in seen_ids:
            raise RegistryError(f"{where}: duplicate ckan_id '{merged['ckan_id']}'")
        seen_ids.add(portal_id)

        if merged.get("name_regex"):
            try:
                re.compile(merged["name_regex"])
            except re.error as exc:
                raise RegistryError(f"{where}: invalid name_regex: {exc}") from exc

        # Normalise list values to tuples so the frozen dataclass stays hashable.
        if "languages" in merged:
            merged["languages"] = tuple(merged["languages"])
        if merged.get("formats"):
            merged["formats"] = tuple(str(f).upper() for f in merged["formats"])

        specs.append(DatasetSpec(**merged))

    return specs


def load_registry(path: str) -> list:
    """Read and validate a YAML registry file (see config/datasets.yml)."""
    import yaml  # imported here so the rest of the module works without PyYAML

    with open(path, "r", encoding="utf-8") as fh:
        return parse_registry(yaml.safe_load(fh))


def select_specs(
    specs: Iterable, domains: Optional[Iterable] = None, keys: Optional[Iterable] = None
) -> list:
    """Return active specs, optionally restricted to some domains and/or keys."""
    domains = set(domains) if domains else None
    keys = set(keys) if keys else None
    return [
        s
        for s in specs
        if s.status == STATUS_ACTIVE
        and (domains is None or s.domain in domains)
        and (keys is None or s.key in keys)
    ]


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def build_session(
    user_agent: str, total_retries: int = 5, backoff_factor: float = 5.0
) -> requests.Session:
    """Create a requests.Session with automatic retries and a clear User-Agent.

    raise_on_status=False returns the last response (e.g. a 504) once retries
    are exhausted; our own validation then raises an UpstreamError with the
    real status code, which is clearer than urllib3's MaxRetryError.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRYABLE_STATUS,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": user_agent})
    return session


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _content_type(response: Any) -> str:
    """Lower-cased Content-Type header, or "" if missing."""
    return (response.headers.get("content-type") or "").lower()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write via a hidden temp file + rename, so readers never see partial files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(TMP_PREFIX + path.name)
    with open(tmp_path, "wb") as fh:
        fh.write(data)
    os.replace(tmp_path, path)


def _safe_token(value: str) -> str:
    """Keep only characters that are safe in a file name."""
    return re.sub(r"[^A-Za-z0-9]", "", value)


def dataset_landing_dir(settings: RunSettings, spec: DatasetSpec) -> Path:
    """<landing_root>/<source>/<key>"""
    return Path(settings.landing_root) / spec.source / spec.key


def dataset_meta_dir(settings: RunSettings, spec: DatasetSpec) -> Path:
    """<meta_root>/<source>/<key>"""
    return Path(settings.meta_root) / spec.source / spec.key


def completed_files(root: Path) -> set:
    """Relative POSIX paths of all completed files under root (temp files excluded)."""
    if not root.exists():
        return set()
    found = set()
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if not name.startswith(TMP_PREFIX):
                found.add((Path(dirpath) / name).relative_to(root).as_posix())
    return found


# ---------------------------------------------------------------------------
# Metadata (CKAN package_show) with cache fallback
# ---------------------------------------------------------------------------


def fetch_metadata(session: Any, spec: DatasetSpec, timeout: tuple) -> dict:
    """Call CKAN package_show and return the validated `result` object.

    Raises:
        UpstreamError: non-200 status, non-JSON body, success=false,
                       or a result without a "resources" list.
        requests.RequestException: network-level failures.
    """
    response = session.get(
        spec.package_show_url, params={"id": spec.ckan_id}, timeout=timeout
    )

    # A 504 HTML page would otherwise fail later with a confusing JSONDecodeError.
    if response.status_code != 200:
        raise UpstreamError(
            f"package_show returned HTTP {response.status_code}: "
            f"{response.text[:200]!r}"
        )
    if "json" not in _content_type(response):
        raise UpstreamError(
            f"package_show returned non-JSON content-type "
            f"{_content_type(response)!r}: {response.text[:200]!r}"
        )

    payload = response.json()
    if not payload.get("success") or "result" not in payload:
        raise UpstreamError(f"CKAN reported failure: {str(payload)[:200]}")

    result = payload["result"]
    if not isinstance(result.get("resources"), list):
        raise UpstreamError("metadata has no 'resources' list")
    return result


def get_metadata(session: Any, spec: DatasetSpec, settings: RunSettings) -> tuple:
    """Return (metadata, source) where source is "fresh" or "cache".

    On success the cache is refreshed. On failure the last good copy is used;
    with no cache yet (first run), an UpstreamError is raised.
    """
    cache = dataset_meta_dir(settings, spec) / "package_show.json"
    try:
        result = fetch_metadata(session, spec, settings.timeout)
    except (requests.RequestException, UpstreamError, ValueError) as exc:
        if cache.exists():
            return json.loads(cache.read_text(encoding="utf-8")), "cache"
        raise UpstreamError(
            f"metadata unavailable and no cache at {cache}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    _atomic_write_bytes(cache, json.dumps(result).encode("utf-8"))
    return result, "fresh"


# ---------------------------------------------------------------------------
# Resource selection and naming
# ---------------------------------------------------------------------------


def _resource_name(resource: dict) -> str:
    """English resource name if available (CKAN may store names per language)."""
    translated = resource.get("name_translated") or {}
    return translated.get("en") or resource.get("name") or ""


def select_resources(result: dict, spec: DatasetSpec) -> list:
    """Return the resources to ingest for this spec, oldest first.

    Filters, all of which must pass:
        - the resource has a URL;
        - at least one of its languages is in spec.languages;
        - its format is in spec.formats (when formats is set);
        - its English name matches spec.name_regex (when set).
    """
    pattern = re.compile(spec.name_regex, re.IGNORECASE) if spec.name_regex else None
    wanted_languages = set(spec.languages)
    selected = []

    for res in result.get("resources", []):
        if not res.get("url"):
            continue
        if not set(res.get("language") or []) & wanted_languages:
            continue
        if spec.formats and str(res.get("format", "")).upper() not in spec.formats:
            continue
        if pattern and not pattern.search(_resource_name(res)):
            continue
        selected.append(res)

    return sorted(selected, key=lambda r: r.get("created") or "")


def url_basename(resource: dict) -> str:
    """Original file name from the resource URL (decoded), or "download"."""
    return unquote(Path(urlparse(resource["url"]).path).name) or "download"


def resource_version(resource: dict) -> Optional[str]:
    """Version token from CKAN timestamps, e.g. "20251023T155027", or None.

    last_modified changes when the file is replaced; metadata_modified is a
    weaker fallback (it also changes when only the description is edited,
    which at worst stores one extra identical snapshot).
    """
    stamp = resource.get("last_modified") or resource.get("metadata_modified")
    if not stamp:
        return None
    # "2025-10-23T15:50:27.245365" -> "20251023T155027" (drop sub-seconds)
    return _safe_token(str(stamp).split(".")[0])


def target_relpath(resource: dict, spec: DatasetSpec, version: Optional[str]) -> str:
    """Relative path of the file inside the dataset's landing folder.

    NEW_FILE:  "<resource_id>_<basename>"
    OVERWRITE: "<resource_id>/<version>_<basename>"
    """
    if spec.update_pattern == NEW_FILE:
        return f"{resource['id']}_{url_basename(resource)}"
    if not version:
        raise ValueError("overwrite_in_place requires a version token")
    return f"{resource['id']}/{version}_{url_basename(resource)}"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def fetch_resource_bytes(session: Any, resource: dict, timeout: tuple) -> bytes:
    """Download one resource and return its bytes after validation.

    Raises:
        UpstreamError: non-200 status, HTML body (an error page), or empty body.
        requests.RequestException: network-level failures.
    """
    response = session.get(resource["url"], timeout=timeout)
    if response.status_code != 200:
        raise UpstreamError(f"HTTP {response.status_code} for {resource['url']}")
    if "text/html" in _content_type(response):
        raise UpstreamError(f"HTML payload (likely an error page) for {resource['url']}")
    if not response.content:
        raise UpstreamError(f"empty payload for {resource['url']}")
    return response.content


def ingest_resource(
    session: Any,
    resource: dict,
    spec: DatasetSpec,
    landing_dir: Path,
    present: set,
    timeout: tuple,
) -> tuple:
    """Ingest one resource. Returns ("downloaded" | "skipped", relative path).

    When the target path can be computed from metadata (NEW_FILE, or OVERWRITE
    with a timestamp), an existing file is skipped WITHOUT downloading.
    Without a timestamp, the file is downloaded and named by content hash, and
    is only written if that exact content is not already stored.
    """
    version = None
    if spec.update_pattern == OVERWRITE:
        version = resource_version(resource)

    if spec.update_pattern == NEW_FILE or version:
        relpath = target_relpath(resource, spec, version)
        if relpath in present:
            return "skipped", relpath
        data = fetch_resource_bytes(session, resource, timeout)
    else:
        data = fetch_resource_bytes(session, resource, timeout)
        version = "sha256-" + hashlib.sha256(data).hexdigest()[:12]
        relpath = target_relpath(resource, spec, version)
        if relpath in present:
            return "skipped", relpath

    _atomic_write_bytes(landing_dir / relpath, data)
    present.add(relpath)
    return "downloaded", relpath


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _utc_run_id(now: Optional[Callable] = None) -> str:
    """Run id such as "20261029T195300Z" (UTC)."""
    moment = (now or (lambda: datetime.now(timezone.utc)))()
    return moment.strftime("%Y%m%dT%H%M%SZ")


def ingest_dataset(
    session: Any, spec: DatasetSpec, settings: RunSettings, run_id: str
) -> DatasetReport:
    """Ingest every selected resource of one dataset; never raises.

    A metadata failure with no cache is recorded in report.error; a failure on
    one file is recorded in report.failed and does not stop the others.
    A JSON run log is written to <meta_root>/<source>/<key>/runs/<run_id>.json.
    """
    report = DatasetReport(key=spec.key)
    landing_dir = dataset_landing_dir(settings, spec)

    try:
        result, report.metadata_source = get_metadata(session, spec, settings)
    except UpstreamError as exc:
        report.error = str(exc)[:300]
        _write_run_log(settings, spec, run_id, report)
        return report

    present = completed_files(landing_dir)
    for index, resource in enumerate(select_resources(result, spec)):
        if index and settings.pause_seconds:
            time.sleep(settings.pause_seconds)
        try:
            outcome, relpath = ingest_resource(
                session, resource, spec, landing_dir, present, settings.timeout
            )
            (report.downloaded if outcome == "downloaded" else report.skipped).append(
                relpath
            )
        except (requests.RequestException, UpstreamError, OSError, ValueError) as exc:
            report.failed.append(
                (resource.get("id", "?"), f"{type(exc).__name__}: {exc}"[:300])
            )

    _write_run_log(settings, spec, run_id, report)
    return report


def _write_run_log(
    settings: RunSettings, spec: DatasetSpec, run_id: str, report: DatasetReport
) -> None:
    """Persist the dataset report as JSON for auditing and debugging."""
    log = {"run_id": run_id, "spec": asdict(spec), "report": asdict(report)}
    path = dataset_meta_dir(settings, spec) / "runs" / f"{run_id}.json"
    _atomic_write_bytes(path, json.dumps(log, indent=2).encode("utf-8"))


def run_ingestion(
    specs: Iterable,
    settings: RunSettings,
    session: Any,
    domains: Optional[Iterable] = None,
    keys: Optional[Iterable] = None,
    run_id: Optional[str] = None,
) -> RunReport:
    """Ingest all active specs (optionally filtered by domain/key).

    One failing dataset never stops the others; check RunReport.ok afterwards
    and fail the job if needed so that job-level retries can kick in.
    """
    report = RunReport(run_id=run_id or _utc_run_id())
    for spec in select_specs(specs, domains=domains, keys=keys):
        report.datasets.append(ingest_dataset(session, spec, settings, report.run_id))
    return report
