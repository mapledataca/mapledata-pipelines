"""
ckan_discover.py

Helpers to find candidate datasets on a CKAN portal, record what was found as
snapshot rows, and draft registry entries for config/datasets.yml. Used by the
tools/discover_datasets notebook.

Nothing here writes to the registry: drafts are text for a human to review and
paste. Network access goes through an injected session, like ckan_ingest.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime
from typing import Any, Callable, Iterable, Optional

import ckan_ingest as ci

#: Longest registry key the drafts will generate.
MAX_KEY_LENGTH = 50

#: Largest `rows` CKAN 2.12 documents for package_search (portals may configure
#: less); bigger pages are rejected before any request is sent.
MAX_PAGE_SIZE = 1000

#: CKAN organization names: lower-case letters, digits, "-" and "_". Checked
#: before a name goes into a Solr filter query.
_ORGANIZATION_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")

#: Prefix of the search_queries entry recorded for an organization search.
ORGANIZATION_LABEL = "org:"

#: Sent with every paged package_search. A total order makes `start` paging
#: stable; the default (score, then modified) reshuffles as datasets change.
SEARCH_SORT = "name asc"

#: Columns of mapledata.metadata.ckan_dataset_snapshot, in order. The table is
#: created by catalog_setup.py, which keeps its own copy of this list; a test
#: checks that the two agree.
SNAPSHOT_COLUMNS = (
    ("run_id", "STRING"),
    ("discovered_at", "TIMESTAMP"),
    ("base_url", "STRING"),
    ("search_queries", "ARRAY<STRING>"),
    ("ckan_id", "STRING"),
    ("name", "STRING"),
    ("title", "STRING"),
    ("organization_name", "STRING"),
    ("organization_title", "STRING"),
    ("formats", "ARRAY<STRING>"),
    ("resource_count", "INT"),
    ("frequency", "STRING"),
    ("coverage_start", "DATE"),
    ("coverage_end", "DATE"),
    ("metadata_modified", "STRING"),
    ("is_registered", "BOOLEAN"),
)

#: The same columns as a Spark DDL schema string, for spark.createDataFrame.
SNAPSHOT_SCHEMA_DDL = ", ".join(f"{name} {kind}" for name, kind in SNAPSHOT_COLUMNS)


# ---------------------------------------------------------------------------
# Search (paginated)
# ---------------------------------------------------------------------------


def _search_page(
    session: Any, params: dict, base_url: str, rows: int, start: int, timeout: tuple
) -> tuple:
    """Fetch one package_search page; return (total_count, packages).

    params holds the search itself (q and/or fq); the paging and ordering
    parameters are added here so every paged request carries them.
    """
    response = session.get(
        base_url.rstrip("/") + "/package_search",
        params={
            **params,
            "rows": rows,
            "start": start,
            "sort": SEARCH_SORT,
            # Negative = no limit. Inert until facets are requested, but
            # harmless; the documented default of 50 would truncate them.
            "facet.limit": -1,
        },
        timeout=timeout,
    )
    if response.status_code != 200:
        raise ci.UpstreamError(
            f"package_search returned HTTP {response.status_code}: "
            f"{response.text[:200]!r}"
        )
    if "json" not in ci._content_type(response):
        raise ci.UpstreamError(
            f"package_search returned non-JSON content-type "
            f"{ci._content_type(response)!r}: {response.text[:200]!r}"
        )

    payload = response.json()
    result = payload.get("result") or {}
    results, count = result.get("results"), result.get("count")
    if not payload.get("success") or not isinstance(results, list) or not isinstance(count, int):
        raise ci.UpstreamError(f"CKAN reported failure: {str(payload)[:200]}")
    return count, results


def _search_all(
    session: Any,
    params: dict,
    base_url: str,
    page_size: int,
    timeout: tuple,
    pause_seconds: float,
    sleep: Callable[[float], None],
) -> list:
    """Page through one package_search (params = its q and/or fq)."""
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    packages, seen, start = [], set(), 0
    while True:
        count, page = _search_page(session, params, base_url, page_size, start, timeout)
        for package in page:
            if package.get("id") not in seen:
                seen.add(package.get("id"))
                packages.append(package)
        start += len(page)
        if not page or start >= count:
            return packages
        sleep(pause_seconds)


def search_packages(
    session: Any,
    query: str,
    base_url: str = ci.DEFAULT_BASE_URL,
    page_size: int = 100,
    timeout: tuple = (10, 60),
    pause_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list:
    """Return every package matching the keyword query, fetching all pages.

    Pages advance by the number of packages actually returned, so a server that
    caps `rows` below page_size is still read to the end. An empty page stops
    the loop. Packages seen twice (the index can shift between pages) are kept
    once.

    Raises:
        ValueError: page_size is outside 1..MAX_PAGE_SIZE.
        ci.UpstreamError: non-200 status, non-JSON body or success=false.
        requests.RequestException: network-level failures.
    """
    return _search_all(
        session, {"q": query}, base_url, page_size, timeout, pause_seconds, sleep
    )


def search_organization(
    session: Any,
    organization: str,
    base_url: str = ci.DEFAULT_BASE_URL,
    page_size: int = 100,
    timeout: tuple = (10, 60),
    pause_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list:
    """Return every package of one organization, fetching all pages.

    Filters with fq=organization:<name> and sends no q. organization_show is
    not used: it returns only the first 10 datasets. Paging and errors work as
    in search_packages.

    Raises:
        ValueError: organization is not a valid CKAN name, or bad page_size.
    """
    if not _ORGANIZATION_NAME.match(organization):
        raise ValueError(f"invalid organization name {organization!r}")
    return _search_all(
        session,
        {"fq": f"organization:{organization}"},
        base_url,
        page_size,
        timeout,
        pause_seconds,
        sleep,
    )


def parse_queries(text: str) -> list:
    """Keywords from a comma-separated string.

    Whitespace is trimmed, empty items are dropped, and repeats (ignoring case)
    keep their first spelling and position. Commas cannot be escaped.
    """
    queries, seen = [], set()
    for item in text.split(","):
        item = item.strip()
        if item and item.lower() not in seen:
            seen.add(item.lower())
            queries.append(item)
    return queries


def parse_organizations(text: str) -> list:
    """Organization names from a comma-separated string, e.g. "cic, esdc-edsc".

    Whitespace is trimmed, empty items are dropped and repeats keep their first
    position. Names are CKAN organization names and are never altered:
    "CIC" is rejected rather than silently lower-cased.

    Raises:
        ValueError: an item is not a valid organization name.
    """
    names = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        if not _ORGANIZATION_NAME.match(item):
            raise ValueError(
                f"invalid organization name {item!r}: use lower-case letters, "
                f"digits, '-' and '_' (the name shown in the portal URL)"
            )
        if item not in names:
            names.append(item)
    return names


def search_many(
    session: Any,
    queries: Iterable = (),
    base_url: str = ci.DEFAULT_BASE_URL,
    page_size: int = 100,
    timeout: tuple = (10, 60),
    pause_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    organizations: Iterable = (),
) -> tuple:
    """Run every keyword search, then every organization search, and merge by ckan_id.

    Returns (packages, matches): packages holds each dataset once, with the
    content of the first search that found it; matches maps ckan_id to the
    searches that found it, in search order. A keyword is recorded as typed and
    an organization as "org:<name>".

    Any failing search raises, so a run never yields partial results.

    Raises:
        ValueError: neither keywords nor organizations given, or an invalid
            organization name or page_size.
        ci.UpstreamError, requests.RequestException: see search_packages.
    """
    queries, organizations = list(queries), list(organizations)
    if not queries and not organizations:
        raise ValueError("at least one search keyword or organization is required")

    searches = [(q, search_packages, q) for q in queries]
    searches += [(ORGANIZATION_LABEL + o, search_organization, o) for o in organizations]

    packages, matches = [], {}
    for index, (label, search, term) in enumerate(searches):
        if index:
            sleep(pause_seconds)
        for package in search(
            session, term, base_url, page_size, timeout, pause_seconds, sleep
        ):
            ckan_id = package.get("id")
            if ckan_id not in matches:
                matches[ckan_id] = []
                packages.append(package)
            if label not in matches[ckan_id]:
                matches[ckan_id].append(label)
    return packages, matches


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------


def package_title(package: dict) -> str:
    """English title if available (CKAN may store titles per language)."""
    translated = package.get("title_translated") or {}
    return translated.get("en") or package.get("title") or package.get("name") or ""


def package_formats(package: dict) -> list:
    """Sorted, upper-case, unique formats of the package's resources."""
    return sorted(
        {
            str(res.get("format", "")).upper()
            for res in package.get("resources", [])
            if res.get("format")
        }
    )


def organization_fields(package: dict) -> tuple:
    """Return (organization_name, organization_title).

    open.canada.ca titles are bilingual ("English | Francais"); only the
    English part is kept.
    """
    organization = package.get("organization") or {}
    title = organization.get("title") or ""
    return organization.get("name") or "", title.split("|")[0].strip()


def parse_date(value: Any) -> Optional[date]:
    """Date from a CKAN timestamp such as "2015-01-01 00:00:00", else None."""
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def slugify_key(title: str) -> str:
    """Folder-safe registry key (lower-case snake_case) drawn from a title."""
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
    slug = slug[:MAX_KEY_LENGTH].rstrip("_")
    return slug if slug and slug[0].isalnum() else "dataset"


# ---------------------------------------------------------------------------
# Registry cross-check
# ---------------------------------------------------------------------------


def registered_ids(specs: Iterable, base_url: str) -> set:
    """ckan_id values of every registry entry (any status) for this portal."""
    wanted = ci.normalize_base_url(base_url)
    return {s.ckan_id for s in specs if ci.normalize_base_url(s.base_url) == wanted}


def is_registered(package: dict, registered: set) -> bool:
    """True when the registry's ckan_id is the package's id or its name."""
    return package.get("id") in registered or package.get("name") in registered


# ---------------------------------------------------------------------------
# Snapshot rows
# ---------------------------------------------------------------------------


def build_snapshot_rows(
    packages: Iterable,
    specs: Iterable,
    base_url: str,
    matches: dict,
    discovered_at: datetime,
    run_id: str,
) -> list:
    """One dict per package, keyed by SNAPSHOT_COLUMNS.

    matches maps ckan_id to the keywords that found the package (see
    search_many). Every row of a call shares discovered_at and run_id.
    """
    registered = registered_ids(specs, base_url)
    rows = []
    for package in packages:
        org_name, org_title = organization_fields(package)
        rows.append(
            {
                "run_id": run_id,
                "discovered_at": discovered_at,
                "base_url": base_url,
                "search_queries": list(matches.get(package.get("id"), [])),
                "ckan_id": package.get("id", ""),
                "name": package.get("name", ""),
                "title": package_title(package),
                "organization_name": org_name,
                "organization_title": org_title,
                "formats": package_formats(package),
                "resource_count": len(package.get("resources", [])),
                "frequency": package.get("frequency"),
                "coverage_start": parse_date(package.get("time_period_coverage_start")),
                "coverage_end": parse_date(package.get("time_period_coverage_end")),
                "metadata_modified": package.get("metadata_modified"),
                "is_registered": is_registered(package, registered),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Registry drafts
# ---------------------------------------------------------------------------


def registry_entry_yaml(package: dict, domain: str) -> str:
    """Draft a datasets.yml entry for a package.

    update_pattern cannot be inferred from metadata, so the draft uses
    new_file_per_period and flags it for review. Check the publisher's
    behaviour (are files replaced in place?) before making the dataset active.
    The draft is a candidate with a placeholder purpose to fill in.
    """
    formats = ", ".join(package_formats(package))
    lines = [
        f"  - key: {slugify_key(package_title(package))}",
        f"    domain: {domain}",
        f"    ckan_id: {package['id']}",
        "    update_pattern: new_file_per_period  # REVIEW: or overwrite_in_place",
    ]
    if formats:
        lines.append(f"    formats: [{formats}]")
    lines.append("    status: candidate  # active = ingested; set after reviewing")
    lines.append('    purpose: "TODO: why this dataset, where it will be used"')
    # A JSON string is also a valid YAML double-quoted scalar.
    lines.append(
        f"    description: {json.dumps(package_title(package), ensure_ascii=False)}"
    )
    return "\n".join(lines)


def unregistered_drafts(
    packages: Iterable, registered: set, domain: str, max_drafts: int
) -> tuple:
    """Return (draft YAML strings, number of unregistered packages left out).

    Only packages missing from the registry get a draft; max_drafts caps the
    output (0 prints none).
    """
    pending = [p for p in packages if not is_registered(p, registered)]
    shown = pending[: max(max_drafts, 0)]
    return [registry_entry_yaml(p, domain) for p in shown], len(pending) - len(shown)
