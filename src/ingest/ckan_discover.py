"""
ckan_discover.py

Helpers to find candidate datasets on a CKAN portal and draft a registry
entry for config/datasets.yml. Used by the tools/discover_datasets notebook.

Nothing here writes to the registry: the output is text for a human to review
and paste. Network access goes through an injected session, like ckan_ingest.
"""

from __future__ import annotations

import json
import re
from typing import Any

import ckan_ingest as ci

#: Longest registry key the drafts will generate.
MAX_KEY_LENGTH = 50


def search_packages(
    session: Any,
    query: str,
    base_url: str = ci.DEFAULT_BASE_URL,
    rows: int = 10,
    timeout: tuple = (10, 60),
) -> list:
    """Call CKAN package_search and return the list of matching packages.

    Raises:
        ci.UpstreamError: non-200 status, non-JSON body or success=false.
        requests.RequestException: network-level failures.
    """
    response = session.get(
        base_url.rstrip("/") + "/package_search",
        params={"q": query, "rows": rows},
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
    results = (payload.get("result") or {}).get("results")
    if not payload.get("success") or not isinstance(results, list):
        raise ci.UpstreamError(f"CKAN reported failure: {str(payload)[:200]}")
    return results


def package_title(package: dict) -> str:
    """English title if available (CKAN may store titles per language)."""
    translated = package.get("title_translated") or {}
    return translated.get("en") or package.get("title") or package.get("name") or ""


def package_formats(package: dict) -> tuple:
    """Sorted, upper-case formats of the package's resources."""
    formats = {
        str(res.get("format", "")).upper()
        for res in package.get("resources", [])
        if res.get("format")
    }
    return tuple(sorted(formats))


def slugify_key(title: str) -> str:
    """Folder-safe registry key (lower-case snake_case) drawn from a title."""
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
    slug = slug[:MAX_KEY_LENGTH].rstrip("_")
    return slug if slug and slug[0].isalnum() else "dataset"


def summarise_package(package: dict) -> dict:
    """The fields worth scanning when choosing a dataset."""
    organization = package.get("organization") or {}
    org_title = organization.get("title_translated") or {}
    return {
        "title": package_title(package),
        "ckan_id": package.get("id", ""),
        "organization": org_title.get("en") or organization.get("title") or "",
        "formats": package_formats(package),
        "resources": len(package.get("resources", [])),
        "metadata_modified": package.get("metadata_modified", ""),
    }


def registry_entry_yaml(package: dict, domain: str) -> str:
    """Draft a datasets.yml entry for a package.

    update_pattern cannot be inferred from metadata, so the draft uses
    new_file_per_period and flags it for review. Check the publisher's
    behaviour (are files replaced in place?) before enabling the dataset.
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
    lines.append("    enabled: false  # enable after the first successful run")
    # A JSON string is also a valid YAML double-quoted scalar.
    lines.append(
        f"    description: {json.dumps(package_title(package), ensure_ascii=False)}"
    )
    return "\n".join(lines)
