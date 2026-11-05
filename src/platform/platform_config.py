"""
platform_config.py

Loads and validates config/platform.yml and derives the names that follow from
it: schema names and volume paths. Pure Python, so it is unit-tested without
Spark. This module (and the config file) are the only places in src/ that spell
out the default catalog name and the /Volumes/ prefix.

Layout (see docs/decisions/0007-catalog-and-schema-layout.md):
    <catalog>.landing                  volumes raw and meta
    <catalog>.metadata                 platform metadata tables
    <catalog>.shared                   conformed dimensions
    <catalog>.bronze_<source>          one schema per registry source
    <catalog>.silver_<domain>          one schema per registry domain
    <catalog>.gold_<domain>            one schema per registry domain
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

#: Unity Catalog names used here: lower-case letters, digits, underscores.
#: Strict on purpose, because names are interpolated into SQL text.
SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")

LAYERS = ("bronze", "silver", "gold")

DEFAULT_CATALOG = "living_canada"
VOLUMES_ROOT = "/Volumes"


class PlatformConfigError(ValueError):
    """config/platform.yml is invalid."""


def check_identifier(name: str, what: str) -> str:
    """Reject anything that is not a plain identifier (also blocks SQL injection)."""
    if not isinstance(name, str) or not SAFE_NAME.match(name):
        raise ValueError(f"invalid {what} name {name!r}; must match {SAFE_NAME.pattern}")
    return name


@dataclass(frozen=True)
class PlatformConfig:
    """Names of the platform layout. Defaults are the documented layout."""

    catalog: str = DEFAULT_CATALOG
    landing_schema: str = "landing"
    raw_volume: str = "raw"
    meta_volume: str = "meta"
    metadata_schema: str = "metadata"
    shared_schema: str = "shared"
    layer_prefixes: dict = field(
        default_factory=lambda: {layer: layer for layer in LAYERS}
    )

    def bronze_schema(self, source: str) -> str:
        return f"{self.layer_prefixes['bronze']}_{check_identifier(source, 'source')}"

    def silver_schema(self, domain: str) -> str:
        return f"{self.layer_prefixes['silver']}_{check_identifier(domain, 'domain')}"

    def gold_schema(self, domain: str) -> str:
        return f"{self.layer_prefixes['gold']}_{check_identifier(domain, 'domain')}"

    def bronze_schemas(self, sources: Iterable[str]) -> list:
        """Bronze schema names, deduplicated and sorted by source."""
        return [self.bronze_schema(s) for s in sorted(set(sources))]

    def domain_schemas(self, domains: Iterable[str]) -> list:
        """Silver then gold schema names per domain, deduplicated, sorted by domain."""
        names = []
        for domain in sorted(set(domains)):
            names += [self.silver_schema(domain), self.gold_schema(domain)]
        return names

    def raw_volume_path(self, catalog: Optional[str] = None) -> str:
        """/Volumes/<catalog>/<landing schema>/<raw volume>"""
        return self._volume_path(catalog, self.raw_volume)

    def meta_volume_path(self, catalog: Optional[str] = None) -> str:
        """/Volumes/<catalog>/<landing schema>/<meta volume>"""
        return self._volume_path(catalog, self.meta_volume)

    def _volume_path(self, catalog: Optional[str], volume: str) -> str:
        c = check_identifier(catalog or self.catalog, "catalog")
        return f"{VOLUMES_ROOT}/{c}/{self.landing_schema}/{volume}"

    def resolve_catalog(self, widget_value: Optional[str]) -> str:
        """The catalog to use: a non-blank widget value overrides the config."""
        value = (widget_value or "").strip()
        return check_identifier(value or self.catalog, "catalog")


_NAME_FIELDS = (
    "catalog",
    "landing_schema",
    "raw_volume",
    "meta_volume",
    "metadata_schema",
    "shared_schema",
)


def parse_platform_config(raw: Optional[dict]) -> PlatformConfig:
    """Validate a parsed platform.yml and return a PlatformConfig.

    Raises PlatformConfigError on unknown keys, bad names or missing layers.
    """
    if not isinstance(raw, dict):
        raise PlatformConfigError("platform config must be a mapping")
    if not raw.get("catalog"):
        raise PlatformConfigError("platform config needs a 'catalog'")

    allowed = set(_NAME_FIELDS) | {"layer_prefixes"}
    unknown = set(raw) - allowed
    if unknown:
        raise PlatformConfigError(f"unknown keys {sorted(unknown)}")

    values = {}
    for name in _NAME_FIELDS:
        if name in raw:
            try:
                values[name] = check_identifier(raw[name], name)
            except ValueError as exc:
                raise PlatformConfigError(str(exc)) from exc

    if "layer_prefixes" in raw:
        prefixes = raw["layer_prefixes"]
        if not isinstance(prefixes, dict) or set(prefixes) != set(LAYERS):
            raise PlatformConfigError(f"layer_prefixes must have exactly the keys {LAYERS}")
        try:
            values["layer_prefixes"] = {
                k: check_identifier(v, f"layer prefix {k}") for k, v in prefixes.items()
            }
        except ValueError as exc:
            raise PlatformConfigError(str(exc)) from exc

    return PlatformConfig(**values)


def load_platform_config(path: str) -> PlatformConfig:
    """Read and validate a YAML platform file (see config/platform.yml)."""
    import yaml  # imported here so the rest of the module works without PyYAML

    with open(path, "r", encoding="utf-8") as fh:
        return parse_platform_config(yaml.safe_load(fh))
