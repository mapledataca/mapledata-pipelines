"""
catalog_setup.py

Builds the Unity Catalog structure for mapledata from zero, idempotently.
Every statement uses IF NOT EXISTS, so running it again is safe.

Structure created
-----------------
    <catalog>
    ├── landing                 schema for raw files
    │   ├── raw     (volume)    data files written by the ingester
    │   └── meta    (volume)    metadata cache and run logs
    ├── shared                  schema for cross-domain dimensions
    └── <domain>_bronze / _silver / _gold   one set per domain

The domains are read from config/datasets.yml, so adding a dataset with a new
domain and re-running the bootstrap creates that domain's schemas.

This module only BUILDS the SQL statements (plain strings), which keeps it
unit-testable without Spark. bootstrap_catalog.py executes them.
"""

from __future__ import annotations

import re
from typing import Iterable

#: Layers created for every domain (medallion architecture).
LAYERS = ("bronze", "silver", "gold")

#: Unity Catalog names used here: lower-case letters, digits, underscores.
#: Strict on purpose, because names are interpolated into SQL text.
_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def _check(name: str, what: str) -> str:
    """Reject anything that is not a plain identifier (also blocks SQL injection)."""
    if not _SAFE_NAME.match(name or ""):
        raise ValueError(f"invalid {what} name {name!r}; must match {_SAFE_NAME.pattern}")
    return name


def domain_schemas(domains: Iterable[str]) -> list:
    """Schema names for the given domains, e.g. ["immigration_bronze", ...].

    Duplicates are removed and the order is deterministic (sorted by domain).
    """
    return [f"{_check(d, 'domain')}_{layer}" for d in sorted(set(domains)) for layer in LAYERS]


def build_ddl(catalog: str, domains: Iterable[str]) -> list:
    """Return the ordered list of idempotent DDL statements.

    Order matters: catalog, then schemas, then volumes inside schemas.
    """
    c = _check(catalog, "catalog")
    statements = [
        f"CREATE CATALOG IF NOT EXISTS `{c}` "
        f"COMMENT 'mapledata.io: Canadian open data, national scope'",
        f"CREATE SCHEMA IF NOT EXISTS `{c}`.`landing` "
        f"COMMENT 'Raw files from external sources'",
        f"CREATE VOLUME IF NOT EXISTS `{c}`.`landing`.`raw` "
        f"COMMENT 'Raw data files written by the ingester'",
        f"CREATE VOLUME IF NOT EXISTS `{c}`.`landing`.`meta` "
        f"COMMENT 'Metadata cache and ingestion run logs'",
        f"CREATE SCHEMA IF NOT EXISTS `{c}`.`shared` "
        f"COMMENT 'Cross-domain dimensions (geography, NOC, date)'",
    ]
    for schema in domain_schemas(domains):
        domain, layer = schema.rsplit("_", 1)
        statements.append(
            f"CREATE SCHEMA IF NOT EXISTS `{c}`.`{schema}` "
            f"COMMENT '{domain} domain, {layer} layer'"
        )
    return statements
