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
    ├── metadata                schema for platform metadata
    │   ├── ckan_dataset_snapshot (table)  append-only discovery results
    │   ├── ckan_dataset_current  (view)   latest row per ckan_id
    │   ├── ckan_resource_snapshot (table) resources CKAN lists, per checked run
    │   ├── ckan_resource_current (view)   latest row per resource_id
    │   └── ingestion_findings    (table)  post-ingestion check findings
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


#: Columns of metadata.ckan_dataset_snapshot as (name, type, comment).
#: ckan_discover.py keeps its own (name, type) copy for writing rows; a test
#: checks that the two agree. Comments must not contain apostrophes.
SNAPSHOT_COLUMNS = (
    ("run_id", "STRING", "Discovery run this row belongs to, e.g. 20261031T120000Z"),
    ("discovered_at", "TIMESTAMP", "When the discovery ran (UTC); the same for every row of a run"),
    ("base_url", "STRING", "CKAN Action API the search ran against"),
    ("search_queries", "ARRAY<STRING>", "Search keywords that found this dataset in this run"),
    ("ckan_id", "STRING", "CKAN package id of the dataset"),
    ("name", "STRING", "CKAN package name (slug) of the dataset"),
    ("title", "STRING", "English dataset title"),
    ("organization_name", "STRING", "Code of the publishing organization, e.g. esdc-edsc"),
    ("organization_title", "STRING", "English display name of the publishing organization"),
    ("formats", "ARRAY<STRING>", "Distinct upper-case formats of the dataset resources"),
    ("resource_count", "INT", "Number of resources the dataset lists"),
    ("frequency", "STRING", "Update frequency as published, e.g. P1M or as_needed"),
    ("coverage_start", "DATE", "Start of the period the data covers; null when not published"),
    ("coverage_end", "DATE", "End of the period the data covers; null when not published"),
    ("metadata_modified", "STRING", "Dataset metadata_modified as published; not a file version"),
    ("is_registered", "BOOLEAN", "True when config/datasets.yml lists the dataset, whatever its status"),
)

#: Table comments, kept next to the column lists above.
DATASET_SNAPSHOT_COMMENT = (
    "Append-only CKAN dataset discovery results, one row per dataset per run"
)
RESOURCE_SNAPSHOT_COMMENT = (
    "Append-only list of the resources CKAN publishes for each registered "
    "dataset, one row per resource per checked ingestion run"
)
FINDINGS_COMMENT = (
    "Findings of the post-ingestion checks, one row per finding per checked "
    "ingestion run"
)


#: Columns of metadata.ckan_resource_snapshot as (name, type, comment).
#: ingestion_checks.py keeps its own (name, type) copy for writing rows; a test
#: checks that the two agree. Comments must not contain apostrophes.
RESOURCE_SNAPSHOT_COLUMNS = (
    ("run_id", "STRING", "Ingestion run this snapshot belongs to, e.g. 20261031T120000Z"),
    ("checked_at", "TIMESTAMP", "When the post-ingestion checks ran (UTC)"),
    ("dataset_key", "STRING", "Registry key of the dataset in config/datasets.yml"),
    ("ckan_id", "STRING", "CKAN package id of the dataset"),
    ("resource_id", "STRING", "CKAN resource id; stable even when the file is replaced"),
    ("name", "STRING", "English resource name as published"),
    ("language", "ARRAY<STRING>", "Languages the publisher tags the resource with"),
    ("format", "STRING", "File format as published, e.g. CSV or XLSX"),
    ("url", "STRING", "Download URL of the file"),
    ("last_modified", "STRING", "Resource last_modified as published; often empty"),
    ("created", "STRING", "Resource creation timestamp as published"),
    ("selected", "BOOLEAN", "True when the registry entry selects this resource for ingestion"),
    ("landed_path", "STRING", "Path of the landed file relative to the dataset landing folder; null when nothing landed this run"),
)

#: Columns of metadata.ingestion_findings as (name, type, comment).
FINDING_COLUMNS = (
    ("run_id", "STRING", "Ingestion run that was checked"),
    ("checked_at", "TIMESTAMP", "When the post-ingestion checks ran (UTC)"),
    ("dataset_key", "STRING", "Registry key of the dataset in config/datasets.yml"),
    ("resource_id", "STRING", "CKAN resource id; null for a finding about the whole dataset"),
    ("check_name", "STRING", "Check that produced the finding, e.g. download_failed"),
    ("severity", "STRING", "ERROR, WARN or INFO"),
    ("message", "STRING", "Human-readable description of the finding"),
    ("details_json", "STRING", "Check-specific details as a JSON object"),
)

#: Every table of the metadata schema: (name, columns, table comment).
METADATA_TABLES = (
    ("ckan_dataset_snapshot", SNAPSHOT_COLUMNS, DATASET_SNAPSHOT_COMMENT),
    ("ckan_resource_snapshot", RESOURCE_SNAPSHOT_COLUMNS, RESOURCE_SNAPSHOT_COMMENT),
    ("ingestion_findings", FINDING_COLUMNS, FINDINGS_COMMENT),
)


def _comment_statements(catalog: str, name: str, columns: tuple, comment: str) -> list:
    """COMMENT ON TABLE plus one ALTER COLUMN ... COMMENT per column."""
    table = f"`{catalog}`.`metadata`.`{name}`"
    return [f"COMMENT ON TABLE {table} IS '{comment}'"] + [
        f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{note}'"
        for col, _kind, note in columns
    ]


def _create_table(catalog: str, name: str, columns: tuple, comment: str) -> str:
    """CREATE TABLE IF NOT EXISTS with a table comment and a comment per column."""
    cols = ", ".join(f"{col} {kind} COMMENT '{note}'" for col, kind, note in columns)
    return (
        f"CREATE TABLE IF NOT EXISTS `{catalog}`.`metadata`.`{name}` ({cols}) "
        f"COMMENT '{comment}'"
    )


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
        f"CREATE SCHEMA IF NOT EXISTS `{c}`.`metadata` "
        f"COMMENT 'Platform metadata, e.g. dataset discovery snapshots'",
        _create_table(
            c, "ckan_dataset_snapshot", SNAPSHOT_COLUMNS, DATASET_SNAPSHOT_COMMENT
        ),
        # CREATE OR REPLACE VIEW is idempotent; Databricks does not allow
        # IF NOT EXISTS together with OR REPLACE.
        f"CREATE OR REPLACE VIEW `{c}`.`metadata`.`ckan_dataset_current` "
        f"COMMENT 'Latest snapshot row per ckan_id' AS "
        f"SELECT * FROM `{c}`.`metadata`.`ckan_dataset_snapshot` "
        f"QUALIFY row_number() OVER ("
        f"PARTITION BY ckan_id ORDER BY discovered_at DESC, run_id DESC) = 1",
        _create_table(
            c, "ckan_resource_snapshot", RESOURCE_SNAPSHOT_COLUMNS, RESOURCE_SNAPSHOT_COMMENT
        ),
        f"CREATE OR REPLACE VIEW `{c}`.`metadata`.`ckan_resource_current` "
        f"COMMENT 'Latest snapshot row per resource_id; a removed resource keeps "
        f"the run_id of its last appearance' AS "
        f"SELECT * FROM `{c}`.`metadata`.`ckan_resource_snapshot` "
        f"QUALIFY row_number() OVER ("
        f"PARTITION BY resource_id ORDER BY checked_at DESC, run_id DESC) = 1",
        _create_table(c, "ingestion_findings", FINDING_COLUMNS, FINDINGS_COMMENT),
    ]
    # CREATE TABLE IF NOT EXISTS leaves an existing table as it is, so the
    # comments are applied again to every table. Setting a comment is idempotent,
    # which lets comments change or arrive later without dropping a table.
    for table, columns, comment in METADATA_TABLES:
        statements += _comment_statements(c, table, columns, comment)
    for schema in domain_schemas(domains):
        domain, layer = schema.rsplit("_", 1)
        statements.append(
            f"CREATE SCHEMA IF NOT EXISTS `{c}`.`{schema}` "
            f"COMMENT '{domain} domain, {layer} layer'"
        )
    return statements
