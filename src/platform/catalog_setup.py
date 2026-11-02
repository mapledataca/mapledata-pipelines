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

Comments on the metadata tables are kept in code, but they are applied only
where they differ from the comments the tables have now: re-applying an
unchanged comment still creates a new Delta version (measured, see
docs/architecture.md). build_comment_statements takes the current comments as
input and returns just the differences.

This module only BUILDS the SQL statements (plain strings) and compares
comments given to it, which keeps it unit-testable without Spark.
bootstrap_catalog.py reads the current comments and executes the statements.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

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
    ("archived", "BOOLEAN", "True when the resource name or description says archived, in any case"),
    ("dataset_archived", "BOOLEAN", "True when the dataset name or title says archived, in any case; the same for every resource of the dataset"),
    ("landed_path", "STRING", "Path of the landed file relative to the dataset landing folder; null when nothing landed this run"),
    # Keep new columns last: ADD COLUMNS appends, so a migrated table then has
    # the same column order as a new one. Rows written before this column existed
    # hold NULL (not checked) and are not backfilled.
    ("rounded_marker", "BOOLEAN", "True when a rounded marker was found (name, description, file head or tail). False means no marker was found, not that values are exact."),
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


@dataclass(frozen=True)
class CommentPlan:
    """What to do about comments, given the comments the tables have now.

    statements:      COMMENT ON TABLE / ALTER COLUMN ... COMMENT, only for
                     comments that differ.
    up_to_date:      how many table and column comments already match.
    missing_columns: (table, column) pairs of existing tables that lack a column
                     this module defines. No comment statement is emitted for
                     them (it would fail); build_add_column_statements adds the
                     column, with its comment.
    """

    statements: list = field(default_factory=list)
    up_to_date: int = 0
    missing_columns: list = field(default_factory=list)


def catalog_exists(catalog_names: Iterable, catalog: str) -> bool:
    """True when `catalog` is among the names SHOW CATALOGS returned.

    Reading the comments of a catalog that does not exist yet fails, so the
    bootstrap asks first and skips the read only in that case. Any other error
    is not hidden. The SHOW CATALOGS documentation does not say whether the
    list is filtered by privileges; a catalog the user cannot see counts as
    missing here, and the CREATE CATALOG IF NOT EXISTS that follows is what
    then exposes the access problem.
    """
    return _check(catalog, "catalog") in set(catalog_names)


def current_comments_queries(catalog: str) -> tuple:
    """SQL that reads the current comments of the metadata tables.

    Returns (tables_query, columns_query), to be run before the DDL. They read
    the catalog's information_schema, which lists only objects the user may
    access and fails while the catalog does not exist yet. Feed the rows to
    parse_current_comments.
    """
    c = _check(catalog, "catalog")
    names = ", ".join(f"'{name}'" for name, _cols, _note in METADATA_TABLES)
    where = f"WHERE table_schema = 'metadata' AND table_name IN ({names})"
    return (
        f"SELECT table_name, comment FROM `{c}`.information_schema.tables {where}",
        f"SELECT table_name, column_name, comment "
        f"FROM `{c}`.information_schema.columns {where}",
    )


def parse_current_comments(table_rows: Iterable, column_rows: Iterable) -> dict:
    """Turn information_schema rows into {table: {"comment", "columns"}}.

    table_rows are (table_name, comment) and column_rows are
    (table_name, column_name, comment); a comment is None when there is none.
    Only tables that appear in table_rows are listed, so a table that does not
    exist yet is simply absent.
    """
    current = {name: {"comment": comment, "columns": {}} for name, comment in table_rows}
    for table, column, comment in column_rows:
        if table in current:
            current[table]["columns"][column] = comment
    return current


def build_comment_statements(catalog: str, current: Optional[dict] = None) -> CommentPlan:
    """Comment statements for the metadata tables that need them.

    current is what parse_current_comments returns. A comment is applied only
    when it is not equal (exact string comparison) to the current one: setting
    an unchanged comment still creates a Delta version.

    A table missing from current does not exist yet. It is created with its
    comments inline (CREATE TABLE ... COMMENT), so it gets no statement here.
    """
    c = _check(catalog, "catalog")
    current = current or {}
    statements, missing, up_to_date = [], [], 0
    for name, columns, comment in METADATA_TABLES:
        state = current.get(name)
        if state is None:
            continue
        table = f"`{c}`.`metadata`.`{name}`"
        if state["comment"] == comment:
            up_to_date += 1
        else:
            statements.append(f"COMMENT ON TABLE {table} IS '{comment}'")
        for col, _kind, note in columns:
            if col not in state["columns"]:
                missing.append((name, col))
            elif state["columns"][col] == note:
                up_to_date += 1
            else:
                statements.append(
                    f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{note}'"
                )
    return CommentPlan(statements, up_to_date, missing)


def build_add_column_statements(catalog: str, missing_columns: Iterable) -> list:
    """ALTER TABLE ... ADD COLUMNS for columns an existing table lacks.

    missing_columns are the (table, column) pairs of CommentPlan.missing_columns.
    They come from comparing the table with information_schema, so a column that
    already exists is never added again: that comparison is the column-exists
    guard (the documented ADD COLUMNS grammar has no IF NOT EXISTS and rejects a
    duplicate name). Running it again finds nothing missing and emits nothing.

    The column arrives with its comment, so no separate comment statement is
    needed. Existing rows hold NULL in it (Delta cannot give an added column a
    default and nothing is backfilled), which stays distinguishable from a value
    written later. Columns are appended at the end of the table, so define a new
    column last in its column list to keep new and migrated tables in the same
    order. Nothing is ever dropped.
    """
    c = _check(catalog, "catalog")
    defined = {
        (table, col): (kind, note)
        for table, columns, _comment in METADATA_TABLES
        for col, kind, note in columns
    }
    statements = []
    for table, col in missing_columns:
        if (table, col) not in defined:
            raise ValueError(f"{table}.{col} is not a column defined in METADATA_TABLES")
        kind, note = defined[(table, col)]
        statements.append(
            f"ALTER TABLE `{c}`.`metadata`.`{table}` "
            f"ADD COLUMNS ({col} {kind} COMMENT '{note}')"
        )
    return statements


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


@dataclass(frozen=True)
class SetupPlan:
    """Everything the bootstrap runs, in the order it must run.

    statements: every statement in execution order: the CREATE statements,
                then ADD COLUMNS, then the comment statements, then the views.
    comments:   the comment plan (what is up to date, what changed).
    migrations: the ADD COLUMNS statements, also contained in statements.
    """

    statements: list
    comments: CommentPlan
    migrations: list


def build_setup_plan(
    catalog: str, domains: Iterable[str], current: Optional[dict] = None
) -> SetupPlan:
    """The ordered statements of a bootstrap, given the current table comments.

    Order: tables, then ADD COLUMNS, then comment statements, then the views
    (CREATE OR REPLACE VIEW) last. A view built with SELECT * is bound to the
    columns the table has when the view is created, so it must be recreated
    after the table has changed. Observed 2026-11-02: a bootstrap that created
    the views first left ckan_resource_current without the new column and with
    the old comment until a second bootstrap.

    current is what parse_current_comments returns (None: no table exists yet).
    """
    statements, views = build_ddl_parts(catalog, domains)
    comments = build_comment_statements(catalog, current)
    migrations = build_add_column_statements(catalog, comments.missing_columns)
    return SetupPlan(statements + migrations + comments.statements + views, comments, migrations)


def build_ddl(catalog: str, domains: Iterable[str]) -> list:
    """Return the ordered list of idempotent DDL statements: everything that
    creates objects, with the views last.

    Altering existing tables (ADD COLUMNS, comments) is not included: see
    build_setup_plan, which puts those between the tables and the views.
    """
    statements, views = build_ddl_parts(catalog, domains)
    return statements + views


def build_ddl_parts(catalog: str, domains: Iterable[str]) -> tuple:
    """Return (statements, views): the idempotent CREATE statements without the
    views, and the CREATE OR REPLACE VIEW statements.

    Order within statements matters: catalog, then schemas, then volumes inside
    schemas, then the tables.
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
        _create_table(
            c, "ckan_resource_snapshot", RESOURCE_SNAPSHOT_COLUMNS, RESOURCE_SNAPSHOT_COMMENT
        ),
        _create_table(c, "ingestion_findings", FINDING_COLUMNS, FINDINGS_COMMENT),
    ]
    # CREATE OR REPLACE VIEW is idempotent; Databricks does not allow
    # IF NOT EXISTS together with OR REPLACE. They are returned separately so
    # they run after the tables are altered.
    views = [
        f"CREATE OR REPLACE VIEW `{c}`.`metadata`.`ckan_dataset_current` "
        f"COMMENT 'Latest snapshot row per ckan_id' AS "
        f"SELECT * FROM `{c}`.`metadata`.`ckan_dataset_snapshot` "
        f"QUALIFY row_number() OVER ("
        f"PARTITION BY ckan_id ORDER BY discovered_at DESC, run_id DESC) = 1",
        f"CREATE OR REPLACE VIEW `{c}`.`metadata`.`ckan_resource_current` "
        f"COMMENT 'Latest snapshot row per resource_id; a removed resource keeps "
        f"the run_id of its last appearance' AS "
        f"SELECT * FROM `{c}`.`metadata`.`ckan_resource_snapshot` "
        f"QUALIFY row_number() OVER ("
        f"PARTITION BY resource_id ORDER BY checked_at DESC, run_id DESC) = 1",
    ]
    # CREATE TABLE IF NOT EXISTS leaves an existing table as it is, so comments
    # that changed (or arrived later) are applied by build_comment_statements,
    # which compares them with the comments the tables have now.
    for schema in domain_schemas(domains):
        domain, layer = schema.rsplit("_", 1)
        statements.append(
            f"CREATE SCHEMA IF NOT EXISTS `{c}`.`{schema}` "
            f"COMMENT '{domain} domain, {layer} layer'"
        )
    return statements, views
