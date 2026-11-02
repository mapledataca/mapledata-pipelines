"""Unit tests for src/platform/catalog_setup.py (no Spark needed)."""

from pathlib import Path

import pytest

import catalog_setup as cs
import ckan_ingest as ci

ROOT = Path(__file__).resolve().parents[1]


def test_build_ddl_order_and_core_objects():
    ddl = cs.build_ddl("mapledata", ["immigration"])
    assert ddl[0].startswith("CREATE CATALOG IF NOT EXISTS `mapledata`")
    assert any("`mapledata`.`landing`.`raw`" in s for s in ddl)
    assert any("`mapledata`.`landing`.`meta`" in s for s in ddl)
    assert any("`mapledata`.`shared`" in s for s in ddl)
    # Schemas must be created before the volumes that live in them.
    landing = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`landing`" in s)
    raw = next(i for i, s in enumerate(ddl) if "`landing`.`raw`" in s)
    assert landing < raw


def test_every_statement_is_idempotent():
    for sql in cs.build_ddl("mapledata", ["immigration", "housing"]):
        # Databricks does not allow IF NOT EXISTS together with OR REPLACE,
        # so the view uses CREATE OR REPLACE VIEW.
        assert "IF NOT EXISTS" in sql or sql.startswith("CREATE OR REPLACE VIEW")


def test_metadata_schema_table_and_view_are_created_in_order():
    ddl = cs.build_ddl("mapledata", ["immigration"])
    schema = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`metadata`" in s)
    table = next(i for i, s in enumerate(ddl) if s.startswith("CREATE TABLE") and "ckan_dataset_snapshot" in s)
    view = next(i for i, s in enumerate(ddl) if "VIEW" in s and "ckan_dataset_current" in s)
    assert schema < table < view


def test_snapshot_table_defines_every_column_with_its_type():
    ddl = cs.build_ddl("mapledata", [])
    table = next(s for s in ddl if s.startswith("CREATE TABLE"))
    assert "`mapledata`.`metadata`.`ckan_dataset_snapshot`" in table
    for name, kind in cs.SNAPSHOT_COLUMNS:
        assert f"{name} {kind}" in table
    assert "run_id STRING" in table and "formats ARRAY<STRING>" in table
    assert "search_queries ARRAY<STRING>" in table
    assert "search_query STRING" not in table


def find(ddl, prefix, text):
    return next(s for s in ddl if s.startswith(prefix) and text in s)


def test_check_tables_and_view_are_created_after_the_metadata_schema():
    ddl = cs.build_ddl("mapledata", [])
    schema = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`metadata`" in s)
    snap = ddl.index(find(ddl, "CREATE TABLE", "ckan_resource_snapshot"))
    view = ddl.index(find(ddl, "CREATE OR REPLACE VIEW", "ckan_resource_current"))
    findings = ddl.index(find(ddl, "CREATE TABLE", "ingestion_findings"))
    assert schema < snap < view and schema < findings


@pytest.mark.parametrize(
    "table, columns",
    [
        ("ckan_resource_snapshot", cs.RESOURCE_SNAPSHOT_COLUMNS),
        ("ingestion_findings", cs.FINDING_COLUMNS),
    ],
)
def test_check_tables_comment_the_table_and_every_column(table, columns):
    statement = find(cs.build_ddl("mapledata", []), "CREATE TABLE", table)
    assert f"`mapledata`.`metadata`.`{table}`" in statement
    assert statement.startswith("CREATE TABLE IF NOT EXISTS")
    for name, kind, note in columns:
        assert f"{name} {kind} COMMENT '{note}'" in statement
        assert note and "'" not in note  # an apostrophe would break the literal
    # One COMMENT per column, plus the table comment after the column list.
    assert statement.count(" COMMENT '") == len(columns) + 1
    assert statement.rstrip().endswith("'") and ") COMMENT '" in statement


def test_resource_current_view_keeps_latest_row_per_resource_id():
    view = find(cs.build_ddl("mapledata", []), "CREATE OR REPLACE VIEW", "ckan_resource_current")
    assert "FROM `mapledata`.`metadata`.`ckan_resource_snapshot`" in view
    assert "QUALIFY row_number() OVER (PARTITION BY resource_id ORDER BY checked_at DESC, run_id DESC) = 1" in view
    assert "COMMENT '" in view


def test_current_view_keeps_latest_row_per_ckan_id():
    ddl = cs.build_ddl("mapledata", [])
    view = next(s for s in ddl if s.startswith("CREATE OR REPLACE VIEW"))
    assert "`mapledata`.`metadata`.`ckan_dataset_current`" in view
    assert "FROM `mapledata`.`metadata`.`ckan_dataset_snapshot`" in view
    assert "QUALIFY row_number() OVER (PARTITION BY ckan_id ORDER BY discovered_at DESC" in view
    assert view.endswith("= 1")


def test_domain_schemas_three_layers_deduplicated_and_sorted():
    assert cs.domain_schemas(["housing", "immigration", "housing"]) == [
        "housing_bronze", "housing_silver", "housing_gold",
        "immigration_bronze", "immigration_silver", "immigration_gold",
    ]


@pytest.mark.parametrize("bad", ["", "Mapledata", "my-catalog", "x; DROP CATALOG y", "1abc"])
def test_invalid_names_are_rejected(bad):
    with pytest.raises(ValueError):
        cs.build_ddl(bad, ["immigration"])
    with pytest.raises(ValueError):
        cs.build_ddl("mapledata", [bad])


def test_shipped_registry_domains_produce_valid_ddl():
    domains = {s.domain for s in ci.load_registry(str(ROOT / "config" / "datasets.yml"))}
    ddl = cs.build_ddl("mapledata", domains)
    assert any("`immigration_bronze`" in s for s in ddl)
