"""Unit tests for src/platform/catalog_setup.py (no Spark needed)."""

from pathlib import Path

import pytest

import catalog_setup as cs
import ckan_ingest as ci

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ["open_canada"]


def test_build_ddl_order_and_core_objects():
    ddl = cs.build_ddl("living_canada", SOURCES, ["immigration"])
    assert ddl[0].startswith("CREATE CATALOG IF NOT EXISTS `living_canada`")
    assert any("`living_canada`.`landing`.`raw`" in s for s in ddl)
    assert any("`living_canada`.`landing`.`meta`" in s for s in ddl)
    assert any("`living_canada`.`shared`" in s for s in ddl)
    # Schemas must be created before the volumes that live in them.
    landing = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`landing`" in s)
    raw = next(i for i, s in enumerate(ddl) if "`landing`.`raw`" in s)
    assert landing < raw


def test_every_statement_is_idempotent():
    for sql in cs.build_ddl("living_canada", SOURCES, ["immigration", "housing"]):
        # Databricks does not allow IF NOT EXISTS together with OR REPLACE,
        # so views use CREATE OR REPLACE VIEW.
        assert "IF NOT EXISTS" in sql or sql.startswith("CREATE OR REPLACE VIEW")


def test_build_ddl_leaves_comments_of_existing_tables_to_the_comment_plan():
    # Re-applying an unchanged comment creates a Delta version, so build_ddl
    # emits none; build_comment_statements applies only the differences.
    ddl = cs.build_ddl("living_canada", SOURCES, ["immigration"])
    assert not any(s.startswith(("COMMENT ON", "ALTER TABLE")) for s in ddl)
    assert not any("DROP" in s.upper() for s in ddl)


def test_metadata_schema_table_and_view_are_created_in_order():
    ddl = cs.build_ddl("living_canada", SOURCES, ["immigration"])
    schema = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`metadata`" in s)
    table = next(i for i, s in enumerate(ddl) if s.startswith("CREATE TABLE") and "ckan_dataset_snapshot" in s)
    view = next(i for i, s in enumerate(ddl) if "VIEW" in s and "ckan_dataset_current" in s)
    assert schema < table < view


def test_snapshot_table_defines_every_column_with_its_type():
    ddl = cs.build_ddl("living_canada", SOURCES, [])
    table = next(s for s in ddl if s.startswith("CREATE TABLE"))
    assert "`living_canada`.`metadata`.`ckan_dataset_snapshot`" in table
    for name, kind, _note in cs.SNAPSHOT_COLUMNS:
        assert f"{name} {kind} COMMENT '" in table
    assert "run_id STRING" in table and "formats ARRAY<STRING>" in table
    assert "search_queries ARRAY<STRING>" in table
    assert "search_query STRING" not in table


def find(ddl, prefix, text):
    return next(s for s in ddl if s.startswith(prefix) and text in s)


def test_check_tables_and_view_are_created_after_the_metadata_schema():
    ddl = cs.build_ddl("living_canada", SOURCES, [])
    schema = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`metadata`" in s)
    snap = ddl.index(find(ddl, "CREATE TABLE", "ckan_resource_snapshot"))
    view = ddl.index(find(ddl, "CREATE OR REPLACE VIEW", "ckan_resource_current"))
    findings = ddl.index(find(ddl, "CREATE TABLE", "ingestion_findings"))
    assert schema < snap < view and schema < findings


def test_every_metadata_table_is_listed_with_a_comment_for_each_column():
    assert [name for name, _, _ in cs.METADATA_TABLES] == [
        "ckan_dataset_snapshot", "ckan_resource_snapshot", "ingestion_findings",
    ]
    for _name, columns, table_comment in cs.METADATA_TABLES:
        assert table_comment and "'" not in table_comment
        for col, kind, note in columns:
            assert col and kind and note and "'" not in note


def desired_current():
    """The current comments of tables that exist and already match the code."""
    return {
        name: {"comment": comment, "columns": {col: note for col, _k, note in columns}}
        for name, columns, comment in cs.METADATA_TABLES
    }


def commentless_current():
    """Tables that exist but have no comment anywhere."""
    return {
        name: {"comment": None, "columns": {col: None for col, _k, _n in columns}}
        for name, columns, _comment in cs.METADATA_TABLES
    }


TOTAL_COMMENTS = sum(1 + len(columns) for _n, columns, _c in cs.METADATA_TABLES)
SNAPSHOT = "`living_canada`.`metadata`.`ckan_dataset_snapshot`"


def test_unchanged_comments_produce_no_statements():
    plan = cs.build_comment_statements("living_canada", desired_current())
    assert plan.statements == []
    assert plan.up_to_date == TOTAL_COMMENTS == 3 + 16 + 16 + 8
    assert plan.missing_columns == []


def test_tables_that_do_not_exist_get_no_comment_statements():
    # They are created with their comments inline, so nothing is left to apply.
    for current in (None, {}):
        plan = cs.build_comment_statements("living_canada", current)
        assert (plan.statements, plan.up_to_date, plan.missing_columns) == ([], 0, [])


def test_tables_without_any_comment_get_every_comment_and_no_drop():
    plan = cs.build_comment_statements("living_canada", commentless_current())
    assert len(plan.statements) == TOTAL_COMMENTS and plan.up_to_date == 0
    assert not any("DROP" in s.upper() for s in plan.statements)
    # The dataset snapshot table existed before it had column comments.
    assert (
        f"ALTER TABLE {SNAPSHOT} ALTER COLUMN search_queries COMMENT "
        "'Search keywords that found this dataset in this run'"
    ) in plan.statements
    assert f"COMMENT ON TABLE {SNAPSHOT} IS '{cs.DATASET_SNAPSHOT_COMMENT}'" in plan.statements


@pytest.mark.parametrize("table, columns, table_comment", cs.METADATA_TABLES)
def test_each_table_and_column_gets_exactly_one_statement_when_missing(
    table, columns, table_comment
):
    current = desired_current()
    current[table] = {"comment": None, "columns": {col: None for col, _k, _n in columns}}
    plan = cs.build_comment_statements("living_canada", current)

    full = f"`living_canada`.`metadata`.`{table}`"
    assert plan.statements[0] == f"COMMENT ON TABLE {full} IS '{table_comment}'"
    assert plan.statements[1:] == [
        f"ALTER TABLE {full} ALTER COLUMN {col} COMMENT '{note}'" for col, _k, note in columns
    ]
    assert plan.up_to_date == TOTAL_COMMENTS - (1 + len(columns))


def test_only_the_differing_comments_are_applied():
    current = desired_current()
    current["ckan_dataset_snapshot"]["columns"]["title"] = "An old description"
    current["ingestion_findings"]["comment"] = "An old table comment"
    plan = cs.build_comment_statements("living_canada", current)

    assert plan.statements == [
        f"ALTER TABLE {SNAPSHOT} ALTER COLUMN title COMMENT 'English dataset title'",
        "COMMENT ON TABLE `living_canada`.`metadata`.`ingestion_findings` "
        f"IS '{cs.FINDINGS_COMMENT}'",
    ]
    assert plan.up_to_date == TOTAL_COMMENTS - 2


@pytest.mark.parametrize("stale", [None, "", "English dataset title "])
def test_a_missing_empty_or_slightly_different_comment_counts_as_a_difference(stale):
    # The comparison is an exact string comparison.
    current = desired_current()
    current["ckan_dataset_snapshot"]["columns"]["title"] = stale
    plan = cs.build_comment_statements("living_canada", current)
    assert plan.statements == [
        f"ALTER TABLE {SNAPSHOT} ALTER COLUMN title COMMENT 'English dataset title'"
    ]


def test_applying_the_plan_makes_the_next_plan_empty():
    current = commentless_current()
    first = cs.build_comment_statements("living_canada", current)
    assert first.statements
    # Simulate the statements having run: every comment now matches.
    second = cs.build_comment_statements("living_canada", desired_current())
    assert second.statements == [] and second.up_to_date == TOTAL_COMMENTS


def test_a_column_the_table_lacks_is_reported_not_commented():
    current = desired_current()
    del current["ckan_resource_snapshot"]["columns"]["archived"]
    plan = cs.build_comment_statements("living_canada", current)
    assert plan.statements == []
    assert plan.missing_columns == [("ckan_resource_snapshot", "archived")]
    assert plan.up_to_date == TOTAL_COMMENTS - 1


def is_view(sql):
    return sql.startswith("CREATE OR REPLACE VIEW")


def is_create_table(sql):
    return sql.startswith("CREATE TABLE")


def is_add_columns(sql):
    return sql.startswith("ALTER TABLE") and " ADD COLUMNS (" in sql


def is_comment(sql):
    return sql.startswith("COMMENT ON TABLE") or " ALTER COLUMN " in sql


def drifted_current():
    """Existing tables that lack a column and carry a stale comment."""
    current = desired_current()
    del current["ckan_resource_snapshot"]["columns"]["rounded_marker"]
    current["ckan_resource_snapshot"]["columns"]["dataset_archived"] = "An old comment"
    return current


def indexes(statements, predicate):
    return [i for i, s in enumerate(statements) if predicate(s)]


def test_views_come_after_every_statement_that_alters_a_table():
    # Observed 2026-11-02: views created before ADD COLUMNS / ALTER COLUMN stayed
    # bound to the old columns and comments until a second bootstrap.
    statements = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], drifted_current()).statements
    altering = indexes(statements, lambda s: is_create_table(s) or is_add_columns(s) or is_comment(s))
    views = indexes(statements, is_view)
    assert altering and len(views) == 2
    assert max(altering) < min(views)


def test_the_bootstrap_runs_tables_then_add_columns_then_comments_then_views():
    statements = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], drifted_current()).statements
    tables = indexes(statements, is_create_table)
    adds = indexes(statements, is_add_columns)
    comments = indexes(statements, is_comment)
    views = indexes(statements, is_view)
    assert len(tables) == 3 and len(adds) == 1 and len(comments) == 1 and len(views) == 2
    assert max(tables) < min(adds) < min(comments) < min(views)
    # The migration adds the column the views must then pick up.
    assert "ADD COLUMNS (rounded_marker BOOLEAN" in statements[adds[0]]
    assert "ALTER COLUMN dataset_archived COMMENT" in statements[comments[0]]


def test_appending_the_alters_to_build_ddl_would_recreate_the_bug():
    # What the bootstrap used to do: build_ddl first, the alters after it. The
    # views then run before ADD COLUMNS and the comment statements.
    plan = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], drifted_current())
    naive = cs.build_ddl("living_canada", SOURCES, ["immigration"]) + plan.migrations + plan.comments.statements
    assert min(indexes(naive, is_view)) < max(indexes(naive, lambda s: is_add_columns(s) or is_comment(s)))
    # build_setup_plan is the order that works.
    ordered = plan.statements
    assert min(indexes(ordered, is_view)) > max(indexes(ordered, lambda s: is_add_columns(s) or is_comment(s)))


def test_views_are_the_last_statements_of_the_plan():
    statements = cs.build_setup_plan("living_canada", SOURCES, ["immigration", "housing"], drifted_current()).statements
    views = [s for s in statements if is_view(s)]
    assert statements[-len(views):] == views            # nothing runs after the views
    assert statements[:-len(views)] == [s for s in statements if not is_view(s)]


def test_on_a_fresh_workspace_the_tables_are_created_before_the_views():
    plan = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], None)
    assert plan.migrations == [] and plan.comments.statements == []
    assert max(indexes(plan.statements, is_create_table)) < min(indexes(plan.statements, is_view))


def test_with_nothing_to_change_the_plan_is_just_the_creates_and_the_views():
    plan = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], desired_current())
    assert plan.statements == cs.build_ddl("living_canada", SOURCES, ["immigration"])
    assert not any(is_add_columns(s) or is_comment(s) for s in plan.statements)


def test_the_plan_contains_its_migrations_and_comment_statements():
    plan = cs.build_setup_plan("living_canada", SOURCES, ["immigration"], drifted_current())
    assert [s for s in plan.statements if is_add_columns(s)] == plan.migrations
    assert [s for s in plan.statements if is_comment(s)] == plan.comments.statements
    ddl = cs.build_ddl("living_canada", SOURCES, ["immigration"])
    assert len(plan.statements) == len(ddl) + len(plan.migrations) + len(plan.comments.statements)


def test_build_ddl_puts_the_views_last_and_matches_its_parts():
    statements, views = cs.build_ddl_parts("living_canada", SOURCES, ["immigration"])
    assert all(is_view(s) for s in views) and len(views) == 2
    assert not any(is_view(s) for s in statements)
    ddl = cs.build_ddl("living_canada", SOURCES, ["immigration"])
    assert ddl == statements + views


def test_a_missing_column_is_added_with_its_comment_and_nothing_else():
    statements = cs.build_add_column_statements(
        "living_canada", [("ckan_resource_snapshot", "archived")]
    )
    assert statements == [
        "ALTER TABLE `living_canada`.`metadata`.`ckan_resource_snapshot` ADD COLUMNS "
        "(archived BOOLEAN COMMENT "
        "'True when the resource name or description says archived, in any case')"
    ]
    for sql in statements:
        assert "DROP" not in sql.upper() and "DEFAULT" not in sql.upper()


def test_add_column_statements_follow_the_order_of_the_missing_columns():
    missing = [
        ("ckan_dataset_snapshot", "title"),
        ("ckan_resource_snapshot", "archived"),
        ("ingestion_findings", "severity"),
    ]
    statements = cs.build_add_column_statements("living_canada", missing)
    assert len(statements) == 3
    for sql, (table, col) in zip(statements, missing):
        assert f"`living_canada`.`metadata`.`{table}` ADD COLUMNS ({col} " in sql


def test_nothing_is_added_when_no_column_is_missing():
    assert cs.build_add_column_statements("living_canada", []) == []


def test_the_migration_comes_from_the_plan_and_the_guard_stops_it_repeating():
    current = desired_current()
    del current["ckan_resource_snapshot"]["columns"]["archived"]
    del current["ckan_resource_snapshot"]["columns"]["dataset_archived"]

    plan = cs.build_comment_statements("living_canada", current)
    first = cs.build_add_column_statements("living_canada", plan.missing_columns)
    assert len(first) == 2
    assert [c for _t, c in plan.missing_columns] == ["archived", "dataset_archived"]
    # The new columns carry their comment inline, so no separate comment runs.
    assert plan.statements == []

    # After the columns exist (what information_schema reports on the next run),
    # nothing is missing and nothing is added.
    again = cs.build_comment_statements("living_canada", desired_current())
    assert again.missing_columns == []
    assert cs.build_add_column_statements("living_canada", again.missing_columns) == []


def test_a_table_that_does_not_exist_yet_gets_no_migration():
    # It is created with all its columns, so it is never in missing_columns.
    plan = cs.build_comment_statements("living_canada", {})
    assert cs.build_add_column_statements("living_canada", plan.missing_columns) == []


def test_add_column_statements_reject_unknown_columns_and_unsafe_catalogs():
    with pytest.raises(ValueError):
        cs.build_add_column_statements("living_canada", [("ckan_dataset_snapshot", "no_such")])
    with pytest.raises(ValueError):
        cs.build_add_column_statements("living_canada", [("no_such_table", "title")])
    with pytest.raises(ValueError):
        cs.build_add_column_statements("Bad Catalog", [])


def test_comment_plan_does_not_change_its_input_and_checks_the_catalog_name():
    current = commentless_current()
    snapshot = {k: {"comment": v["comment"], "columns": dict(v["columns"])} for k, v in current.items()}
    cs.build_comment_statements("living_canada", current)
    assert current == snapshot
    with pytest.raises(ValueError):
        cs.build_comment_statements("Bad-Catalog", current)


def test_parse_current_comments_builds_the_input_of_the_plan():
    current = cs.parse_current_comments(
        [("ckan_dataset_snapshot", "A table"), ("ingestion_findings", None)],
        [
            ("ckan_dataset_snapshot", "title", "A title"),
            ("ckan_dataset_snapshot", "name", None),
            ("some_other_table", "x", "ignored: its table is not listed"),
        ],
    )
    assert current == {
        "ckan_dataset_snapshot": {"comment": "A table", "columns": {"title": "A title", "name": None}},
        "ingestion_findings": {"comment": None, "columns": {}},
    }
    assert cs.parse_current_comments([], []) == {}


def test_current_comment_queries_read_the_metadata_schema_of_the_catalog():
    tables_sql, columns_sql = cs.current_comments_queries("living_canada")
    assert "`living_canada`.information_schema.tables" in tables_sql
    assert "`living_canada`.information_schema.columns" in columns_sql
    for sql in (tables_sql, columns_sql):
        assert "table_schema = 'metadata'" in sql
        for name, _cols, _note in cs.METADATA_TABLES:
            assert f"'{name}'" in sql
        assert "comment" in sql
    assert "column_name" in columns_sql
    with pytest.raises(ValueError):
        cs.current_comments_queries("x; DROP CATALOG y")


def test_catalog_exists_looks_for_the_exact_name_in_the_show_catalogs_result():
    names = ["system", "living_canada", "samples"]
    assert cs.catalog_exists(names, "living_canada") is True
    assert cs.catalog_exists(names, "maple") is False        # no partial match
    assert cs.catalog_exists([], "living_canada") is False
    assert cs.catalog_exists(iter(names), "samples") is True  # any iterable


def test_catalog_exists_rejects_an_unsafe_catalog_name():
    with pytest.raises(ValueError):
        cs.catalog_exists(["living_canada"], "map-le; drop")


def test_catalog_exists_documents_the_permission_caveat():
    assert "privileges" in cs.catalog_exists.__doc__


def test_build_ddl_is_deterministic_so_reruns_repeat_the_same_statements():
    assert cs.build_ddl("living_canada", SOURCES, ["housing"]) == cs.build_ddl("living_canada", SOURCES, ["housing"])


@pytest.mark.parametrize(
    "table, columns",
    [
        ("ckan_resource_snapshot", cs.RESOURCE_SNAPSHOT_COLUMNS),
        ("ingestion_findings", cs.FINDING_COLUMNS),
    ],
)
def test_check_tables_comment_the_table_and_every_column(table, columns):
    statement = find(cs.build_ddl("living_canada", SOURCES, []), "CREATE TABLE", table)
    assert f"`living_canada`.`metadata`.`{table}`" in statement
    assert statement.startswith("CREATE TABLE IF NOT EXISTS")
    for name, kind, note in columns:
        assert f"{name} {kind} COMMENT '{note}'" in statement
        assert note and "'" not in note  # an apostrophe would break the literal
    # One COMMENT per column, plus the table comment after the column list.
    assert statement.count(" COMMENT '") == len(columns) + 1
    assert statement.rstrip().endswith("'") and ") COMMENT '" in statement


def test_resource_current_view_keeps_latest_row_per_resource_id():
    view = find(cs.build_ddl("living_canada", SOURCES, []), "CREATE OR REPLACE VIEW", "ckan_resource_current")
    assert "FROM `living_canada`.`metadata`.`ckan_resource_snapshot`" in view
    assert "QUALIFY row_number() OVER (PARTITION BY resource_id ORDER BY checked_at DESC, run_id DESC) = 1" in view
    assert "COMMENT '" in view


def test_current_view_keeps_latest_row_per_ckan_id():
    ddl = cs.build_ddl("living_canada", SOURCES, [])
    view = next(s for s in ddl if s.startswith("CREATE OR REPLACE VIEW"))
    assert "`living_canada`.`metadata`.`ckan_dataset_current`" in view
    assert "FROM `living_canada`.`metadata`.`ckan_dataset_snapshot`" in view
    assert "QUALIFY row_number() OVER (PARTITION BY ckan_id ORDER BY discovered_at DESC" in view
    assert view.endswith("= 1")


def test_schemas_are_layer_first_per_source_and_domain():
    ddl = cs.build_ddl("living_canada", ["statcan", "open_canada", "statcan"], ["housing", "immigration"])
    schemas = [
        sql.split("`.`")[1].rstrip("`").split("` ")[0]
        for sql in ddl
        if sql.startswith("CREATE SCHEMA")
    ]
    assert schemas == [
        "landing", "shared", "metadata",
        "bronze_open_canada", "bronze_statcan",
        "silver_housing", "gold_housing", "silver_immigration", "gold_immigration",
    ]


@pytest.mark.parametrize("bad", ["", "Mapledata", "my-catalog", "x; DROP CATALOG y", "1abc"])
def test_invalid_names_are_rejected(bad):
    with pytest.raises(ValueError):
        cs.build_ddl(bad, SOURCES, ["immigration"])
    with pytest.raises(ValueError):
        cs.build_ddl("living_canada", SOURCES, [bad])
    with pytest.raises(ValueError):
        cs.build_ddl("living_canada", [bad], ["immigration"])


def test_shipped_registry_produces_the_documented_schemas():
    specs = ci.load_registry(str(ROOT / "config" / "datasets.yml"))
    ddl = cs.build_ddl(
        "living_canada", {s.source for s in specs}, {s.domain for s in specs}
    )
    assert any("`bronze_open_canada`" in s for s in ddl)
    assert any("`silver_immigration`" in s for s in ddl)
    assert any("`gold_immigration`" in s for s in ddl)
