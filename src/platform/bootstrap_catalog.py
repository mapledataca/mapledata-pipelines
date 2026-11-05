# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bootstrap the catalog
# MAGIC Creates the catalog, schemas and volumes from zero. Safe to re-run: every
# MAGIC statement uses `IF NOT EXISTS`. The catalog and schema conventions come from
# MAGIC `config/platform.yml`; sources and domains come from `config/datasets.yml`
# MAGIC (`bronze_<source>`, `silver_<domain>`, `gold_<domain>`).
# MAGIC
# MAGIC Run this once per workspace, and again after adding a dataset whose
# MAGIC source or domain is new. `dry_run` prints the full plan without running it.
# MAGIC The `catalog` widget overrides the configured catalog (empty = config).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])

# COMMAND ----------

import sys
from pathlib import Path

# This notebook lives in src/platform/; the repository root is two levels up.
repo_root = Path.cwd().parents[1]
sys.path.insert(0, str(repo_root / "src" / "ingest"))
sys.path.insert(0, str(repo_root / "src" / "platform"))

import catalog_setup as cs
import ckan_ingest as ci
import platform_config as pc

config = pc.load_platform_config(str(repo_root / "config" / "platform.yml"))
catalog = config.resolve_catalog(dbutils.widgets.get("catalog"))
specs = ci.load_registry(str(repo_root / "config" / "datasets.yml"))
# Include non-active datasets: schemas are cheap.
sources = {s.source for s in specs}
domains = {s.domain for s in specs}

# Read the comments the metadata tables have now, before any DDL runs. Re-applying
# an unchanged comment still creates a Delta version, so only differences run.
# Only a catalog that does not exist yet is skipped; any other error is raised.
catalog_names = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
if cs.catalog_exists(catalog_names, catalog):
    tables_sql, columns_sql = cs.current_comments_queries(catalog)
    current = cs.parse_current_comments(
        [(r[0], r[1]) for r in spark.sql(tables_sql).collect()],
        [(r[0], r[1], r[2]) for r in spark.sql(columns_sql).collect()],
    )
else:
    print(f"Catalog {catalog} does not exist yet: no table has comments to compare")
    current = {}
# The plan is in the order it must run: tables, ADD COLUMNS (never a DROP: the
# column-exists check is the comparison above), comment statements, and the views
# last. A SELECT * view is bound when it is created, so it has to be recreated
# after the tables changed.
plan = cs.build_setup_plan(catalog, sources, domains, current, config)

dry_run = dbutils.widgets.get("dry_run") == "true"
for sql in plan.statements:
    print(("DRY RUN  " if dry_run else "EXECUTE  ") + sql)
    if not dry_run:
        spark.sql(sql)

print(f"\n{plan.comments.up_to_date} comments up to date")
print(
    f"Done: {len(plan.statements)} statements for catalog {catalog}, "
    f"sources {sorted(sources)}, domains {sorted(domains)}, "
    f"{len(plan.migrations)} columns added, "
    f"{len(plan.comments.statements)} comment statements"
)