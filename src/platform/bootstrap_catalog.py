# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bootstrap the mapledata catalog
# MAGIC Creates the catalog, schemas and volumes from zero. Safe to re-run: every
# MAGIC statement uses `IF NOT EXISTS`. Domains come from `config/datasets.yml`.
# MAGIC
# MAGIC Run this once per workspace, and again after adding a dataset whose
# MAGIC domain is new.

# COMMAND ----------

dbutils.widgets.text("catalog", "mapledata")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])

# COMMAND ----------

import sys
from pathlib import Path

# This notebook lives in src/platform/; the repository root is two levels up.
repo_root = Path.cwd().parents[1]
sys.path.insert(0, str(repo_root / "src" / "ingest"))

import catalog_setup as cs
import ckan_ingest as ci

catalog = dbutils.widgets.get("catalog")
specs = ci.load_registry(str(repo_root / "config" / "datasets.yml"))
domains = {s.domain for s in specs}  # include non-active datasets: schemas are cheap
statements = cs.build_ddl(catalog, domains)

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
comments = cs.build_comment_statements(catalog, current)
# A column the code defines but an existing table lacks is added, never by
# dropping the table: the column-exists check is the comparison above.
migrations = cs.build_add_column_statements(catalog, comments.missing_columns)

dry_run = dbutils.widgets.get("dry_run") == "true"
for sql in statements + migrations + comments.statements:
    print(("DRY RUN  " if dry_run else "EXECUTE  ") + sql)
    if not dry_run:
        spark.sql(sql)

print(f"\n{comments.up_to_date} comments up to date")
print(
    f"Done: {len(statements)} statements for domains {sorted(domains)}, "
    f"{len(migrations)} columns added, {len(comments.statements)} comment statements"
)