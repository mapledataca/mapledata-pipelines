# Databricks notebook source
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

specs = ci.load_registry(str(repo_root / "config" / "datasets.yml"))
domains = {s.domain for s in specs}  # include disabled datasets: schemas are cheap
statements = cs.build_ddl(dbutils.widgets.get("catalog"), domains)

dry_run = dbutils.widgets.get("dry_run") == "true"
for sql in statements:
    print(("DRY RUN  " if dry_run else "EXECUTE  ") + sql)
    if not dry_run:
        spark.sql(sql)

print(f"\nDone: {len(statements)} statements for domains {sorted(domains)}")
