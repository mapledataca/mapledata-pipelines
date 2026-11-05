# Databricks notebook source
# MAGIC %md
# MAGIC # Run CKAN ingestion
# MAGIC Reads `config/datasets.yml` and downloads new files into the landing volume.
# MAGIC Use as a notebook task in a Lakeflow Job. Parameters (job or widgets):
# MAGIC - `domains`: comma-separated domains to run, empty = all active datasets
# MAGIC - `keys`: comma-separated dataset keys, empty = all
# MAGIC - `catalog`: catalog holding the landing volumes; empty = `config/platform.yml`.
# MAGIC   The volume paths are derived from it.

# COMMAND ----------

dbutils.widgets.text("domains", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("user_agent", "mapledata.io pipeline")

# COMMAND ----------

import sys
from pathlib import Path

import ckan_ingest as ci  # this notebook sits next to ckan_ingest.py

# The repository root is two levels up from src/ingest/.
repo_root = Path.cwd().parents[1]
sys.path.insert(0, str(repo_root / "src" / "platform"))

import platform_config as pc

config = pc.load_platform_config(str(repo_root / "config" / "platform.yml"))
catalog = config.resolve_catalog(dbutils.widgets.get("catalog"))


def _csv(name):
    """Comma-separated widget value -> list, or None when empty."""
    value = dbutils.widgets.get(name).strip()
    return [v.strip() for v in value.split(",") if v.strip()] or None


specs = ci.load_registry(str(repo_root / "config" / "datasets.yml"))
settings = ci.RunSettings(
    landing_root=config.raw_volume_path(catalog),
    meta_root=config.meta_volume_path(catalog),
    pause_seconds=1.0,
)
session = ci.build_session(dbutils.widgets.get("user_agent"))

report = ci.run_ingestion(
    specs, settings, session, domains=_csv("domains"), keys=_csv("keys")
)
print(report.summary())

# Fail the task when anything failed, so job-level retries and alerts kick in.
assert report.ok, "Ingestion had failures; see summary above and the run logs."