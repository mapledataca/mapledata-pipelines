# Databricks notebook source
# MAGIC %md
# MAGIC # Post-ingestion checks
# MAGIC Looks at what CKAN publishes for each active dataset, what the ingestion run
# MAGIC did and what landed, and reports what needs attention. Run it right after
# MAGIC `run_ingestion` (for example as the next task of the same job): the cached
# MAGIC metadata is overwritten by every successful ingestion run.
# MAGIC
# MAGIC Appends to `<catalog>.metadata.ckan_resource_snapshot` and
# MAGIC `<catalog>.metadata.ingestion_findings`, prints a summary, writes a Markdown
# MAGIC report to `<meta_root>/reports/<run_id>.md`, and fails only when there is at
# MAGIC least one ERROR finding. Run `src/platform/bootstrap_catalog` first.
# MAGIC
# MAGIC Checking the same run again replaces that run's rows (Delta `replaceWhere`,
# MAGIC one atomic commit), so it never duplicates them.
# MAGIC
# MAGIC Widgets:
# MAGIC - `run_id`: run to check; empty = the latest run found in the run logs
# MAGIC - `domains`, `keys`: comma-separated filters, empty = all active datasets
# MAGIC - `catalog`: Unity Catalog catalog holding the `metadata` schema and the landing
# MAGIC   volumes; empty = `config/platform.yml`. The volume paths used by the
# MAGIC   ingestion are derived from it.

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("domains", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("catalog", "")

# COMMAND ----------

import sys
from datetime import datetime, timezone
from pathlib import Path

# This notebook lives in src/checks/; the repository root is two levels up.
repo_root = Path.cwd().parents[1]
sys.path.insert(0, str(repo_root / "src" / "ingest"))
sys.path.insert(0, str(repo_root / "src" / "checks"))
sys.path.insert(0, str(repo_root / "src" / "platform"))

import ckan_ingest as ci
import ingestion_checks as ic
import platform_config as pc


def _csv(name):
    """Comma-separated widget value -> list, or None when empty."""
    value = dbutils.widgets.get(name).strip()
    return [v.strip() for v in value.split(",") if v.strip()] or None


config = pc.load_platform_config(str(repo_root / "config" / "platform.yml"))
catalog = config.resolve_catalog(dbutils.widgets.get("catalog"))
meta_root = config.meta_volume_path(catalog)
landing_root = config.raw_volume_path(catalog)

specs = ci.select_specs(
    ci.load_registry(str(repo_root / "config" / "datasets.yml")),
    domains=_csv("domains"),
    keys=_csv("keys"),
)
run_id = dbutils.widgets.get("run_id").strip() or ic.latest_run_id(meta_root, specs)
assert run_id, f"No run logs found under {meta_root}; run the ingestion first."
ic.validate_run_id(run_id)
checked_at = datetime.now(timezone.utc)

# A dataset without a log for this run took no part in it: leave it out.
run_logs = {s.key: ic.load_run_log(meta_root, s, run_id) for s in specs}
run_logs = {key: run_log for key, run_log in run_logs.items() if run_log}
checked = [s for s in specs if s.key in run_logs]
skipped = [s.key for s in specs if s.key not in run_logs]
metadata = {s.key: ic.load_metadata(meta_root, s) for s in checked}

snapshot_table = ic.qualified_table(catalog, pc.METADATA_SCHEMA, "ckan_resource_snapshot")
findings_table = ic.qualified_table(catalog, pc.METADATA_SCHEMA, "ingestion_findings")

# The previous snapshot of each dataset, for added and removed resources.
previous = {}
for s in checked:
    rows = spark.table(snapshot_table).where(f"dataset_key = '{s.key}'").collect()
    previous[s.key] = ic.select_previous_rows([r.asDict() for r in rows], run_id)

# COMMAND ----------

# One pass: the findings and the snapshot rows share a single scan of the landed
# files (the start and the end of each CSV), so each file is read once.
findings, snapshot_rows = ic.run_all(
    run_id,
    checked_at,
    checked,
    metadata,
    run_logs,
    previous_rows_by_key=previous,
    read_head=ic.file_head_reader(),
    read_tail=ic.file_tail_reader(),
    landing_root=landing_root,
)

# COMMAND ----------


def replace_run_rows(table, rows, schema):
    """Replace this run's rows for the checked datasets, atomically."""
    predicate = ic.replace_where_predicate(run_id, [s.key for s in checked])
    if rows:
        (
            spark.createDataFrame(rows, schema=schema)
            .write.mode("overwrite")
            .option("replaceWhere", predicate)
            .saveAsTable(table)
        )
    else:
        # Nothing to write; just remove rows left by an earlier check of this run.
        spark.sql(f"DELETE FROM {table} WHERE {predicate}")


if checked:
    replace_run_rows(snapshot_table, snapshot_rows, ic.SNAPSHOT_SCHEMA_DDL)
    replace_run_rows(findings_table, [f.as_row() for f in findings], ic.FINDINGS_SCHEMA_DDL)

# COMMAND ----------

summary = ic.summarise(run_id, checked_at, findings, [s.key for s in checked], skipped)
print("\n".join(ic.summary_lines(summary)))

report = ic.render_markdown(findings, summary)
path = ic.report_path(meta_root, run_id)
ci._atomic_write_bytes(path, report.encode("utf-8"))
print(f"Report written to {path}")

# Fail only on ERROR findings, after the tables and the report are written.
errors = summary["by_severity"][ic.ERROR]
assert errors == 0, f"{errors} ERROR finding(s); see {path}"
