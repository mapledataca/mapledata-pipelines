# Databricks notebook source
# MAGIC %md
# MAGIC # Discover CKAN datasets
# MAGIC Search a CKAN portal, fetching every result page, and record what was
# MAGIC found in `<catalog>.metadata.ckan_dataset_snapshot` (append-only; the view
# MAGIC `ckan_dataset_current` keeps the latest row per dataset). Datasets missing
# MAGIC from `config/datasets.yml` get a draft registry entry printed below.
# MAGIC Nothing is written to the registry: review each draft, then paste it by hand.
# MAGIC
# MAGIC Run `src/platform/bootstrap_catalog` first so the snapshot table exists.
# MAGIC
# MAGIC Widgets:
# MAGIC - `query`: comma-separated search keywords, e.g. `permanent residents, LMIA`.
# MAGIC   Each keyword is searched in turn; a dataset found by several keywords is
# MAGIC   recorded once, with all of them in `search_queries`. If any search fails,
# MAGIC   the whole run fails and nothing is written.
# MAGIC - `domain`: domain for the draft entries, e.g. `immigration`
# MAGIC - `page_size`: results fetched per request
# MAGIC - `base_url`: CKAN Action API base URL
# MAGIC - `catalog`: Unity Catalog catalog holding the `metadata` schema
# MAGIC - `max_drafts`: most drafts to print (0 prints none)
# MAGIC - `save_snapshot`: `true` appends the results to the snapshot table

# COMMAND ----------

dbutils.widgets.text("query", "")
dbutils.widgets.text("domain", "immigration")
dbutils.widgets.text("page_size", "100")
dbutils.widgets.text("base_url", "https://open.canada.ca/data/api/action")
dbutils.widgets.text("catalog", "mapledata")
dbutils.widgets.text("max_drafts", "20")
dbutils.widgets.dropdown("save_snapshot", "true", ["true", "false"])
dbutils.widgets.text("user_agent", "mapledata.io pipeline")

# COMMAND ----------

import sys
from datetime import datetime, timezone
from pathlib import Path

# Walk up from this notebook's folder to the repo root (it contains config/datasets.yml).
here = Path.cwd()
root = next(p for p in [here, *here.parents] if (p / "config" / "datasets.yml").exists())
sys.path.insert(0, str(root / "src" / "ingest"))

import ckan_discover as cd
import ckan_ingest as ci

queries = cd.parse_queries(dbutils.widgets.get("query"))
assert queries, "Set the 'query' widget first."
base_url = dbutils.widgets.get("base_url")

specs = ci.load_registry(str(root / "config" / "datasets.yml"))
session = ci.build_session(dbutils.widgets.get("user_agent"))
packages, matches = cd.search_many(
    session, queries, base_url=base_url, page_size=int(dbutils.widgets.get("page_size"))
)

for query in queries:
    found = sum(query in hit for hit in matches.values())
    print(f"{query!r}: {found} datasets")
print(f"{len(packages)} datasets after removing duplicates across keywords")

# One timestamp and run id for the whole run, shared by every row.
discovered_at = datetime.now(timezone.utc)
rows = cd.build_snapshot_rows(
    packages,
    specs,
    base_url,
    matches,
    discovered_at=discovered_at,
    run_id=ci._utc_run_id(lambda: discovered_at),
)

df = spark.createDataFrame(rows, schema=cd.SNAPSHOT_SCHEMA_DDL)
display(df)

# COMMAND ----------

if dbutils.widgets.get("save_snapshot") == "true":
    table = f"{dbutils.widgets.get('catalog')}.metadata.ckan_dataset_snapshot"
    df.write.mode("append").saveAsTable(table)
    print(f"Appended {len(rows)} rows to {table}")
else:
    print("save_snapshot=false: nothing written")

# COMMAND ----------

drafts, left_out = cd.unregistered_drafts(
    packages,
    cd.registered_ids(specs, base_url),
    dbutils.widgets.get("domain"),
    int(dbutils.widgets.get("max_drafts")),
)
for draft in drafts:
    print(draft, end="\n\n")
print(f"{len(drafts)} drafts printed, {left_out} unregistered datasets not shown")
