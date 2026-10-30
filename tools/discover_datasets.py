# Databricks notebook source
# MAGIC %md
# MAGIC # Discover CKAN datasets
# MAGIC Search a CKAN portal and draft `config/datasets.yml` entries. Nothing is
# MAGIC written to the registry: review each draft, then paste it by hand.
# MAGIC
# MAGIC Widgets:
# MAGIC - `query`: search text, e.g. `permanent residents`
# MAGIC - `domain`: domain for the draft entries, e.g. `immigration`
# MAGIC - `rows`: maximum number of results
# MAGIC - `base_url`: CKAN Action API base URL

# COMMAND ----------

dbutils.widgets.text("query", "")
dbutils.widgets.text("domain", "immigration")
dbutils.widgets.text("rows", "10")
dbutils.widgets.text("base_url", "https://open.canada.ca/data/api/action")
dbutils.widgets.text("user_agent", "mapledata.io pipeline")

# COMMAND ----------

import sys
from pathlib import Path

# Walk up from this notebook's folder to the repo root (it contains config/datasets.yml).
here = Path.cwd()
root = next(p for p in [here, *here.parents] if (p / "config" / "datasets.yml").exists())
sys.path.insert(0, str(root / "src" / "ingest"))

import ckan_discover as cd
import ckan_ingest as ci

query = dbutils.widgets.get("query").strip()
assert query, "Set the 'query' widget first."

session = ci.build_session(dbutils.widgets.get("user_agent"))
packages = cd.search_packages(
    session,
    query,
    base_url=dbutils.widgets.get("base_url"),
    rows=int(dbutils.widgets.get("rows")),
)

display(spark.createDataFrame([cd.summarise_package(p) for p in packages]))

for package in packages:
    print(cd.registry_entry_yaml(package, dbutils.widgets.get("domain")), end="\n\n")
