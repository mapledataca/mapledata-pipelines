# mapledata-pipelines

Data pipelines on Databricks that turn Canadian open data into national and
provincial insights for [mapledata.io](https://mapledata.io), starting with
Manitoba.

Data is collected at **national scope**; the website starts by presenting
Manitoba and expands to other provinces over time.

## Domains (roadmap)

| Order | Domain | Main sources | Status |
|---|---|---|---|
| 1 | Immigration | open.canada.ca (ESDC LMIA, IRCC monthly updates) | In progress |
| 2 | Cost of living | Statistics Canada (CPI) | Planned |
| 3 | Housing | Statistics Canada, CMHC | Planned |
| 4 | Health | CIHI, Statistics Canada, Open Database of Healthcare Facilities | Planned |
| 5 | Education | Statistics Canada | Planned |

## Architecture at a glance

```
open.canada.ca (CKAN API) ──► ckan_ingest (registry-driven, idempotent)
                                   │
                                   ▼
            /Volumes/mapledata/landing/raw/<source>/<dataset>/...   raw files
                                   │  Auto Loader
                                   ▼
            <domain>_bronze ──► <domain>_silver ──► <domain>_gold   Lakeflow pipelines
                                   ▲
                               shared (dim_geography, dim_noc, dim_date)
```

Orchestrated by Lakeflow Jobs. Full design and decisions:
[docs/architecture.md](docs/architecture.md). Why each major choice was made:
[docs/decisions/](docs/decisions/). What each data source is like (update
patterns, quirks, open questions): [docs/sources/](docs/sources/).

## Repository layout

```
config/datasets.yml              dataset registry (add datasets here)
src/ingest/ckan_ingest.py        generic CKAN ingester
src/ingest/ckan_discover.py      search CKAN, build snapshot rows, draft entries
src/ingest/run_ingestion.py      Databricks notebook: run ingestion (job task)
src/platform/catalog_setup.py    builds the Unity Catalog DDL
src/platform/bootstrap_catalog.py Databricks notebook: create catalog from zero
tools/                           development notebooks (run_tests, discover_datasets)
tests/                           unit tests (no network, no Spark)
docs/architecture.md             design and conventions
docs/decisions/                  architecture decision records (ADRs)
docs/sources/                    per-source notes: update patterns, quirks, open questions
CLAUDE.md                        instructions for AI coding agents
```

## Getting started

1. Clone this repository into a Databricks Git folder (under your user folder).
2. Run `src/platform/bootstrap_catalog` once (set `dry_run=true` first to
   review the statements). It creates the catalog, schemas, volumes and the
   `metadata` snapshot table and view used by `tools/discover_datasets`.
3. Run the tests from a notebook at the repository root:
   ```python
   %pip install pytest
   ```
   ```python
   import sys, pytest
   sys.dont_write_bytecode = True
   pytest.main(["-v", "-p", "no:cacheprovider", "tests/"])
   ```
4. Run `src/ingest/run_ingestion` (or schedule it as a Lakeflow Job task).
   Use the `domains` widget to limit the run, e.g. `immigration`.

## Adding a dataset

Add an entry to `config/datasets.yml` (see the comments in that file). Every
entry needs a `status` (`candidate`, `active` or `deprecated`) and a `purpose`;
only `active` entries are ingested. Run the tests, then run ingestion for that
key. If the domain is new, re-run `bootstrap_catalog` to create its schemas.
`tools/discover_datasets` can search CKAN and draft entries for you.

## Data sources and licences

Raw data is **not** stored in this repository. Published results follow each
source's licence:

| Source | Licence |
|---|---|
| open.canada.ca (ESDC, IRCC), Statistics Canada | [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada) |
| City of Brandon | City of Brandon Open Data Licence — "Contains public sector Datasets made available under the City of Brandon's Open Data Licence" |
| DataMB (Province of Manitoba) | Open Government Licence – Manitoba |
| CIHI | [CIHI terms of use](https://www.cihi.ca/en/about-cihi/terms-of-use) (review before publishing) |

## Licence

Code: [MIT](LICENSE).
