# Architecture and design decisions

This document records how the platform is built and **why**. Update it
whenever a design decision changes.

## 1. Scope

- **National data, provincial presentation.** Ingestion, bronze and silver
  keep data for all of Canada. Filtering to a province happens only in gold
  (as a `province_code` dimension) or in the website. Expanding to another
  province is a parameter change, not a pipeline rebuild.
- **Aggregates only.** No person-level datasets (for example, no scraping of
  physician registries into per-person records).

## 2. Unity Catalog layout

```
mapledata                          catalog
├── landing                        schema: raw files
│   ├── raw     (managed volume)   data files from the ingester
│   └── meta    (managed volume)   metadata cache + run logs
├── shared                         schema: dim_geography, dim_noc, dim_date
└── <domain>_bronze|silver|gold    one set of schemas per domain
```

- Created from zero by `src/platform/bootstrap_catalog` (idempotent).
- Domains come from `config/datasets.yml`.
- Managed volumes: data is used only inside Databricks.
- Metadata lives in its own volume, so Auto Loader on `raw` never reads it.

### Landing paths

```
/Volumes/mapledata/landing/raw/<source>/<dataset_key>/...
/Volumes/mapledata/landing/meta/<source>/<dataset_key>/package_show.json
/Volumes/mapledata/landing/meta/<source>/<dataset_key>/runs/<run_id>.json
```

## 3. Ingestion

### Pattern

Databricks' guide *Ingest data from an API in pipelines* lists three custom
patterns when no Lakeflow Connect managed connector exists. We use
**scheduled job + landing volume + Auto Loader** because:

- the sources publish **files** (CSV/XLSX), not record streams;
- raw files can be **replayed** if downstream logic changes;
- upstream quirks (open.canada.ca returns intermittent 504s) stay isolated
  in the ingester, away from transformation logic.

Lakeflow Connect has no CKAN connector. A Lakeflow Connect *custom connector*
(Beta) is a possible future upgrade, not a current goal.

### Registry-driven

Datasets are declared in `config/datasets.yml`. Adding a dataset is a
configuration change. The registry is validated at load time (required keys,
unknown keys, duplicate keys, safe names, valid regex).

### Update patterns

| Pattern | Publisher behaviour | Stored as |
|---|---|---|
| `new_file_per_period` | adds a new file each period (LMIA, quarterly) | `<resource_id>_<basename>` |
| `overwrite_in_place` | replaces the same file, may revise history (IRCC monthly) | `<resource_id>/<version>_<basename>` |

`<version>` is the resource's `last_modified` (fallback `metadata_modified`),
or `sha256-<12 hex>` of the content when no timestamp exists. Snapshots feed
**AUTO CDC FROM SNAPSHOT** downstream.

### Reliability rules

- HTTP retries with back-off for 429/5xx.
- If CKAN `package_show` fails, fall back to the last cached metadata.
- Never store HTML error pages or empty bodies.
- Atomic writes (hidden `_tmp_` file + rename): Auto Loader never sees partial files.
- One failing file or dataset never stops the others; the job fails at the
  end if anything failed, so job-level retries apply.

## 4. Transformation (planned)

- **Bronze:** Auto Loader into streaming tables, one table per dataset.
  Excel files are converted to CSV/Parquet first (Auto Loader has no Excel format).
- **Silver:** cleaning and conformed codes; **no province filter**.
  Record semantic changes as columns, e.g. LMIA `noc_version` (2011 vs 2021)
  and `includes_pr_only` (included from 2023Q4).
- **Gold:** metrics with a `province_code` dimension; materialized views and
  Unity Catalog metric views.
- Data quality via pipeline expectations (quarantine, drop or fail).

## 5. Shared dimensions

- `dim_geography`: Statistics Canada Standard Geographical Classification
  codes (province, census division, census subdivision, CMA/CA). Provincial
  groupings (for example, Manitoba regional health authorities) are separate
  mapping tables attached to it.
- `dim_noc`: NOC 2011 ↔ NOC 2021 concordance.
- `dim_date`.

## 6. Sources (planned adapters)

| Adapter | Sources |
|---|---|
| `ckan` (done) | open.canada.ca |
| `statcan` (planned) | Statistics Canada Web Data Service, full-table CSV, delta file |
| `cmhc` (to assess) | CMHC data tables |

## 7. Conventions

- Names: lower-case `snake_case` for catalogs, schemas, tables, volumes,
  registry keys.
- No data and no secrets in Git. Credentials go in Databricks secrets.
- Every chart on the website states its data year and source licence.
- Use current Databricks product names (Lakeflow Jobs, Lakeflow Spark
  Declarative Pipelines, Declarative Automation Bundles, AUTO CDC, Git folders).

## 8. Open items to verify

- IRCC permanent residents: confirm the resource name for `name_regex`
  and whether files are updated in place (check `last_modified`).
- Free Edition: confirm outbound access per domain (`opendata.brandon.ca`
  timed out; `data.winnipeg.ca` and `open.canada.ca` reachable).
- CIHI terms of use before publishing CIHI-derived charts.
