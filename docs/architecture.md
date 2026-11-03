# Architecture and design decisions

This document records how the platform is built and **why**. Update it
whenever a design decision changes.

Related: individual decisions with their context and alternatives are in
[decisions/](decisions/); what each data source is like (update patterns,
quirks, open questions) is in [sources/](sources/).

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
├── metadata                       schema: platform metadata
│   ├── ckan_dataset_snapshot      table: append-only discovery results
│   ├── ckan_dataset_current       view: latest row per ckan_id
│   ├── ckan_resource_snapshot     table: resources CKAN lists, per checked run
│   ├── ckan_resource_current      view: latest row per resource_id
│   └── ingestion_findings         table: post-ingestion check findings
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

Databricks' guide
[*Ingest data from an API in pipelines*](https://docs.databricks.com/aws/en/ldp/api-ingestion)
lists three custom patterns when no Lakeflow Connect managed connector exists.
We use its pattern 3, **scheduled job + landing volume + Auto Loader**: a
scheduled job writes files to a Unity Catalog volume and the pipeline ingests
them with Auto Loader. We chose it because:

- the sources publish **files** (CSV/XLSX), not record streams;
- raw files can be **replayed** if downstream logic changes;
- upstream quirks (open.canada.ca returns intermittent 504s) stay isolated
  in the ingester, away from transformation logic.

Lakeflow Connect has no CKAN connector. A Lakeflow Connect *custom connector*
(Beta) is a possible future upgrade, not a current goal.

### Registry-driven

Datasets are declared in `config/datasets.yml`. Adding a dataset is a
configuration change. The registry is validated at load time (required keys,
unknown keys, duplicate keys and CKAN ids, safe names, valid regex, valid
`status`, non-empty `purpose`).

Every entry has a `status` and a `purpose`:

| `status` | Meaning |
|---|---|
| `candidate` | registered, not ingested yet (under evaluation) |
| `active` | ingested by `run_ingestion` |
| `deprecated` | kept for the record, no longer ingested |
| `excluded` | reviewed and rejected; never ingested |

`purpose` records why the dataset is wanted and where it will be used; for an
`excluded` entry it states why the dataset was rejected. An `active` or
`excluded` entry cannot keep a placeholder purpose (one starting with `TODO`,
as written by discovery); candidates and deprecated entries may. An `excluded`
entry still counts as registered, so discovery stops proposing it. Only `active`
entries are ingested. The old `enabled` flag is gone; the parser
rejects it with a pointer to `status`.

### Dataset discovery

`tools/discover_datasets` searches a CKAN portal with one or more
comma-separated keywords (all result pages of each) and appends one row per
dataset to `metadata.ckan_dataset_snapshot`. A dataset found by several
keywords is recorded once per run, with all of them in `search_queries`. If
any search fails, the run fails and writes nothing. Rows of one run (all
keywords) share `run_id` and `discovered_at`; `is_registered` says whether
`config/datasets.yml` already lists the dataset (matched by CKAN id or name,
same portal, any `status`). Rows are never deduplicated, so the table is a
history; `metadata.ckan_dataset_current` keeps the newest row per `ckan_id`
(`QUALIFY row_number()`). Discovery only prints draft registry entries (as
`candidate`, with a placeholder `purpose` to fill in); adding a dataset stays a
manual, reviewed change to the registry.

`CREATE TABLE IF NOT EXISTS` does not alter an existing table, and the snapshot
holds history, so a schema change adds columns to the table and never drops it
(see "Changing a metadata table" in section 8).

### Update patterns

| Pattern | Publisher behaviour | Stored as |
|---|---|---|
| `new_file_per_period` | adds a new file each period (LMIA, quarterly) | `<resource_id>_<basename>` |
| `overwrite_in_place` | replaces the same file, may revise history (IRCC monthly) | `<resource_id>/<version>_<basename>` |

`<version>` is the resource's `last_modified`, and nothing else. When a
resource has no `last_modified`, the file is downloaded and `<version>` is
`sha256-<12 hex>` of its content; identical content is never stored twice.
Snapshots feed **AUTO CDC FROM SNAPSHOT** downstream.

`metadata_modified` is deliberately not a fallback: it can stay unchanged after
a file's content is replaced, which would skip the update without any sign.
The cost is that a resource without `last_modified` (the IRCC monthly datasets)
is downloaded again on every run, because the hash needs the bytes; a resource
with `last_modified` is still skipped from metadata alone.

Rationale and consequences: [ADR 0003](decisions/0003-snapshot-versioning.md).
Per-source behaviour: [open_canada.md](sources/open_canada.md),
[esdc_lmia.md](sources/esdc_lmia.md), [ircc_monthly.md](sources/ircc_monthly.md).

### Reliability rules

- HTTP retries with back-off for 429/5xx.
- If CKAN `package_show` fails, fall back to the last cached metadata.
- Never store HTML error pages or empty bodies.
- Atomic writes (hidden `_tmp_` file + rename): Auto Loader never sees partial files.
- One failing file or dataset never stops the others; the job fails at the
  end if anything failed, so job-level retries apply.

### Post-ingestion checks

`src/checks/ingestion_checks.py` (pure Python) and the notebook
`src/checks/run_checks` look at an ingestion run afterwards and report what
needs a human. They only read; ingestion behaviour is unchanged. Run the
notebook right after `run_ingestion`, for example as the next task of the same
job: the cached `package_show.json` it reads is overwritten by every
successful ingestion run, so a later run would hide what an earlier one saw.

**Inputs** (per dataset): the cached metadata and the run log under
`<meta_root>/<source>/<key>/`, the landed files, and the dataset's previous
snapshot.
**Outputs:** one row per CKAN resource in `metadata.ckan_resource_snapshot`
(with `selected` and `landed_path`), the findings in
`metadata.ingestion_findings`, and a Markdown report at
`<meta_root>/reports/<run_id>.md`. `metadata.ckan_resource_current` keeps the
latest row per `resource_id`; a removed resource stays in it with the `run_id`
of its last appearance.

| Check | Severity | What it flags |
|---|---|---|
| `download_failed` | ERROR | a failure in the run log, per resource or for the whole dataset |
| `metadata_stale` | WARN | the run used the cached metadata because a fresh fetch failed |
| `resource_removed` | WARN | a resource of the previous snapshot is no longer listed |
| `resource_added` | INFO | a resource the previous snapshot did not have (neither check runs on a first run) |
| `format_gap` | WARN | tables found only in a format the entry does not select (a lower bound, see below) |
| `missing_last_modified` | INFO | `overwrite_in_place` resources without a usable `last_modified`, versioned by hash ([ADR 0003](decisions/0003-snapshot-versioning.md)) |
| `archived` | WARN / INFO | "archived" in the dataset name or title, or in a selected resource's name or description: WARN when newly archived or on the first run, otherwise one aggregated INFO per dataset (see below) |
| `description_mentions_archived` | INFO | the dataset description mentions archived tables; the matching sentences are in `details_json` |
| `rounded_values` | INFO | "rounded" in a selected resource's name, description, or the head or tail of its landed CSV |

- **`format_gap` is a lower bound; no finding does not mean full coverage.**
  Pairing one table across formats by name is unreliable: on the active
  datasets, CSV and XLSX versions of a table rarely share a name or file name
  (permanent residents: 16 CSV and 18 XLSX resources, no matching pair). So
  nothing is paired. Among resources passing the entry's language and
  `name_regex` filters, if an unselected tabular format has `n_f` resources and
  the selected formats have `n_s`, at least `n_f - n_s` tables exist only in
  that format. The report repeats this note.
- **Aggregated findings.** `missing_last_modified` and `rounded_values` give
  one finding per dataset; the affected resource count and `resource_id` list
  are in `details_json`. `rounded_values` records where each resource matched:
  `name`, `description`, `file_head` or `file_tail`. The end of the file is read
  too because a footnote can sit there; the tail reader skips the region the
  head reader covers, so a match is not reported twice.
- **`archived` reports changes, not states.** "Archived" means no longer
  updated, not no value, and archived resources are landed on purpose
  ([ADR 0005](decisions/0005-prefer-csv-land-excel-too.md)). A WARN is raised
  when a resource (or the dataset name or title) becomes archived compared with
  the previous snapshot, and for everything archived on the first run.
  Otherwise there is one INFO per dataset, "N archived resources landed,
  unchanged", with the `resource_id` list in `details_json`. To compare, the
  snapshot stores `archived` (per resource) and `dataset_archived` (the same on
  every row of a dataset), because the previous state cannot be rebuilt from
  names alone.
- **A description that mentions archived tables is not an archived dataset.**
  `dataset_archived` is set from the dataset name and title only. ESDC's
  description says "Table 5, 8, 17, and 24 will no longer be updated but will
  remain as archived tables": some tables stopped, not the dataset, and the
  resource names do not say archived, so the resource-level check cannot see
  them. `description_mentions_archived` reports it as a separate INFO, with the
  matching sentence in `details_json`, on every run. Rows written when
  `dataset_archived` still looked at the description keep their value; a True
  that is now False is not reported.
- **`rounded_marker`.** The snapshot has `rounded_marker`, True when a marker
  was found in the resource name, description, file head or file tail. **False
  means no marker was found, not that the values are exact**: IRCC marks some
  tables and not others, and an unmarked table may be rounded too. Bronze and
  gold must not treat `rounded_marker = false` as safe to sum. Rows written
  before the column existed hold NULL, meaning not checked; they are not
  backfilled.
- **Failing.** The notebook fails only when there is at least one ERROR, after
  the tables and the report are written.
- **Re-running a check.** Checking the same `run_id` again must not duplicate
  rows. The notebook writes with Delta `replaceWhere` (predicate: this
  `run_id` and the checked datasets), which replaces exactly those rows in one
  atomic commit
  ([selective overwrite](https://docs.databricks.com/aws/en/delta/selective-overwrite)).
  A MERGE needs a unique key and findings have none (`resource_id` can be
  null, one resource can have several findings); delete-then-append is two
  commits and a failure in between loses rows. When there is nothing to write,
  the notebook runs a `DELETE` with the same predicate, because the page
  documents the empty-source case only for SQL `REPLACE WHERE`.
- **Validated in Databricks, and what is not.** Validated on 2026-11-02 and 03
  (run `20261103T004037Z`): the snapshot and findings are written; all 39 IRCC
  CSV files were read and neither a file head nor a file tail matched, so every
  rounded marker comes from the resource name; `rounded_marker` was written for
  every row of the run (0 NULL) and the marked counts match the findings
  (permanent residents 16, study permits 3, TR→PR 4, work permits 1);
  `description_mentions_archived` was reported for the annual and quarterly ESDC
  statistics, `dataset_archived` is no longer set for them, and there were no
  WARNs. Not validated yet: the view-order fix of `build_setup_plan`, which
  needs a bootstrap run after a schema change.

## 4. Transformation (planned)

- **Bronze:** Auto Loader into streaming tables, one table per dataset.
  Bronze must join `metadata.ckan_resource_current` on `resource_id` to carry
  the resource name and the `rounded_marker` and `archived` flags, because
  those facts are not in the file content. The resource id is the prefix of the
  landed path (`<resource_id>_<name>` or `<resource_id>/<version>_<name>`, see
  [ADR 0003](decisions/0003-snapshot-versioning.md)). Gold must not treat
  `rounded_marker = false` as "safe to sum" (see the post-ingestion checks).
  Excel files are converted to CSV/Parquet first (Auto Loader has no Excel format).
  CSV is preferred; Excel is also landed in raw when a table has no CSV, and
  bronze uses the CSV when both exist
  ([ADR 0005](decisions/0005-prefer-csv-land-excel-too.md)).
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

## 8. Metadata model

Each kind of metadata has one home, so nothing is documented twice.

| Question | Where it lives | How it is kept |
|---|---|---|
| What data exists outside? | `metadata.ckan_dataset_snapshot` (append-only, safe to re-run) and the view `metadata.ckan_dataset_current` (latest state) | written by `tools/discover_datasets` |
| What do the registered datasets publish, and did the run go well? | `metadata.ckan_resource_snapshot`, `metadata.ckan_resource_current`, `metadata.ingestion_findings` | written by `src/checks/run_checks` after each ingestion run |
| Which of it do we use, and why? | `status` and `purpose` in `config/datasets.yml` | reviewed in Git |
| Where does data flow? | Unity Catalog lineage | captured automatically |
| What do the fields mean? | Unity Catalog table comments and column comments | written in the pipeline code |
| Domain, source, licence, where it is published | Unity Catalog tags | written in the pipeline code |

Tag keys such as `domain`, `source`, `licence` and `publication` are proposed
names; fix them when the first tables are built.

### Rules that follow from the platform

- **Lineage is automatic.** Unity Catalog captures lineage, down to columns,
  for queries run through Spark DataFrame or Databricks SQL interfaces
  ([Lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)).
  Nothing to maintain, but it is **not preserved when a catalog, schema, table,
  view or column is renamed**. Announce a rename before doing it.
- **Comments and tags live in code.** Set them in the DDL or pipeline code, not
  only in the Databricks UI, so a rebuild does not lose them. Tags use
  `ALTER ... SET TAGS` (Runtime 13.3+) or `SET TAG` (16.1+); columns are tagged one
  per statement; keys are case sensitive and cannot contain
  `. , - = / :` ([Tags](https://docs.databricks.com/aws/en/database-objects/tags)).
- **Comment syntax.** `CREATE TABLE ... USING` takes `COMMENT '...'` after a
  column's type and a table-level `COMMENT '...'` clause
  ([CREATE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using)).
  `catalog_setup.py` uses it when it creates the tables in the `metadata`
  schema. Because `CREATE TABLE IF NOT EXISTS` leaves an existing table alone,
  comments that were added or changed later are applied with
  `COMMENT ON TABLE ... IS '...'` and
  `ALTER TABLE ... ALTER COLUMN <col> COMMENT '...'`, without dropping a table
  ([COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment),
  [ALTER TABLE column](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column)).
  Both need the `MODIFY` privilege on the table. Keep apostrophes out of
  comment text; the comments live in `catalog_setup.py` and a test checks every
  table and column has one.
- **Comments are applied only where they differ.** The Databricks pages do not
  say what re-applying an unchanged comment does. Measured on 2026-11-02:
  **it still creates a new Delta version**. A second `bootstrap_catalog` run
  took `ckan_dataset_snapshot` from version 20 to version 37 (one
  `SET TBLPROPERTIES` and 16 `CHANGE COLUMN` operations), although nothing had
  changed. So `bootstrap_catalog` first reads the current comments from the
  catalog's `information_schema.tables` and `information_schema.columns`
  (`COMMENT` is a nullable `STRING` in both; they list only objects the user
  can access,
  [tables](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables),
  [columns](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/columns)),
  and `build_comment_statements` returns only the differences (exact string
  comparison; no comment counts as a difference). The builder is pure: the
  current comments are its input, so it is unit-tested without Spark. A table
  that does not exist yet gets its comments inline in `CREATE TABLE`, and a
  column an existing table lacks is added with its comment (see "Changing a
  metadata table" below) instead of being commented. Only a catalog that does
  not exist yet is skipped when reading comments; any other error is raised.
  With
  `dry_run` the notebook prints only the statements that would run, then
  "N comments up to date".
- **Changing a metadata table.** The metadata tables accumulate history that
  later runs compare against (for example `ckan_resource_snapshot` feeds the
  added, removed and archived checks). **Add columns with idempotent
  `ALTER TABLE ... ADD COLUMNS`, guarded by a column-exists check; never DROP a
  metadata table that holds history.** The guard is needed because the
  documented `ADD COLUMNS` grammar has no `IF NOT EXISTS` and requires the name
  to be unique in the table; added columns are NULL for existing rows, and the
  statement needs the `MODIFY` privilege
  ([ALTER TABLE column](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column)).
  Also add the column to the `CREATE TABLE` definition, so new workspaces get
  it, and define it last so a migrated table keeps the column order of a new
  one. `bootstrap_catalog` does the migration: it reads the columns of the
  existing tables from `information_schema.columns` (that read is the
  column-exists check), `build_comment_statements` lists the columns the code
  defines that a table lacks, and `build_add_column_statements` turns them into
  `ADD COLUMNS` statements with the column comment inline. A second run finds
  nothing missing and adds nothing. Existing rows hold NULL and are not
  backfilled. The first column added this way is `rounded_marker` of
  `ckan_resource_snapshot`.
- **Views are recreated last.** Observed 2026-11-02: after one bootstrap,
  `ckan_resource_snapshot` had `rounded_marker` and the new `dataset_archived`
  comment, but the view `ckan_resource_current` had neither, because
  `CREATE OR REPLACE VIEW` ran before `ADD COLUMNS` and `ALTER COLUMN`; a
  second bootstrap fixed it. **`SELECT *` views are bound when created;
  recreate them after schema changes.** So `build_setup_plan` returns the
  bootstrap statements in this order: tables, `ADD COLUMNS`, comment
  statements, then the views. A test asserts that every view statement comes
  after every statement that creates or alters a table, and that putting the
  alters after `build_ddl` would reproduce the problem.
- **Not confirmed in the docs.** The Free Edition limitations page does not
  mention lineage or tags, but does not say they are supported either: check in
  the workspace.

## 9. Open items to verify

- IRCC datasets: all of them have `name_regex` removed, so every CSV table
  lands under one dataset key. Bronze must tell the tables apart by resource id.
- IRCC candidates (asylum claimants, Express Entry, glossaries): assumed to
  behave like the four active IRCC monthly datasets (updated in place, no
  `last_modified`). Confirm when each becomes active.
- Free Edition: confirm outbound access per domain (`opendata.brandon.ca`
  timed out; `data.winnipeg.ca` and `open.canada.ca` reachable).
- CIHI terms of use before publishing CIHI-derived charts.
