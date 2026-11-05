# CLAUDE.md

Guidance for AI coding agents (Claude Code) working in this repository.
Read `README.md` and `docs/architecture.md` before making changes.

## Project

Databricks data platform for mapledata.io: Canadian open data at national
scope, presented first for Manitoba. Domains are built one at a time:
immigration → cost of living → housing → health → education.

## Commands

```bash
pip install -r requirements-dev.txt   # local dev dependencies (runtime + pytest, pytest-cov)
pytest -v tests/                      # run all unit tests (no network, no Spark)
pytest --cov=src --cov-report=term-missing tests/   # optional coverage (notebooks show 0%)
CKAN_LIVE_TEST=1 pytest -v -k live tests/   # optional live smoke test
```

All tests must pass before committing.

## Repository map

- `config/datasets.yml`: dataset registry. Adding a dataset = adding an entry.
- `config/platform.yml`: catalog name and schema conventions; read by `src/platform/platform_config.py`.
- `src/ingest/ckan_ingest.py`: generic CKAN ingester (pure Python, injected session).
- `src/ingest/ckan_discover.py`: searches CKAN and drafts registry entries (used by `tools/discover_datasets.py`).
- `src/ingest/run_ingestion.py`: Databricks notebook (source format) used as a job task.
- `src/checks/ingestion_checks.py`: post-ingestion checks (pure Python): resource snapshot rows, findings, Markdown report.
- `src/checks/run_checks.py`: Databricks notebook that runs the checks after ingestion and writes the metadata tables and report.
- `src/platform/platform_config.py`: loads `config/platform.yml`; derives schema names and volume paths.
- `src/platform/catalog_setup.py`: builds idempotent Unity Catalog DDL.
- `src/platform/bootstrap_catalog.py`: Databricks notebook that executes the DDL.
- `tools/`: development Databricks notebooks (e.g. `run_tests`, `discover_datasets`); not run as job tasks.
- `docs/sources/`: one file per data source: update pattern, quirks (with date and evidence), open questions.
- `docs/questions.md`: analytical questions per domain; datasets are judged against them.
- `docs/registry_review.md`: how to review discovered datasets (criteria, rounds, exclusion reason codes).
- `docs/backlog.md`: temporary backlog (may move to GitHub Issues).
- `docs/decisions/`: architecture decision records (ADRs); see its `README.md` for the list.
- `tests/`: pytest suites; `conftest.py` puts `src/ingest`, `src/platform` and `src/checks` on `sys.path`.

## Rules

1. **Keep logic testable.** Put logic in plain Python modules; keep Databricks
   notebooks thin (widgets, wiring, `spark.sql`). Network access goes through
   an injected session so tests can use fakes. Add or update tests with every change.
   Follow ADR 0006: keep parsing and business rules in portable Python/SQL.
2. **Idempotent and safe ingestion.** Never store HTML error pages; write
   files atomically; one failure must not stop other datasets; preserve the
   metadata-cache fallback.
3. **National scope.** Do not filter by province in ingestion, bronze or
   silver. Province filtering belongs in gold or the website.
4. **Registry first.** Prefer configuration in `config/datasets.yml` over
   new code paths. Validate new registry fields in `parse_registry`. Every
   entry needs a `status` (`candidate`, `active`, `deprecated`, `excluded`) and a
   `purpose`; only `active` entries are ingested. When reviewing datasets,
   follow `docs/registry_review.md`; record new questions in `docs/questions.md`.
5. **No data, no secrets in Git.** Credentials come from Databricks secrets.
   Do not hard-code personal emails or tokens.
6. **Unity Catalog names** are lower-case `snake_case` and layer first:
   `landing` (volumes `raw` and `meta`), `metadata`, `shared`, `bronze_<source>`,
   `silver_<domain>`, `gold_<domain>`. The catalog and these conventions are in
   `config/platform.yml`; never hard-code a catalog name or a `/Volumes/` path
   (ADR 0007).
7. **Comments and docs in English.** Update `docs/architecture.md` when a
   design decision changes.
8. **Notebooks are source-format `.py`.** Write every Databricks notebook as a
   `.py` file in Databricks source format (first line `# Databricks notebook source`,
   `# MAGIC %md` for markdown, `# COMMAND ----------` between cells), like
   `src/ingest/run_ingestion.py`. Do not create `.ipynb` files.
9. **Metadata lives in code.** Table comments, column comments and Unity
   Catalog tags must be written in the DDL or pipeline code, never only in the
   Databricks UI. Renaming a catalog, schema, table, view or column loses
   its lineage, so say so before doing it.
10. **Record quirks and decisions.** When you find a new source quirk, update
    `docs/sources/<source>.md`. When a design decision changes, add a new ADR
    (never edit an accepted ADR's decision; supersede it).

## Databricks guidance

- Use current product names: Lakeflow Jobs, Lakeflow Spark Declarative
  Pipelines (streaming tables, materialized views, expectations), AUTO CDC,
  Auto Loader, Declarative Automation Bundles, Git folders, Unity Catalog volumes.
- Ground Databricks behaviour in official docs (docs.databricks.com) and cite
  the page. If a behaviour cannot be confirmed in the docs, say so explicitly
  instead of guessing.
- The development workspace is Databricks **Free Edition**: serverless compute
  only, outbound internet restricted to allowed domains, non-commercial use.
  Check feature availability before relying on it.
