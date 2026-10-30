# CLAUDE.md

Guidance for AI coding agents (Claude Code) working in this repository.
Read `README.md` and `docs/architecture.md` before making changes.

## Project

Databricks data platform for mapledata.io: Canadian open data at national
scope, presented first for Manitoba. Domains are built one at a time:
immigration → cost of living → housing → health → education.

## Commands

```bash
pip install requests pyyaml pytest   # local dev dependencies
pytest -v tests/                      # run all unit tests (no network, no Spark)
CKAN_LIVE_TEST=1 pytest -v -k live tests/   # optional live smoke test
```

All tests must pass before committing.

## Repository map

- `config/datasets.yml`: dataset registry. Adding a dataset = adding an entry.
- `src/ingest/ckan_ingest.py`: generic CKAN ingester (pure Python, injected session).
- `src/ingest/run_ingestion.py`: Databricks notebook (source format) used as a job task.
- `src/platform/catalog_setup.py`: builds idempotent Unity Catalog DDL.
- `src/platform/bootstrap_catalog.py`: Databricks notebook that executes the DDL.
- `tools/`: development Databricks notebooks (e.g. `run_tests`, `discover_datasets`); not run as job tasks.
- `tests/`: pytest suites; `conftest.py` puts `src/ingest` and `src/platform` on `sys.path`.

## Rules

1. **Keep logic testable.** Put logic in plain Python modules; keep Databricks
   notebooks thin (widgets, wiring, `spark.sql`). Network access goes through
   an injected session so tests can use fakes. Add or update tests with every change.
2. **Idempotent and safe ingestion.** Never store HTML error pages; write
   files atomically; one failure must not stop other datasets; preserve the
   metadata-cache fallback.
3. **National scope.** Do not filter by province in ingestion, bronze or
   silver. Province filtering belongs in gold or the website.
4. **Registry first.** Prefer configuration in `config/datasets.yml` over
   new code paths. Validate new registry fields in `parse_registry`.
5. **No data, no secrets in Git.** Credentials come from Databricks secrets.
   Do not hard-code personal emails or tokens.
6. **Unity Catalog names** are lower-case `snake_case`; volumes are
   `landing.raw` and `landing.meta`.
7. **Comments and docs in English.** Update `docs/architecture.md` when a
   design decision changes.

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
