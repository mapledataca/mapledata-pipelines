# 0006. Portable core, platform-specific edges

- Status: Accepted
- Date: 2026-11-05

## Context

Databricks is the platform for now: the project is also exam practice for a
Databricks certification in January, so the platform features are worth using.
A later second implementation on DuckDB + dbt is planned (backlog L3). It should
reuse as much of the first one as possible, and the two versions should be
comparable.

Most of the value of this project is not platform-specific: what a source
publishes, how its files must be read, and what the numbers mean. That knowledge
should not be locked into one platform's syntax.

## Decision

Keep the core portable and use platform features deliberately at the edges.

**Portable core**

- **Ingestion and checks are pure Python**, with network access through an
  injected session (already the rule in CLAUDE.md). Notebooks stay thin.
- **File-parsing rules are pure Python functions**, unit-tested without Spark:
  multi-row headers, wide-to-long reshaping, `--` meaning a suppressed count of
  1 to 4 (not zero, not missing), and reading only the published sheet of a
  workbook.
- **Transformation SQL is standard SQL where possible.** A Databricks-only
  function or syntax is isolated (a small, named place such as one CTE or view)
  and commented with why it is used and what a portable version would need.
- **Business rules are written in prose** (docs, column and table comments) as
  well as enforced in expectations or tests, so they can be re-implemented
  without reading the first implementation's code.
- **Storage formats are open:** CSV and Excel in raw, Parquet or Delta below it.

**Platform edges, used on purpose**

Auto Loader, Lakeflow Spark Declarative Pipelines (streaming tables,
materialized views, expectations), AUTO CDC, Lakeflow Jobs, Declarative
Automation Bundles and Unity Catalog lineage. These are where Databricks adds
value, and where exam practice happens. They contain wiring, not business rules.

**Concept mapping (conceptual, for planning L3)**

| Databricks | DuckDB + dbt | Note |
|---|---|---|
| Unity Catalog volume (`landing.raw`) | Directory or object-store prefix | Raw files stay the shared input |
| Auto Loader | `read_csv` over a glob, plus a model that tracks processed files | Auto Loader's incremental file tracking has no direct equivalent |
| Streaming table | dbt incremental model | |
| Materialized view | dbt `table` or `view` model | Refresh semantics differ |
| Pipeline expectations | dbt tests (and model contracts) | Quarantine and drop actions need explicit models |
| AUTO CDC | dbt snapshot or incremental `merge` | |
| Lakeflow Jobs | External orchestrator (cron, CI, Dagster, ...) | |
| Declarative Automation Bundles | dbt `profiles.yml` targets and repo config | |
| Unity Catalog lineage | dbt docs lineage graph | |
| Table and column comments, tags | dbt `description` and `meta` | |
| Delta tables | Parquet files or DuckDB tables | |

The mapping is a planning aid, not verified behaviour. Check each equivalence in
the DuckDB and dbt documentation before relying on it.

## Alternatives considered

- **Platform-agnostic framework now:** rejected. It is premature with one
  implementation, adds abstraction before we know where the two platforms
  differ, and weakens exam practice because the Databricks features would be
  hidden behind it.
- **Databricks-only, no portability rules:** rejected. Parsing rules and
  business logic would end up inside notebooks and platform syntax, and the
  DuckDB + dbt implementation would have to rediscover them.

## Consequences

- New logic goes into pure Python modules under `src/` with tests; notebooks only
  wire widgets, `spark.sql` and platform calls.
- Reviews check that a Databricks-only function is isolated and commented.
- Some code is more verbose than the idiomatic Databricks version (for example,
  parsing in Python instead of a Spark-only reader); this is accepted.
- Business rules are written down twice (prose and enforcement); this is
  accepted, because the prose is what the second implementation starts from.
- Not yet decided: the exact boundary for transformation SQL (for example,
  whether `MERGE` counts as standard). Decide when the first silver model is
  written.

See also ADR 0007 (catalog, schema layout and config-driven paths).
