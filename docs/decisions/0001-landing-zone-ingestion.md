# 0001. Ingest through a landing zone

- Status: Accepted
- Date: 2026-11-02

## Context

The sources (open.canada.ca and similar CKAN portals) publish files, mostly CSV
and XLSX, not record streams. open.canada.ca returns intermittent 504 errors
(see [open_canada.md](../sources/open_canada.md)). Lakeflow Connect has no
managed connector for CKAN, and Lakeflow Connect custom connectors are in Beta.

## Decision

A scheduled job downloads files into a Unity Catalog volume (`landing.raw`);
Auto Loader reads them into bronze later. This follows the pattern "scheduled
job + landing volume + Auto Loader" (pattern 3) from the Databricks
documentation page
[Ingest data from an API in pipelines](https://docs.databricks.com/aws/en/ldp/api-ingestion):
a scheduled job writes files to a Unity Catalog volume, and the pipeline
ingests them with Auto Loader.

Raw files are never modified after they are written.

## Alternatives considered

- **Lakeflow Connect managed connector:** none exists for CKAN.
- **Lakeflow Connect custom connector:** Beta. A possible later upgrade, not a
  current goal.

## Consequences

- Raw files can be replayed if downstream logic changes.
- Upstream problems (timeouts, HTML error pages, 504s) stay in the ingester,
  away from transformation logic.
- The ingester must write files atomically and reject HTML error pages, so Auto
  Loader never sees a partial or invalid file.
- Metadata and run logs live in a separate volume (`landing.meta`), so Auto
  Loader on `landing.raw` never reads them.
