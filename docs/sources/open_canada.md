# open.canada.ca (CKAN portal)

Portal-level notes. They apply to every dataset with `source: open_canada`.
Dataset-specific notes live in the other files of this folder.

Quirks carry the date they were observed and their evidence. "Observed on or
before 2026-10-31" means the finding was made during the 2026-10-29 to
2026-10-31 sessions, without a more exact date.

## Overview

open.canada.ca is the Government of Canada's open data portal, built on CKAN.
Datasets are published by departments and agencies; each is a CKAN package with
resources (files).

Organization codes seen on the portal:

| Code | Organization |
|---|---|
| `cic` | IRCC (62 datasets) |
| `esdc-edsc` | ESDC |
| `irb-cisr` | IRB |
| `cbsa-asfc` | CBSA |
| `statcan` | Statistics Canada |
| `mb` | Manitoba |

## API/endpoints

CKAN Action API: `https://open.canada.ca/data/api/action`.

| Action | Used by |
|---|---|
| `package_show` | ingestion (`ckan_ingest`) |
| `package_search` | discovery (`ckan_discover`, `tools/discover_datasets`) |

## Update pattern

Varies by dataset. See [esdc_lmia.md](esdc_lmia.md) and
[ircc_monthly.md](ircc_monthly.md), and "Update patterns" in
[architecture.md](../architecture.md).

## Formats

Varies by dataset. See the per-source files.

## Known quirks

- **`package_search` is full-text, so keywords match more than the topic.**
  Observed on or before 2026-10-31. Evidence: the keyword "immigration" also returns
  Statistics Canada tables that have citizenship dimensions, and federated
  provincial datasets. Quoting a multi-word keyword barely changed the number
  of results (about 1,200 before, about 1,170 after).
  **Decision:** discover by organization, not by keyword tuning.
- **`package_show` is slow and unstable.** Observed on or before 2026-10-31.
  Evidence: all of these were observed: `ReadTimeout`; an HTML page instead of JSON, which
  surfaced as a `JSONDecodeError`; and `504 Gateway Time-out`.
  **Mitigation:** retries with back-off, a `(10, 300)` timeout, a status and
  content-type check before parsing, and the metadata cache fallback.
- **Resource `last_modified` is often null, and dataset `metadata_modified` is
  not a file version.** Observed on or before 2026-10-31. Evidence: for the permanent
  residents census-subdivision (CSD) resource, the resource's date is
  2026-08-18 while the dataset's `metadata_modified` is 2026-10-20. See
  [ADR 0003](../decisions/0003-snapshot-versioning.md).
- **Files can be hosted off-portal**, for example on `www.esdc.gc.ca`.
  Observed on or before 2026-10-31. These downloads work from Databricks Free
  Edition.
- **Free Edition outbound access differs by domain.** Observed on or before
  2026-10-31. Evidence: `open.canada.ca` and `data.winnipeg.ca` are reachable;
  `opendata.brandon.ca` times out (`ConnectTimeout`).

## Open questions

None recorded.
