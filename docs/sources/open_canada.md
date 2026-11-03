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
| `cic` | IRCC (62 datasets on 2026-11-03) |
| `esdc-edsc` | ESDC (269 datasets on 2026-11-03) |
| `irb-cisr` | IRB (2 datasets on 2026-11-03) |
| `cbsa-asfc` | CBSA |
| `statcan` | Statistics Canada |
| `mb` | Manitoba |

## API/endpoints

CKAN Action API: `https://open.canada.ca/data/api/3/action`.

The `3` pins the API version. CKAN's documentation lists version 3 as the only
version, and says the unversioned URL (`.../api/action`) uses the latest one.
On 2026-11-03 `package_show` returned identical results from both URLs. Code
that compares portals ignores the version segment (`normalize_base_url`), so
old and new spellings still mean the same portal. Snapshot rows written before
the change keep the URL they were written with.

| Action | Used by |
|---|---|
| `package_show` | ingestion (`ckan_ingest`) |
| `package_search` | discovery (`ckan_discover`, `tools/discover_datasets`) |
| `organization_list` | not used yet (backlog P10) |

### What the CKAN documentation says, and what we verified

Source: [CKAN 2.12 API guide](https://docs.ckan.org/en/2.12/api/index.html)
(`package_search`, `organization_list`, `organization_show`), read on
2026-11-03. "Live" means our own read-only requests to open.canada.ca on the
same day. A portal can configure limits differently from the documented
defaults.

| Topic | Documentation | Live (2026-11-03) |
|---|---|---|
| `package_search` `q` | default `*:*` (everything) | Organization mode sends no `q`. |
| `package_search` `fq` | Solr filter query, `field:value` syntax | `fq=organization:cic` returned 62 datasets, `esdc-edsc` 269, `irb-cisr` 2, quoted or not. Verified by our runs, not by an example in the documentation. |
| `package_search` `rows` | default 10, maximum 1000 | `rows=1000` accepted. Behaviour above 1000 not verified (the filters we tried matched fewer than 1001 datasets). |
| `package_search` `sort` | default `score desc, metadata_modified desc` | `sort=name asc` accepted. We send it on every paged request so paging is stable. |
| `package_search` `facet.limit` | default 50; negative means no limit | A probe asking for the organization facet returned 133 entries with and without `facet.limit`, so this portal did not cap at 50. We send `facet.limit=-1` anyway; it has no effect until a request asks for facets. |
| `package_search` `count` | total number of matches | Used to know when paging is complete. |
| `organization_list` | `limit` defaults to 25 when `all_fields=true` (1000 otherwise); page with `offset` | `all_fields=true` without `limit` returned all 353 organizations in one call. Code should still pass `limit` and `offset` explicitly. |
| `organization_show` | `include_datasets` returns only the first 10 datasets | `cic`: `package_count` 62, 10 datasets returned. Do not use it to list a publisher's datasets. |
| `current_package_list_with_resources` | datasets newest-modified first; `limit` and `offset` | `limit=3, offset=0` returned `success` with 0 results; the reason is not known (backlog P11). |
| Errors and limits | Always check `success`. Rate limits are not documented. | Non-200 responses happen (`504` seen). |

Every client of the API checks the HTTP status, the content type and
`success` before using a response (see `_search_page` and `ckan_ingest`).

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
  **Decision:** discover by organization, not by keyword tuning. See
  "Discovery scope" below.
- **Resource `hash`, `size`, `mimetype` and `last_modified` can all be empty.**
  Observed 2026-11-03. Evidence: for `ircc_permanent_residents_monthly`, all 52
  resources have an empty `hash`, `size`, `mimetype` and `last_modified`; only
  `created` is filled (52 of 52). CKAN documents these fields as optional when
  a resource is created, so the publisher decides. The portal metadata alone
  cannot tell us whether a file changed; see
  [ADR 0003](../decisions/0003-snapshot-versioning.md). The HTTP headers
  `ETag` and `Last-Modified` of the file download are untested (backlog P6).
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

## Discovery scope

`tools/discover_datasets` has two modes, which can be combined:

- **Organizations** (`organizations` widget): every dataset of a publisher,
  paged with `fq=organization:<name>`. This is the complete view of a
  publisher. The standard run uses `cic,esdc-edsc,irb-cisr,cbsa-asfc`.
- **Keywords** (`query` widget): full-text search, used to find publishers we
  do not know yet, not as the main way to list datasets.

No subject filter. The `subject` field is a broad controlled list shared by
all portal content, so it cannot separate immigration from everything else.
Keywords (`tags`) are chosen by each publisher, are bilingual, and are not
consistent between publishers.

A dataset found by an organization search that is not in the registry becomes
a draft candidate. The discovery report ends with a recall check: registry
datasets (active or candidate) that the run did not find. It is relative to
the run's search scope, so a run with fewer organizations than the standard
one lists the others as not found.

## Open questions

None recorded.
