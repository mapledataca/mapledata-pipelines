# IRCC monthly datasets

Notes on the four active IRCC monthly datasets in `config/datasets.yml`:
`ircc_permanent_residents_monthly`, `ircc_study_permit_holders_monthly`,
`ircc_work_permit_holders_monthly` and `ircc_tr_to_pr_transition_monthly`.
Portal-level notes are in [open_canada.md](open_canada.md).

Quirks carry the date they were observed and their evidence. "Observed on or
before 2026-10-31" means the finding was made during the 2026-10-29 to
2026-10-31 sessions, without a more exact date.

## Overview

Monthly IRCC updates published by IRCC (organization code `cic`) on
open.canada.ca. Each dataset holds many tables, one resource per table, and
every CSV table lands under the dataset's key (no `name_regex`).

## API/endpoints

CKAN `package_show` on `https://open.canada.ca/data/api/action`. See
[open_canada.md](open_canada.md).

## Update pattern

- Updated in place (`overwrite_in_place`). The resources were created in
  2017–2018.
- The resources have no `last_modified`, so each file is versioned by content
  hash (`sha256-<12 hex>`, [ADR 0003](../decisions/0003-snapshot-versioning.md))
  and downloaded again on every run. See "Update patterns" in
  [architecture.md](../architecture.md).
- Idempotency verified 2026-10-31: a second run downloaded the files again but
  stored nothing new (`downloaded=0`, everything skipped).

## Formats

CSV is ingested ([ADR 0004](../decisions/0004-csv-first.md)). The CSV files
cover the province-level study-permit tables. The Dec 31 stock tables are
XLSX-only.

## Known quirks

- **CSVs carry "[rounded - not for calculations]".** Observed on or before
  2026-10-31. Evidence: the marker in the table names. Silver and gold must use the
  published totals, not sums of rounded cells.
- **Some datasets have finer-grained files.** Observed on or before
  2026-10-31. Evidence: permanent residents has a census-subdivision file (`ODP-PR-CSD.csv`); study
  permits have DLI-level tables.
- **The candidate `ircc_asylum_claimants_monthly` has `last_modified` on only 1
  of its 8 CSV resources.** Observed on or before 2026-10-31. Evidence:
  registry key `ircc_asylum_claimants_monthly`.

## Open questions

- Province-level CSV coverage for work permits, TR→PR and permanent residents
  has not been checked yet.

## History

- 2026-10-31: because the version rule changed (`metadata_modified` is no
  longer used), the IRCC raw directories were emptied and the datasets were
  ingested again.
