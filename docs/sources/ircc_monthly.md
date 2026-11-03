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

CKAN `package_show` on `https://open.canada.ca/data/api/3/action`. See
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

CSV is preferred, and Excel is also ingested where tables are not published as
CSV ([ADR 0005](../decisions/0005-prefer-csv-land-excel-too.md), which
supersedes [ADR 0004](../decisions/0004-csv-first.md)). Registry formats:
`[CSV, XLSX]` for permanent residents and work permits, `[CSV, XLSX, XLS]` for
study permits, `[CSV]` for TR→PR, where the `format_gap` check found nothing.
The CSV files cover the province-level study-permit tables. The Dec 31 stock
tables are XLSX-only.

The English XLSX and XLS resources have no `last_modified` either (observed
2026-11-02 on the live `package_show` metadata): permanent residents 0 of 18
XLSX, study permits 0 of 9 XLSX and 0 of 3 XLS, work permits 0 of 19 XLSX. They
are downloaded on every run and versioned by content hash, like the CSV files.

Why the study permit XLS resources are ingested although they are archived:
"archived" means no longer updated, not no value. Two of them (holders by
country of citizenship, and by census metropolitan area) have no similarly named
CSV or XLSX resource, so they are probably the only copy of that history. See
the quirk below and ADR 0005.

## Known quirks

- **Tables carry "[rounded - not for calculations]", and the marker exists only
  in the resource names.** Observed on or before 2026-10-31 (the marker in the
  table names). Silver and gold must use the published totals, not sums of
  rounded cells. Observed 2026-11-02 on the live `package_show` metadata
  (`rounded_values` check, resource names): permanent residents 16 of 16
  selected resources, TR→PR 4 of 4, study permits 3 of 6, work permits 1 of 13.
  Observed in Databricks: on 2026-11-02 all 39 IRCC CSV files were read and no
  file head matched; on 2026-11-03 (run `20261103T004037Z`) heads and tails were
  read, and again no `file_head` and no `file_tail` matched, so every marker
  comes from a resource name. The snapshot's `rounded_marker` was written for
  every row of that run (0 NULL) and the marked counts match the findings:
  permanent residents 16, study permits 3, TR→PR 4, work permits 1. A resource
  without the marker may still be rounded: absence of the marker does not mean
  the values are exact, so `rounded_marker = false` is never a licence to sum.
- **Some tables exist only as XLSX.** Observed 2026-11-02 on the live
  `package_show` metadata. Evidence: the `format_gap` check reports at least 2
  tables only as XLSX for `ircc_permanent_residents_monthly`, at least 3 for
  `ircc_study_permit_holders_monthly` and at least 6 for
  `ircc_work_permit_holders_monthly`. These are lower bounds; no finding for
  `ircc_tr_to_pr_transition_monthly` does not prove full coverage. XLSX is now
  selected for the three datasets with findings (ADR 0005), so `format_gap` no
  longer reports them.
- **Some resources are marked "[ARCHIVED]" in their names.** Observed
  2026-11-02 on the live `package_show` metadata. Evidence:
  `ircc_study_permit_holders_monthly` has 3 of 3 XLS resources
  (`IRCC_M_TRStudy_0004_E.xls`, `0005` and `0006`, created 2017-04-12) and 1 of
  9 XLSX resources (`IRCC_M_TRStudy_0007_E.xlsx`); no other active IRCC dataset
  has any. For `0004` (citizenship) and `0005` (census metropolitan area) the
  closest CSV or XLSX name has a text similarity of only 0.59 to 0.73; `0006`
  (study level, calendar year) has similar tables with a different time
  dimension. They are landed on purpose (ADR 0005). The `archived` check reports
  changes: a WARN when a resource becomes archived or on the first run,
  otherwise one INFO per dataset.
- **Some datasets have finer-grained files.** Observed on or before
  2026-10-31. Evidence: permanent residents has a census-subdivision file (`ODP-PR-CSD.csv`); study
  permits have DLI-level tables.
- **The candidate `ircc_asylum_claimants_monthly` has `last_modified` on only 1
  of its 8 CSV resources.** Observed on or before 2026-10-31. Evidence:
  registry key `ircc_asylum_claimants_monthly`.

## Open questions

- Province-level CSV coverage for work permits, TR→PR and permanent residents
  has not been checked yet.
- How are the CSV and XLSX versions of one table paired, so that bronze can use
  the CSV when both exist (ADR 0005)? Their names rarely match: permanent
  residents has 16 CSV and 18 XLSX resources and no matching pair by name.
- Do the XLSX files carry the marker inside the workbook? Only CSV files are
  read for the marker, so XLSX resources are judged by their names alone.
- Are resources without the marker rounded too? Absence of the marker is not
  evidence that the values are exact; the publisher's documentation would have
  to say.

## Resolved questions

- Does "rounded" appear in the head or at the end of the landed CSV files (a
  footnote)? No. Validated 2026-11-03 in Databricks (run `20261103T004037Z`):
  all 39 IRCC CSV files were read, with no `file_head` and no `file_tail`
  match. The marker is only in the resource names.

## History

- 2026-10-31: because the version rule changed (`metadata_modified` is no
  longer used), the IRCC raw directories were emptied and the datasets were
  ingested again.
