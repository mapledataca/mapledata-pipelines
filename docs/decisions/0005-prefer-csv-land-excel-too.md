# 0005. Prefer CSV, and also land Excel when a table has no CSV

- Status: Accepted
- Date: 2026-11-02
- Supersedes: [0004](0004-csv-first.md)

## Context

[ADR 0004](0004-csv-first.md) ingested CSV only and left Excel for a later
staging step. Sources that replace files in place (`overwrite_in_place`) lose
history that we do not capture: a table we do not land is never snapshotted
([ADR 0003](0003-snapshot-versioning.md)), so its earlier versions cannot be
recovered later.

Evidence, observed 2026-11-02 on the live `package_show` metadata with the
`format_gap` and `archived` checks (see
[esdc_lmia.md](../sources/esdc_lmia.md) and
[ircc_monthly.md](../sources/ircc_monthly.md)):

- `esdc_lmia_statistics_annual` has 4 CSV and 24 XLSX English resources, so at
  least 20 tables are only published as XLSX. Its 4 CSV tables (5, 8, 17 and
  24) are exactly the tables the dataset description calls archived ("will no
  longer be updated"). CSV-only ingestion would capture only tables that no
  longer change.
- At least 2 tables of `ircc_permanent_residents_monthly`, 3 of
  `ircc_study_permit_holders_monthly` and 6 of `ircc_work_permit_holders_monthly`
  are only published as XLSX.

These counts are lower bounds: `format_gap` does not pair tables across
formats, because the names of the CSV and XLSX versions of a table rarely match.

`ircc_study_permit_holders_monthly` also lists 3 XLS resources, all marked
`[ARCHIVED]` in their names. "Archived" means no longer updated, not no value:
two of them (study permit holders by country of citizenship, and by census
metropolitan area) have no similarly named CSV or XLSX resource; the closest
names are only loosely similar (text similarity 0.59 to 0.73). Names rarely
match across formats, so this is evidence, not proof, that they are the only
copy. The third (study level and calendar year) has similar CSV and XLSX
tables, but with a different time dimension. Observed 2026-11-02 on the live
`package_show` metadata.

## Decision

Prefer CSV. When a table is not published as CSV, also land the Excel file in
raw. Raw stays byte-identical to the source
([ADR 0001](0001-landing-zone-ingestion.md)); Excel is staged to CSV or Parquet
in bronze later, since Auto Loader has no Excel format. When both formats exist
for a table, bronze uses the CSV.

Registry: `XLSX` is added to `formats` for `esdc_lmia_statistics_annual` and the
active IRCC datasets that have `format_gap` findings:
`ircc_permanent_residents_monthly`, `ircc_study_permit_holders_monthly` and
`ircc_work_permit_holders_monthly`. `ircc_study_permit_holders_monthly` also
gets `XLS`: its 3 XLS resources are archived, and archived data stays worth
keeping when it is probably the only copy of that history (above).
`ircc_tr_to_pr_transition_monthly` stays CSV only: it has no finding, which
does not prove full coverage.

Archived resources are therefore landed on purpose. The `archived` check
reports changes, not states: a WARN when a resource becomes archived (or on the
first run), otherwise one INFO per dataset that counts the archived resources
landed and says they are unchanged.

## Alternatives considered

- **CSV only ([ADR 0004](0004-csv-first.md)):** rejected. The history of
  XLSX-only tables is lost, and the annual statistics dataset would capture only
  archived tables.
- **Land only the Excel files that have no CSV twin:** not possible today. The
  registry filters by format, and CSV and XLSX versions of one table cannot be
  paired by name.
- **Leave the 3 archived XLS resources unselected:** rejected. Two of them
  probably have no counterpart in another format, so skipping them could lose
  that history for good.
- **Convert Excel to CSV during ingestion:** still rejected, for the reason in
  ADR 0004: raw must stay byte-identical to the source.

## Consequences

- The `formats` filter works per format, so for these four datasets every
  English XLSX resource is landed, including twins of CSV tables. Resources
  selected per dataset (English, CSV + XLSX), counted 2026-11-02:

  | Dataset | CSV | XLSX |
  |---|---|---|
  | `esdc_lmia_statistics_annual` | 4 | 24 |
  | `ircc_permanent_residents_monthly` | 16 | 18 |
  | `ircc_study_permit_holders_monthly` | 6 | 9 (plus 3 XLS) |
  | `ircc_work_permit_holders_monthly` | 13 | 19 |

- The annual XLSX resources have `last_modified` (24 of 24), so they are skipped
  from metadata once landed. The IRCC XLSX resources have none (0 of 18, 9 and
  19), nor do the 3 XLS resources, so those 49 files are downloaded on every
  run and versioned by content hash (ADR 0003), like the IRCC CSV files.
- Open question: "bronze uses the CSV when both exist" needs a rule that pairs
  the CSV and XLSX versions of one table, and names do not pair them reliably.
  Until the rule exists, bronze cannot tell which XLSX files duplicate a CSV.
- `format_gap` goes quiet for these datasets because XLSX is now selected. That
  does not mean full coverage.
