# ESDC: Labour Market Impact Assessments (LMIA)

Four datasets from Employment and Social Development Canada (`esdc-edsc`),
published on open.canada.ca. Portal-level notes are in
[open_canada.md](open_canada.md).

| Registry key | Content |
|---|---|
| `lmia_positive_employers` | Positive LMIA employers list |
| `esdc_lmia_negative_employers` | Negative LMIA employers list |
| `esdc_lmia_statistics_quarterly` | LMIA statistics, quarterly tables |
| `esdc_lmia_statistics_annual` | LMIA statistics, annual tables |

## Overview

Employer lists and statistics for the Temporary Foreign Worker Program. See the
`purpose` of each entry in `config/datasets.yml` for how the data is used.

## API/endpoints

CKAN `package_show` on `https://open.canada.ca/data/api/action`. Files are
hosted off-portal (for example on `www.esdc.gc.ca`) and download from
Databricks Free Edition. See [open_canada.md](open_canada.md).

## Update pattern

| Datasets | Pattern | Detail |
|---|---|---|
| Positive and negative employer lists | `new_file_per_period` | A new resource is added each quarter. |
| LMIA statistics (quarterly and annual) | `overwrite_in_place` | `last_modified` is present on the resources. |

## Formats

Employer lists: the format varies over the years: CSV, XLSX and XLS. Registry
formats are `[CSV, XLSX, XLS]`.

## Known quirks

- **After 2026Q2 was published on 2026-10-30, each employer list has 40
  English resources (80 in total, with French).** Evidence:
  `lmia_positive_employers` and `esdc_lmia_negative_employers`.
- **For the statistics datasets the resource id is stable but the file name
  changes each period.** Evidence: resource ids have been stable since 2016,
  while the file name carries the period, for example
  `tfwp2024_q1_table_5_e.csv` (`esdc_lmia_statistics_quarterly`). Bronze must
  read the period from the file name or the content, never from the resource
  id.
- **The statistics resources all carry `last_modified`.** Observed 2026-10-31
  on the live `package_show` metadata. Evidence: 17 of 17 CSV resources in
  `esdc_lmia_statistics_quarterly` and 4 of 4 in
  `esdc_lmia_statistics_annual`.
- **Tables 5, 8, 17 and 24 are archived.** Observed 2026-11-02 on the live
  `package_show` metadata. Evidence: the dataset description of both
  `esdc_lmia_statistics_quarterly` and `esdc_lmia_statistics_annual` says
  "Table 5, 8, 17, and 24 will no longer be updated but will remain as archived
  tables". The `archived` check flags it.
- **Most annual tables have no CSV.** Observed 2026-11-02 on the live
  `package_show` metadata. Evidence: `esdc_lmia_statistics_annual` has 4 CSV
  and 24 XLSX English resources, and the registry selects only CSV. The
  `format_gap` check reports at least 20 tables available only as XLSX. It is a
  lower bound.

## Open questions

None recorded.
