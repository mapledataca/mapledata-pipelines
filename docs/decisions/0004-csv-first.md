# 0004. Ingest CSV first

- Status: Superseded by [0005](0005-prefer-csv-land-excel-too.md)
- Date: 2026-11-02

## Context

Many sources publish the same table as CSV and XLSX. Auto Loader has no Excel
format. Some tables exist only as XLSX, for example the IRCC Dec 31 stock
tables (see [ircc_monthly.md](../sources/ircc_monthly.md)).

## Decision

Ingest CSV first. Excel files will be staged to CSV or Parquet in bronze later;
the raw files stay untouched.

## Alternatives considered

- **Ingest every published format (CSV + XLSX + XLS):** rejected for now. It
  duplicates the same table and doubles the parsing work in bronze.
- **Convert Excel to CSV during ingestion:** rejected. Raw must stay
  byte-identical to the source ([ADR 0001](0001-landing-zone-ingestion.md)).

## Consequences

- Known gap: tables that are XLSX-only, such as the IRCC Dec 31 stocks, are not
  ingested until the bronze Excel staging exists.
- A dataset whose recent files are only XLSX keeps XLSX in its registry
  `formats`. Example: `lmia_positive_employers` (CSV until 2020Q3, XLSX
  afterwards) uses `[CSV, XLSX, XLS]`.
