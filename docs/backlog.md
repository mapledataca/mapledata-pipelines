# Backlog

Temporary; may move to GitHub Issues. Keep the IDs when it does.

## Now

- **N1** Read project docs (README, architecture, ADRs, sources, CLAUDE.md); note unclear or disputed points.
- **N2** Map exam guide sections to bronze/silver/gold; only objectives stated in the guide.
- **N3** Read-only file profiling: CSV header/footer lines, XLSX sheets, CSV<->sheet pairing, ESDC period source.

## Next: data layers

- **D1** Bronze: Auto Loader from raw; stage Excel to CSV/Parquet (raw untouched); join `ckan_resource_current` on `resource_id` to carry `resource_name`, `rounded_marker`, `archived`; ESDC period from filename or content.
- **D2** Silver: no province filter; `noc_version`, `includes_pr_only`; use published totals, never sums of rounded values.
- **D3** Gold: geography levels incl. province; Manitoba first.
- **D4** Shared dimensions: geography (StatCan standard codes; country > province > CMA > CSD), NOC, date.

## Later: platform

- **P1** Lakeflow Job: ingestion -> checks as two tasks; schedule.
- **P2** Declarative Automation Bundles: `databricks.yml`, dev/prod targets, catalog variable per target, schema/volume resources.
- **P3** SQL alert on `ingestion_findings` (check Free Edition support first).
- **P4** Weekly discovery job (organizations + keywords); `config/discovery.yml`.
- **P5** Evaluate the 5 candidate datasets.
- **P6** Avoid re-downloading hash-versioned files. Status: portal metadata has no hash, size or `last_modified` for `ircc_permanent_residents_monthly` (2026-11-03, see [open_canada.md](sources/open_canada.md)). Only the HTTP `ETag` and `Last-Modified` headers of the download remain; to be tested.
- **P7** Tests batch 2: contract tests with real `package_show.json` fixtures; integration tests in a `mapledata_test` catalog.
- **P8** Bootstrap: update catalog/schema/volume comments.
- **P9** Check province-level CSV coverage for work permits, TR->PR, PR.
- **P10** One-time review of all organizations via `organization_list` to find other immigration publishers. The documentation gives `limit` 25 when `all_fields=true` (page with `offset`); on 2026-11-03 one call without `limit` returned all 353 organizations, so pass `limit` and `offset` explicitly and check the count.
- **P11** `current_package_list_with_resources` for site-wide new-dataset detection (idea only, not implemented). The documentation says it returns datasets newest-modified first, with `limit` and `offset`. A probe on 2026-11-03 (`limit=3`, `offset=0`) returned `success` with 0 results; the reason is unknown and the behaviour is unverified.
- **P12** Triage the ~50 unregistered `cic` datasets after organization mode, following [registry_review.md](registry_review.md) and judging them against [questions.md](questions.md); mark each `candidate` or `excluded`. (`cic` has 62 datasets on 2026-11-03.)

## Sources to find

- **S1** Official figures source: Immigration Levels Plan and the annual report to Parliament (verify format and history), for question I3 in [questions.md](questions.md).
- **S2** Statistics Canada sources for interprovincial mobility and immigrant retention (verify the tables), for question I5.
- **S3** IRCC in-process and backlog data (verify whether it is published as open data, and with what history), for question I2.

## Open questions

- **Q1** Do XLSX files carry a rounded marker? Are unmarked resources rounded too?
- **Q2** CSV <-> XLSX pairing (one XLSX may hold several tables).
- **Q3** ESDC annual archived tables 5, 8, 17, 24: map via table number in filename.
- **Q4** Most quirk observation dates are "on or before 2026-10-31".

## Long term

- Other domains: cost of living, housing, health, education.
- mapledata.io static site with source and licence attribution.
