# IRCC monthly datasets

Notes on the four active IRCC monthly datasets in `config/datasets.yml`:
`ircc_permanent_residents_monthly`, `ircc_study_permit_holders_monthly`,
`ircc_work_permit_holders_monthly` and `ircc_tr_to_pr_transition_monthly`.

- Updated in place. The resources have no `last_modified`, so each file is
  versioned by content hash (`sha256-<12 hex>`) and downloaded again on every
  run. See "Update patterns" in [architecture.md](../architecture.md).

## History

- 2026-10-31: because the version rule changed (`metadata_modified` is no
  longer used), the IRCC raw directories were emptied and the datasets were
  ingested again.
