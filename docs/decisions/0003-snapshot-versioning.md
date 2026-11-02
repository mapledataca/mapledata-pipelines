# 0003. Snapshot versioning of landed files

- Status: Accepted
- Date: 2026-11-02
- Decided: 2026-10-31

## Context

CKAN publishers update datasets in two ways (`update_pattern` in
`config/datasets.yml`): they add a new file each period, or they replace the
same file in place, sometimes revising history. Every distinct version of a
replaced file must be kept, to feed AUTO CDC FROM SNAPSHOT downstream.

A version token has to come from somewhere. Resource `last_modified` is often
null. Dataset `metadata_modified` is not a file version: for the permanent
residents census-subdivision resource, the resource's date is 2026-08-18 while
the dataset's `metadata_modified` is 2026-10-20 (see
[open_canada.md](../sources/open_canada.md)). `metadata_modified` can also stay
unchanged after a file's content is replaced, which would hide an update.

## Decision

Path of a file inside the dataset's landing folder:

| `update_pattern` | Path |
|---|---|
| `new_file_per_period` | `<resource_id>_<basename>` |
| `overwrite_in_place` | `<resource_id>/<version>_<basename>` |

`version` is the resource's `last_modified` if it is usable, otherwise
`sha256-<first 12 hex of the content>`.

## Alternatives considered

- **Fall back to `metadata_modified` when `last_modified` is missing:** this was
  the first version of the rule. Rejected because `metadata_modified` lags the
  file (evidence above) and can miss an update.

## Consequences

- A resource without `last_modified` must be downloaded on every run, because
  the hash needs the bytes. The IRCC monthly datasets are in this case. A
  resource with `last_modified` is still skipped from metadata alone.
- Identical content is never stored twice, so a repeated download adds no file.
  Verified on 2026-10-31 for the IRCC datasets: a second run stored nothing new.
- Because the rule changed, the IRCC raw directories were cleared and
  re-ingested on 2026-10-31. See [ircc_monthly.md](../sources/ircc_monthly.md).
