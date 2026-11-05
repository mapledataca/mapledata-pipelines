# Reviewing discovered datasets

How to decide what to do with a dataset that `tools/discover_datasets` found.
The outcome is a registry status in `config/datasets.yml`: `candidate`,
`active` or `excluded` (see "Registry-driven" in
[architecture.md](architecture.md)). Datasets are judged against the questions
in [questions.md](questions.md).

## Criteria

1. **Relevance.** It answers at least one question in
   [questions.md](questions.md). Name the question in the entry's `purpose`.
2. **Currency.** Look at `frequency` and `coverage_end`. A stale dataset is
   usually excluded, unless it holds unique history. Precedent: the archived
   study-permit XLS resources (`IRCC_M_TRStudy_0004` and `0005`) are kept
   because they are probably the only copy of that history
   ([ADR 0005](decisions/0005-prefer-csv-land-excel-too.md)).
3. **Geographic granularity.** Province or finer is preferred. National-only
   data is context.
4. **Format.** CSV or XLSX is required. A dataset with only PDF or HTML
   resources is excluded.
5. **Duplication.** It does not repeat an active dataset (for example the same
   figures at national level only).
6. **Joinable standard dimensions.** NOC, country, province and similar codes
   that match the shared dimensions in [questions.md](questions.md).
7. **Licence.** Usually the Open Government Licence – Canada. Confirm it on the
   dataset page; the snapshot table does not carry the licence.
8. **Processing cost.** Many files, Excel-only tables, or files that must be
   downloaded on every run (no `last_modified`) affect the priority only, not
   the decision.

## Process

### Round 1: SQL screening

Screen the datasets that no registry entry covers, using the latest row per
dataset (`ckan_dataset_current`). Set the catalog name if it is not
`living_canada` (see `config/platform.yml`). This query has not been run on Databricks yet; check the output
against the portal before relying on it.

```sql
-- Unregistered datasets with a usable format that are not clearly stale.
SELECT organization_name, name, title, formats, frequency,
       coverage_start, coverage_end, resource_count, search_queries
FROM living_canada.metadata.ckan_dataset_current
WHERE NOT is_registered
  AND arrays_overlap(formats, array('CSV', 'XLSX', 'XLS'))
  AND (coverage_end IS NULL OR coverage_end >= add_months(current_date(), -36))
ORDER BY organization_name, name;
```

A dataset with no `coverage_end` is kept in this round, because many datasets
leave it empty; it is judged on the dataset page. The three years is a screen,
not a rule: change it when it hides something relevant.

To see what the format and currency filters drop (and to give each of them an
`excluded` reason), run the opposite checks:

```sql
-- No CSV/XLSX/XLS resource: pdf_only candidates.
SELECT organization_name, name, title, formats
FROM living_canada.metadata.ckan_dataset_current
WHERE NOT is_registered
  AND NOT arrays_overlap(formats, array('CSV', 'XLSX', 'XLS'))
ORDER BY organization_name, name;

-- Old coverage: stale candidates.
SELECT organization_name, name, title, frequency, coverage_end
FROM living_canada.metadata.ckan_dataset_current
WHERE NOT is_registered
  AND coverage_end < add_months(current_date(), -36)
ORDER BY coverage_end;
```

### Round 2: dataset page

Open the dataset page for each survivor of round 1. Check the criteria that
metadata cannot show: what the resources really are, the licence, the
description, whether files are replaced in place. Add survivors to
`config/datasets.yml` as `candidate` with a real `purpose`; add the rest as
`excluded` with a reason code (below).

### Round 3: file profiling

Profile the files (backlog N3: header and footer lines, sheets, pairing of CSV
and XLSX, period source) before a candidate becomes `active`.

## Recording an exclusion

An `excluded` entry stays in the registry so discovery stops proposing the
dataset. Its `purpose` starts with a reason code and a colon, then a short
explanation. `parse_registry` rejects any other start.

| Code | Use for |
|---|---|
| `stale:` | old coverage or no longer updated, with no unique history |
| `pdf_only:` | only PDF or HTML resources |
| `duplicate:` | repeats an active dataset |
| `out_of_scope:` | not about a question in [questions.md](questions.md) |
| `other:` | anything else; the explanation is required |

Example:

```yaml
status: excluded
purpose: "duplicate: same as ircc_permanent_residents_monthly at national level"
```
