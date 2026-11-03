# Analytical questions: immigration domain

These are open questions to test with data, not conclusions. Datasets are
judged against them (see [registry_review.md](registry_review.md)): a dataset
is worth ingesting when it can help answer at least one question. Add new
questions here as they come up.

Dataset names below are registry keys in `config/datasets.yml`. A dataset or
source is listed as available only when it is in the registry; anything else is
marked "to verify" and may not exist, or may not have the history or detail the
question needs.

Status values:

- **data available**: at least one active registry dataset covers the question;
  coverage of each dimension still has to be checked.
- **candidate**: a registry dataset with status `candidate` may cover it.
- **source missing**: no dataset known; a source has to be found.
- **to verify**: a possible source exists but has not been checked.

## Questions

### I1. Transitions to permanent residence

How many people become permanent residents, by pathway (immigration
category), by province, census metropolitan area (CMA) and census subdivision
(CSD, i.e. city), and how does this change over time?

- **Datasets:** `ircc_permanent_residents_monthly` (includes the CSD file),
  `ircc_tr_to_pr_transition_monthly`; candidate:
  `ircc_express_entry_pr_monthly`.
- **Status:** data available; Express Entry pathway breakdown is candidate.
- **Notes:** the geographic levels and the history each table offers are not
  checked yet (backlog P9, N3). Small geographies are the most affected by
  rounding; parent totals come from published rows, never from summing
  children ([ADR 0002](decisions/0002-national-scope.md)).

### I2. Temporary residents

What types of temporary residents are there (study, work, asylum)? How do
monthly new arrivals compare with the stock, by region? How many move on to
permanent residence? How many applications are in process, by stage?

- **Datasets:** `ircc_study_permit_holders_monthly`,
  `ircc_work_permit_holders_monthly`, `ircc_tr_to_pr_transition_monthly`;
  candidates: `ircc_asylum_claimants_monthly`,
  `ircc_express_entry_invitations_monthly`.
- **Status:** data available for permit holders and transitions to permanent
  residence; candidate for asylum; to verify for applications in process.
- **Notes:**
  - IRCC tables mix **stock** ("holders with a valid permit") and **flow**
    ("year the permit became effective"). Keep them distinct; comparing them
    directly would be wrong.
  - `ircc_express_entry_invitations_monthly` is an upstream pipeline
    indicator: invitation, application, in process, admission. Invitations are
    not admissions.
  - In-process and backlog data: to verify during the `cic` triage (backlog
    P12, S3).

### I3. Long-term trends and comparison with official figures

How do admissions and temporary resident numbers change over the long term by
occupation, age and other dimensions? How do officially published figures
compare with what the open data shows, for example Immigration Levels Plan
targets against actual admissions?

- **Datasets:** `ircc_permanent_residents_monthly`,
  `ircc_work_permit_holders_monthly` (occupational skill level),
  `esdc_lmia_statistics_quarterly` and `esdc_lmia_statistics_annual`
  (LMIA statistics; the occupation detail is to verify).
- **Status:** data available for the trends, to verify for the dimensions;
  official figures: to verify.
- **Notes:** official plans and reports are likely PDF or web pages, so the
  source, format and history are to verify (backlog S1). A small, manually
  curated table with citations may be needed. Expect small differences caused
  by rounding.

### I4. Temporary residents and public services

How does the growth of temporary residents relate to planned admissions and to
service capacity (health, education) over time and by region?

- **Datasets:** the temporary resident tables from I2, plus data from the
  health and education domains, which are not built yet.
- **Status:** source missing for now.
- **Notes:** this is a hypothesis to test, not a conclusion. It needs data from
  later domains, joined by time and geography. Correlation is not causation;
  policy timelines (for example study-permit caps) give context.

### I5. Interprovincial mobility of immigrants after landing

Where do immigrants live after landing, and how many move between provinces?

- **Datasets:** none. Immigration data records the province at landing only.
- **Status:** source missing for now.
- **Notes:** possible Statistics Canada sources (immigrant retention or
  longitudinal data, interprovincial migration estimates) are to verify
  (backlog S2).

## Shared dimensions

The questions above need these dimensions. They are the input to backlog D4
(shared dimensions in the `shared` schema).

| Dimension | Values and notes |
|---|---|
| Time | month, quarter, year |
| Geography | country > province > CMA > CSD, with Statistics Canada standard codes |
| Immigration category / temporary resident permit type | category and age groupings may differ between datasets and need a mapping |
| Country of citizenship | |
| Age group | groupings may differ between datasets and need a mapping |
| Gender | |
| Occupation | NOC 2016 and NOC 2021 both appear, hence a `noc_version` column |
| Study level / designated learning institution (DLI) | |
