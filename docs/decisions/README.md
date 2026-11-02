# Architecture decision records

One file per decision. An accepted ADR's decision is never edited; to change
it, add a new ADR that supersedes the old one and mark the old one
"Superseded by NNNN".

Each ADR has: Title, Status, Date, Context, Decision, Alternatives considered,
Consequences.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-landing-zone-ingestion.md) | Ingest through a landing zone | Accepted |
| [0002](0002-national-scope.md) | National scope in ingestion, bronze and silver | Accepted |
| [0003](0003-snapshot-versioning.md) | Snapshot versioning of landed files | Accepted |
| [0004](0004-csv-first.md) | Ingest CSV first | Accepted |

Source-specific findings (quirks, update patterns, open questions) live in
[docs/sources/](../sources/).
