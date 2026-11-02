# 0002. National scope in ingestion, bronze and silver

- Status: Accepted
- Date: 2026-11-02

## Context

mapledata.io presents Manitoba first, but the sources publish national data and
other provinces will follow.

## Decision

No province filter in ingestion, bronze or silver. Province filtering happens in
gold (as a `province_code` dimension) or on the website. Manitoba is the first
presentation, not a data boundary.

## Alternatives considered

- **Filter to Manitoba at ingestion:** rejected. Expanding to another province
  would then mean rebuilding the pipelines.

## Consequences

- Expanding to another province is a parameter change, not a pipeline rebuild.
- Raw, bronze and silver hold data for all of Canada, so they are larger than a
  Manitoba-only copy would be.
