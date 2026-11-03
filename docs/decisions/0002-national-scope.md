# 0002. National scope in ingestion, bronze and silver

- Status: Accepted
- Date: 2026-11-02

## Context

mapledata.io presents Manitoba first, but the sources publish national data and
other provinces will follow. Some sources publish finer geographies than
provinces (for example, the permanent residents census-subdivision file), and
small geographies are the most affected by rounding.

## Decision

National scope at every published geographic level: province, census
metropolitan area (CMA), census subdivision (CSD) and institution. No province
filter in ingestion, bronze or silver. Province filtering happens only in gold
(as a `province_code` dimension) or on the website. Manitoba is the first
presentation, not a data boundary. A parent total comes from the rows the
source publishes for it, never from summing its children.

## Alternatives considered

- **Filter to Manitoba at ingestion:** rejected. Expanding to another province
  would then mean rebuilding the pipelines.

## Consequences

- Expanding to another province is a parameter change, not a pipeline rebuild.
- Raw, bronze and silver hold data for all of Canada, so they are larger than a
  Manitoba-only copy would be.
