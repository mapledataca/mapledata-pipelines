# 0007. Catalog and schema layout

- Status: Accepted
- Date: 2026-11-05

## Context

The platform needs one stable layout for Unity Catalog: where raw files, platform
metadata, conformed dimensions and each layer of each domain live. Databricks'
[medallion architecture page](https://docs.databricks.com/aws/en/lakehouse/medallion.html)
describes bronze, silver and gold layers as a recommendation, not a requirement,
and its example uses one schema per layer. This project has several sources, and
a dataset can serve more than one domain, so one schema per layer would mix
unrelated data, and one set of schemas per domain would tie a source to a domain.

## Decision

**Catalog.** One catalog, `living_canada`, named for its content (socio-economic
data about living in Canada). It is not named after the company, a data source,
or anything that resembles an official agency. One catalog per data product;
environments come later through Declarative Automation Bundles variables
(for example `living_canada_dev`).

**Schemas.** Layer first, lower-case snake_case:

| Schema | Holds |
|---|---|
| `landing` | volumes `raw` and `meta`; files are organized by source in the path |
| `metadata` | platform metadata tables |
| `shared` | conformed dimensions used by several domains (geography, time, NOC) |
| `bronze_<source>` | one schema per source, e.g. `bronze_open_canada` |
| `silver_<domain>`, `gold_<domain>` | one schema each per domain, e.g. `silver_immigration` |

Bronze is organized by source, because a dataset can serve several domains.
Silver and gold are organized by domain. Bronze schemas are derived from the
registry's `source` values and silver and gold schemas from its `domain` values;
both must be valid snake_case identifiers.

**Config-driven names.** The catalog name, the layer prefixes and the volume
paths (`/Volumes/<catalog>/landing/raw` and `/Volumes/<catalog>/landing/meta`)
come from `config/platform.yml`, read by the loader in
`src/platform/platform_config.py`. No notebook or module spells out a catalog
name or a `/Volumes/` path. A notebook's `catalog` widget may override the
configured catalog, for example to use a test catalog.

**Cross-domain dependencies at silver and gold.**

- Default: read the other domain's table directly by its three-part name; Unity
  Catalog records the lineage.
- Use a consumer-side view only to adapt a table (subset, rename, filter). It
  lists its columns explicitly, never `SELECT *`, and there is one level only (a
  view never reads another adapting view).
- Promote a table to `shared` when three or more domains use it with one meaning.
- Dependencies are one-way and there are no cycles. The direction follows the
  roadmap order: a later domain may read an earlier one, never the reverse, and
  every domain may read `shared`. The domain order and the dependencies are
  documented in [architecture.md](../architecture.md).

## Alternatives considered

- **One schema per layer** (the Databricks example): rejected. Sources and
  domains would share one namespace, and table names would carry the source
  or domain as a prefix instead.
- **A set of schemas per domain** (`<domain>_bronze`): rejected for bronze, since
  a source dataset can feed several domains. It is also not layer first, so
  the schemas of one layer do not sort together.
- **Catalog named after the company or a source:** rejected; a catalog is a data
  product, and an agency-like name could suggest an official source.
- **One catalog per environment from the start:** deferred until Bundles
  variables exist (backlog P2).

## Consequences

- Adding a source or a domain to the registry and re-running the bootstrap creates
  its schemas; nothing else changes.
- Renaming a catalog, schema, table, view or column loses its lineage, so the
  names above are chosen to stay stable.
- Reading across domains is visible in lineage and in review, because each
  dependency is a three-part name.
- Tests check that `src/` holds no catalog name or `/Volumes/` path outside the
  config loader.
