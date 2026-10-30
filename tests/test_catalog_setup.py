"""Unit tests for src/platform/catalog_setup.py (no Spark needed)."""

from pathlib import Path

import pytest

import catalog_setup as cs
import ckan_ingest as ci

ROOT = Path(__file__).resolve().parents[1]


def test_build_ddl_order_and_core_objects():
    ddl = cs.build_ddl("mapledata", ["immigration"])
    assert ddl[0].startswith("CREATE CATALOG IF NOT EXISTS `mapledata`")
    assert any("`mapledata`.`landing`.`raw`" in s for s in ddl)
    assert any("`mapledata`.`landing`.`meta`" in s for s in ddl)
    assert any("`mapledata`.`shared`" in s for s in ddl)
    # Schemas must be created before the volumes that live in them.
    landing = next(i for i, s in enumerate(ddl) if s.startswith("CREATE SCHEMA") and "`landing`" in s)
    raw = next(i for i, s in enumerate(ddl) if "`landing`.`raw`" in s)
    assert landing < raw


def test_every_statement_is_idempotent():
    for sql in cs.build_ddl("mapledata", ["immigration", "housing"]):
        assert "IF NOT EXISTS" in sql


def test_domain_schemas_three_layers_deduplicated_and_sorted():
    assert cs.domain_schemas(["housing", "immigration", "housing"]) == [
        "housing_bronze", "housing_silver", "housing_gold",
        "immigration_bronze", "immigration_silver", "immigration_gold",
    ]


@pytest.mark.parametrize("bad", ["", "Mapledata", "my-catalog", "x; DROP CATALOG y", "1abc"])
def test_invalid_names_are_rejected(bad):
    with pytest.raises(ValueError):
        cs.build_ddl(bad, ["immigration"])
    with pytest.raises(ValueError):
        cs.build_ddl("mapledata", [bad])


def test_shipped_registry_domains_produce_valid_ddl():
    domains = {s.domain for s in ci.load_registry(str(ROOT / "config" / "datasets.yml"))}
    ddl = cs.build_ddl("mapledata", domains)
    assert any("`immigration_bronze`" in s for s in ddl)
