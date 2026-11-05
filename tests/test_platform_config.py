"""Tests for platform_config: parsing, validation, schema names, volume paths."""

import re
from pathlib import Path

import pytest

import platform_config as pc

ROOT = Path(__file__).resolve().parents[1]


def test_repo_config_loads_and_matches_defaults():
    cfg = pc.load_platform_config(str(ROOT / "config" / "platform.yml"))
    assert cfg == pc.PlatformConfig()
    assert cfg.catalog == "living_canada"


def test_minimal_config_uses_default_layout():
    cfg = pc.parse_platform_config({"catalog": "test_catalog"})
    assert cfg.catalog == "test_catalog"
    assert cfg.landing_schema == "landing"
    assert cfg.layer_prefixes == {"bronze": "bronze", "silver": "silver", "gold": "gold"}


@pytest.mark.parametrize(
    "raw",
    [None, [], "x", {}, {"catalog": ""}, {"catalog": "Bad-Name"}, {"catalog": "ok", "extra": 1},
     {"catalog": "ok", "raw_volume": "Raw"},
     {"catalog": "ok", "layer_prefixes": {"bronze": "b"}},
     {"catalog": "ok", "layer_prefixes": {"bronze": "b", "silver": "s", "gold": "G!"}}],
)
def test_invalid_config_is_rejected(raw):
    with pytest.raises(pc.PlatformConfigError):
        pc.parse_platform_config(raw)


def test_schema_names_are_layer_first():
    cfg = pc.PlatformConfig()
    assert cfg.bronze_schema("open_canada") == "bronze_open_canada"
    assert cfg.silver_schema("immigration") == "silver_immigration"
    assert cfg.gold_schema("immigration") == "gold_immigration"


def test_schema_lists_are_sorted_and_deduplicated():
    cfg = pc.PlatformConfig()
    assert cfg.bronze_schemas(["statcan", "open_canada", "statcan"]) == [
        "bronze_open_canada", "bronze_statcan",
    ]
    assert cfg.domain_schemas(["housing", "immigration", "housing"]) == [
        "silver_housing", "gold_housing", "silver_immigration", "gold_immigration",
    ]


@pytest.mark.parametrize("bad", ["", "Open", "open-canada", "1abc", "x; DROP SCHEMA y"])
def test_invalid_source_and_domain_are_rejected(bad):
    cfg = pc.PlatformConfig()
    with pytest.raises(ValueError):
        cfg.bronze_schema(bad)
    with pytest.raises(ValueError):
        cfg.silver_schema(bad)
    with pytest.raises(ValueError):
        cfg.gold_schema(bad)
    with pytest.raises(ValueError):
        cfg.domain_schemas([bad])


def test_volume_paths_derive_from_the_catalog():
    cfg = pc.PlatformConfig()
    assert cfg.raw_volume_path() == "/Volumes/living_canada/landing/raw"
    assert cfg.meta_volume_path() == "/Volumes/living_canada/landing/meta"
    assert cfg.raw_volume_path("test_catalog") == "/Volumes/test_catalog/landing/raw"
    assert cfg.meta_volume_path("test_catalog") == "/Volumes/test_catalog/landing/meta"


def test_volume_path_rejects_a_bad_catalog():
    with pytest.raises(ValueError):
        pc.PlatformConfig().raw_volume_path("bad/../catalog")


def test_resolve_catalog_prefers_a_widget_value():
    cfg = pc.PlatformConfig()
    assert cfg.resolve_catalog("") == "living_canada"
    assert cfg.resolve_catalog("   ") == "living_canada"
    assert cfg.resolve_catalog(None) == "living_canada"
    assert cfg.resolve_catalog(" living_canada_dev ") == "living_canada_dev"
    with pytest.raises(ValueError):
        cfg.resolve_catalog("Not Valid")


def test_no_catalog_or_volume_literals_in_src_outside_the_config_loader():
    """Catalog names and /Volumes/ paths come from config/platform.yml."""
    offenders = []
    for path in (ROOT / "src").rglob("*.py"):
        if path.name == "platform_config.py":
            continue
        text = path.read_text(encoding="utf-8")
        for literal in ("living_canada", "/Volumes/"):
            if literal in text:
                offenders.append(f"{path.relative_to(ROOT)}: {literal}")
        # The old catalog name, but not the website name mapledata.io.
        if re.search(r"mapledata(?![.\w-])", text):
            offenders.append(f"{path.relative_to(ROOT)}: mapledata")
    assert offenders == []
