# Databricks notebook source
import os, sys, pytest
from pathlib import Path

# Walk up from this notebook's folder to the repo root (it contains config/datasets.yml).
here = Path.cwd()
root = next(p for p in [here, *here.parents] if (p / "config" / "datasets.yml").exists())
os.chdir(root)

sys.dont_write_bytecode = True
pytest.main(["-v", "-p", "no:cacheprovider", "tests/"])
