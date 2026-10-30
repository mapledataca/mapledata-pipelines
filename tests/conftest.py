"""Make src/ingest importable from the tests folder (local and Databricks)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingest"))
sys.path.insert(0, str(ROOT / "src" / "platform"))
