"""Path constants used across modules.

Kept separate so tests and runtime can import DB_PATH without pulling heavy logic.
"""
from __future__ import annotations
import os

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(PACKAGE_DIR)  # repository root (matches original sd_index_manager BASE_DIR)

_custom_db = os.getenv("SD_DB_PATH", "").strip()
if _custom_db:
	DB_PATH = os.path.abspath(_custom_db)
else:
	DB_PATH = os.path.join(BASE_DIR, "sd_index.db")

# Place FTS flag alongside the active DB file
FTS_REBUILDING_FLAG = os.path.join(os.path.dirname(DB_PATH), "fts_rebuilding.flag")

__all__ = ["DB_PATH", "FTS_REBUILDING_FLAG", "BASE_DIR"]
