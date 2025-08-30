"""sd_index package: modularized core logic for Stable Diffusion image index management.

Public API (re-exported for backward compatibility with original sd_index_manager module):
  init_db, ensure_fts, fts_exists, drop_fts
  check_and_repair_db, vacuum_repair_db
  index_files, de_duplicate_by_hash
  clear_database, run_webui
  DB_PATH, FTS_REBUILDING_FLAG, BASE_DIR

Internal helpers live in submodules (progress, scanning, etc.).
"""

from .paths import DB_PATH, FTS_REBUILDING_FLAG, BASE_DIR
from .db_schema import init_db, ensure_fts, fts_exists, drop_fts
from .db_repair import check_and_repair_db, vacuum_repair_db
from .indexing import index_files
from .duplicates import de_duplicate_by_hash
from .cleanup import clear_database
from .webui_launcher import run_webui
from .extraction import extract_models

__all__ = [
  'DB_PATH', 'FTS_REBUILDING_FLAG', 'BASE_DIR',
  'init_db', 'ensure_fts', 'fts_exists', 'drop_fts',
  'check_and_repair_db', 'vacuum_repair_db',
  'index_files', 'de_duplicate_by_hash',
  'clear_database', 'run_webui',
  'extract_models'
]
