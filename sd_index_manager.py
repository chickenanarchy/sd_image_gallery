"""Backward-compatible facade for the refactored sd_index package.

Existing code & tests import this module as `sd_index_manager` (aliased as sim).
We re-export the previous public API from the new modular structure.
"""

from sd_index import (
	DB_PATH, FTS_REBUILDING_FLAG, BASE_DIR,
	init_db, ensure_fts, fts_exists, drop_fts,
	check_and_repair_db, vacuum_repair_db,
	index_files, de_duplicate_by_hash,
	clear_database, run_webui
)
from sd_index.extraction import extract_models

__all__ = [
	'DB_PATH', 'FTS_REBUILDING_FLAG', 'BASE_DIR',
	'init_db', 'ensure_fts', 'fts_exists', 'drop_fts',
	'check_and_repair_db', 'vacuum_repair_db',
	'index_files', 'de_duplicate_by_hash',
	'clear_database', 'run_webui',
	'extract_models'
]

if __name__ == '__main__':  # pragma: no cover
	from sd_index.cli import main
	main()


