"""Test fixture overriding DB path to use an isolated test database.

Placed after core conftest to ensure environment variable is set early.
"""
import os
from pathlib import Path
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
TEST_DB = BASE_DIR / 'test_sd_index.db'

@pytest.fixture(scope='session', autouse=True)
def set_test_db_env():
    # Remove any prior test DB to ensure clean state
    if TEST_DB.exists():
        try:
            TEST_DB.unlink()
        except Exception:
            pass
    os.environ['SD_DB_PATH'] = str(TEST_DB)
    yield
    # Cleanup after session
    for p in (TEST_DB, Path(str(TEST_DB)+'-wal'), Path(str(TEST_DB)+'-shm')):
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
