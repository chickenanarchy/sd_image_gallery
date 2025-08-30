import os
import shutil
import sqlite3
import time
import datetime as dt
import pytest
from pathlib import Path

# Ensure local imports work
import importlib.util
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
PROD_DB_PATH = BASE_DIR / 'sd_index.db'
BACKUP_PATH = BASE_DIR / 'backup_sd_index.db'

# Always use an isolated test database (removes need for rename juggling).
TEST_DB_PATH = BASE_DIR / 'test_sd_index.db'
os.environ['SD_DB_PATH'] = str(TEST_DB_PATH)
DB_PATH = TEST_DB_PATH  # compatibility name for rest of this file
TEST_IMG_ENV = os.getenv('TEST_IMAGE_DIR')
TEST_IMAGES_DIR = Path(TEST_IMG_ENV) if TEST_IMG_ENV else (BASE_DIR / 'test_images')
if not TEST_IMAGES_DIR.exists():
    # fallback to 'test' folder already present in repo
    fallback = BASE_DIR / 'test'
    if fallback.exists():
        TEST_IMAGES_DIR = fallback

PERF_LOG = BASE_DIR / 'test_integration' / 'perf_log.txt'

@pytest.fixture(scope='session', autouse=True)
def isolated_test_db():
    """Ensure a clean dedicated test DB (test_sd_index.db) each session.

    Removes any existing test DB before tests and cleans up afterward.
    Production db (sd_index.db) is untouched.
    """
    # Pre-test cleanup
    for p in (DB_PATH, Path(str(DB_PATH)+'-wal'), Path(str(DB_PATH)+'-shm')):
        if p.exists():
            try: p.unlink()
            except Exception: pass
    yield
    # Post-test cleanup
    for p in (DB_PATH, Path(str(DB_PATH)+'-wal'), Path(str(DB_PATH)+'-shm')):
        if p.exists():
            try: p.unlink()
            except Exception: pass

@pytest.fixture(scope='session')
def perf_log():
    PERF_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PERF_LOG, 'a', encoding='utf-8') as f:
        f.write(f"\n=== RUN {dt.datetime.utcnow().isoformat()}Z ===\n")
    return PERF_LOG

@pytest.fixture(scope='function')
def test_image_dir(tmp_path_factory):
    """Provide a per-test image directory to avoid destructive side-effects.

    Copies repository test images if present; otherwise generates a few tiny
    placeholder PNG files so indexing always has content. This prevents tests
    that delete or modify files from impacting later tests.
    """
    work_dir = tmp_path_factory.mktemp('imgs')
    src_files = list(TEST_IMAGES_DIR.glob('*')) if TEST_IMAGES_DIR.exists() else []
    if src_files:
        for f in src_files:
            try:
                shutil.copy2(f, work_dir / f.name)
            except Exception:
                pass
    # If still empty, synthesize a few minimal PNG files
    if not any(work_dir.iterdir()):
        png_stub = (b"\x89PNG\r\n\x1a\n"  # header
                    b"\x00\x00\x00\rIHDR"  # IHDR chunk
                    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"  # 1x1 RGB
                    b"\x90wS\xDE"  # CRC
                    b"\x00\x00\x00\x0AIDAT\x08\xD7c``\x00\x00\x00\x04\x00\x01"  # IDAT
                    b"\x0D\x0A\x2D\xB4"  # CRC
                    b"\x00\x00\x00\x00IEND\xAE\x42\x60\x82")
        for i in range(3):
            (work_dir / f'sample_{i}.png').write_bytes(png_stub)
    return str(work_dir)

@pytest.fixture(scope='function')
def index_database(test_image_dir, perf_log, monkeypatch):
    """Build (or rebuild) the test index each test function.

    Removes brittle caching so tests that clear/recreate the DB do not
    inadvertently leave later tests with an empty database while still
    returning a cached positive result.
    The image corpus is small so re-index cost is negligible; if it grows
    dramatically we can reintroduce a cache with a fresh row-count check.
    """
    start = time.time()
    monkeypatch.setattr('builtins.input', lambda prompt='': test_image_dir)
    import sd_index_manager as sim
    # Ensure a fresh DB each test (prevents size bloat and stale FTS triggers)
    for p in (Path(sim.DB_PATH), Path(sim.DB_PATH+'-wal'), Path(sim.DB_PATH+'-shm')):
        if p.exists():
            try: p.unlink()
            except Exception: pass
    sim.init_db()
    sim.index_files()
    duration = time.time() - start
    with open(perf_log, 'a', encoding='utf-8') as f:
        f.write(f"index_time_sec={duration:.3f}\n")
    with sqlite3.connect(sim.DB_PATH) as conn:
        cur = conn.cursor(); cur.execute('SELECT COUNT(*) FROM files')
        count = cur.fetchone()[0]
    # Allow zero-image corpus only for explicit empty corpus testing; otherwise generate assertion.
    assert count > 0, 'Expected at least one indexed file (fixture guarantees placeholder creation)'
    return {'count': count, 'duration': duration}

@pytest.fixture(scope='function')
def test_client(index_database):  # ensure DB built first
    from fastapi.testclient import TestClient
    from webui.main import app
    return TestClient(app)

def pytest_configure(config):
    config.addinivalue_line('markers', 'integration: integration test suite')
