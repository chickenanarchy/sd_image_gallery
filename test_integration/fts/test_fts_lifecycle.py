import sqlite3, pytest
import sd_index_manager as sim
from fastapi.testclient import TestClient
from webui.main import app

@pytest.mark.integration
@pytest.mark.fts
def test_fts_initialized_after_init_db(tmp_path):
    db_path = tmp_path / 'init_test.db'
    import sd_index.paths as p, sd_index.db_schema as ds, sd_index as core
    original = sim.DB_PATH
    p.DB_PATH = ds.DB_PATH = core.DB_PATH = sim.DB_PATH = str(db_path)
    try:
        sim.init_db()
        with sqlite3.connect(str(db_path)) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
            assert cur.fetchone(), 'files_fts missing'
            cols = {r[1] for r in cur.execute('PRAGMA table_info(files_fts)').fetchall()}
            assert {'metadata_json','path','path_norm'} <= cols
    finally:
        p.DB_PATH = ds.DB_PATH = core.DB_PATH = sim.DB_PATH = original

@pytest.mark.integration
@pytest.mark.fts
def test_fts_migration_recreates_table():
    sim.clear_database(); sim.init_db()
    with sqlite3.connect(sim.DB_PATH) as conn:
        c = conn.cursor()
        c.execute('DROP TABLE IF EXISTS files_fts')
        c.execute("CREATE VIRTUAL TABLE files_fts USING fts5(metadata_json, content='files', content_rowid='id')")
        conn.commit()
        sim.ensure_fts(conn)
        row = c.execute("SELECT sql FROM sqlite_master WHERE name='files_fts'").fetchone()
        assert row and 'path_norm' in row[0]

@pytest.mark.integration
@pytest.mark.fts
def test_refresh_fts_endpoint(index_database):
    client = TestClient(app)
    resp = client.post('/refresh_fts')
    assert resp.status_code == 200 and 'has_fts' in resp.json()
