import sqlite3, pytest
import sd_index_manager as sim
from fastapi.testclient import TestClient
from webui.main import app

@pytest.mark.integration
@pytest.mark.extraction
def test_extraction_populates_models_table(index_database):
    with sqlite3.connect(sim.DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM files WHERE metadata_json IS NOT NULL AND metadata_json != ''")
        meta_files = cur.fetchone()[0]
        models_count = cur.execute('SELECT COUNT(*) FROM models').fetchone()[0]
        if meta_files:
            assert models_count > 0

@pytest.mark.integration
@pytest.mark.extraction
def test_extraction_summary_endpoint_shapes(index_database):
    client = TestClient(app)
    resp = client.get('/extraction_summary')
    assert resp.status_code == 200
    data = resp.json()
    for key in ['files','models','with_lora','distinct_loras','top_loras']:
        assert key in data
    # Some metadata rows can enumerate multiple models, so models may exceed file count.
    assert data['files'] >= 0 and data['models'] >= 0
    assert isinstance(data['top_loras'], list)

@pytest.mark.integration
@pytest.mark.extraction
def test_extraction_idempotent_second_run(index_database):
    from sd_index.extraction import extract_models
    with sqlite3.connect(sim.DB_PATH) as conn:
        summary = extract_models(conn)
    assert summary['new'] == 0 and summary['updated'] == 0
