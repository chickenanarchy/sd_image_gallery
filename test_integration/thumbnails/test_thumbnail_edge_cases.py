import os, pytest
from fastapi.testclient import TestClient

@pytest.mark.integration
@pytest.mark.thumbnails
def test_thumbnail_fallback_on_image_error(monkeypatch, test_client: TestClient):
    from PIL import Image as PILImage
    monkeypatch.setattr(PILImage, 'open', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom')))
    fid = test_client.get('/matching_ids').json()['ids'][0]
    r2 = test_client.get(f'/thumb/{fid}')
    assert r2.status_code == 200

@pytest.mark.integration
@pytest.mark.thumbnails
def test_thumbnail_missing_file_placeholder(test_client: TestClient, tmp_path):
    ids = test_client.get('/matching_ids').json()['ids']
    if not ids:
        pytest.skip('No IDs to test missing-file placeholder')
    target_id = ids[0]
    import webui.main as wm
    with wm.get_db_connection() as conn:
        c = conn.cursor(); c.execute('SELECT file_path FROM files WHERE id=?', (target_id,)); row = c.fetchone()
    if not row:
        pytest.skip('Could not fetch file_path for test id')
    fp = row['file_path']
    if os.path.exists(fp):
        try:
            os.remove(fp)
        except Exception:
            pytest.skip('Unable to remove file to simulate missing state')
    r = test_client.get(f'/thumb/{target_id}')
    assert r.status_code == 200
    assert r.headers.get('content-type','').startswith(('image/png','image/svg'))
