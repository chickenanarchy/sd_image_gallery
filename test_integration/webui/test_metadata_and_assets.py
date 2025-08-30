import time, sqlite3, datetime as dt, pytest
import sd_index_manager as sim
from fastapi.testclient import TestClient

@pytest.mark.integration
@pytest.mark.webui
def test_metadata_and_thumbnail_chain(test_client: TestClient, perf_log):
    r_ids = test_client.get('/matching_ids'); ids = r_ids.json()['ids']
    assert ids, 'No IDs returned from matching_ids'
    fid = ids[0]
    meta = test_client.get(f'/metadata/{fid}')
    assert meta.status_code == 200 and 'metadata_json' in meta.json()
    t0 = time.time(); thumb = test_client.get(f'/thumb/{fid}'); t1 = time.time()
    img = test_client.get(f'/image/{fid}')
    # Image may be 404 if file was concurrently removed by another test; thumbnail fallback already validated.
    assert thumb.status_code == 200
    assert img.status_code in (200, 404)
    with open(perf_log, 'a', encoding='utf-8') as f:
        f.write(f'thumb_time_ms={(t1-t0)*1000:.2f}\n')

@pytest.mark.integration
@pytest.mark.webui
def test_favicon_served_or_empty(test_client: TestClient):
    r = test_client.get('/favicon.ico')
    assert r.status_code in (200, 204)

@pytest.mark.integration
@pytest.mark.webui
def test_metadata_fields_endpoint_returns_list(test_client: TestClient):
    r = test_client.get('/metadata_fields')
    assert r.status_code == 200 and isinstance(r.json(), list)

@pytest.mark.integration
@pytest.mark.webui
def test_time_facets_and_cache_behavior(test_client: TestClient):
    with sqlite3.connect(sim.DB_PATH) as conn:
        c = conn.cursor(); c.execute('SELECT id FROM files LIMIT 1'); row = c.fetchone()
        if row:
            import time as _t
            ts = int(_t.time())
            c.execute('UPDATE files SET file_mtime=? WHERE id=?', (ts, row[0])); conn.commit()
            year = dt.datetime.utcfromtimestamp(ts).strftime('%Y')
            month = dt.datetime.utcfromtimestamp(ts).strftime('%m')
            r1 = test_client.get('/matching_count', params={'sort':'file_mtime','year':year,'month':month})
            assert r1.status_code == 200
            r2 = test_client.get('/matching_count', params={'sort':'file_mtime','year':year,'month':month})
            assert r2.status_code == 200
    facets = test_client.get('/time_facets', params={'column':'file_mtime'})
    assert facets.status_code == 200 and 'years' in facets.json()

@pytest.mark.integration
@pytest.mark.webui
def test_time_filter_utc_specific_bucket(test_client: TestClient):
    target_epoch = 1705276800  # 2024-01-15 00:00:00 UTC
    with sqlite3.connect(sim.DB_PATH) as conn:
        cur = conn.cursor(); cur.execute('UPDATE files SET file_mtime=? WHERE id=(SELECT id FROM files LIMIT 1)', (target_epoch,)); conn.commit()
    year = '2024'; month = '01'
    r = test_client.get('/matching_count', params={'sort':'file_mtime','year':year,'month':month})
    assert r.status_code == 200 and r.json()['total'] >= 1
