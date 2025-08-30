import time, pytest
from fastapi.testclient import TestClient

@pytest.mark.integration
@pytest.mark.fileops
def test_async_file_operation_ids_scope(test_client: TestClient, tmp_path):
    ids = test_client.get('/matching_ids').json()['ids'][:3]
    if not ids:
        pytest.skip('No IDs available for async IDs scope test')
    dest = tmp_path / 'async_ids'; dest.mkdir()
    payload = {"operation":"copy","scope":{"type":"ids","ids":ids},"destination": str(dest)}
    job = test_client.post('/file_operation_async', json=payload)
    assert job.status_code == 200
    job_id = job.json()['job_id']
    for _ in range(60):
        status = test_client.get(f'/file_operation_status/{job_id}').json()
        if status['status'] in ('completed','failed'): break
        time.sleep(0.1)
    assert status['status'] == 'completed'

@pytest.mark.integration
@pytest.mark.fileops
def test_async_file_operation_query_scope(test_client: TestClient, tmp_path):
    ids_resp = test_client.get('/matching_ids'); ids = ids_resp.json()['ids']
    if not ids:
        pytest.skip('No ids to test async query scope')
    excluded = ids[:1]
    dest = tmp_path / 'async_query'; dest.mkdir()
    payload = {'operation': 'copy','scope': {'type': 'query','search': '', 'logics': [], 'values': [], 'sort': 'last_scanned','year': '', 'month': '', 'excluded': excluded,},'destination': str(dest)}
    job_resp = test_client.post('/file_operation_async', json=payload)
    assert job_resp.status_code == 200
    job_id = job_resp.json()['job_id']
    for _ in range(120):
        js = test_client.get(f'/file_operation_status/{job_id}').json()
        if js['status'] in ('completed','failed'): break
        time.sleep(0.05)
    assert js['status'] == 'completed'
    assert js['counts']['copied'] <= (len(ids) - len(excluded))

@pytest.mark.integration
@pytest.mark.fileops
def test_matching_ids_truncation_simulated(monkeypatch, test_client: TestClient):
    import webui.main as wm
    orig = wm.get_db_connection
    class FakeConn:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self
        def execute(self, *a, **k): return self
        def fetchall(self): return [{'id': i} for i in range(105000)]
    monkeypatch.setattr(wm, 'get_db_connection', lambda: FakeConn())
    resp = test_client.get('/matching_ids')
    data = resp.json(); assert data['truncated'] is True and len(data['ids']) == 100000
    monkeypatch.setattr(wm, 'get_db_connection', orig)
