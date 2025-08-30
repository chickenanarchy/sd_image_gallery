import time, sqlite3, pytest
import sd_index_manager as sim
from fastapi.testclient import TestClient
from pathlib import Path

@pytest.mark.integration
@pytest.mark.fileops
def test_copy_then_delete_round_trip(tmp_path, test_client: TestClient, perf_log):
    ids_resp = test_client.get('/matching_ids'); ids = ids_resp.json()['ids'][:2]
    assert ids, 'Need at least one id for file ops test'
    dest_dir = tmp_path / 'copies'; dest_dir.mkdir()
    with sqlite3.connect(sim.DB_PATH) as conn:
        pre_count = conn.execute('SELECT COUNT(*) FROM files').fetchone()[0]
        pre_paths = {row[0] for row in conn.execute('SELECT file_path FROM files')}
    t0 = time.time()
    resp_copy = test_client.post('/file_operation', json={'operation':'copy','ids':ids,'destination':str(dest_dir)})
    assert resp_copy.status_code == 200, resp_copy.text
    data = resp_copy.json(); assert data['counts']['copied'] == len(ids)
    new_files = list(dest_dir.iterdir()); assert len(new_files) == len(ids)
    with sqlite3.connect(sim.DB_PATH) as conn:
        cur = conn.cursor(); mid_count = cur.execute('SELECT COUNT(*) FROM files').fetchone()[0]
        assert mid_count == pre_count + len(ids)
        post_rows = list(cur.execute('SELECT id, file_path FROM files'))
    post_paths = {p for _, p in post_rows}
    new_paths = list(post_paths - pre_paths)
    assert len(new_paths) == len(ids)
    new_path_set = set(new_paths)
    delete_ids = [i for i, p in post_rows if p in new_path_set]
    assert len(delete_ids) == len(ids)
    resp_del = test_client.post('/file_operation', json={'operation':'delete','ids':delete_ids})
    assert resp_del.status_code == 200
    with sqlite3.connect(sim.DB_PATH) as conn:
        post_count = conn.execute('SELECT COUNT(*) FROM files').fetchone()[0]
    assert post_count == pre_count
    with open(perf_log, 'a', encoding='utf-8') as f:
        f.write(f'file_ops_time_ms={(time.time()-t0)*1000:.2f}\n')
