import time, pytest
from fastapi.testclient import TestClient

@pytest.mark.integration
@pytest.mark.webui
def test_gallery_root_renders(test_client: TestClient):
    r = test_client.get('/')
    assert r.status_code == 200
    assert 'gallery' in r.text.lower()
    # New regression guard: ensure at least one thumbnail <article> rendered when DB has rows
    # We rely on prior fixture guaranteeing >0 files in DB.
    assert '<article' in r.text and '/thumb/' in r.text, 'Expected at least one rendered thumbnail article'

@pytest.mark.integration
@pytest.mark.webui
def test_matching_count_and_ids_consistent(test_client: TestClient):
    r_count = test_client.get('/matching_count')
    r_ids = test_client.get('/matching_ids')
    assert r_count.status_code == 200 and r_ids.status_code == 200
    data_c = r_count.json(); data_i = r_ids.json()
    assert 'total' in data_c and data_c['total'] >= 0
    assert 'ids' in data_i and isinstance(data_i['ids'], list)

@pytest.mark.integration
@pytest.mark.webui
@pytest.mark.parametrize('sort_field', ["last_scanned","file_name","file_size","file_mtime","file_ctime","width","height","id"]) 
@pytest.mark.parametrize('order', ['asc','desc'])
def test_sort_orders_valid(test_client: TestClient, sort_field, order):
    r = test_client.get('/', params={'sort': sort_field, 'order': order, 'page_size': 5})
    assert r.status_code == 200, f"failed sort={sort_field} order={order}"

@pytest.mark.integration
@pytest.mark.webui
def test_search_queries_and_perf_logged(test_client: TestClient, perf_log):
    term = 'a'
    t0 = time.time()
    r = test_client.get('/', params={'search': term})
    assert r.status_code == 200
    r2 = test_client.get('/', params={'search': term, 'values': ['{}','LEN>0'], 'logics': ['AND','AND']})
    assert r2.status_code == 200
    elapsed_ms = (time.time() - t0) * 1000
    with open(perf_log, 'a', encoding='utf-8') as f:
        f.write(f'search_sample_ms={elapsed_ms:.2f}\n')
