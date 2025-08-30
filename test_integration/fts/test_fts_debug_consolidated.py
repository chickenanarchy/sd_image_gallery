import os, sqlite3, pytest
import sd_index_manager as sim
from sd_index.db_schema import ensure_fts

@pytest.mark.integration
@pytest.mark.fts
def test_fts_populated_and_searchable(index_database):
    """Ensure FTS index has same number of docs as files and term search works when data present.

    This consolidates prior ad-hoc debug scripts (_debug_search, _debug_fts_schema, etc.).
    If there are no rows containing the probe term we only assert non-zero docs for FTS.
    """
    db_path = sim.DB_PATH
    assert os.path.exists(db_path), 'DB missing'
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # Force ensure to catch legacy / empty states
        ensure_fts(conn)
        cur.execute('SELECT COUNT(*) FROM files')
        file_count = cur.fetchone()[0]
        assert file_count >= 0
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='files_fts'")
        ddl = cur.fetchone()
        assert ddl and 'path' in ddl[0] and 'path_norm' in ddl[0]
        cur.execute('SELECT COUNT(*) FROM files_fts')
        fts_docs = cur.fetchone()[0]
        # Either zero files (fresh DB) or FTS should have docs.
        if file_count > 0:
            assert fts_docs == file_count, f"FTS docs {fts_docs} != files {file_count}"
        # Probe a term present in sample test data if available
        probe_term = 'Letterism'
        cur.execute('SELECT COUNT(*) FROM files WHERE metadata_json LIKE ?', (f'%{probe_term}%',))
        like_hits = cur.fetchone()[0]
        if like_hits:
            cur.execute('SELECT COUNT(*) FROM files_fts WHERE files_fts MATCH ?', (probe_term,))
            fts_hits = cur.fetchone()[0]
            assert fts_hits == like_hits, f"FTS hits {fts_hits} differ from LIKE hits {like_hits} for term {probe_term}"
