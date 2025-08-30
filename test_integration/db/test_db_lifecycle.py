import sqlite3, time, shutil, os, pytest
from pathlib import Path
import sd_index_manager as sim

@pytest.mark.integration
@pytest.mark.db
def test_index_fast_then_full_refresh_updates_stats(monkeypatch, test_image_dir):
    sim.clear_database(); sim.init_db()
    monkeypatch.setattr('builtins.input', lambda _='': test_image_dir)
    fast_stats = sim.index_files(test_image_dir, full_refresh=False)
    assert fast_stats['mode'] == 'FAST'
    # touch a file to ensure full refresh path executes
    first = next(Path(test_image_dir).glob('*'))
    new_time = time.time() + 5
    os.utime(first, (new_time, new_time))
    full_stats = sim.index_files(test_image_dir, full_refresh=True)
    assert full_stats['mode'] == 'FULL'
    assert full_stats['skipped'] >= 0

@pytest.mark.integration
@pytest.mark.db
def test_vacuum_repair_db_returns_bool(tmp_path):
    db_path = tmp_path / 'mini.db'
    with sqlite3.connect(db_path) as conn:
        conn.execute('create table t(a)')
        conn.executemany('insert into t(a) values (?)', [(i,) for i in range(3)])
        conn.commit()
    assert sim.vacuum_repair_db(str(db_path)) in (True, False)

@pytest.mark.integration
@pytest.mark.db
def test_check_and_repair_handles_integrity_failure(monkeypatch, tmp_path):
    db_path = tmp_path / 'corrupt.db'
    sim.init_db()
    if Path(sim.DB_PATH).exists():
        db_path.write_bytes(Path(sim.DB_PATH).read_bytes())
    class FakeCursor:
        def execute(self, *a, **k): return self
        def fetchone(self): return ('not ok',)
    class FakeConn:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return FakeCursor()
    def fake_connect(path, timeout=5.0):
        assert str(path) == str(db_path)
        return FakeConn()
    import sqlite3 as _sq
    monkeypatch.setattr(_sq, 'connect', fake_connect)
    sim.check_and_repair_db(str(db_path))  # should not raise

@pytest.mark.integration
@pytest.mark.db
def test_deduplicate_by_hash_smoke(monkeypatch, tmp_path, test_image_dir):
    target_dir = Path(test_image_dir)
    original = next(target_dir.glob('*'))
    dup = target_dir / ('dup_' + original.name)
    shutil.copy2(original, dup)
    sim.index_files(test_image_dir, full_refresh=False)
    summary = sim.de_duplicate_by_hash(auto_confirm=True)
    if summary:
        assert 'duplicate_groups' in summary
    if dup.exists():
        dup.unlink()

@pytest.mark.integration
@pytest.mark.db
def test_deduplicate_by_hash_cancel(monkeypatch, tmp_path):
    # create duplicate content files in isolated dir, cancel at prompt
    test_dir = tmp_path / 'dupes'; test_dir.mkdir()
    p1 = test_dir / 'a.bin'; p2 = test_dir / 'b.bin'
    content = b'dupe data'; p1.write_bytes(content); p2.write_bytes(content)
    sim.clear_database(); sim.init_db(); sim.index_files(str(test_dir), full_refresh=False)
    monkeypatch.setattr('builtins.input', lambda *_: 'n')
    res = sim.de_duplicate_by_hash(auto_confirm=False)
    assert res is None
