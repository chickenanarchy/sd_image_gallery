"""Database integrity & repair utilities."""
from __future__ import annotations
import os, sqlite3, datetime as _dt
from .paths import DB_PATH
from .db_schema import init_db

def vacuum_repair_db(db_path: str = DB_PATH) -> bool:
    try:
        ts = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
        tmp_path = f"{db_path}.vacuum-{ts}.db"
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            try: cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except sqlite3.Error: pass
            escaped = tmp_path.replace("'", "''")
            cur.execute(f"VACUUM INTO '{escaped}'")
        for suffix in ("-wal", "-shm"):
            try:
                wal_path = f"{db_path}{suffix}"
                if os.path.exists(wal_path):
                    os.remove(wal_path)
            except Exception:
                pass
        os.replace(tmp_path, db_path)
        return True
    except Exception:
        return False

def check_and_repair_db(db_path: str = DB_PATH) -> bool:
    if not os.path.exists(db_path):
        return True
    try:
        with sqlite3.connect(db_path, timeout=5.0) as conn:
            cur = conn.cursor()
            try: cur.execute("PRAGMA busy_timeout = 5000;")
            except sqlite3.Error: pass
            try: cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except sqlite3.Error: pass
            cur.execute("PRAGMA integrity_check")
            result = cur.fetchone()
            if not result:
                print("Integrity check returned no result.")
                return False
            if result[0] == 'ok':
                return True
            print("Integrity check failed:", result[0])
    except sqlite3.DatabaseError as e:
        print("DatabaseError during integrity check:", e)
    try:
        if vacuum_repair_db(db_path):
            print("VACUUM-based rebuild completed.")
            return True
    except Exception:  # pragma: no cover
        pass
    try:
        ts = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
        corrupt_name = f"{db_path}.corrupt-{ts}.db"
        os.replace(db_path, corrupt_name)
        for suffix in ("-wal", "-shm"):
            try:
                p = f"{db_path}{suffix}"
                if os.path.exists(p):
                    os.replace(p, f"{corrupt_name}{suffix}")
            except Exception:
                pass
        print(f"Corrupted database renamed to {corrupt_name}. Creating new database...")
        init_db()
        return True
    except PermissionError as e:
        if hasattr(e, 'winerror') and e.winerror == 32:
            try:
                ts2 = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
                backup_name = f"{db_path}.backup-{ts2}.db"
                with sqlite3.connect(db_path, timeout=5.0) as src, sqlite3.connect(backup_name) as dst:
                    src.backup(dst)
                print("Database appears in use; backup created at:", backup_name)
            except Exception as be:
                print("Failed to create backup while locked:", be)
            return False
        print("Failed to auto-repair database:", e)
        return False
    except Exception as e:  # pragma: no cover
        print("Failed to auto-repair database:", e)
        return False

__all__ = ['check_and_repair_db', 'vacuum_repair_db']
