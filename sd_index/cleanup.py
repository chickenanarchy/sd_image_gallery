"""General maintenance helpers."""
from __future__ import annotations
import os
from .paths import DB_PATH

def clear_database():
    removed_any = False
    for p in (DB_PATH, f"{DB_PATH}-wal", f"{DB_PATH}-shm"):
        try:
            if os.path.exists(p):
                os.remove(p)
                removed_any = True
        except Exception as e:
            print(f"Failed to remove {p}: {e}")
    if removed_any:
        print("Database removed.")
    else:
        print("No database found to remove.")

__all__ = ['clear_database']
