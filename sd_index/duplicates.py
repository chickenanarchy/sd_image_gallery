"""Duplicate detection & deletion by hash."""
from __future__ import annotations
import os, sqlite3, time
from .paths import DB_PATH
from .progress import _progress_bar

def de_duplicate_by_hash(auto_confirm: bool = False):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_hash, COUNT(*) c FROM files WHERE file_hash IS NOT NULL AND file_hash <> '' GROUP BY file_hash HAVING c>1")
            dupes = cur.fetchall()
    except sqlite3.Error as e:
        print("Database error during duplicate scan:", e); return None
    if not dupes:
        print("No duplicate file hashes found."); return None
    groups = len(dupes)
    files_to_delete = sum(row[1]-1 for row in dupes if row[1] > 1)
    print(f"Found {groups} duplicate hash group(s). {files_to_delete} file(s) would be deleted (keeping one per group).")
    if not auto_confirm:
        confirm = input("Type 'y' to proceed (anything else to cancel): ").strip().lower()
        if confirm != 'y':
            print("Cancelled."); return None
    try:
        with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            files_deleted = rows_removed = bytes_freed = 0
            with _progress_bar(groups, title="Deleting duplicates") as bar2:
                for h,count in dupes:
                    cur.execute("SELECT id, file_path, file_size FROM files WHERE file_hash=? ORDER BY id ASC", (h,))
                    rows = cur.fetchall()
                    if not rows:
                        bar2(); continue
                    keep_index = 0
                    for i,r in enumerate(rows):
                        try:
                            if os.path.isfile(r['file_path']): keep_index=i; break
                        except Exception:
                            continue
                    for i,r in enumerate(rows):
                        if i == keep_index:
                            continue
                        path = r['file_path']
                        size_val = None
                        try: size_val = int(r['file_size']) if r['file_size'] is not None else None
                        except Exception: pass
                        if os.path.isfile(path):
                            try:
                                os.remove(path); files_deleted += 1
                                if isinstance(size_val, int): bytes_freed += size_val
                            except Exception: pass
                        cur.execute("DELETE FROM files WHERE id=?", (r['id'],)); rows_removed += 1
                    bar2()
            conn.commit()
    except sqlite3.Error as e:
        print("Database error during de-duplication:", e); return None
    print(f"De-duplication complete. Groups: {groups} Files deleted: {files_deleted} Rows removed: {rows_removed}")
    return {"duplicate_groups": groups, "files_deleted": files_deleted, "rows_removed": rows_removed, "space_freed_bytes": bytes_freed}

__all__ = ['de_duplicate_by_hash']
