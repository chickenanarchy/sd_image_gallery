# sd_index_manager.py

import os
import sqlite3
import sys

def ensure_fts(conn: sqlite3.Connection):
    """Ensure FTS5 virtual table and triggers exist; backfill if newly created.

    Uses content=files with content_rowid=id so that we can leverage automatic data linkage.
    Triggers keep the FTS index in sync for INSERT / UPDATE / DELETE.
    """
    cursor = conn.cursor()
    # Detect whether FTS table already exists
    cursor.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'""")
    exists = cursor.fetchone() is not None
    try:
        if not exists:
            # Create FTS table with prefix indexes for faster wildcard (prefix) searching
            cursor.execute(
                """
                CREATE VIRTUAL TABLE files_fts USING fts5(
                    metadata_json,
                    content='files',
                    content_rowid='id',
                    prefix='2 3 4'
                );
                """
            )
            # Triggers to keep FTS data in sync
            cursor.executescript(
                """
                CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
                    INSERT INTO files_fts(rowid, metadata_json) VALUES (new.id, new.metadata_json);
                END;
                CREATE TRIGGER files_ad AFTER DELETE ON files BEGIN
                    INSERT INTO files_fts(files_fts, rowid, metadata_json) VALUES('delete', old.id, old.metadata_json);
                END;
                CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
                    INSERT INTO files_fts(files_fts, rowid, metadata_json) VALUES('delete', old.id, old.metadata_json);
                    INSERT INTO files_fts(rowid, metadata_json) VALUES (new.id, new.metadata_json);
                END;
                """
            )
            # Backfill existing rows
            cursor.execute("""INSERT INTO files_fts(rowid, metadata_json)
                               SELECT id, metadata_json FROM files WHERE metadata_json IS NOT NULL""")
            conn.commit()
            print("FTS index created and backfilled.")
    except sqlite3.OperationalError as e:
        # Likely FTS5 not compiled; warn user once
        print("Warning: Could not create FTS index (", e, ") - continuing without FTS.")

def fts_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
        return cur.fetchone() is not None
    except sqlite3.Error:
        return False

def drop_fts(conn: sqlite3.Connection):
    """Drop FTS5 table & associated triggers if present (best-effort)."""
    cur = conn.cursor()
    try:
        cur.executescript(
            """
            DROP TRIGGER IF EXISTS files_ai;
            DROP TRIGGER IF EXISTS files_ad;
            DROP TRIGGER IF EXISTS files_au;
            DROP TABLE IF EXISTS files_fts;
            """
        )
        conn.commit()
        print("Dropped existing FTS index & triggers for fast bulk load.")
    except sqlite3.Error as e:  # pragma: no cover - best effort
        print("Warning: failed to drop FTS objects:", e)

def init_db():
    creating = not os.path.exists("sd_index.db")
    if creating:
        print("Creating new sd_index.db...")
    with sqlite3.connect("sd_index.db") as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            file_path TEXT UNIQUE NOT NULL,
            file_hash TEXT,
            metadata_json TEXT,
            last_scanned DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        # Indexes (id is primary). last_scanned often used for ordering; file_hash for lookup.
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_last_scanned ON files(last_scanned DESC)")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_file_hash ON files(file_hash)")
        except sqlite3.OperationalError:
            pass
        # Schema migration: add file_size, file_mtime if missing
        cursor.execute("PRAGMA table_info(files)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        if 'file_size' not in existing_cols:
            cursor.execute("ALTER TABLE files ADD COLUMN file_size INTEGER")
            print("Added column file_size")
        if 'file_mtime' not in existing_cols:
            cursor.execute("ALTER TABLE files ADD COLUMN file_mtime INTEGER")
            print("Added column file_mtime")
        if 'file_ctime' not in existing_cols:
            cursor.execute("ALTER TABLE files ADD COLUMN file_ctime INTEGER")
            print("Added column file_ctime")
        if 'width' not in existing_cols:
            cursor.execute("ALTER TABLE files ADD COLUMN width INTEGER")
            print("Added column width")
        if 'height' not in existing_cols:
            cursor.execute("ALTER TABLE files ADD COLUMN height INTEGER")
            print("Added column height")
        conn.commit()
        ensure_fts(conn)
    if creating:
        print("Database initialized.")
    else:
        print("sd_index.db already exists (schema ensured / FTS checked).")

def check_and_repair_db(db_path: str = "sd_index.db") -> bool:
    """Run PRAGMA integrity_check; if malformed attempt automated repair.

    Strategy: rename corrupted file to *.corrupt-<timestamp>.db and recreate
    a fresh schema. Returns True if DB is healthy (or repaired), False if
    unrecoverable without user intervention.
    """
    if not os.path.exists(db_path):
        return True
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
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
    # Attempt repair
    try:
        import datetime as _dt
        ts = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
        corrupt_name = f"{db_path}.corrupt-{ts}.db"
        os.replace(db_path, corrupt_name)
        print(f"Corrupted database renamed to {corrupt_name}. Creating new database...")
        init_db()
        return True
    except Exception as e:  # pragma: no cover (best-effort)
        print("Failed to auto-repair database:", e)
        return False

def clear_database():
    if os.path.exists("sd_index.db"):
        os.remove("sd_index.db")
        print("Database removed.")
    else:
        print("No database found to remove.")

import hashlib
import json
import datetime
from sd_parsers import ParserManager
from alive_progress import alive_bar
import os

def serialize_obj(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, list):
        return [serialize_obj(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize_obj(value) for key, value in obj.items()}
    else:
        if hasattr(obj, "__dict__"):
            return {key: serialize_obj(value) for key, value in obj.__dict__.items() if not key.startswith("_")}
        else:
            return str(obj)

def scan_dir(path):
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scan_dir(entry.path)
        elif entry.is_file(follow_symlinks=False):
            yield entry.path

def index_files():
    parser_manager = ParserManager()
    import_path = input("Enter the root directory to scan for images: ").strip()
    if not os.path.isdir(import_path):
        print(f"Directory '{import_path}' does not exist.")
        return

    if not check_and_repair_db():
        print("Aborting indexing due to unrecoverable database corruption.")
        return

    # Optional fast bulk mode (drop & rebuild FTS afterwards)
    fast_bulk = False
    try:
        fb_in = input("Enable fast bulk mode (drop & rebuild FTS at end)? [y/N]: ").strip().lower()
        fast_bulk = fb_in in ('y','yes')
    except Exception:
        pass

    supported_exts = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tiff'}
    files_new = 0
    files_updated = 0
    files_skipped = 0
    files_deleted = 0
    BATCH_SIZE = 5000  # increased batch size for fewer commits
    WAL_CHECK_INTERVAL = 10_000  # run checkpoint every N processed files
    processed_files = 0
    insert_batch = []  # tuples matching INSERT columns
    update_batch = []  # tuples matching UPDATE setters

    # Normalize root path (ensure trailing separator for LIKE pattern)
    root_norm = os.path.abspath(import_path)
    if not root_norm.endswith(os.sep):
        root_norm += os.sep
    like_pattern = root_norm + '%'
    seen_paths = set()

    with sqlite3.connect("sd_index.db") as conn:
        cursor = conn.cursor()
        # Safer PRAGMA tuning: keep WAL to reduce corruption risk while still performant.
        try:
            cursor.execute("PRAGMA journal_mode = WAL;")
        except sqlite3.Error:
            pass
        try:
            cursor.execute("PRAGMA synchronous = NORMAL;")  # FAST + durable with WAL
        except sqlite3.Error:
            pass
        cursor.execute("PRAGMA temp_store = MEMORY;")
        cursor.execute("PRAGMA mmap_size = 300000000;")  # allow mmap for faster reads (best-effort)

        pre_existing_fts = fts_exists(conn)
        if fast_bulk and pre_existing_fts:
            drop_fts(conn)

        # Count total candidate files for progress bar (only supported extensions)
        total_files = 0
        for p in scan_dir(import_path):
            ext = os.path.splitext(p)[1].lower()
            if ext in supported_exts:
                total_files += 1

        try:
            with alive_bar(total_files, title="Indexing images (incremental)") as bar:
                for fpath in scan_dir(import_path):
                    ext = os.path.splitext(fpath)[1].lower()
                    if ext not in supported_exts:
                        bar()
                        continue
                    try:
                        st = os.stat(fpath)
                        file_size = st.st_size
                        file_mtime = int(st.st_mtime)
                        file_ctime = int(getattr(st, 'st_ctime', file_mtime))
                        width = height = None
                        # Extract image dimensions (Pillow)
                        try:
                            from PIL import Image
                            with Image.open(fpath) as im:
                                width, height = im.size
                        except Exception:
                            pass
                        seen_paths.add(os.path.abspath(fpath))
                        cursor.execute("SELECT id, file_mtime, file_size FROM files WHERE file_path = ?", (fpath,))
                        row = cursor.fetchone()
                        unchanged = False
                        if row and row[1] is not None and row[2] is not None:
                            if row[1] == file_mtime and row[2] == file_size:
                                unchanged = True
                        if unchanged:
                            files_skipped += 1
                        else:
                            # Need to (re)parse + hash
                            try:
                                prompt_info = parser_manager.parse(fpath)
                            except Exception:
                                prompt_info = None
                            if prompt_info:
                                metadata_obj = serialize_obj(prompt_info)
                                metadata = json.dumps(metadata_obj)
                            else:
                                # Ensure file still indexed with empty metadata (LEN=0 support)
                                metadata = ''
                            hasher = hashlib.sha256()
                            with open(fpath, "rb") as f:
                                for chunk in iter(lambda: f.read(8192), b''):
                                    hasher.update(chunk)
                            file_hash = hasher.hexdigest()
                            now = datetime.datetime.now()
                            if row:
                                update_batch.append((file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height, fpath))
                                files_updated += 1
                            else:
                                insert_batch.append((fpath, file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height))
                                files_new += 1
                            if len(insert_batch) >= BATCH_SIZE:
                                cursor.executemany("""
                                    INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ON CONFLICT(file_path) DO UPDATE SET
                                        file_hash=excluded.file_hash,
                                        metadata_json=excluded.metadata_json,
                                        last_scanned=excluded.last_scanned,
                                        file_size=excluded.file_size,
                                        file_mtime=excluded.file_mtime,
                                        file_ctime=excluded.file_ctime,
                                        width=excluded.width,
                                        height=excluded.height
                                """, insert_batch)
                                conn.commit()
                                insert_batch.clear()
                            if len(update_batch) >= BATCH_SIZE:
                                cursor.executemany("""
                                    UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=?
                                    WHERE file_path=?
                                """, update_batch)
                                conn.commit()
                                update_batch.clear()
                            # Periodic WAL checkpoint & stats every interval
                            processed_files += 1
                            if processed_files % WAL_CHECK_INTERVAL == 0:
                                try:
                                    cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                                    # Lightweight progress stats
                                    cursor.execute("SELECT COUNT(*) FROM files")
                                    total_indexed = cursor.fetchone()[0]
                                    print(f"\n[Checkpoint] Files in DB: {total_indexed} | WAL truncated | processed this run: {processed_files}")
                                except sqlite3.Error:
                                    pass
                    except sqlite3.DatabaseError as db_err:
                        if 'malformed' in str(db_err).lower():
                            print("Encountered malformed database during indexing. Attempting automatic repair...")
                            # Break early to attempt repair after loop
                            raise
                        else:
                            print("SQLite error for file", fpath, db_err)
                    except Exception:
                        # On unexpected failure still record file with empty metadata and dimensions unknown
                        now = datetime.datetime.now()
                        try:
                            st = os.stat(fpath)
                            file_size = st.st_size
                            file_mtime = int(st.st_mtime)
                            file_ctime = int(getattr(st, 'st_ctime', file_mtime))
                        except Exception:
                            file_size = file_mtime = file_ctime = 0
                        width = height = None
                        try:
                            if cursor.execute("SELECT 1 FROM files WHERE file_path=?", (fpath,)).fetchone():
                                update_batch.append(('', '', now, file_size, file_mtime, file_ctime, width, height, fpath))
                            else:
                                insert_batch.append((fpath, '', '', now, file_size, file_mtime, file_ctime, width, height))
                        except sqlite3.Error:
                            pass
                    bar()
            # Flush remaining batches inside try
            if insert_batch:
                cursor.executemany("""
                    INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(file_path) DO UPDATE SET
                        file_hash=excluded.file_hash,
                        metadata_json=excluded.metadata_json,
                        last_scanned=excluded.last_scanned,
                        file_size=excluded.file_size,
                        file_mtime=excluded.file_mtime,
                        file_ctime=excluded.file_ctime,
                        width=excluded.width,
                        height=excluded.height
                """, insert_batch)
            if update_batch:
                cursor.executemany("""
                    UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=?
                    WHERE file_path=?
                """, update_batch)
            # Deletions
            cursor.execute("SELECT file_path FROM files WHERE file_path LIKE ?", (like_pattern,))
            to_check = cursor.fetchall()
            delete_list = []
            for (db_path,) in to_check:
                abs_db_path = os.path.abspath(db_path)
                if abs_db_path not in seen_paths and not os.path.exists(abs_db_path):
                    delete_list.append(db_path)
            if delete_list:
                CHUNK = 1000
                for i in range(0, len(delete_list), CHUNK):
                    chunk = delete_list[i:i+CHUNK]
                    placeholders = ','.join(['?']*len(chunk))
                    cursor.execute(f"DELETE FROM files WHERE file_path IN ({placeholders})", chunk)
                files_deleted = len(delete_list)
            conn.commit()
        except sqlite3.DatabaseError as db_err:
            if 'malformed' in str(db_err).lower():
                print("Database marked as malformed. Initiating repair cycle...")
                if check_and_repair_db():
                    print("Repair completed. Please rerun indexing.")
                else:
                    print("Automatic repair failed. Consider removing sd_index.db manually.")
                return
            else:
                print("SQLite error aborted indexing:", db_err)
                return
            if update_batch:
                cursor.executemany("""
                    UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=?
                    WHERE file_path=?
                """, update_batch)

            # Deletions: remove db entries under root not seen this run
            cursor.execute("SELECT file_path FROM files WHERE file_path LIKE ?", (like_pattern,))
            to_check = cursor.fetchall()
            delete_list = []
            for (db_path,) in to_check:
                abs_db_path = os.path.abspath(db_path)
                if abs_db_path not in seen_paths and not os.path.exists(abs_db_path):
                    delete_list.append(db_path)
            if delete_list:
                # Delete in chunks
                CHUNK = 1000
                for i in range(0, len(delete_list), CHUNK):
                    chunk = delete_list[i:i+CHUNK]
                    placeholders = ','.join(['?']*len(chunk))
                    cursor.execute(f"DELETE FROM files WHERE file_path IN ({placeholders})", chunk)
                files_deleted = len(delete_list)
        # Rebuild FTS if we deferred it
        if fast_bulk and pre_existing_fts:
            print("Rebuilding FTS index (this may take a moment)...")
            ensure_fts(conn)
        # Ensure WAL mode persists
        try:
            cursor.execute("PRAGMA journal_mode = WAL;")
        except sqlite3.Error:
            pass
        # Final checkpoint to shrink WAL after heavy writes
        try:
            cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except sqlite3.Error:
            pass

    print(
        "Indexing complete. New: {n} Updated: {u} Skipped (unchanged): {s} Deleted (missing): {d}".format(
            n=files_new, u=files_updated, s=files_skipped, d=files_deleted
        )
    )

def run_webui():
    import subprocess
    import sys
    webui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webui", "main.py")
    if not os.path.exists(webui_path):
        print("WebUI not found at", webui_path)
        return
    print("Launching Web UI at http://127.0.0.1:8000 ...")
    try:
        subprocess.run([sys.executable, "-m", "uvicorn", "webui.main:app", "--reload"], check=True)
    except Exception as e:
        print("Failed to launch Web UI:", e)

if __name__ == "__main__":
    init_db()
    while True:
        print("\nChoose an option:")
        print("1. Index/Re-index SD files")
        print("2. Clear database")
        print("3. Run WebUI")
        print("4. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            index_files()
        elif choice == '2':
            clear_database()
            init_db()
        elif choice == '3':
            run_webui()
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Try again.")
