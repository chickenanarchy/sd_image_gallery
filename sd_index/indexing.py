"""File indexing logic (migrated from monolithic manager)."""
from __future__ import annotations
import os, sqlite3, hashlib, json, datetime, time
from .paths import DB_PATH, FTS_REBUILDING_FLAG
from .extraction import extract_models
from .db_schema import ensure_fts, drop_fts, fts_exists
from .db_repair import check_and_repair_db
from .scanning import serialize_obj, scan_dir
from .progress import _progress_bar

def index_files(import_path: str | None = None, *, full_refresh: bool | None = None, parser_manager=None):
    if import_path is None:
        import_path = input("Enter the root directory to scan for images: ").strip()
    if full_refresh is None:
        full_refresh = os.getenv("SD_INDEX_FULL_REFRESH", "0") == "1"
    if parser_manager is None:
        try:
            from sd_parsers import ParserManager  # type: ignore
            parser_manager = ParserManager()
        except Exception as e:  # pragma: no cover - environment optional
            print("Warning: sd_parsers not available (", e, ") - metadata parsing will be skipped.")
            parser_manager = None
    if not import_path or not os.path.isdir(import_path):
        print(f"Directory '{import_path}' does not exist.")
        return
    strict_full = os.getenv("SD_INDEX_STRICT_FULL", "0") == "1"
    if full_refresh:
        if strict_full:
            print("Running FULL refresh (STRICT) mode: hashing all existing files regardless of (mtime,size).")
        else:
            print("Running FULL refresh mode: hashing & checking files whose (mtime,size) changed.")
    else:
        print("Running FAST add-only mode: only new files hashed + missing removed. Existing files skipped entirely.")
    if not check_and_repair_db():
        print("Aborting indexing due to unrecoverable database corruption.")
        return
    fast_bulk = True
    if os.getenv("SD_INDEX_KEEP_FTS", "0") == "1":
        fast_bulk = False
    supported_exts = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tiff'}
    files_new = files_updated = files_skipped = files_deleted = 0
    BATCH_SIZE = 5000
    processed_files = 0
    insert_batch = []
    update_batch = []
    root_norm = os.path.abspath(import_path)
    if not root_norm.endswith(os.sep):
        root_norm += os.sep
    like_pattern = root_norm + '%'
    seen_paths: set[str] = set()
    with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
        cursor = conn.cursor()
        for pragma in [
            ("journal_mode", "WAL"),
            ("synchronous", "NORMAL"),
            ("busy_timeout", 30000),
            ("temp_store", "MEMORY"),
            ("mmap_size", 300000000),
        ]:
            try: cursor.execute(f"PRAGMA {pragma[0]} = {pragma[1]};")
            except sqlite3.Error: pass
        pre_existing_fts = fts_exists(conn)
        if fast_bulk and pre_existing_fts:
            drop_fts(conn)
        existing_path_map: dict[str, str] = {}
        cursor.execute("SELECT file_path, file_mtime, file_size FROM files WHERE file_path LIKE ?", (like_pattern,))
        preloaded_meta = {}
        for row in cursor.fetchall():
            stored_path = row[0]
            p_abs = os.path.abspath(stored_path)
            existing_path_map[p_abs] = stored_path
            if full_refresh:
                preloaded_meta[p_abs] = (row[1], row[2])
        try:
            with _progress_bar(None, title="Indexing images (incremental)") as bar:
                for fpath in scan_dir(import_path):
                    ext = os.path.splitext(fpath)[1].lower()
                    if ext not in supported_exts:
                        bar(); continue
                    try:
                        abs_path = os.path.abspath(fpath)
                        seen_paths.add(abs_path)
                        if abs_path in existing_path_map and not full_refresh:
                            files_skipped += 1; bar(); continue
                        st = os.stat(fpath)
                        file_size = st.st_size
                        file_mtime = int(st.st_mtime)
                        file_ctime = int(getattr(st, 'st_ctime', file_mtime))
                        # Early skip BEFORE computing dimensions / metadata / hash
                        if abs_path in existing_path_map and full_refresh and not strict_full:
                            prev_m, prev_s = preloaded_meta.get(abs_path, (None, None))
                            if prev_m == file_mtime and prev_s == file_size:
                                files_skipped += 1; bar(); processed_files += 1; continue
                        width = height = None
                        try:
                            from PIL import Image
                            with Image.open(fpath) as im: width, height = im.size
                        except Exception: pass
                        prompt_info = None
                        if parser_manager is not None:
                            try: prompt_info = parser_manager.parse(fpath)
                            except Exception: prompt_info = None
                        metadata = json.dumps(serialize_obj(prompt_info)) if prompt_info else ''
                        hasher = hashlib.sha256()
                        with open(fpath, 'rb') as f:
                            for chunk in iter(lambda: f.read(8192), b''): hasher.update(chunk)
                        file_hash = hasher.hexdigest()
                        now = datetime.datetime.utcnow()
                        if abs_path in existing_path_map:
                            stored_path = existing_path_map[abs_path]
                            update_batch.append((file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height, stored_path)); files_updated += 1
                        else:
                            # Store absolute path to ensure UI can always resolve file regardless of cwd
                            insert_batch.append((abs_path, file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height)); files_new += 1
                        if len(insert_batch) >= BATCH_SIZE:
                            cursor.executemany("""
                                INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(file_path) DO UPDATE SET file_hash=excluded.file_hash, metadata_json=excluded.metadata_json, last_scanned=excluded.last_scanned, file_size=excluded.file_size, file_mtime=excluded.file_mtime, file_ctime=excluded.file_ctime, width=excluded.width, height=excluded.height
                            """, insert_batch); conn.commit(); insert_batch.clear()
                        if full_refresh and len(update_batch) >= BATCH_SIZE:
                            cursor.executemany("""
                                UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=? WHERE file_path=?
                            """, update_batch); conn.commit(); update_batch.clear()
                        processed_files += 1
                    except sqlite3.DatabaseError as db_err:
                        if 'malformed' in str(db_err).lower():
                            print("Encountered malformed database during indexing. Attempting automatic repair..."); raise
                        else:
                            print("SQLite error for file", fpath, db_err)
                    except Exception:
                        now = datetime.datetime.utcnow()
                        try:
                            st = os.stat(fpath); file_size = st.st_size; file_mtime = int(st.st_mtime); file_ctime = int(getattr(st,'st_ctime',file_mtime))
                        except Exception:
                            file_size = file_mtime = file_ctime = 0
                        if cursor.execute("SELECT 1 FROM files WHERE file_path=?", (fpath,)).fetchone():
                            update_batch.append(('', '', now, file_size, file_mtime, file_ctime, None, None, fpath))
                        else:
                            # Use absolute path on fallback path too
                            insert_batch.append((abs_path, '', '', now, file_size, file_mtime, file_ctime, None, None))
                    bar()
            if insert_batch:
                cursor.executemany("""
                    INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(file_path) DO UPDATE SET file_hash=excluded.file_hash, metadata_json=excluded.metadata_json, last_scanned=excluded.last_scanned, file_size=excluded.file_size, file_mtime=excluded.file_mtime, file_ctime=excluded.file_ctime, width=excluded.width, height=excluded.height
                """, insert_batch)
            if update_batch:
                cursor.executemany("""UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=? WHERE file_path=?""", update_batch)
            delete_list = []
            for db_path_abs, stored_path in existing_path_map.items():
                if db_path_abs not in seen_paths and not os.path.exists(db_path_abs):
                    delete_list.append(stored_path)
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
                if check_and_repair_db(): print("Repair completed. Please rerun indexing.")
                else: print("Automatic repair failed. Consider removing sd_index.db manually.")
                return
            else:
                print("SQLite error aborted indexing:", db_err); return
        if fast_bulk and pre_existing_fts:
            try:
                if os.path.exists(FTS_REBUILDING_FLAG):
                    os.remove(FTS_REBUILDING_FLAG)
                with open(FTS_REBUILDING_FLAG, 'w', encoding='utf-8') as _f:
                    _f.write('rebuilding')
            except Exception: pass
            print("Rebuilding FTS index (this may take a moment)..."); ensure_fts(conn)
            try:
                if os.path.exists(FTS_REBUILDING_FLAG):
                    os.remove(FTS_REBUILDING_FLAG)
            except Exception: pass
        try: cursor.execute("PRAGMA journal_mode = WAL;")
        except sqlite3.Error: pass
        try: cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except sqlite3.Error: pass
    if full_refresh:
        print(f"Indexing complete (FULL). New: {files_new} Updated: {files_updated} Skipped: {files_skipped} Deleted: {files_deleted}")
    else:
        print(f"Indexing complete (FAST). New: {files_new} Existing skipped: {files_skipped} Deleted: {files_deleted}")
    # Automatic extraction phase (can disable via SD_DISABLE_EXTRACTION=1)
    if os.getenv('SD_DISABLE_EXTRACTION','0') != '1':
        try:
            with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
                summary = extract_models(conn)
            print(f"Extraction: processed={summary['processed']} new={summary['new']} updated={summary['updated']} skipped={summary['skipped']}")
        except Exception as e:  # pragma: no cover
            import traceback, sys
            print('Warning: extraction failed:', type(e).__name__, e)
            traceback.print_exc(limit=3, file=sys.stdout)
    return {"mode": "FULL" if full_refresh else "FAST", "new": files_new, "updated": files_updated, "skipped": files_skipped, "deleted": files_deleted}

__all__ = ['index_files']
