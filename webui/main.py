import os
import sqlite3
import time
import threading
import uuid
from typing import List, Optional, Sequence, Tuple, Dict, Any

from fastapi import FastAPI, Request, Query, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import re
from .search_utils import build_where, SearchBuildError

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.path.dirname(BASE_DIR), "sd_index.db")
MAX_PAGE_SIZE = 200

# Optional configured allowed roots for file operations (restrict destructive ops)
ALLOWED_ROOTS: Sequence[str] = [os.path.abspath(os.path.join(os.path.dirname(BASE_DIR)))]  # project root by default

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

def get_db_connection():
    """Return a new sqlite3 connection tuned for read speed.

    We use WAL mode already (set during indexing). For read-heavy
    web usage we can safely set a few pragmas per connection.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        # Hints that can help large page navigation (best-effort)
        conn.execute("PRAGMA cache_size = -80000")  # ~80MB page cache (negative => KB)
        conn.execute("PRAGMA temp_store = MEMORY")
    except sqlite3.Error:
        pass
    return conn

def ensure_fts_flag():
    """Cache presence of FTS5 virtual table once per process."""
    if not hasattr(app.state, 'has_fts'):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
                app.state.has_fts = cur.fetchone() is not None
        except sqlite3.Error:
            app.state.has_fts = False
    return app.state.has_fts

ensure_fts_flag()

# Simple in-process count cache to avoid repeating COUNT(*) for rapid page flips.
# Key: (where_sql, tuple(params)) -> (total, timestamp)
COUNT_CACHE_TTL = 5.0  # seconds
MAX_COUNT_CACHE_ENTRIES = 128
if not hasattr(app.state, 'count_cache'):
    # runtime attribute; keep simple to avoid Python <3.11 attribute annotation issues
    app.state.count_cache = {}  # type: ignore[attr-defined]
if not hasattr(app.state, 'jobs'):
    app.state.jobs = {}  # type: ignore[attr-defined]

def _cached_total(where_sql: str, params: List[Any]) -> int:
    """Return cached total row count for a WHERE clause if fresh; else compute & cache."""
    key = (where_sql, tuple(params))
    now = time.time()
    cache = app.state.count_cache
    if key in cache:
        total, ts = cache[key]
        if now - ts < COUNT_CACHE_TTL:
            return total
        else:
            # stale; drop so we recompute
            cache.pop(key, None)
    # Need compute
    with get_db_connection() as conn:
        cur = conn.cursor()
        if where_sql:
            cur.execute(f"SELECT COUNT(*) FROM files WHERE {where_sql}", params)
        else:
            cur.execute("SELECT COUNT(*) FROM files")
        total = cur.fetchone()[0]
    # Maintain size bound (naive LRU eviction)
    if len(cache) >= MAX_COUNT_CACHE_ENTRIES:
        # Remove oldest
        oldest_key = min(cache.items(), key=lambda kv: kv[1][1])[0]
        cache.pop(oldest_key, None)
    cache[key] = (total, now)
    return total

def _validate_allowed(path: str) -> bool:
    p = os.path.abspath(path)
    for root in ALLOWED_ROOTS:
        if p == root or p.startswith(root + os.sep):
            return True
    return False

def _safe_collision_path(dest_dir: str, filename: str) -> str:
    target = os.path.join(dest_dir, filename)
    if not os.path.exists(target):
        return target
    stem, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(target):
        target = os.path.join(dest_dir, f"{stem}_{counter}{ext}")
        counter += 1
    return target

@app.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
    page: int = 1,
    page_size: int = 100,
) -> HTMLResponse:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE
    has_fts = ensure_fts_flag()
    try:
        where_sql, params = build_where(search, logics or [], values or [], has_fts)
    except SearchBuildError as e:
        raise HTTPException(status_code=400, detail=str(e))
    offset = (page - 1) * page_size
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Important performance change: only fetch columns required for gallery list.
            select_cols = "id, file_path, file_hash, last_scanned"
            if where_sql:
                query = f"SELECT {select_cols} FROM files WHERE {where_sql} ORDER BY last_scanned DESC, id DESC LIMIT ? OFFSET ?"
                cursor.execute(query, params + [page_size, offset])
            else:
                cursor.execute(f"SELECT {select_cols} FROM files ORDER BY last_scanned DESC, id DESC LIMIT ? OFFSET ?", (page_size, offset))
            files = cursor.fetchall()
            # Use (short-lived) cached total
            total = _cached_total(where_sql, params)
    except sqlite3.Error:
        return templates.TemplateResponse(
            "gallery.html",
            {
                "request": request,
                "files": [],
                "search": search,
                "logics": logics if logics else [],
                "fields": values if values else [],
                "values": values if values else [],
                "error": "Database error occurred.",
                "page": page,
                "page_size": page_size,
                "total": 0,
            },
        )

    return templates.TemplateResponse(
        "gallery.html",
        {
            "request": request,
            "files": files,
            "search": search,
            "logics": logics if logics else [],
            "fields": values if values else [],
            "values": values if values else [],
            "page": page,
            "page_size": page_size,
            "total": total,
            "error": None,
        },
    )

@app.get("/metadata/{file_id}")
def get_metadata(file_id: int):
    """Return metadata_json for a file (fetched lazily for modal to avoid large page payloads)."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT metadata_json FROM files WHERE id=?", (file_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Not found")
            return {"metadata_json": row["metadata_json"] or ''}
    except sqlite3.Error:
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/image/{file_id}")
def get_image(file_id: int, request: Request) -> FileResponse:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM files WHERE id = ?", (file_id,))
            row = cursor.fetchone()
    except sqlite3.Error:
        raise HTTPException(status_code=500, detail="Database error")

    if not row:
        raise HTTPException(status_code=404, detail="Image not found")

    file_path = row["file_path"]
    if not os.path.isfile(file_path):
        # Log warning about missing file if needed
        raise HTTPException(status_code=404, detail="File not found on disk")

    if 'v' in request.query_params:
        headers = {"Cache-Control": "public, max-age=31536000, immutable"}
    else:
        headers = {"Cache-Control": "no-cache"}
    return FileResponse(file_path, headers=headers)

import shutil

@app.get("/metadata_fields")
def metadata_fields():
    """Return distinct top-level JSON keys observed in metadata_json.

    This is a heuristic (simple JSON_EXTRACT on first level keys) and may
    return duplicates if JSON1 extension is not available; if JSON1 is
    missing we just return an empty list gracefully.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Attempt: parse keys by simple regex on stored JSON strings (fallback)
            cur.execute("SELECT metadata_json FROM files WHERE metadata_json IS NOT NULL AND metadata_json != '' LIMIT 500")
            keys = set()
            import json as _json
            for (mj,) in cur.fetchall():
                if not mj:
                    continue
                try:
                    obj = _json.loads(mj)
                    if isinstance(obj, dict):
                        for k in obj.keys():
                            if isinstance(k, str) and len(k) <= 64:
                                keys.add(k)
                except Exception:
                    continue
            return sorted(keys)
    except sqlite3.Error:
        return []

@app.get("/matching_ids")
def matching_ids(
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
):
    has_fts = ensure_fts_flag()
    try:
        where_sql, params = build_where(search, logics or [], values or [], has_fts)
    except SearchBuildError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            if where_sql:
                cur.execute(f"SELECT id FROM files WHERE {where_sql}", params)
            else:
                cur.execute("SELECT id FROM files")
            ids = [row["id"] for row in cur.fetchall()]
    except sqlite3.Error:
        raise HTTPException(status_code=500, detail="Database error")
    # Soft cap response size to mitigate over-large payloads (warn via truncation)
    MAX_IDS = 100_000
    truncated = False
    if len(ids) > MAX_IDS:
        ids = ids[:MAX_IDS]
        truncated = True
    return {"ids": ids, "truncated": truncated}

@app.post("/file_operation")
def file_operation(
    operation: str = Body(..., embed=True),
    ids: List[int] = Body(..., embed=True),
    destination: Optional[str] = Body(None, embed=True),
):
    """Perform file operations in chunks to avoid SQLite parameter limits.

    High-priority fixes implemented:
      - Chunk processing (avoid >999 parameter limit & huge IN clauses)
      - Copy now inserts a new DB row (so FTS triggers fire and UI sees file next refresh)
      - Safe filename collision handling when moving/copying
      - Returns per-operation counts & first few errors for transparency
    """
    if operation not in {"move", "copy", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid operation")
    if not ids:
        raise HTTPException(status_code=400, detail="No ids supplied")

    # Basic destination validation early
    dest_abs: Optional[str] = None
    if operation in {"move", "copy"}:
        if not destination or not os.path.isdir(destination):
            raise HTTPException(status_code=400, detail="Invalid destination folder")
        dest_abs = os.path.abspath(destination)
        if not any(dest_abs == r or dest_abs.startswith(r + os.sep) for r in ALLOWED_ROOTS):
            raise HTTPException(status_code=400, detail="Destination outside allowed roots")

    # Stats & error collection
    moved = copied = deleted = 0
    errors: List[str] = []
    start_time = time.time()

    BATCH = 500  # reasonable balance; below SQLite default param limit (999)
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            for i in range(0, len(ids), BATCH):
                batch_ids = ids[i:i + BATCH]
                placeholders = ','.join(['?'] * len(batch_ids))
                # When copying we need all columns; otherwise minimal for speed
                if operation == 'copy':
                    cur.execute(
                        f"SELECT id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height FROM files WHERE id IN ({placeholders})",
                        batch_ids,
                    )
                else:
                    cur.execute(
                        f"SELECT id, file_path FROM files WHERE id IN ({placeholders})",
                        batch_ids,
                    )
                rows = cur.fetchall()
                for row in rows:
                    try:
                        src = row["file_path"]
                        src_abs = os.path.abspath(src)
                        if not any(src_abs == r or src_abs.startswith(r + os.sep) for r in ALLOWED_ROOTS):
                            errors.append(f"Outside allowed root: {src}")
                            continue
                        if operation in {"move", "copy"}:
                            assert dest_abs is not None
                            base_name = os.path.basename(src)
                            target = os.path.join(dest_abs, base_name)
                            # Collision-safe naming
                            if os.path.exists(target):
                                stem, ext = os.path.splitext(base_name)
                                counter = 1
                                while os.path.exists(target):
                                    target = os.path.join(dest_abs, f"{stem}_{counter}{ext}")
                                    counter += 1
                            if operation == 'move':
                                shutil.move(src, target)
                                cur.execute("UPDATE files SET file_path=? WHERE id=?", (target, row['id']))
                                moved += 1
                            else:  # copy
                                shutil.copy2(src, target)
                                # Insert new row duplicating metadata (fast path: reuse hash & metadata)
                                now_ts = int(time.time())
                                # Attempt to preserve times/size if available
                                cur.execute(
                                    """
                                    INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
                                    ON CONFLICT(file_path) DO NOTHING
                                    """,
                                    (
                                        target,
                                        row.get("file_hash") if "file_hash" in row.keys() else None,
                                        row.get("metadata_json") if "metadata_json" in row.keys() else None,
                                        row.get("file_size") if "file_size" in row.keys() else None,
                                        row.get("file_mtime") if "file_mtime" in row.keys() else None,
                                        row.get("file_ctime") if "file_ctime" in row.keys() else None,
                                        row.get("width") if "width" in row.keys() else None,
                                        row.get("height") if "height" in row.keys() else None,
                                    ),
                                )
                                copied += 1
                        elif operation == 'delete':
                            if os.path.isfile(src):
                                try:
                                    os.remove(src)
                                except FileNotFoundError:
                                    pass
                            cur.execute("DELETE FROM files WHERE id=?", (row['id'],))
                            deleted += 1
                    except Exception as fe:  # per-file error; continue
                        errors.append(f"{operation} error for id {row['id']}: {fe}")
                # Commit per batch to release locks & ensure FTS triggers fire progressively
                conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File operation failed: {e}")

    duration = time.time() - start_time
    summary = {
        "status": "success",
        "operation": operation,
        "counts": {"moved": moved, "copied": copied, "deleted": deleted},
        "errors": errors[:25],  # return only first 25 to limit payload
        "error_count": len(errors),
        "processed": moved + copied + deleted + len(errors),
        "requested": len(ids),
        "duration_sec": round(duration, 3),
    }
    return summary

@app.get("/matching_count")
def matching_count(
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
):
    has_fts = ensure_fts_flag()
    try:
        where_sql, params = build_where(search, logics or [], values or [], has_fts)
    except SearchBuildError as e:
        raise HTTPException(status_code=400, detail=str(e))
    total = _cached_total(where_sql, params)
    return {"total": total}

@app.post("/file_operation_async")
def start_file_operation_async(
    operation: str = Body(...),
    scope: Dict[str, Any] = Body(...),  # {type: 'query'|'ids', ...}
    destination: Optional[str] = Body(None),
):
    if operation not in {"move", "copy", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid operation")
    if scope.get('type') not in {"query", "ids"}:
        raise HTTPException(status_code=400, detail="Invalid scope type")
    dest_abs: Optional[str] = None
    if operation in {"move", "copy"}:
        if not destination or not os.path.isdir(destination):
            raise HTTPException(status_code=400, detail="Invalid destination folder")
        dest_abs = os.path.abspath(destination)
        if not _validate_allowed(dest_abs):
            raise HTTPException(status_code=400, detail="Destination outside allowed roots")

    job_id = uuid.uuid4().hex
    app.state.jobs[job_id] = {
        "id": job_id,
        "operation": operation,
        "status": "pending",
        "started": time.time(),
        "updated": time.time(),
        "processed": 0,
        "total": None,
        "counts": {"moved": 0, "copied": 0, "deleted": 0},
        "error_count": 0,
        "errors": [],  # first few errors
        "scope": scope,
        "destination": dest_abs,
        "duration_sec": None,
    }

    def runner():  # background processing
        job = app.state.jobs[job_id]
        job['status'] = 'running'
        try:
            if scope['type'] == 'ids':
                ids = scope.get('ids') or []
                if not isinstance(ids, list) or not all(isinstance(x, int) for x in ids):
                    raise ValueError("Scope ids must be list[int]")
                _process_ids_sync(ids, operation, dest_abs, job, async_mode=True)
            else:
                # Query scope
                search = scope.get('search', '')
                logics = scope.get('logics', []) or []
                values = scope.get('values', []) or []
                has_fts = ensure_fts_flag()
                where_sql, params = build_where(search, logics, values, has_fts)
                # Determine total upfront
                job['total'] = _cached_total(where_sql, params)
                _process_query_scope(where_sql, params, operation, dest_abs, job)
            job['status'] = 'completed'
        except Exception as e:
            job['status'] = 'failed'
            job['errors'].append(str(e))
            job['error_count'] += 1
        finally:
            job['updated'] = time.time()
            start = job.get('started')
            if start:
                job['duration_sec'] = round(time.time() - start, 3)

    threading.Thread(target=runner, daemon=True).start()
    return {"job_id": job_id}

def _process_ids_sync(ids: List[int], operation: str, dest_abs: Optional[str], job: Dict[str, Any], async_mode: bool = False):
    # Reuse existing chunked logic but update job state
    BATCH = 500
    total = len(ids)
    job['total'] = total
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            for i in range(0, len(ids), BATCH):
                batch_ids = ids[i:i + BATCH]
                placeholders = ','.join(['?'] * len(batch_ids))
                need_full = operation == 'copy'
                select_cols = "id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height" if need_full else "id, file_path"
                cur.execute(f"SELECT {select_cols} FROM files WHERE id IN ({placeholders})", batch_ids)
                rows = cur.fetchall()
                _process_rows(rows, operation, dest_abs, conn, job)
                job['updated'] = time.time()
    except Exception as e:
        raise e

def _process_query_scope(where_sql: str, params: List[Any], operation: str, dest_abs: Optional[str], job: Dict[str, Any]):
    BATCH = 500
    last_id = 0
    total = job.get('total') or 0
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            while True:
                # Build incremental query
                if where_sql:
                    query = f"SELECT id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height FROM files WHERE ({where_sql}) AND id > ? ORDER BY id LIMIT ?"
                    cur.execute(query, params + [last_id, BATCH])
                else:
                    cur.execute("SELECT id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height FROM files WHERE id > ? ORDER BY id LIMIT ?", (last_id, BATCH))
                rows = cur.fetchall()
                if not rows:
                    break
                _process_rows(rows, operation, dest_abs, conn, job)
                last_id = rows[-1]['id']
                job['updated'] = time.time()
    except Exception as e:
        raise e

def _process_rows(rows: Sequence[sqlite3.Row], operation: str, dest_abs: Optional[str], conn: sqlite3.Connection, job: Dict[str, Any]):
    cur = conn.cursor()
    for row in rows:
        try:
            src = row['file_path']
            if not _validate_allowed(src):
                job['errors'].append(f"Outside allowed root: {src}")
                job['error_count'] += 1
                continue
            if operation in {"move", "copy"}:
                assert dest_abs is not None
                target = _safe_collision_path(dest_abs, os.path.basename(src))
                if operation == 'move':
                    if os.path.exists(src):
                        shutil.move(src, target)
                    cur.execute("UPDATE files SET file_path=? WHERE id=?", (target, row['id']))
                    job['counts']['moved'] += 1
                else:  # copy
                    if os.path.exists(src):
                        shutil.copy2(src, target)
                    cur.execute(
                        """
                        INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
                        ON CONFLICT(file_path) DO NOTHING
                        """,
                        (
                            target,
                            row.get('file_hash'),
                            row.get('metadata_json'),
                            row.get('file_size'),
                            row.get('file_mtime'),
                            row.get('file_ctime'),
                            row.get('width'),
                            row.get('height'),
                        ),
                    )
                    job['counts']['copied'] += 1
            elif operation == 'delete':
                if os.path.isfile(src):
                    try:
                        os.remove(src)
                    except FileNotFoundError:
                        pass
                cur.execute("DELETE FROM files WHERE id=?", (row['id'],))
                job['counts']['deleted'] += 1
        except Exception as fe:
            job['errors'].append(f"{operation} error for id {row['id']}: {fe}")
            job['error_count'] += 1
        finally:
            job['processed'] += 1
        if len(job['errors']) > 50:
            # Trim retained errors to first 50
            job['errors'] = job['errors'][:50]
    conn.commit()

@app.get("/file_operation_status/{job_id}")
def file_operation_status(job_id: str):
    job = app.state.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    # Shallow copy without scope internals that may be large
    payload = {k: v for k, v in job.items() if k != 'scope'}
    payload['scope_type'] = job.get('scope', {}).get('type')
    return payload

