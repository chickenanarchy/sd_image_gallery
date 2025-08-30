import os
import sqlite3
import time
import threading
import uuid
import datetime as _dt
import calendar as _cal
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple, Dict, Any

from fastapi import FastAPI, Request, Query, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import re
from .search_utils import build_where, SearchBuildError
from io import BytesIO
from PIL import Image

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Reuse core DB path (supports SD_DB_PATH env override). Fallback to legacy location if import fails.
try:
    from sd_index import DB_PATH as CORE_DB_PATH  # type: ignore
    DB_PATH = CORE_DB_PATH
except Exception:  # pragma: no cover - fallback safety
    DB_PATH = os.path.join(os.path.dirname(BASE_DIR), "sd_index.db")
MAX_PAGE_SIZE = 200
PLACEHOLDER_ON_MISSING = os.getenv("SD_THUMB_PLACEHOLDER_ON_MISSING", "1") == "1"

# Optional configured allowed roots for file operations (restrict destructive ops)
def _canonical_path(p: str) -> str:
    """Return a canonical form of a path for comparisons (normcase + abspath)."""
    return os.path.normcase(os.path.abspath(p))

_env_roots = os.getenv("SD_ALLOWED_ROOTS", "").strip()
if _env_roots:
    _parsed = [r for r in (s.strip() for s in _env_roots.split(os.pathsep)) if r]
    ALLOWED_ROOTS: Optional[Sequence[str]] = [_canonical_path(r) for r in _parsed]
else:
    # If user explicitly requires allowed roots, enforce disabling destructive ops unless configured
    if os.getenv("SD_REQUIRE_ALLOWED_ROOTS", "0") == "1":
        ALLOWED_ROOTS = []  # empty list => no path passes validation
    else:
        ALLOWED_ROOTS = None  # unrestricted (default local usage)

DESTRUCTIVE_DISABLED = os.getenv("SD_DISABLE_DESTRUCTIVE_OPS", "0") == "1"

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    """Serve a favicon to avoid noisy 404s in logs.

    Uses the existing placeholder image; modern browsers accept PNG at /favicon.ico.
    """
    fav_path = os.path.join(BASE_DIR, "static", "placeholder.png")
    if os.path.exists(fav_path):
        # Cache for a week
        headers = {"Cache-Control": "public, max-age=604800"}
        return FileResponse(fav_path, media_type="image/png", headers=headers)
    return Response(status_code=204)

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
        conn.execute("PRAGMA mmap_size = 300000000")
    except sqlite3.Error:
        pass
    return conn

def ensure_fts_flag(force_recheck: bool = False):
    """Cache (with occasional refresh) presence of suitable FTS5 table.

    If FTS was missing at startup but later created (after indexing in another
    process) we eventually pick it up. A manual recheck can be forced by passing
    force_recheck=True (used by /refresh_fts endpoint) or automatically every
    REFRESH_INTERVAL seconds when previously false.
    """
    REFRESH_INTERVAL = 60.0
    now = time.time()
    if force_recheck or not hasattr(app.state, 'has_fts') or not getattr(app.state, 'has_fts', False):
        last = getattr(app.state, 'fts_checked_at', 0)
        if force_recheck or (now - last) > REFRESH_INTERVAL:
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='files_fts'")
                    row = cur.fetchone()
                    if not row:
                        app.state.has_fts = False
                    else:
                        ddl = row[0] if isinstance(row[0], str) else ''
                        has_paths = ('path' in ddl and 'path_norm' in ddl)
                        valid = bool(has_paths)
                        if valid:
                            # Additional integrity heuristic: if FTS docsize table has zero rows while files table has rows, treat FTS as unusable.
                            try:
                                cur.execute("SELECT COUNT(*) FROM files")
                                total_files = cur.fetchone()[0]
                                cur.execute("SELECT COUNT(*) FROM files_fts_docsize")
                                fts_docs = cur.fetchone()[0]
                                if total_files > 0 and fts_docs == 0:
                                    # Flag unusable; caller will fall back to LIKE search.
                                    valid = False
                                    if not getattr(app.state, 'fts_empty_warned', False):
                                        print("[WARN] Detected empty FTS index (0 docs) while files table has", total_files, "rows. Falling back to non-FTS search. Re-run indexing to rebuild FTS.")
                                        app.state.fts_empty_warned = True  # type: ignore[attr-defined]
                            except sqlite3.Error:
                                # On any error assume invalid so we don't block searches.
                                valid = False
                        app.state.has_fts = valid
            except sqlite3.Error:
                app.state.has_fts = False
            app.state.fts_checked_at = now
    return getattr(app.state, 'has_fts', False)

ensure_fts_flag()

# Simple in-process count cache to avoid repeating COUNT(*) for rapid page flips.
# Key: (where_sql, tuple(params)) -> (total, timestamp)
COUNT_CACHE_TTL = 5.0  # seconds
def _invalidate_count_cache():
    try:
        app.state.count_cache.clear()  # type: ignore[attr-defined]
    except Exception:
        pass
MAX_COUNT_CACHE_ENTRIES = 128
if not hasattr(app.state, 'count_cache'):
    # runtime attribute; keep simple to avoid Python <3.11 attribute annotation issues
    app.state.count_cache = {}  # type: ignore[attr-defined]
import threading as _threading
if not hasattr(app.state, 'jobs'):
    app.state.jobs = {}  # type: ignore[attr-defined]
if not hasattr(app.state, 'jobs_lock'):
    app.state.jobs_lock = _threading.Lock()  # type: ignore[attr-defined]

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
    if not ALLOWED_ROOTS:  # None or empty => unrestricted
        return True
    p = _canonical_path(path)
    for root in ALLOWED_ROOTS:
        if p == root or p.startswith(root + os.sep):
            return True
    return False

def _is_under_allowed(path: str) -> bool:
    return _validate_allowed(path)

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

def _apply_time_filter(where_sql: str, params: List[Any], sort: str, year: str, month: str) -> Tuple[str, List[Any], str, str]:
    """Given current WHERE SQL & params, append a time range filter based on sort/year/month.

    Returns updated (where_sql, params, normalized_year, normalized_month).
    Time filtering only applies when sorting by file_mtime/file_ctime and a year is provided.
    Normalization: blank or 'ALL' -> ''
    """
    time_sort_columns = {"file_mtime": "file_mtime", "file_ctime": "file_ctime"}
    time_col = time_sort_columns.get(sort)
    year_norm = (year or '').strip()
    if year_norm.upper() == 'ALL':
        year_norm = ''
    month_norm = (month or '').strip()
    if month_norm.upper() == 'ALL':
        month_norm = ''
    if time_col and year_norm:
        try:
            y_int = int(year_norm)
            if month_norm:
                m_int = int(month_norm)
                if not 1 <= m_int <= 12:
                    raise ValueError("Month out of range")
                start_dt = datetime(y_int, m_int, 1, 0, 0, 0, tzinfo=timezone.utc)
                # compute next month
                if m_int == 12:
                    end_dt = datetime(y_int + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                else:
                    end_dt = datetime(y_int, m_int + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            else:
                start_dt = datetime(y_int, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                end_dt = datetime(y_int + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            start_epoch = int(start_dt.timestamp())
            end_epoch = int(end_dt.timestamp())
            range_clause = f"{time_col} >= ? AND {time_col} < ?"
            if where_sql:
                where_sql = f"({where_sql}) AND {range_clause}"
            else:
                where_sql = range_clause
            params = params + [start_epoch, end_epoch]
        except ValueError:
            # Silently ignore malformed year/month
            pass
    return where_sql, params, year_norm, month_norm

@app.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
    page: int = 1,
    page_size: int = 100,
    sort: str = Query(default="last_scanned"),
    order: str = Query(default="desc"),
    year: str = Query(default=""),  # YYYY or ''/ALL
    month: str = Query(default=""), # MM or ''/ALL
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
    # Sort validation & mapping (whitelist to prevent injection)
    sort_map = {
        "last_scanned": "last_scanned",
        "file_name": "file_path",
        "file_size": "file_size",
        "file_mtime": "file_mtime",
        "file_ctime": "file_ctime",
        "width": "width",
        "height": "height",
        "id": "id",
    }
    sort_key = sort_map.get(sort, "last_scanned")
    order_dir = "ASC" if str(order).lower() == "asc" else "DESC"
    # For file_name we want case-insensitive ordering for consistency
    if sort_key == "file_path":
        primary_order = f"{sort_key} COLLATE NOCASE {order_dir}"
    else:
        primary_order = f"{sort_key} {order_dir}"
    # Stable secondary order (id DESC or ASC to make pagination deterministic)
    secondary_order = "id DESC" if order_dir == "DESC" else "id ASC"
    order_clause = f"{primary_order}, {secondary_order}"
    # Apply time filter (if any) using shared helper
    where_sql, params, year_norm, month_norm = _apply_time_filter(where_sql, params, sort, year, month)
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Important performance change: only fetch columns required for gallery list.
            select_cols = "id, file_path, file_hash, last_scanned"
            if where_sql:
                query = f"SELECT {select_cols} FROM files WHERE {where_sql} ORDER BY {order_clause} LIMIT ? OFFSET ?"
                cursor.execute(query, params + [page_size, offset])
            else:
                cursor.execute(f"SELECT {select_cols} FROM files ORDER BY {order_clause} LIMIT ? OFFSET ?", (page_size, offset))
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
                "sort": sort,
                "order": order.lower(),
                "year": year_norm,
                "month": month_norm,
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
            "sort": sort,
            "order": order.lower(),
            "year": year_norm,
            "month": month_norm,
            "error": None,
        },
    )

@app.get("/time_facets")
def time_facets(column: str, year: Optional[str] = None):
    """Return distinct years (and months for a selected year) for a time column.

    column must be one of file_mtime | file_ctime. Returns JSON {years:[..], months:[..]}
    Months only returned (non-empty) if a specific year (not ALL/blank) is provided.
    """
    col_map = {"file_mtime": "file_mtime", "file_ctime": "file_ctime"}
    col = col_map.get(column)
    if not col:
        raise HTTPException(status_code=400, detail="Invalid column")
    years: List[str] = []
    months: List[str] = []
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT DISTINCT strftime('%Y', datetime({col}, 'unixepoch')) AS y FROM files WHERE {col} IS NOT NULL ORDER BY y DESC")
            years = [r[0] for r in cur.fetchall() if r[0]]
            if year and year.upper() != 'ALL':
                cur.execute(
                    f"SELECT DISTINCT strftime('%m', datetime({col}, 'unixepoch')) AS m FROM files WHERE {col} IS NOT NULL AND strftime('%Y', datetime({col}, 'unixepoch'))=? ORDER BY m ASC",
                    (year,)
                )
                months = [r[0] for r in cur.fetchall() if r[0]]
    except sqlite3.Error:
        pass
    return {"years": years, "months": months}

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

@app.get("/thumb/{file_id}")
def get_thumbnail(file_id: int, request: Request, h: int = Query(default=256, ge=32, le=1024)):
    """Return a downscaled JPEG thumbnail to speed up gallery loading.

    Height parameter controls size; width preserves aspect ratio. Cached for a year when hash provided.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path, file_hash FROM files WHERE id=?", (file_id,))
            row = cur.fetchone()
    except sqlite3.Error:
        raise HTTPException(status_code=500, detail="Database error")

    if not row:
        raise HTTPException(status_code=404, detail="Image not found")
    fp = row["file_path"]
    if not os.path.isfile(fp):
        # Serve placeholder (optional) instead of noisy 404s which spam logs & trigger many network errors
        if PLACEHOLDER_ON_MISSING:
            ph_png = os.path.join(BASE_DIR, "static", "placeholder.png")
            ph_svg = os.path.join(BASE_DIR, "static", "placeholder.svg")
            ph = ph_png if os.path.isfile(ph_png) else (ph_svg if os.path.isfile(ph_svg) else None)
            if ph:
                # Cache short if no hash; encourage browser reuse but allow recovery if file restored later
                headers = {"Cache-Control": "public, max-age=3600"}
                media_type = "image/png" if ph.endswith('.png') else "image/svg+xml"
                return FileResponse(ph, media_type=media_type, headers=headers)
        raise HTTPException(status_code=404, detail="File not found on disk")
    # Best-effort thumbnailing
    try:
        with Image.open(fp) as im:
            im.thumbnail((h * 2, h), Image.Resampling.LANCZOS)  # slight width bump to keep details
            buf = BytesIO()
            im.convert("RGB").save(buf, format="JPEG", quality=82, optimize=True)
            buf.seek(0)
        headers = {"Cache-Control": "public, max-age=31536000, immutable"} if row["file_hash"] else {"Cache-Control": "no-cache"}
        return Response(content=buf.getvalue(), media_type="image/jpeg", headers=headers)
    except Exception:
        # Fallback to original delivery
        return get_image(file_id, request)

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
    if DESTRUCTIVE_DISABLED:
        raise HTTPException(status_code=400, detail="Destructive operations disabled (SD_DISABLE_DESTRUCTIVE_OPS=1)")
    if not ids:
        raise HTTPException(status_code=400, detail="No ids supplied")

    # Basic destination validation early
    dest_abs: Optional[str] = None
    if operation in {"move", "copy"}:
        if not destination or not os.path.isdir(destination):
            raise HTTPException(status_code=400, detail="Invalid destination folder")
        dest_abs = _canonical_path(destination)
        if not _is_under_allowed(dest_abs):
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
                        src_abs = _canonical_path(src)
                        if not _is_under_allowed(src_abs):
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
                                        row['file_hash'] if 'file_hash' in row.keys() else None,
                                        row['metadata_json'] if 'metadata_json' in row.keys() else None,
                                        row['file_size'] if 'file_size' in row.keys() else None,
                                        row['file_mtime'] if 'file_mtime' in row.keys() else None,
                                        row['file_ctime'] if 'file_ctime' in row.keys() else None,
                                        row['width'] if 'width' in row.keys() else None,
                                        row['height'] if 'height' in row.keys() else None,
                                    ),
                                )
                                # Only count as copied if DB insert actually succeeded
                                if cur.rowcount > 0:
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
    _invalidate_count_cache()
    status = "success" if not errors else "partial"
    summary = {
        "status": status,
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
    sort: str = Query(default="last_scanned"),
    year: str = Query(default=""),
    month: str = Query(default=""),
):
    has_fts = ensure_fts_flag()
    try:
        where_sql, params = build_where(search, logics or [], values or [], has_fts)
    except SearchBuildError as e:
        raise HTTPException(status_code=400, detail=str(e))
    # Apply time filter consistency with gallery
    where_sql, params, _, _ = _apply_time_filter(where_sql, params, sort, year, month)
    total = _cached_total(where_sql, params)
    return {"total": total}

@app.get("/extraction_summary")
def extraction_summary():
    """Return aggregate extraction stats (counts + top LoRA)."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM files")
            files = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM models")
            models = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM models WHERE lora_count>0")
            with_lora = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT lora_name) FROM lora_usages")
            distinct_loras = cur.fetchone()[0]
            cur.execute("SELECT lora_name, COUNT(*) c FROM lora_usages GROUP BY lora_name ORDER BY c DESC LIMIT 20")
            top = [{'name': r[0], 'count': r[1]} for r in cur.fetchall()]
            return {
                'files': files,
                'models': models,
                'with_lora': with_lora,
                'distinct_loras': distinct_loras,
                'top_loras': top
            }
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f'Database error: {e}')

@app.post("/file_operation_async")
def start_file_operation_async(
    operation: str = Body(...),
    scope: Dict[str, Any] = Body(...),  # {type: 'query'|'ids', ...}
    destination: Optional[str] = Body(None),
):
    if operation not in {"move", "copy", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid operation")
    if DESTRUCTIVE_DISABLED:
        raise HTTPException(status_code=400, detail="Destructive operations disabled (SD_DISABLE_DESTRUCTIVE_OPS=1)")
    if scope.get('type') not in {"query", "ids"}:
        raise HTTPException(status_code=400, detail="Invalid scope type")
    dest_abs: Optional[str] = None
    if operation in {"move", "copy"}:
        if not destination or not os.path.isdir(destination):
            raise HTTPException(status_code=400, detail="Invalid destination folder")
        dest_abs = _canonical_path(destination)
        if not _validate_allowed(dest_abs):
            raise HTTPException(status_code=400, detail="Destination outside allowed roots")

    job_id = uuid.uuid4().hex
    with app.state.jobs_lock:  # type: ignore[attr-defined]
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
                # Query scope (optionally with exclusions)
                search = scope.get('search', '')
                logics = scope.get('logics', []) or []
                values = scope.get('values', []) or []
                sort = scope.get('sort', 'last_scanned') or 'last_scanned'
                year = scope.get('year', '') or ''
                month = scope.get('month', '') or ''
                exclusions = scope.get('excluded', []) or []
                if exclusions and not all(isinstance(x, int) for x in exclusions):
                    raise ValueError("Excluded must be list[int]")
                has_fts = ensure_fts_flag()
                where_sql, params = build_where(search, logics, values, has_fts)
                where_sql, params, _, _ = _apply_time_filter(where_sql, params, sort, year, month)
                # Determine total upfront (minus exclusions that may exist in result set)
                total_raw = _cached_total(where_sql, params)
                job['total'] = max(0, total_raw - len(exclusions))
                _process_query_scope(where_sql, params, operation, dest_abs, job, exclusions)
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

def _process_query_scope(where_sql: str, params: List[Any], operation: str, dest_abs: Optional[str], job: Dict[str, Any], exclusions: Optional[Sequence[int]] = None):
    BATCH = 500
    last_id = 0
    total = job.get('total') or 0
    exclusion_set = set(exclusions or [])
    debug_logged = False
    # Capture the current maximum id BEFORE we start modifying the table (important for copy operations
    # which insert new rows; we don't want to repeatedly process newly inserted copies).
    try:
        with get_db_connection() as _conn_max:
            curm = _conn_max.cursor()
            curm.execute("SELECT COALESCE(MAX(id),0) FROM files")
            original_max_id = curm.fetchone()[0]
    except sqlite3.Error:
        original_max_id = None  # fallback: process until natural exhaustion (legacy behavior)
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            while True:
                # Build incremental query
                max_id_clause = " AND id <= ?" if original_max_id is not None else ""
                if where_sql:
                    query = f"SELECT id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height FROM files WHERE ({where_sql}) AND id > ?{max_id_clause} ORDER BY id LIMIT ?"
                    execute_params = params + [last_id]
                    if original_max_id is not None:
                        execute_params.append(original_max_id)
                    execute_params.append(BATCH)
                    cur.execute(query, execute_params)
                else:
                    base = f"SELECT id, file_path, file_hash, metadata_json, file_size, file_mtime, file_ctime, width, height FROM files WHERE id > ?{max_id_clause} ORDER BY id LIMIT ?"
                    if original_max_id is not None:
                        cur.execute(base, (last_id, original_max_id, BATCH))
                    else:
                        cur.execute(base, (last_id, BATCH))
                rows = cur.fetchall()
                if not rows:
                    break
                # Filter rows against exclusions before processing to keep processed count aligned with job['total']
                if exclusion_set:
                    rows_to_process = [r for r in rows if r['id'] not in exclusion_set]
                else:
                    rows_to_process = rows
                if rows_to_process:
                    if not debug_logged:
                        try:
                            sample = rows_to_process[:3]
                            print(f"[DEBUG file_operation] First batch sample (operation={operation} total_est={total}):")
                            for r in sample:
                                print("  id=", r['id'], "path=", r['file_path'])
                        except Exception:
                            pass
                        debug_logged = True
                    _process_rows(rows_to_process, operation, dest_abs, conn, job)
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
                            row['file_hash'] if 'file_hash' in row.keys() else None,
                            row['metadata_json'] if 'metadata_json' in row.keys() else None,
                            row['file_size'] if 'file_size' in row.keys() else None,
                            row['file_mtime'] if 'file_mtime' in row.keys() else None,
                            row['file_ctime'] if 'file_ctime' in row.keys() else None,
                            row['width'] if 'width' in row.keys() else None,
                            row['height'] if 'height' in row.keys() else None,
                        ),
                    )
                    # Only count as copied if DB insert actually succeeded
                    if cur.rowcount > 0:
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
    _invalidate_count_cache()

@app.get("/file_operation_status/{job_id}")
def file_operation_status(job_id: str):
    with app.state.jobs_lock:  # type: ignore[attr-defined]
        job = app.state.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    # Shallow copy without scope internals that may be large
    payload = {k: v for k, v in job.items() if k != 'scope'}
    payload['scope_type'] = job.get('scope', {}).get('type')
    return payload

@app.post("/refresh_fts")
def refresh_fts():
    """Force a re-check of FTS availability (useful after external migration)."""
    has_fts = ensure_fts_flag(force_recheck=True)
    return {"has_fts": has_fts}

