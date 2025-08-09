import os
import sqlite3
from typing import List, Optional, Sequence

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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            if where_sql:
                query = f"SELECT * FROM files WHERE {where_sql} ORDER BY last_scanned DESC LIMIT ? OFFSET ?"
                cursor.execute(query, params + [page_size, (page - 1) * page_size])
                files = cursor.fetchall()
                cursor.execute(f"SELECT COUNT(*) FROM files WHERE {where_sql}", params)
                total = cursor.fetchone()[0]
            else:
                cursor.execute("SELECT * FROM files ORDER BY last_scanned DESC LIMIT ? OFFSET ?", (page_size, (page - 1) * page_size))
                files = cursor.fetchall()
                cursor.execute("SELECT COUNT(*) FROM files")
                total = cursor.fetchone()[0]
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
    if operation not in {"move", "copy", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid operation")
    if not ids:
        raise HTTPException(status_code=400, detail="No ids supplied")
    if len(ids) > 50_000:
        raise HTTPException(status_code=400, detail="Too many ids in one request")
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            placeholders = ','.join(['?'] * len(ids))
            cur.execute(f"SELECT id, file_path FROM files WHERE id IN ({placeholders})", ids)
            files = cur.fetchall()
            if operation in {"move", "copy"}:
                if not destination or not os.path.isdir(destination):
                    raise HTTPException(status_code=400, detail="Invalid destination folder")
                dest_abs = os.path.abspath(destination)
                # Ensure destination inside allowed roots
                if not any(dest_abs.startswith(r + os.sep) or dest_abs == r for r in ALLOWED_ROOTS):
                    raise HTTPException(status_code=400, detail="Destination outside allowed roots")
            for row in files:
                src = row["file_path"]
                src_abs = os.path.abspath(src)
                if not any(src_abs.startswith(r + os.sep) or src_abs == r for r in ALLOWED_ROOTS):
                    raise HTTPException(status_code=400, detail=f"File outside allowed roots: {src}")
                filename = os.path.basename(src)
                dest_path = os.path.join(destination, filename) if destination else None
                if operation == 'move':
                    if not dest_path:
                        raise HTTPException(status_code=400, detail="Destination required for move")
                    shutil.move(src, dest_path)
                    cur.execute("UPDATE files SET file_path=? WHERE id=?", (dest_path, row['id']))
                elif operation == 'copy':
                    if not dest_path:
                        raise HTTPException(status_code=400, detail="Destination required for copy")
                    shutil.copy2(src, dest_path)
                elif operation == 'delete':
                    if os.path.isfile(src):
                        os.remove(src)
                    cur.execute("DELETE FROM files WHERE id=?", (row['id'],))
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File operation failed: {e}")
    return {"status": "success"}

