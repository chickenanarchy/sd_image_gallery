import os
import sqlite3
from typing import List, Optional

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.path.dirname(BASE_DIR), "sd_index.db")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
    page: int = 1,
    page_size: int = 100,
) -> HTMLResponse:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Validate logics to allowed values only
            allowed_logics = {"AND", "OR", "NOT"}

            # Build list of all clauses and their operators
            clauses = []
            clause_params = []

            # Add main search term if present (first clause, no operator)
            if search:
                search_stripped = search.strip()
                if search_stripped.upper() == 'NULL':
                    clauses.append((None, "metadata_json IS NULL"))
                elif search_stripped == '{}':
                    clauses.append((None, "metadata_json = ?"))
                    clause_params.append('{}')
                else:
                    clauses.append((None, "metadata_json LIKE ?"))
                    clause_params.append(f"%{search}%")

            # Add additional values with their logical operators
            for i, value in enumerate(values):
                logic = "AND"  # default logic
                if i < len(logics):
                    candidate_logic = logics[i].upper()
                    if candidate_logic in allowed_logics:
                        logic = candidate_logic
                clauses.append((logic, "metadata_json LIKE ?"))
                clause_params.append(f"%{value}%")

            # Construct the WHERE clause string
            if clauses:
                where_statement = ""
                first = True
                for operator, clause in clauses:
                    if first:
                        # First clause, no operator prefix
                        where_statement += f"({clause})"
                        first = False
                    else:
                        if operator == "NOT":
                            where_statement += f" AND NOT ({clause})"
                        else:
                            where_statement += f" {operator} ({clause})"
                params = clause_params.copy()

                query = f"SELECT * FROM files WHERE {where_statement} ORDER BY last_scanned DESC LIMIT ? OFFSET ?"
                cursor.execute(query, params + [page_size, (page - 1) * page_size])
                files = cursor.fetchall()
                cursor.execute(f"SELECT COUNT(*) FROM files WHERE {where_statement}", params)
                total = cursor.fetchone()[0]
            else:
                cursor.execute("SELECT * FROM files ORDER BY last_scanned DESC LIMIT ? OFFSET ?", (page_size, (page - 1) * page_size))
                files = cursor.fetchall()
                cursor.execute("SELECT COUNT(*) FROM files")
                total = cursor.fetchone()[0]
    except sqlite3.Error as e:
        # Log error or handle accordingly
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
        },
    )

@app.get("/image/{file_id}")
def get_image(file_id: int) -> FileResponse:
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

    return FileResponse(file_path)

from fastapi import Body
from fastapi.responses import JSONResponse
import shutil
import tkinter as tk
from tkinter import filedialog
import threading

@app.post("/select_folder")
def select_folder():
    # Run tkinter folder picker in a separate thread to avoid blocking
    folder_path = {}

    def pick_folder():
        root = tk.Tk()
        root.withdraw()
        folder = filedialog.askdirectory()
        folder_path['path'] = folder
        root.quit()

    thread = threading.Thread(target=pick_folder)
    thread.start()
    thread.join()

    if not folder_path.get('path'):
        return JSONResponse(status_code=400, content={"error": "No folder selected"})

    return {"folder": folder_path['path']}

@app.get("/matching_ids")
def matching_ids(
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            allowed_logics = {"AND", "OR", "NOT"}
            clauses = []
            clause_params = []

            if search:
                search_stripped = search.strip()
                if search_stripped.upper() == 'NULL':
                    clauses.append((None, "metadata_json IS NULL"))
                elif search_stripped == '{}':
                    clauses.append((None, "metadata_json = ?"))
                    clause_params.append('{}')
                else:
                    clauses.append((None, "metadata_json LIKE ?"))
                    clause_params.append(f"%{search}%")

            for i, value in enumerate(values):
                logic = "AND"
                if i < len(logics):
                    candidate_logic = logics[i].upper()
                    if candidate_logic in allowed_logics:
                        logic = candidate_logic
                clauses.append((logic, "metadata_json LIKE ?"))
                clause_params.append(f"%{value}%")

            if clauses:
                where_statement = ""
                first = True
                for operator, clause in clauses:
                    if first:
                        where_statement += f"({clause})"
                        first = False
                    else:
                        if operator == "NOT":
                            where_statement += f" AND NOT ({clause})"
                        else:
                            where_statement += f" {operator} ({clause})"
                params = clause_params.copy()
                query = f"SELECT id FROM files WHERE {where_statement}"
                cursor.execute(query, params)
                ids = [row["id"] for row in cursor.fetchall()]
            else:
                cursor.execute("SELECT id FROM files")
                ids = [row["id"] for row in cursor.fetchall()]
    except sqlite3.Error:
        raise HTTPException(status_code=500, detail="Database error")
    return {"ids": ids}

@app.post("/file_operation")
def file_operation(
    operation: str = Body(..., embed=True),
    ids: list[int] = Body(..., embed=True),
    destination: str = Body(None, embed=True),
):
    if operation not in {"move", "copy", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid operation")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id, file_path FROM files WHERE id IN ({','.join(['?']*len(ids))})", ids)
            files = cursor.fetchall()

            if operation in {"move", "copy"}:
                if not destination or not os.path.isdir(destination):
                    raise HTTPException(status_code=400, detail="Invalid destination folder")

            for file in files:
                src = file["file_path"]
                filename = os.path.basename(src)
                dest_path = os.path.join(destination, filename) if destination else None

                if operation == "move":
                    shutil.move(src, dest_path)
                    cursor.execute("UPDATE files SET file_path = ? WHERE id = ?", (dest_path, file["id"]))
                elif operation == "copy":
                    shutil.copy2(src, dest_path)
                    # Do NOT insert new record for copied file to keep copies extraneous to the web UI
                    # Copies will only appear if the folder is indexed later
                elif operation == "delete":
                    if os.path.isfile(src):
                        os.remove(src)
                    cursor.execute("DELETE FROM files WHERE id = ?", (file["id"],))

            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File operation failed: {str(e)}")

    return {"status": "success"}
