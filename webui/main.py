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

                query = f"SELECT * FROM files WHERE {where_statement} ORDER BY last_scanned DESC LIMIT 100"
                cursor.execute(query, params)
            else:
                cursor.execute("SELECT * FROM files ORDER BY last_scanned DESC LIMIT 100")

            files = cursor.fetchall()
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
