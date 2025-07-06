import os
import sqlite3
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
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

from fastapi import Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from typing import List, Optional

@app.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    search: str = "",
    logics: Optional[List[str]] = Query(default=[]),
    values: Optional[List[str]] = Query(default=[]),
):
    conn = get_db_connection()
    cursor = conn.cursor()

    if values:
        # Build SQL WHERE clause with logical operators, including main search
        where_clauses = []
        params = []
        if search:
            where_clauses.append("(metadata_json LIKE ?)")
            params.append(f"%{search}%")
        for i, value in enumerate(values):
            clause = "metadata_json LIKE ?"
            if i == 0 and not search:
                where_clauses.append(f"({clause})")
            else:
                logic = logics[i - 1].upper() if i - 1 < len(logics) else "AND"
                if logic == "AND":
                    where_clauses.append(f"AND ({clause})")
                elif logic == "OR":
                    where_clauses.append(f"OR ({clause})")
                elif logic == "NOT":
                    where_clauses.append(f"AND NOT ({clause})")
                else:
                    where_clauses.append(f"AND ({clause})")
            params.append(f"%{value}%")
        where_statement = " ".join(where_clauses)
        query = f"SELECT * FROM files WHERE {where_statement} ORDER BY last_scanned DESC LIMIT 100"
        cursor.execute(query, params)
    elif search:
        cursor.execute("SELECT * FROM files WHERE metadata_json LIKE ? ORDER BY last_scanned DESC LIMIT 100", (f"%{search}%",))
    else:
        cursor.execute("SELECT * FROM files ORDER BY last_scanned DESC LIMIT 100")

    files = cursor.fetchall()
    conn.close()
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

# Removed /metadata_fields endpoint as it is no longer used

from fastapi.responses import FileResponse
from fastapi import HTTPException

@app.get("/image/{file_id}")
def get_image(file_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT file_path FROM files WHERE id = ?", (file_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Image not found")
    file_path = row["file_path"]
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="File not found on disk")
    return FileResponse(file_path)
