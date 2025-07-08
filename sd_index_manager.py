# sd_index_manager.py

import os
import sqlite3
import sys

def init_db():
    if not os.path.exists("sd_index.db"):
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
            conn.commit()
        print("Database initialized.")
    else:
        print("sd_index.db already exists.")

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

    supported_exts = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tiff'}
    files_indexed = 0
    files_skipped = 0
    BATCH_SIZE = 1000
    batch = []

    with sqlite3.connect("sd_index.db") as conn:
        cursor = conn.cursor()
        # SQLite PRAGMA tuning for bulk insert
        cursor.execute("PRAGMA journal_mode = OFF;")
        cursor.execute("PRAGMA synchronous = OFF;")
        cursor.execute("PRAGMA temp_store = MEMORY;")

        # Count total files for progress bar
        total_files = 0
        for _ in scan_dir(import_path):
            total_files += 1

        with alive_bar(total_files, title="Indexing images") as bar:
            for fpath in scan_dir(import_path):
                ext = os.path.splitext(fpath)[1].lower()
                if ext not in supported_exts:
                    files_skipped += 1
                    bar()
                    continue
                try:
                    prompt_info = parser_manager.parse(fpath)
                    if not prompt_info:
                        files_skipped += 1
                        bar()
                        continue
                    metadata_obj = serialize_obj(prompt_info)
                    metadata = json.dumps(metadata_obj)
                    hasher = hashlib.sha256()
                    with open(fpath, "rb") as f:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            hasher.update(chunk)
                    file_hash = hasher.hexdigest()
                    batch.append((fpath, file_hash, metadata, datetime.datetime.now()))
                    files_indexed += 1
                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany("""
                            INSERT INTO files (file_path, file_hash, metadata_json, last_scanned)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(file_path) DO UPDATE SET
                                file_hash=excluded.file_hash,
                                metadata_json=excluded.metadata_json,
                                last_scanned=excluded.last_scanned
                        """, batch)
                        batch.clear()
                except Exception:
                    # Suppress individual file errors, just count skipped
                    files_skipped += 1
                bar()
            if batch:
                cursor.executemany("""
                    INSERT INTO files (file_path, file_hash, metadata_json, last_scanned)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(file_path) DO UPDATE SET
                        file_hash=excluded.file_hash,
                        metadata_json=excluded.metadata_json,
                        last_scanned=excluded.last_scanned
                """, batch)
        conn.commit()
        # Restore PRAGMA settings if needed (optional)
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")

    print(f"Indexing complete. Files indexed: {files_indexed}, files skipped: {files_skipped}")

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
