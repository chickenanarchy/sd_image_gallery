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

def index_files():
    parser_manager = ParserManager()
    import_path = input("Enter the root directory to scan for images: ").strip()
    if not os.path.isdir(import_path):
        print(f"Directory '{import_path}' does not exist.")
        return

    supported_exts = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tiff'}
    files_indexed = 0
    files_skipped = 0

    with sqlite3.connect("sd_index.db") as conn:
        cursor = conn.cursor()
        for root, _, files in os.walk(import_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in supported_exts:
                    files_skipped += 1
                    continue
                fpath = os.path.join(root, fname)
                try:
                    prompt_info = parser_manager.parse(fpath)
                    if not prompt_info:
                        files_skipped += 1
                        continue
                    # Serialize entire prompt_info object recursively
                    metadata_obj = serialize_obj(prompt_info)
                    metadata = json.dumps(metadata_obj)
                    # Compute file hash (optional, for deduplication)
                    hasher = hashlib.sha256()
                    with open(fpath, "rb") as f:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            hasher.update(chunk)
                    file_hash = hasher.hexdigest()
                    # Upsert into files table
                    cursor.execute("""
                        INSERT INTO files (file_path, file_hash, metadata_json, last_scanned)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(file_path) DO UPDATE SET
                            file_hash=excluded.file_hash,
                            metadata_json=excluded.metadata_json,
                            last_scanned=excluded.last_scanned
                    """, (fpath, file_hash, metadata, datetime.datetime.now()))
                    files_indexed += 1
                except Exception as e:
                    print(f"Error processing {fpath}: {e}")
                    files_skipped += 1
        conn.commit()

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
