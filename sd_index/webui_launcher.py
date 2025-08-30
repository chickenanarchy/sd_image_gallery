"""Launch the FastAPI WebUI via uvicorn."""
from __future__ import annotations
import os, subprocess, sys
from .paths import BASE_DIR, DB_PATH

def run_webui():
    webui_path = os.path.join(BASE_DIR, "webui", "main.py")
    if not os.path.exists(webui_path):
        print("WebUI not found at", webui_path)
        return
    print(f"Launching Web UI at http://127.0.0.1:8000 (DB: {DB_PATH}) ...")
    try:
        subprocess.run([sys.executable, "-m", "uvicorn", "webui.main:app", "--reload"], check=True)
    except Exception as e:
        print("Failed to launch Web UI:", e)

__all__ = ['run_webui']
