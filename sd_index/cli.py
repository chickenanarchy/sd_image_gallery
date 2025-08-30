"""Interactive CLI loop (extracted from original __main__)."""
from __future__ import annotations
import os, sqlite3
from .paths import DB_PATH
from .db_schema import init_db
from .db_repair import check_and_repair_db, vacuum_repair_db
from .duplicates import de_duplicate_by_hash
from .cleanup import clear_database
from .indexing import index_files
from .webui_launcher import run_webui
from .progress import _run_with_spinner

def main():  # pragma: no cover - interactive
    init_db()
    while True:
        print("\nChoose an option:")
        print("1. Index/Re-index SD files")
        print("2. Check and repair database")
        print("3. Clear database")
        print("4. Run WebUI")
        print("5. De-duplicate files by hash (delete duplicates)")
        print("6. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            index_files()
        elif choice == '2':
            if not os.path.exists(DB_PATH):
                print("No database found; initializing new database...")
                init_db(); continue
            try:
                def _integrity():
                    with sqlite3.connect(DB_PATH) as conn:
                        cur = conn.cursor(); cur.execute("PRAGMA integrity_check"); return cur.fetchone()
                res = _run_with_spinner("Running integrity check", _integrity)
                status = res[0] if res else None
                if status == 'ok':
                    print("Database integrity check: OK")
                else:
                    print("Database integrity check failed:", status)
                    print("Attempting VACUUM-based repair...")
                    if _run_with_spinner("VACUUM repair", vacuum_repair_db, DB_PATH):
                        print("VACUUM repair succeeded.")
                    else:
                        print("VACUUM repair failed. Attempting fallback repair...")
                        if _run_with_spinner("Fallback repair", check_and_repair_db, DB_PATH):
                            print("Fallback repair completed. A new database may have been created; please re-index.")
                        else:
                            print("Repair failed. Consider backing up and recreating the database.")
            except sqlite3.Error as e:
                print("Error running integrity check:", e)
        elif choice == '3':
            clear_database(); init_db()
        elif choice == '4':
            run_webui()
        elif choice == '5':
            try: de_duplicate_by_hash(auto_confirm=False)
            except Exception as e:  # pragma: no cover
                print("Error running duplicate removal:", e)
        elif choice == '6':
            print("Goodbye!"); break
        else:
            print("Invalid choice. Try again.")

__all__ = ['main']
