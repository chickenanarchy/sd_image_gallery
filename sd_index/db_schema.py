"""Database schema initialization & FTS management.

Provides:
  init_db(), ensure_fts(), drop_fts(), fts_exists()
Auto-migrates missing columns for backwards compatibility.
"""
from __future__ import annotations
import os, sqlite3, time
from .paths import DB_PATH

def fts_exists(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
        return cur.fetchone() is not None
    except sqlite3.Error:
        return False

def drop_fts(conn: sqlite3.Connection):
    cur = conn.cursor()
    try:
        cur.executescript(
            """
            DROP TRIGGER IF EXISTS files_ai;
            DROP TRIGGER IF EXISTS files_ad;
            DROP TRIGGER IF EXISTS files_au;
            DROP TABLE IF EXISTS files_fts;
            """
        )
        conn.commit()
    except sqlite3.Error:
        pass

def ensure_fts(conn: sqlite3.Connection):
    """Ensure primary files FTS exists with expected columns.

    Migration heuristic improved: we read PRAGMA table_info to verify columns
    rather than loose substring matches, reducing false negatives/positives.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='files_fts'")
        row = cursor.fetchone()
        exists = bool(row)
        needs_migration = False
        legacy_content_mode = False
        if exists:
            ddl = row[0] if row and isinstance(row[0], str) else ''
            if "content='files'" in ddl:
                legacy_content_mode = True
            # Always rebuild triggers to guarantee we have the expected delete+insert semantics for UPDATE (avoids integrity errors)
            try:
                cursor.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='files_au'")
                trg = cursor.fetchone()
                if trg and trg[0]:
                    # If trigger body does NOT contain the delete token pattern, mark for migration
                    if "VALUES('delete'" not in trg[0]:
                        needs_migration = True
            except sqlite3.Error:
                needs_migration = True
            try:
                cursor.execute("PRAGMA table_info(files_fts)")
                cols = {r[1] for r in cursor.fetchall()}
                required = {"metadata_json", "path", "path_norm"}
                if not required.issubset(cols):
                    needs_migration = True
            except sqlite3.Error:
                needs_migration = True
            # Heuristic: if docsize has zero rows but files has >0, treat as broken
            if not needs_migration and not legacy_content_mode:
                try:
                    cursor.execute("SELECT COUNT(*) FROM files")
                    total_files = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM files_fts_docsize")
                    fts_docs = cursor.fetchone()[0]
                    if total_files > 0 and fts_docs == 0:
                        needs_migration = True
                except sqlite3.Error:
                    needs_migration = True
        if not exists or needs_migration or legacy_content_mode:
            # Drop old structure
            if exists:
                drop_fts(conn)
            # Recreate as a standalone (contentless) FTS so we can store derived columns safely.
            cursor.execute(
                """
                CREATE VIRTUAL TABLE files_fts USING fts5(
                    metadata_json,
                    path,
                    path_norm,
                    prefix='2 3 4'
                );
                """
            )
            cursor.executescript(
                """
                CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
                    INSERT INTO files_fts(rowid, metadata_json, path, path_norm)
                    VALUES (new.id,new.metadata_json,new.file_path,REPLACE(REPLACE(REPLACE(REPLACE(new.file_path,'_',' '),'-',' '),'.',' '),'/', ' '));
                END;
                CREATE TRIGGER files_ad AFTER DELETE ON files BEGIN
                    DELETE FROM files_fts WHERE rowid = old.id;
                END;
                CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
                    -- For contentless FTS we can replace by inserting the new rowid; prior tokens auto-obsoleted.
                    INSERT OR REPLACE INTO files_fts(rowid, metadata_json, path, path_norm)
                    VALUES (new.id,new.metadata_json,new.file_path,REPLACE(REPLACE(REPLACE(REPLACE(new.file_path,'_',' '),'-',' '),'.',' '),'/', ' '));
                END;
                """
            )
            # Bulk populate
            cursor.execute(
                """
                INSERT INTO files_fts(rowid, metadata_json, path, path_norm)
                SELECT id, metadata_json, file_path, REPLACE(REPLACE(REPLACE(REPLACE(file_path,'_',' '),'-',' '),'.',' '),'/', ' ') FROM files
                """
            )
            try:
                cursor.execute("INSERT INTO files_fts(files_fts) VALUES('optimize')")
            except sqlite3.Error:
                pass
            conn.commit()
    except sqlite3.Error:
        # Intentionally silent (legacy behavior) but could be logged.
        pass

def ensure_prompts_fts(conn: sqlite3.Connection):
    """Ensure prompts FTS exists with required columns."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prompts_fts'")
        exists = cursor.fetchone() is not None
        needs_migration = False
        if exists:
            try:
                cursor.execute("PRAGMA table_info(prompts_fts)")
                cols = {r[1] for r in cursor.fetchall()}
                if not {"clean_positive", "clean_negative"}.issubset(cols):
                    needs_migration = True
            except sqlite3.Error:
                needs_migration = True
        if not exists or needs_migration:
            if exists and needs_migration:
                try:
                    cursor.executescript(
                        """
                        DROP TRIGGER IF EXISTS prompts_mi;
                        DROP TRIGGER IF EXISTS prompts_md;
                        DROP TRIGGER IF EXISTS prompts_mu;
                        DROP TABLE IF EXISTS prompts_fts;
                        """
                    )
                except sqlite3.Error:
                    pass
            cursor.execute(
                """
                CREATE VIRTUAL TABLE prompts_fts USING fts5(
                  clean_positive,
                  clean_negative,
                  content='models',
                  content_rowid='file_id',
                  prefix='2 3 4'
                );
                """
            )
            cursor.executescript(
                """
                CREATE TRIGGER prompts_mi AFTER INSERT ON models BEGIN
                  INSERT INTO prompts_fts(rowid, clean_positive, clean_negative)
                  VALUES (new.file_id, new.clean_positive, new.clean_negative);
                END;
                CREATE TRIGGER prompts_md AFTER DELETE ON models BEGIN
                  INSERT INTO prompts_fts(prompts_fts, rowid, clean_positive, clean_negative)
                  VALUES('delete', old.file_id, old.clean_positive, old.clean_negative);
                END;
                CREATE TRIGGER prompts_mu AFTER UPDATE ON models BEGIN
                  INSERT INTO prompts_fts(prompts_fts, rowid, clean_positive, clean_negative)
                  VALUES('delete', old.file_id, old.clean_positive, old.clean_negative);
                  INSERT INTO prompts_fts(rowid, clean_positive, clean_negative)
                  VALUES (new.file_id, new.clean_positive, new.clean_negative);
                END;
                """
            )
            cursor.execute(
                """
                INSERT INTO prompts_fts(rowid, clean_positive, clean_negative)
                SELECT file_id, clean_positive, clean_negative FROM models
                """
            )
            try:
                cursor.execute("INSERT INTO prompts_fts(prompts_fts) VALUES('optimize')")
            except sqlite3.Error:
                pass
            conn.commit()
    except sqlite3.Error:
        pass

def init_db():
    creating = not os.path.exists(DB_PATH)
    with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA busy_timeout = 10000;")
        except sqlite3.Error:
            pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
              id INTEGER PRIMARY KEY,
              file_path TEXT UNIQUE NOT NULL,
              file_hash TEXT,
              metadata_json TEXT,
              last_scanned DATETIME DEFAULT CURRENT_TIMESTAMP,
              last_extracted_hash TEXT,
              last_extracted_at DATETIME,
              no_metadata INTEGER DEFAULT 0,
              has_lora INTEGER DEFAULT 0,
              extraction_version INTEGER DEFAULT 0,
              prompt_truncated INTEGER DEFAULT 0
            )
            """
        )
        cur.execute("PRAGMA table_info(files)")
        existing = {r[1] for r in cur.fetchall()}
        for col, ddl in [
            ("file_size", "ALTER TABLE files ADD COLUMN file_size INTEGER"),
            ("file_mtime", "ALTER TABLE files ADD COLUMN file_mtime INTEGER"),
            ("file_ctime", "ALTER TABLE files ADD COLUMN file_ctime INTEGER"),
            ("width", "ALTER TABLE files ADD COLUMN width INTEGER"),
            ("height", "ALTER TABLE files ADD COLUMN height INTEGER"),
        ]:
            if col not in existing:
                try:
                    cur.execute(ddl)
                except sqlite3.Error:
                    pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS models (
              file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
              model_name TEXT,
              model_hash_short TEXT,
              vae TEXT,
              vae_hash TEXT,
              refiner_model TEXT,
              refiner_switch_at REAL,
              model_hash_full TEXT,
              model_hash_auto_v3 TEXT,
              hash_type TEXT,
              display_hash TEXT,
              steps INTEGER,
              sampler TEXT,
              scheduler TEXT,
              cfg_scale REAL,
              seed INTEGER,
              subseed INTEGER,
              subseed_strength REAL,
              clip_skip INTEGER,
              denoising_strength REAL,
              tiling INTEGER,
              face_restoration TEXT,
              width INTEGER,
              height INTEGER,
              size_raw TEXT,
              hires_upscaler TEXT,
              hires_steps INTEGER,
              hires_denoising REAL,
              raw_positive TEXT,
              raw_negative TEXT,
              clean_positive TEXT,
              clean_negative TEXT,
              lora_count INTEGER,
              metadata_hash TEXT NOT NULL,
              extraction_time_ms INTEGER,
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lora_usages (
              id INTEGER PRIMARY KEY,
              file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
              lora_name TEXT NOT NULL,
              weight REAL NOT NULL,
              context TEXT CHECK(context IN ('positive','negative')),
              position_index INTEGER,
              UNIQUE(file_id, lora_name, context, position_index)
            )
            """
        )
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_files_last_scanned ON files(last_scanned DESC)",
            "CREATE INDEX IF NOT EXISTS idx_files_last_scanned_id ON files(last_scanned DESC, id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_files_file_hash ON files(file_hash)",
            "CREATE INDEX IF NOT EXISTS idx_files_file_mtime ON files(file_mtime)",
            "CREATE INDEX IF NOT EXISTS idx_files_file_ctime ON files(file_ctime)",
            "CREATE INDEX IF NOT EXISTS idx_files_last_extracted_hash ON files(last_extracted_hash)",
            "CREATE INDEX IF NOT EXISTS idx_files_has_lora ON files(has_lora, id)",
        ]:
            try:
                cur.execute(stmt)
            except sqlite3.Error:
                pass
        for col in ("file_size", "width", "height", "file_path"):
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_files_{col}_id ON files({col} DESC, id DESC)")
            except sqlite3.Error:
                pass
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_models_model_name ON models(model_name)",
            "CREATE INDEX IF NOT EXISTS idx_models_model_hash_short ON models(model_hash_short)",
            "CREATE INDEX IF NOT EXISTS idx_models_metadata_hash ON models(metadata_hash)",
            "CREATE INDEX IF NOT EXISTS idx_models_seed ON models(seed)",
            "CREATE INDEX IF NOT EXISTS idx_lora_name ON lora_usages(lora_name)",
            "CREATE INDEX IF NOT EXISTS idx_lora_file ON lora_usages(file_id)",
        ]:
            try:
                cur.execute(stmt)
            except sqlite3.Error:
                pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_file_path_asc ON files(file_path ASC, id ASC)")
        except sqlite3.Error:
            pass
        # Ensure FTS structures while connection open
        ensure_fts(conn)
        ensure_prompts_fts(conn)
        conn.commit()
    if creating:
        print("Database initialized.")

__all__ = ["init_db", "ensure_fts", "ensure_prompts_fts", "fts_exists", "drop_fts"]
