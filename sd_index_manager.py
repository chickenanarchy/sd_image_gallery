# sd_index_manager.py

import os
import sqlite3
import sys
import threading
import time
import hashlib
import json
import datetime

# Resolve database path relative to this script to avoid CWD surprises
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "sd_index.db")

# Lightweight progress-bar wrapper with graceful fallback if alive-progress isn't installed
def _progress_bar(total=None, title: str = ""):
	try:
		from alive_progress import alive_bar as _alive_bar  # type: ignore
		return _alive_bar(total=total, title=title)
	except Exception:
		class _NoBar:
			def __enter__(self_inner):
				return (lambda: None)

			def __exit__(self_inner, exc_type, exc, tb):
				return False

		return _NoBar()

def ensure_fts(conn: sqlite3.Connection):
	"""Ensure FTS5 virtual table and triggers exist and include path columns.

	Schema includes:
	  - metadata_json: raw metadata text
	  - path: original file_path
	  - path_norm: file_path with common separators (underscore, dash, dot) replaced by spaces for better tokenization

	If an older files_fts table exists without path columns, it will be dropped and rebuilt.
	Uses content=files with content_rowid=id so that data stays in sync via triggers.
	"""
	cursor = conn.cursor()
	# Detect whether FTS table already exists and whether it has required columns
	cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='files_fts'")
	row = cursor.fetchone()
	exists = row is not None
	needs_migration = False
	if exists and row and isinstance(row[0], str):
		ddl = row[0]
		# crude check: ensure both 'path' and 'path_norm' present
		needs_migration = not ("path" in ddl and "path_norm" in ddl)
	try:
		if not exists or needs_migration:
			if exists and needs_migration:
				# Drop old FTS + triggers, then recreate with new schema
				drop_fts(conn)
			# Create FTS table with prefix indexes for faster wildcard (prefix) searching
			cursor.execute(
				"""
				CREATE VIRTUAL TABLE files_fts USING fts5(
					metadata_json,
					path,
					path_norm,
					content='files',
					content_rowid='id',
					prefix='2 3 4'
				);
				"""
			)
			# Triggers to keep FTS data in sync
			# Note: We normalize path by replacing common separators with spaces to improve token matching.
			cursor.executescript(
				"""
				CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
					INSERT INTO files_fts(rowid, metadata_json, path, path_norm)
					VALUES (
						new.id,
						new.metadata_json,
						new.file_path,
						REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(new.file_path, '_', ' '), '-', ' '), '.', ' '), '/', ' '), '\\', ' ')
					);
				END;
				CREATE TRIGGER files_ad AFTER DELETE ON files BEGIN
					INSERT INTO files_fts(files_fts, rowid, metadata_json, path, path_norm)
					VALUES('delete', old.id, old.metadata_json, old.file_path,
						   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(old.file_path, '_', ' '), '-', ' '), '.', ' '), '/', ' '), '\\', ' '));
				END;
				CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
					INSERT INTO files_fts(files_fts, rowid, metadata_json, path, path_norm)
					VALUES('delete', old.id, old.metadata_json, old.file_path,
						   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(old.file_path, '_', ' '), '-', ' '), '.', ' '), '/', ' '), '\\', ' '));
					INSERT INTO files_fts(rowid, metadata_json, path, path_norm)
					VALUES (
						new.id,
						new.metadata_json,
						new.file_path,
						REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(new.file_path, '_', ' '), '-', ' '), '.', ' '), '/', ' '), '\\', ' ')
					);
				END;
				"""
			)
			# Backfill existing rows
			cursor.execute(
				"""
				INSERT INTO files_fts(rowid, metadata_json, path, path_norm)
				SELECT id,
					   metadata_json,
					   file_path,
					   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(file_path, '_', ' '), '-', ' '), '.', ' '), '/', ' '), '\\', ' ')
				FROM files
				"""
			)
			# Optimize FTS index for faster queries (best-effort)
			try:
				cursor.execute("INSERT INTO files_fts(files_fts) VALUES('optimize')")
			except sqlite3.Error:
				pass
			conn.commit()
			print("FTS index created/migrated and backfilled.")
	except sqlite3.OperationalError as e:
		# Likely FTS5 not compiled; warn user once
		print("Warning: Could not create/migrate FTS index (", e, ") - continuing without FTS.")

def fts_exists(conn: sqlite3.Connection) -> bool:
	cur = conn.cursor()
	try:
		cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'")
		return cur.fetchone() is not None
	except sqlite3.Error:
		return False

def drop_fts(conn: sqlite3.Connection):
	"""Drop FTS5 table & associated triggers if present (best-effort)."""
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
		print("Dropped existing FTS index & triggers for fast bulk load.")
	except sqlite3.Error as e:  # pragma: no cover - best effort
		print("Warning: failed to drop FTS objects:", e)

def init_db():
	creating = not os.path.exists(DB_PATH)
	if creating:
		print("Creating new sd_index.db...")
	with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
		cursor = conn.cursor()
		try:
			cursor.execute("PRAGMA busy_timeout = 10000;")
		except sqlite3.Error:
			pass
		cursor.execute("""
		CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY,
			file_path TEXT UNIQUE NOT NULL,
			file_hash TEXT,
			metadata_json TEXT,
			last_scanned DATETIME DEFAULT CURRENT_TIMESTAMP
		)
		""")
		# Schema migration: add file_size, file_mtime if missing
		cursor.execute("PRAGMA table_info(files)")
		existing_cols = {row[1] for row in cursor.fetchall()}
		if 'file_size' not in existing_cols:
			cursor.execute("ALTER TABLE files ADD COLUMN file_size INTEGER")
			print("Added column file_size")
		if 'file_mtime' not in existing_cols:
			cursor.execute("ALTER TABLE files ADD COLUMN file_mtime INTEGER")
			print("Added column file_mtime")
		if 'file_ctime' not in existing_cols:
			cursor.execute("ALTER TABLE files ADD COLUMN file_ctime INTEGER")
			print("Added column file_ctime")
		if 'width' not in existing_cols:
			cursor.execute("ALTER TABLE files ADD COLUMN width INTEGER")
			print("Added column width")
		if 'height' not in existing_cols:
			cursor.execute("ALTER TABLE files ADD COLUMN height INTEGER")
			print("Added column height")
		# Indexes (id is primary). Create after columns are ensured.
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_last_scanned ON files(last_scanned DESC)")
		except sqlite3.OperationalError:
			pass
		# Composite index to accelerate ORDER BY last_scanned DESC, id DESC pagination
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_last_scanned_id ON files(last_scanned DESC, id DESC)")
		except sqlite3.OperationalError:
			pass
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_file_hash ON files(file_hash)")
		except sqlite3.OperationalError:
			pass
		# Indexes for temporal filtering (added for year/month range queries)
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_file_mtime ON files(file_mtime)")
		except sqlite3.OperationalError:
			pass
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_file_ctime ON files(file_ctime)")
		except sqlite3.OperationalError:
			pass
		# Covering indexes helpful for sorts used in gallery pagination
		for col in ("file_size", "width", "height", "file_path"):
			try:
				# Include id to serve as a stable tiebreaker for pagination
				cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_files_{col}_id ON files({col} DESC, id DESC)")
			except sqlite3.OperationalError:
				pass
		# Dedicated ascending index for file_path to help ORDER BY file_path COLLATE NOCASE
		try:
			cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_file_path_asc ON files(file_path ASC, id ASC)")
		except sqlite3.OperationalError:
			pass
		conn.commit()
		ensure_fts(conn)
	if creating:
		print("Database initialized.")
	else:
		print("sd_index.db already exists (schema ensured / FTS checked).")

def check_and_repair_db(db_path: str = DB_PATH) -> bool:
	"""Run PRAGMA integrity_check; if malformed attempt automated repair.

	Strategy: rename corrupted file to *.corrupt-<timestamp>.db and recreate
	a fresh schema. Returns True if DB is healthy (or repaired), False if
	unrecoverable without user intervention.
	"""
	if not os.path.exists(db_path):
		return True
	# Step 1: run integrity_check with a small busy timeout and WAL checkpoint
	try:
		with sqlite3.connect(db_path, timeout=5.0) as conn:
			cur = conn.cursor()
			try:
				cur.execute("PRAGMA busy_timeout = 5000;")
			except sqlite3.Error:
				pass
			try:
				cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
			except sqlite3.Error:
				pass
			cur.execute("PRAGMA integrity_check")
			result = cur.fetchone()
			if not result:
				print("Integrity check returned no result.")
				return False
			if result[0] == 'ok':
				return True
			print("Integrity check failed:", result[0])
	except sqlite3.DatabaseError as e:
		print("DatabaseError during integrity check:", e)

	# Step 2: Try VACUUM-based in-place rebuild first (works even if rename fails later)
	try:
		if vacuum_repair_db(db_path):
			print("VACUUM-based rebuild completed.")
			return True
	except Exception:
		pass

	# Step 3: Attempt to preserve the corrupted DB and recreate schema.
	# On Windows, rename can fail if another process has the file open (WinError 32).
	try:
		import datetime as _dt
		ts = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
		corrupt_name = f"{db_path}.corrupt-{ts}.db"
		# Move main DB file
		os.replace(db_path, corrupt_name)
		# Move WAL/SHM siblings if present
		for suffix in ("-wal", "-shm"):
			try:
				p = f"{db_path}{suffix}"
				if os.path.exists(p):
					os.replace(p, f"{corrupt_name}{suffix}")
			except Exception:
				# Best effort: ignore
				pass
		print(f"Corrupted database renamed to {corrupt_name}. Creating new database...")
		init_db()
		return True
	except PermissionError as e:
		# Likely locked by another process; try to make a backup copy using SQLite backup API
		if hasattr(e, 'winerror') and e.winerror == 32:
			try:
				import datetime as _dt
				ts2 = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
				backup_name = f"{db_path}.backup-{ts2}.db"
				with sqlite3.connect(db_path, timeout=5.0) as src, sqlite3.connect(backup_name) as dst:
					src.backup(dst)
				print(
					"Database appears to be in use by another process. Created a backup copy at:", backup_name,
					"\nClose applications that may be using sd_index.db (e.g., the WebUI, Explorer previews, antivirus scans) and retry."
				)
			except Exception as be:
				print("Failed to create backup while locked:", be)
			return False
		print("Failed to auto-repair database:", e)
		return False
	except Exception as e:  # pragma: no cover (best-effort)
		print("Failed to auto-repair database:", e)
		return False

def vacuum_repair_db(db_path: str = DB_PATH) -> bool:
	"""Attempt to rebuild the SQLite database in-place using VACUUM INTO.

	Creates a new compacted copy and replaces the original atomically. Returns
	True on success, False otherwise. Requires SQLite 3.27+.
	"""
	try:
		import datetime as _dt
		ts = _dt.datetime.now().strftime("%Y%m%d%H%M%S")
		tmp_path = f"{db_path}.vacuum-{ts}.db"
		# Ensure current connection is closed; we create a fresh one
		with sqlite3.connect(db_path) as conn:
			cur = conn.cursor()
			# Best-effort WAL checkpoint before vacuum
			try:
				cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")
			except sqlite3.Error:
				pass
			escaped = tmp_path.replace("'", "''")
			cur.execute(f"VACUUM INTO '{escaped}'")
		# Replace original DB with vacuumed copy
		# Remove WAL/SHM of old DB to prevent stale state
		for suffix in ("-wal", "-shm"):
			try:
				wal_path = f"{db_path}{suffix}"
				if os.path.exists(wal_path):
					os.remove(wal_path)
			except Exception:
				pass
		os.replace(tmp_path, db_path)
		return True
	except Exception:
		return False

def _run_with_spinner(title: str, func, *args, **kwargs):
	"""Run a function in a worker thread while showing an alive-progress spinner."""
	result = {}
	error = {}
	def _target():
		try:
			result['value'] = func(*args, **kwargs)
		except Exception as e:
			error['exc'] = e
	t = threading.Thread(target=_target, daemon=True)
	t.start()
	try:
		from alive_progress import alive_bar as _alive_bar  # type: ignore
		with _alive_bar(total=None, title=title) as bar:
			while t.is_alive():
				time.sleep(0.1)
				bar()
	except Exception:
		# Fallback: simple wait loop without spinner
		while t.is_alive():
			time.sleep(0.1)
	finally:
		t.join()
	if 'exc' in error:
		raise error['exc']
	return result.get('value')

def clear_database():
	removed_any = False
	for p in (DB_PATH, f"{DB_PATH}-wal", f"{DB_PATH}-shm"):
		try:
			if os.path.exists(p):
				os.remove(p)
				removed_any = True
		except Exception as e:
			print(f"Failed to remove {p}: {e}")
	if removed_any:
		print("Database removed.")
	else:
		print("No database found to remove.")

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
	"""Yield file paths under path, skipping folders/files we cannot access."""
	try:
		with os.scandir(path) as it:
			for entry in it:
				try:
					if entry.is_dir(follow_symlinks=False):
						yield from scan_dir(entry.path)
					elif entry.is_file(follow_symlinks=False):
						yield entry.path
				except (PermissionError, FileNotFoundError, OSError):
					continue
	except (PermissionError, FileNotFoundError, OSError):
		return

def index_files():
	# Lazy import to avoid heavy startup cost and potential hangs
	parser_manager = None
	try:
		from sd_parsers import ParserManager  # type: ignore
		parser_manager = ParserManager()
	except Exception as e:
		print("Warning: sd_parsers not available (", e, ") - metadata parsing will be skipped.")
	import_path = input("Enter the root directory to scan for images: ").strip()
	if not os.path.isdir(import_path):
		print(f"Directory '{import_path}' does not exist.")
		return

	# Default behavior: FAST add-only mode, no redundant prompts
	full_refresh = False
	print("Running FAST add-only mode: only new files hashed + missing removed. Existing files skipped entirely.")

	if not check_and_repair_db():
		print("Aborting indexing due to unrecoverable database corruption.")
		return

	# Fast bulk mode: always drop & rebuild FTS afterwards
	fast_bulk = True

	supported_exts = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tiff'}
	files_new = 0
	files_updated = 0  # only used in full refresh
	files_skipped = 0  # unchanged or pre-existing
	files_deleted = 0
	BATCH_SIZE = 5000  # increased batch size for fewer commits
	WAL_CHECK_INTERVAL = 10_000  # run checkpoint every N processed files
	processed_files = 0
	insert_batch = []  # tuples matching INSERT columns
	update_batch = []  # tuples matching UPDATE setters

	# Normalize root path (ensure trailing separator for LIKE pattern)
	root_norm = os.path.abspath(import_path)
	if not root_norm.endswith(os.sep):
		root_norm += os.sep
	like_pattern = root_norm + '%'
	seen_paths = set()

	with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
		cursor = conn.cursor()
		# Safer PRAGMA tuning: keep WAL to reduce corruption risk while still performant.
		try:
			cursor.execute("PRAGMA journal_mode = WAL;")
		except sqlite3.Error:
			pass
		try:
			cursor.execute("PRAGMA synchronous = NORMAL;")  # FAST + durable with WAL
		except sqlite3.Error:
			pass
		try:
			cursor.execute("PRAGMA busy_timeout = 30000;")
		except sqlite3.Error:
			pass
		try:
			cursor.execute("PRAGMA temp_store = MEMORY;")
		except sqlite3.Error:
			pass
		try:
			cursor.execute("PRAGMA mmap_size = 300000000;")  # allow mmap for faster reads (best-effort)
		except sqlite3.Error:
			pass

		pre_existing_fts = fts_exists(conn)
		if fast_bulk and pre_existing_fts:
			drop_fts(conn)

		# Preload existing file paths for this root to allow O(1) membership tests (fast mode)
		existing_paths = set()
		cursor.execute("SELECT file_path, file_mtime, file_size FROM files WHERE file_path LIKE ?", (like_pattern,))
		preloaded_meta = {}
		for row in cursor.fetchall():
			existing_paths.add(os.path.abspath(row[0]))
			if full_refresh:
				preloaded_meta[os.path.abspath(row[0])] = (row[1], row[2])  # (mtime,size)

		# Count total candidate files for progress bar (only supported extensions).
		# This requires a pre-pass; if performance becomes a concern we can make this optional.
		total_files = 0
		for p in scan_dir(import_path):
			if os.path.splitext(p)[1].lower() in supported_exts:
				total_files += 1

		try:
			with _progress_bar(total_files, title="Indexing images (incremental)") as bar:
				for fpath in scan_dir(import_path):
					ext = os.path.splitext(fpath)[1].lower()
					if ext not in supported_exts:
						bar()
						continue
					try:
						abs_path = os.path.abspath(fpath)
						seen_paths.add(abs_path)
						if abs_path in existing_paths:
							if not full_refresh:
								# Fast mode: skip entirely
								files_skipped += 1
								bar()
								continue
						# For full refresh OR brand new file, gather stats & potentially update
						st = os.stat(fpath)
						file_size = st.st_size
						file_mtime = int(st.st_mtime)
						file_ctime = int(getattr(st, 'st_ctime', file_mtime))
						width = height = None
						try:
							from PIL import Image
							with Image.open(fpath) as im:
								width, height = im.size
						except Exception:
							pass
						if abs_path in existing_paths:
							if full_refresh:
								# Determine unchanged via preloaded metadata without extra SELECT
								prev_mtime, prev_size = preloaded_meta.get(abs_path, (None, None))
								if prev_mtime == file_mtime and prev_size == file_size:
									files_skipped += 1
									bar()
									# Count processed even if skipped for accurate checkpoints
									processed_files += 1
									if full_refresh and processed_files % WAL_CHECK_INTERVAL == 0:
										try:
											cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
											cursor.execute("SELECT COUNT(*) FROM files")
											total_indexed = cursor.fetchone()[0]
											print(f"\n[Checkpoint] Files in DB: {total_indexed} | WAL truncated | processed this run: {processed_files}")
										except sqlite3.Error:
											pass
									continue
						# New or changed file: parse + hash
						prompt_info = None
						if parser_manager is not None:
							try:
								prompt_info = parser_manager.parse(fpath)
							except Exception:
								prompt_info = None
						if prompt_info:
							metadata_obj = serialize_obj(prompt_info)
							metadata = json.dumps(metadata_obj)
						else:
							metadata = ''
						hasher = hashlib.sha256()
						with open(fpath, "rb") as f:
							for chunk in iter(lambda: f.read(8192), b''):
								hasher.update(chunk)
						file_hash = hasher.hexdigest()
						now = datetime.datetime.utcnow()
						if abs_path in existing_paths:
							# Only in full refresh path
							update_batch.append((file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height, fpath))
							files_updated += 1
						else:
							insert_batch.append((fpath, file_hash, metadata, now, file_size, file_mtime, file_ctime, width, height))
							files_new += 1
						# Flush batches when they reach threshold (applies to both inserts and updates)
						if len(insert_batch) >= BATCH_SIZE:
							cursor.executemany("""
								INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
								VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
								ON CONFLICT(file_path) DO UPDATE SET
									file_hash=excluded.file_hash,
									metadata_json=excluded.metadata_json,
									last_scanned=excluded.last_scanned,
									file_size=excluded.file_size,
									file_mtime=excluded.file_mtime,
									file_ctime=excluded.file_ctime,
									width=excluded.width,
									height=excluded.height
							""", insert_batch)
							conn.commit()
							insert_batch.clear()
						if full_refresh and len(update_batch) >= BATCH_SIZE:
							cursor.executemany("""
								UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=?
								WHERE file_path=?
							""", update_batch)
							conn.commit()
							update_batch.clear()
						# Periodic WAL checkpoint & stats every interval
						processed_files += 1
						if full_refresh and processed_files % WAL_CHECK_INTERVAL == 0:
							try:
								cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
								cursor.execute("SELECT COUNT(*) FROM files")
								total_indexed = cursor.fetchone()[0]
								print(f"\n[Checkpoint] Files in DB: {total_indexed} | WAL truncated | processed this run: {processed_files}")
							except sqlite3.Error:
								pass
					except sqlite3.DatabaseError as db_err:
						if 'malformed' in str(db_err).lower():
							print("Encountered malformed database during indexing. Attempting automatic repair...")
							# Break early to attempt repair after loop
							raise
						else:
							print("SQLite error for file", fpath, db_err)
					except Exception:
						# On unexpected failure still record file with empty metadata and dimensions unknown
						now = datetime.datetime.utcnow()
						try:
							st = os.stat(fpath)
							file_size = st.st_size
							file_mtime = int(st.st_mtime)
							file_ctime = int(getattr(st, 'st_ctime', file_mtime))
						except Exception:
							file_size = file_mtime = file_ctime = 0
						width = height = None
						try:
							if cursor.execute("SELECT 1 FROM files WHERE file_path=?", (fpath,)).fetchone():
								update_batch.append(('', '', now, file_size, file_mtime, file_ctime, width, height, fpath))
							else:
								insert_batch.append((fpath, '', '', now, file_size, file_mtime, file_ctime, width, height))
						except sqlite3.Error:
							pass
					bar()
			# Flush remaining batches inside try
			if insert_batch:
				cursor.executemany("""
					INSERT INTO files (file_path, file_hash, metadata_json, last_scanned, file_size, file_mtime, file_ctime, width, height)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
					ON CONFLICT(file_path) DO UPDATE SET
						file_hash=excluded.file_hash,
						metadata_json=excluded.metadata_json,
						last_scanned=excluded.last_scanned,
						file_size=excluded.file_size,
						file_mtime=excluded.file_mtime,
						file_ctime=excluded.file_ctime,
						width=excluded.width,
						height=excluded.height
				""", insert_batch)
			if full_refresh and update_batch:
				cursor.executemany("""
					UPDATE files SET file_hash=?, metadata_json=?, last_scanned=?, file_size=?, file_mtime=?, file_ctime=?, width=?, height=?
					WHERE file_path=?
				""", update_batch)
			# Deletions
			# existing_paths already contains all DB paths; compute missing
			delete_list = []
			for db_path_abs in existing_paths:
				if db_path_abs not in seen_paths and not os.path.exists(db_path_abs):
					delete_list.append(db_path_abs)
			if delete_list:
				CHUNK = 1000
				for i in range(0, len(delete_list), CHUNK):
					chunk = delete_list[i:i+CHUNK]
					placeholders = ','.join(['?']*len(chunk))
					cursor.execute(f"DELETE FROM files WHERE file_path IN ({placeholders})", chunk)
				files_deleted = len(delete_list)
			conn.commit()
		except sqlite3.DatabaseError as db_err:
			if 'malformed' in str(db_err).lower():
				print("Database marked as malformed. Initiating repair cycle...")
				if check_and_repair_db():
					print("Repair completed. Please rerun indexing.")
				else:
					print("Automatic repair failed. Consider removing sd_index.db manually.")
				return
			else:
				print("SQLite error aborted indexing:", db_err)
				return
		# Rebuild FTS if we deferred it during fast bulk mode
		if fast_bulk and pre_existing_fts:
			print("Rebuilding FTS index (this may take a moment)...")
			ensure_fts(conn)
		# Ensure WAL mode persists
		try:
			cursor.execute("PRAGMA journal_mode = WAL;")
		except sqlite3.Error:
			pass
		# Final checkpoint to shrink WAL after heavy writes
		try:
			cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
		except sqlite3.Error:
			pass

	if full_refresh:
		print(
			"Indexing complete (FULL). New: {n} Updated: {u} Skipped: {s} Deleted: {d}".format(
				n=files_new, u=files_updated, s=files_skipped, d=files_deleted
			)
		)
	else:
		print(
			"Indexing complete (FAST). New: {n} Existing skipped: {s} Deleted: {d}".format(
				n=files_new, s=files_skipped, d=files_deleted
			)
		)

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
				init_db()
				continue
			try:
				def _integrity():
					with sqlite3.connect(DB_PATH) as conn:
						cur = conn.cursor()
						cur.execute("PRAGMA integrity_check")
						return cur.fetchone()
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
			clear_database()
			init_db()
		elif choice == '4':
			run_webui()
		elif choice == '5':
			# New option: de-dupe by exact SHA-256 hash
			def _format_bytes(n: int) -> str:
				try:
					for unit in ['B','KB','MB','GB','TB']:
						if n < 1024:
							return f"{n:.1f} {unit}" if unit != 'B' else f"{n} {unit}"
						n /= 1024
					return f"{n:.1f} PB"
				except Exception:
					return str(n)

			# Pre-scan to show counts with progress
			try:
				with sqlite3.connect(DB_PATH) as conn:
					cur = conn.cursor()
					cur.execute("SELECT COUNT(*) FROM files WHERE file_hash IS NOT NULL AND file_hash <> ''")
					total_candidates_row = cur.fetchone()
					total_candidates = int(total_candidates_row[0]) if total_candidates_row and total_candidates_row[0] else 0
					if total_candidates == 0:
						print("No files with hashes found.")
						continue
					print("Scanning for duplicate hashes (this may take a moment)...")
					counts: dict[str, int] = {}
					cur.execute("SELECT file_hash FROM files WHERE file_hash IS NOT NULL AND file_hash <> ''")
					FETCH = 10000
					with _progress_bar(total_candidates, title="Scanning hashes") as bar:
						while True:
							rows = cur.fetchmany(FETCH)
							if not rows:
								break
							for (h,) in rows:
								if not h:
									bar()
									continue
								counts[h] = counts.get(h, 0) + 1
								bar()
				groups = sum(1 for c in counts.values() if c > 1)
				files_to_delete = sum(c - 1 for c in counts.values() if c > 1)
				duplicate_hashes = [h for h, c in counts.items() if c > 1]
			except sqlite3.Error as e:
				print("Database error during duplicate scan:", e)
				continue

			if groups == 0:
				print("No duplicate file hashes found.")
				continue

			print(f"\nFound {groups} duplicate hash group(s).")
			print(f"{files_to_delete} file(s) would be deleted (keeping one per group).")
			print("\nDe-duplicate by exact hash will:"
				  "\n - Identify groups of files with the same SHA-256 (file_hash)"
				  "\n - Keep one file in each group and DELETE the rest from disk and database."
				  "\nThis is a destructive action.")
			confirm = input("Type 'y' to proceed (anything else to cancel): ").strip().lower()
			if confirm != 'y':
				print("Cancelled.")
				continue

			# Execute deletion
			try:
				with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
					conn.row_factory = sqlite3.Row
					cur = conn.cursor()
					try:
						cur.execute("PRAGMA journal_mode = WAL;")
					except sqlite3.Error:
						pass
					try:
						cur.execute("PRAGMA busy_timeout = 30000;")
					except sqlite3.Error:
						pass
					# Use scanned duplicate list
					hashes = duplicate_hashes
					total_groups = len(hashes)
					files_deleted = 0
					rows_deleted = 0
					bytes_freed = 0
					with _progress_bar(total_groups, title="Deleting duplicates") as bar2:
						for h in hashes:
							try:
								cur.execute(
									"SELECT id, file_path, file_size FROM files WHERE file_hash=? ORDER BY id ASC",
									(h,),
								)
								rows = cur.fetchall()
							except sqlite3.Error as qerr:
								print("Query error during duplicate group fetch:", qerr)
								bar2()
								continue
							if not rows:
								bar2()
								continue
							# Choose keeper: first row whose file exists; else first by id
							keep_index = 0
							for i, row in enumerate(rows):
								try:
									if os.path.isfile(row["file_path"]):
										keep_index = i
										break
								except Exception:
									continue
							for i, row in enumerate(rows):
								if i == keep_index:
									continue
								path = row["file_path"]
								size_val = None
								try:
									size_val = int(row["file_size"]) if row["file_size"] is not None else None
								except Exception:
									size_val = None
								# Delete file on disk if present
								if os.path.isfile(path):
									try:
										os.remove(path)
										files_deleted += 1
										if isinstance(size_val, int):
											bytes_freed += size_val
									except PermissionError as fe:
										print(f"Failed to delete file (permission/locked): {path} -> {fe}")
									except Exception as fe:
										print(f"Failed to delete file: {path} -> {fe}")
								# If file is missing, assume already deleted; continue to clean DB row quietly
								# Remove DB row
								try:
									cur.execute("DELETE FROM files WHERE id=?", (row["id"],))
									rows_deleted += 1
								except sqlite3.Error as de:
									print(f"Failed to delete DB row id={row['id']}: {de}")
							# Commit after each group to release locks early
							try:
								conn.commit()
							except sqlite3.Error:
								pass
							bar2()
					# Final commit (no-op if already per-group committed)
					try:
						conn.commit()
					except sqlite3.Error:
						pass
					print(
						"De-duplication complete.\n"
						f" - Duplicate groups processed: {total_groups}\n"
						f" - Files deleted: {files_deleted}\n"
						f" - DB rows removed: {rows_deleted}\n"
						f" - Estimated space freed: {_format_bytes(bytes_freed)}"
					)
			except sqlite3.Error as e:
				print("Database error during de-duplication:", e)
		elif choice == '6':
			print("Goodbye!")
			break
		else:
			print("Invalid choice. Try again.")

