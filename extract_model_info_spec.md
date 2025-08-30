def sha256_file(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
# Model & LoRA Extraction Plan (Concise Implementation Spec)
Version: 2.0  
Goal: Single, precise blueprint a developer can implement directly.

---
## 1. Targets & Outcomes
Input: Image files (PNG primary; also JPEG/WebP) with embedded Stable Diffusion generation metadata (primarily Automatic1111 style).  
Output (DB):
1. Row in `models` (1:1 with image file) containing model + generation params.  
2. 0..N rows in `lora_usages` capturing each `<lora:...>` reference.  
3. Incremental behavior: unchanged metadata skipped in < O(1) time via hash comparison.

Non‑goals (v2.0): ComfyUI full graph, remote CivitAI enrichment, training `.safetensors` metadata ingestion.

---
## 2. Metadata Source Resolution (in order)
1. PNG tEXt/iTXt/zTXt keys (case-insensitive): `parameters`, `Description`, `Comment`, `Software` (choose first non-empty).  
2. WebP: XMP / EXIF description fields.  
3. JPEG: EXIF UserComment, then XMP dc:description.  
4. Sidecar `<basename>.json` only if no embedded block or config `allow_sidecar_merge=True`.  
Fail → mark file `no_metadata` (skip model/LoRA extraction).

---
## 3. Canonical Text Structure (Automatic1111)
```
[positive prompt possibly multi-line]
Negative prompt: [negative prompt possibly multi-line]
Steps: 30, Sampler: Euler a, CFG scale: 7, Seed: 1234, Size: 512x768, Model: modelName_vX, Clip skip: 2
```
Detection:
- Find first line matching `^Negative prompt:` (case-insensitive). If none → entire text = positive; negative = ''.
- Parameter segment: first subsequent line (or concatenated wrapped lines) containing ≥3 `Key: Value` pairs separated by comma+space.

---
## 4. Field Set (Normalized)
Core model row fields (NULL if unavailable):
model_name, model_hash_short, model_hash_full (optional future), steps, sampler, scheduler, cfg_scale, seed, subseed, subseed_strength, clip_skip, denoising_strength, width, height, size_raw, hires_upscaler, hires_steps, hires_denoising, vae, vae_hash, refiner_model, refiner_switch_at, tiling (0/1), face_restoration, version_tag, generation_timestamp, raw_positive, raw_negative, clean_positive, clean_negative, metadata_hash.

LoRA usage row fields: file_id, lora_name, weight, context (positive|negative), position_index.

Derivable extras (optional now, easy later): base_model, postprocessors.

---
## 5. Parsing Algorithm (Per File)
Pseudocode (succinct):
```
raw = extract_raw_metadata(file)
if not raw: mark skip; return
raw_hash = sha256(raw)
if files.last_extracted_hash == raw_hash: fast-skip (optional pre-parse optimization)
lines = normalize_newlines(raw).split('\n')
neg_idx = first index where line matches /^Negative prompt:/i (or -1)
pos_lines = lines[:neg_idx] if neg_idx>=0 else lines
rest = lines[neg_idx:] if neg_idx>=0 else []
negative_block, param_text = split_rest_into_negative_and_param(rest)
kv_tokens = tokenize_param_text(param_text)
kv_map = parse_tokens(kv_tokens)  # normalize keys
pos_prompt = '\n'.join(pos_lines).strip()
neg_prompt = negative_block.strip()
lora_matches = extract_lora_usages(pos_prompt, 'positive') + extract_lora_usages(neg_prompt, 'negative')
clean_pos = remove_lora_tags(pos_prompt)
clean_neg = remove_lora_tags(neg_prompt)
model_record = build_model_record(kv_map, pos_prompt, neg_prompt, clean_pos, clean_neg)
canonical_json = canonicalize(model_record)
metadata_hash = sha256(canonical_json)
if files.last_extracted_hash == metadata_hash: skip DB writes (semantic no-op)
else: upsert model row + replace lora_usages for file
update files.last_extracted_hash = metadata_hash; last_extracted_at = now
```

Batch: process in chunks (e.g. 500) → accumulate rows → single executemany per table → commit.

---
## 6. Tokenization & Key Normalization
Parameter text split by `, ` (only comma+space). Accept tokens matching:
`^([A-Za-z0-9 _\-/]+?):\s*(.+)$`
Transform key: lowercase → spaces & hyphens → underscore → strip.  
Aliases map:
```
model|model_name -> model_name
hash|model_hash -> model_hash_short
clip skip|clip_skip|clip-skip -> clip_skip
cfg|cfg scale|cfg_scale -> cfg_scale
variation seed|variation_seed -> subseed
variation seed strength|variation_seed_strength -> subseed_strength
hires upscaler -> hires_upscaler
hires steps -> hires_steps
hires denoising strength|hires upscale strength -> hires_denoising
face restoration -> face_restoration
refiner switch at -> refiner_switch_at
```
Size handling:
- If token `Size: WxH` → width=int(W), height=int(H), size_raw=literal.
- If separate `width:` / `height:` appear, override size_raw accordingly.

Numeric parsing: int for `^[0-9]+$`, float for `^[0-9]+\.[0-9]+$`.  
Booleans: `true/false` → 1/0 (tiling).  
Sampler / scheduler left as strings (trim).

---
## 7. LoRA Extraction
Patterns (priority):
1. `<lora:NAME:WEIGHT>` → regex `<lora:([^>:]+):([0-9]*\.?[0-9]+)>`
2. `<lora:NAME>` (weight=1.0)
3. `<lyco:NAME:WEIGHT>` treat as LoRA
4. Plain `lora:NAME:WEIGHT` word boundary `\blora:([^:\s]+):([0-9]*\.?[0-9]+)`
Normalize NAME: strip extensions (`.safetensors|.pt|.ckpt`), trim punctuation `.,;:` end, convert spaces -> underscore.  
Validate: 2 ≤ len(NAME) ≤ 120, weight 0.0–4.0 inclusive.
Order: capture sequentially for position_index.
Cleaning: Remove matched substrings verbatim, collapse multiple spaces → single space.

---
## 8. Canonical JSON & Hash
1. Assemble dict with only non-null scalar fields + always include raw/clean prompts (even if empty).  
2. Stable float formatting: `'{:.6g}'.format(x)` for any float.  
3. Serialize: `json.dumps(obj, ensure_ascii=False, separators=(',',':'), sort_keys=True)`.
4. metadata_hash = sha256(UTF-8 bytes).  
Reason: reordering or cosmetic whitespace in raw input yields same hash if semantics unchanged.

---
## 9. Database Schema (Minimal)
```
CREATE TABLE IF NOT EXISTS models (
  file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
  model_name TEXT,
  model_hash_short TEXT,
  model_hash_full TEXT,
  steps INTEGER, sampler TEXT, scheduler TEXT, cfg_scale REAL,
  seed INTEGER, subseed INTEGER, subseed_strength REAL,
  clip_skip INTEGER, denoising_strength REAL,
  width INTEGER, height INTEGER, size_raw TEXT,
  hires_upscaler TEXT, hires_steps INTEGER, hires_denoising REAL,
  vae TEXT, vae_hash TEXT,
  refiner_model TEXT, refiner_switch_at REAL,
  tiling INTEGER, face_restoration TEXT, version_tag TEXT,
  generation_timestamp DATETIME,
  raw_positive TEXT, raw_negative TEXT,
  clean_positive TEXT, clean_negative TEXT,
  metadata_hash TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_models_model_name ON models(model_name);
CREATE INDEX IF NOT EXISTS idx_models_model_hash_short ON models(model_hash_short);

CREATE TABLE IF NOT EXISTS lora_usages (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  lora_name TEXT NOT NULL,
  weight REAL NOT NULL,
  context TEXT CHECK(context IN ('positive','negative')),
  position_index INTEGER,
  UNIQUE(file_id, lora_name, context, position_index)
);
CREATE INDEX IF NOT EXISTS idx_lora_name ON lora_usages(lora_name);
```

---
## 10. Incremental Strategy
Preferred: Single `metadata_hash` only (drop raw_hash column).  
Skip parse if (a) `metadata_json` empty OR (b) `last_extracted_hash == sha256(raw_text)` fast-path (optional).  
After parsing compute semantic `metadata_hash`; if equals stored hash → no-op.  
Else: upsert model row, delete + reinsert lora_usages for file (cheaper than diff for small N), update `files.last_extracted_hash` & `last_extracted_at`.

---
## 11. Performance Practices
- Precompile regex patterns once per batch.  
- Use streaming sha256 for large prompt sources only if needed (prompt text is small; file hashing optional).  
- Batch DB writes (size ~500).  
- Use WAL + `busy_timeout` 30s.  
- Avoid per-row SELECT by pre-fetching `files (id, metadata_json, last_extracted_hash)`.

---
## 12. Error & Edge Handling
| Case | Action |
|------|--------|
| Empty / missing metadata | Skip model & lora insertion (leave existing rows? configurable) |
| Malformed tokens | Ignore token; continue |
| Duplicate keys | Last wins |
| Huge prompt (>64KB) | Truncate (store truncated flag if desired) |
| LoRA invalid weight/name | Discard that match |
| DB constraint violation | Log + continue batch |
| Unicode decode error | Use `errors='replace'` |

Security: never eval; regex only; parameterized SQL.

---
## 13. Core Regex Set
```
NEG_HEADER = re.compile(r'^Negative prompt:\s*(.*)$', re.I)
KV_TOKEN   = re.compile(r'^([A-Za-z0-9 _\-/]+?):\s*(.+)$')
SIZE_RX    = re.compile(r'^(\d{2,5})[xX](\d{2,5})$')
LORA_FULL  = re.compile(r'<lora:([^>:]+):([0-9]*\.?[0-9]+)>')
LORA_SHORT = re.compile(r'<lora:([^>:]+)>')
LORA_PLAIN = re.compile(r'\blora:([^:\s]+):([0-9]*\.?[0-9]+)')
LYCO_FULL  = re.compile(r'<lyco:([^>:]+):([0-9]*\.?[0-9]+)>')
```

---
## 14. Minimal Implementation Skeleton (Python-ish)
```python
def extract_all(conn):
    rows = conn.execute("SELECT id, metadata_json, last_extracted_hash FROM files WHERE metadata_json IS NOT NULL").fetchall()
    model_rows, lora_rows, updates = [], [], []
    for fid, meta_json, last_hash in rows:
        raw = meta_json or ''
        if not raw: continue
        # Optional fast skip on raw hash (disabled if not stored)
        parsed = parse_sd_block(raw)
        if not parsed: continue
        canonical_json = canonicalize(parsed)
        meta_hash = sha256(canonical_json.encode()).hexdigest()
        if last_hash == meta_hash: continue
        model_rows.append(build_model_row(fid, parsed, meta_hash))
        for idx, lr in enumerate(parsed['lora_list']):
            lora_rows.append((fid, lr['name'], lr['weight'], lr['context'], idx))
        updates.append((meta_hash, datetime.utcnow(), fid))
    if model_rows:
        executemany_upsert_models(conn, model_rows)
    if updates:
        conn.executemany("UPDATE files SET last_extracted_hash=?, last_extracted_at=? WHERE id=?", updates)
    # Replace loras (simple strategy)
    if lora_rows:
        conn.execute("DELETE FROM lora_usages WHERE file_id IN (SELECT id FROM files WHERE last_extracted_at = DATE('now'))")  # or collect fids
        conn.executemany("INSERT OR IGNORE INTO lora_usages (file_id,lora_name,weight,context,position_index) VALUES (?,?,?,?,?)", lora_rows)
    conn.commit()
```

---
## 15. Quick Test Matrix
| Scenario | Expect |
|----------|--------|
| Standard A1111 block | All keyed fields mapped |
| No negative prompt | negative='' |
| Multiple wrapped param lines | All tokens captured |
| 3 LoRA tags (mixed forms) | 3 rows, ordered |
| Re-run unchanged | 0 writes |
| Reordered tokens | Same metadata_hash |
| Missing Model: | model_name NULL |
| Size token absent + width/height present | Derived size_raw="WxH" |

---
## 16. Implementation Priority Order
1. Core parser (sections 3–7)  
2. Canonical JSON + hash (8)  
3. DB schema + upsert logic (9,10)  
4. Incremental skip (10)  
5. LoRA detection refinements (7)  
6. Edge/error hardening (12)  
7. Performance batching (11)  
8. Optional model file hashing (future extension)

---
## 17. Glossary (Slim)
Canonical JSON – Deterministic serialized form producing stable hash.  
Metadata hash – Semantic content hash after normalization.  
LoRA usage – Inline reference `<lora:name:weight>` indicating adapter applied.

---
END OF SPEC
The standalone LoRA metadata viewer provides practical patterns for handling large `.safetensors` model / LoRA files, hash variants, and dynamic enrichment. These lessons should refine and extend earlier spec sections.

### 26.1 Hash Strategy (AutoV2 vs AutoV3)
The viewer distinguishes multiple hash forms used by CivitAI:
- AutoV2 (full SHA256 of entire file) → truncated (first 10 hex) commonly surfaced.
- AutoV3 (SHA256 of tensor data excluding metadata header) → truncated (first 12 hex) for improved uniqueness when metadata mutates.

Implementation guidance:
1. When hashing a `.safetensors` LoRA/model file locally, compute both if feasible (size < threshold, e.g. 2GB) to maximize match rate with external registries.
2. Fallback order for remote lookup: user-provided hash (if any) → AutoV2 → AutoV3.
3. Persist which hash type produced a successful external match (`hash_type` enum: `provided`, `autov2`, `autov3`).
4. Store truncated display hashes separately from canonical full SHA256 to avoid ambiguity.

### 26.2 Large File Hashing Heuristic
For files larger than a threshold (viewer uses 2GB):
- Skip AutoV3 (since it needs parsing header and slicing) and instead stream chunked SHA256 (AutoV2) with a larger chunk size (e.g. 16MB) for efficiency.
- Report partial progress (optional) if user UX requires.

### 26.3 Metadata Section Offset (AutoV3 Computation)
AutoV3 logic (per viewer) reads:
- First 8 bytes: little-endian uint32 `n` (metadata JSON length or header component) and uses `offset = n + 8` to slice raw tensor region.
- Hash only the tensor data bytes after `offset`.

Spec Addendum:
- When implementing AutoV3, validate that `offset < file_size`; otherwise abort AutoV3 and log a warning.

### 26.4 Custom / Derived Fields Pipeline
Viewer evaluates user-defined expressions referencing:
- Raw file metadata (`fileMetadata`)
- External enrichment (`civitaiMetadata`)
- Previously computed custom fields (`customMetadata`)

Apply analogous extensibility:
- Provide ordered hook chain after core parse allowing deterministic creation of derived fields (prevent circular references by one-pass order).
- On error in derived field computation, either (a) omit field or (b) store error marker if debugging mode.

### 26.5 Field Naming Conventions (ss_* Prefix)
Viewer surfaces many `ss_` fields (e.g. `ss_clip_skip`, `ss_network_dim`, `ss_learning_rate`) typically originating from training metadata embedded in LoRA `.safetensors` JSON.
Guidance:
- When ingesting training metadata from `.safetensors` (distinct from inference image prompts), retain original keys but map a curated subset into normalized columns if you extend schema (future phase). For now, store raw training metadata JSON under a separate column (`training_meta_json`) to avoid polluting inference-focused `models` table.

### 26.6 LoRA Network Args Object
Keys like `ss_network_args` may themselves be nested structures (JSON object). Strategy:
- Parse nested JSON values if they are serialized strings; flatten selected keys (`conv_dim`, `conv_alpha`, `algo`, `dora_wd`) into derived fields when needed.

### 26.7 Error / Fallback Messaging
Viewer distinguishes between:
- Metadata parse success but hash failure (e.g. huge file) → user-facing message about size.
- Network / CORS error vs. 404 (no model match).

Extraction service should:
- Record `lookup_status` per attempted external enrichment: `skipped`, `matched`, `not_found`, `error_network`, `error_parse`.
- Optional table `model_enrichment (file_id, hash_type, lookup_status, enriched_at, source_url, payload_json)` for audit.

### 26.8 Progressive Enhancement Order
1. Local parse first (guaranteed fields).
2. Hash computation (may be deferred / async if expensive).
3. External registry lookup (optional, can be queued).
4. Derived custom fields (after all enrichment present).

### 26.9 Deterministic Truncation of Display Hashes
Viewer uses `.substring(0,10)` (AutoV2) and `.substring(0,12)` (AutoV3). For consistency:
- Adopt: `display_hash_length = 10` for any standard short hash; store original truncated length metadata if using multiple types to prevent mismatch.
- AutoV3 12-char form may remain for compatibility; persist `display_len` if mixed.

### 26.10 Safety: Avoid `eval` on Untrusted Expressions
Viewer uses `eval` for user-configured custom fields (trusted local context). Server-side implementation MUST NOT execute arbitrary expressions from untrusted sources. Provide a safe mini-expression interpreter OR restrict derived fields to a curated set of deterministic functions.

### 26.11 Integration Into Existing Spec
| Enhancement Area | Spec Section Reference | Added Guidance |
|------------------|------------------------|----------------|
| Dual hash strategy | §8 | Compute & store both AutoV2 + optional AutoV3; record `hash_type`. |
| Large file handling | §15 | Adaptive chunk size; skip AutoV3 for oversized files. |
| Enrichment staging | §16 | Add post-enrichment hook phase prior to canonical hash. |
| Derived custom fields | §16 | Ordered deterministic evaluation; safe execution environment. |
| Lookup audit | NEW | Introduce optional `model_enrichment` table. |
| Training metadata separation | §5 / future schema | Store training JSON separately (`training_meta_json`). |
| Nested network args flattening | §7 | Flatten selected keys into derived fields if present. |
| Display hash consistency | §8 | Standardize 10-char short hash; allow extended length metadata. |
| Error classification | §11 | Expand statuses for enrichment failures. |

### 26.12 Optional Table: model_enrichment
```
CREATE TABLE IF NOT EXISTS model_enrichment (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  hash_type TEXT,              -- provided | autov2 | autov3
  lookup_status TEXT,          -- skipped | matched | not_found | error_network | error_parse
  display_hash TEXT,
  source_url TEXT,
  enriched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  payload_json TEXT            -- raw JSON from external API (if matched)
);
CREATE INDEX IF NOT EXISTS idx_model_enrichment_file ON model_enrichment(file_id);
```

### 26.13 Update to Incremental Logic (Hash Timing)
- Permit deferring remote enrichment to a separate queue; initial extraction writes core `models` row with `enrichment_pending=1` (boolean column) if remote lookup not yet attempted.
- A subsequent enrichment worker resolves remaining fields, updates row, sets `enrichment_pending=0`.

### 26.14 Pseudocode Delta (Hash + Enrichment)
```python
# After core metadata parse but before final canonical hash (if enrichment affects fields you hash):
if need_checkpoint_hash and model_path:
    auto_v2_full = sha256_file(model_path)
    short_hash = auto_v2_full[:10]
    model_record['model_hash_full'] = auto_v2_full
    model_record['model_hash_short'] = short_hash

if enable_auto_v3 and file_is_safetensors and file_size < AUTO_V3_MAX:
    auto_v3_hash = compute_auto_v3(model_path)
    model_record['model_hash_auto_v3'] = auto_v3_hash

if perform_remote_lookup:
    enrichment = lookup_registry(model_record)
    store_model_enrichment(file_id, enrichment)
    merge_enrichment(model_record, enrichment)

canonical_json = canonicalize(model_record)  # after merge if enrichment alters canonical fields
```

---
