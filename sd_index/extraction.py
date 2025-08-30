"""Extraction of Stable Diffusion generation parameters & LoRA usage.

Phase 1 scope: derive everything only from embedded prompt / metadata JSON
already stored in `files.metadata_json` (no external network calls).

Populates / updates:
  - models (1:1 with files)
  - lora_usages (N:1 per file)

Skips work if `files.last_extracted_hash` matches recomputed canonical hash.
Can be invoked standalone via `extract_models()` or runs automatically at the
end of `index_files` unless `SD_DISABLE_EXTRACTION=1`.
"""
from __future__ import annotations

import json, re, sqlite3, hashlib, time
from typing import Any, Dict, List, Optional, Tuple

# --- Regex patterns -------------------------------------------------------
NEG_HEADER = re.compile(r'^Negative prompt:\s*(.*)$', re.I)
KV_TOKEN = re.compile(r'^([A-Za-z0-9 _\-/]+?):\s*(.+)$')
SIZE_RX = re.compile(r'^(\d{2,5})[xX](\d{2,5})$')
LORA_FULL = re.compile(r'<lora:([^>:]+):([0-9]*\.?[0-9]+)>', re.IGNORECASE)
LORA_SHORT = re.compile(r'<lora:([^>:]+)>', re.IGNORECASE)
LORA_PLAIN = re.compile(r'\blora:([^:\s]+):([0-9]*\.?[0-9]+)', re.IGNORECASE)
LYCO_FULL = re.compile(r'<lyco:([^>:]+):([0-9]*\.?[0-9]+)>', re.IGNORECASE)
PAREN_TAG = re.compile(r'\((lora|lyco):([^():]+):([0-9]*\.?[0-9]+)\)', re.IGNORECASE)

_ALIAS = {
    'model': 'model_name', 'model_name': 'model_name',
    'hash': 'model_hash_short', 'model_hash': 'model_hash_short',
    'clip skip': 'clip_skip', 'clip_skip': 'clip_skip', 'clip-skip': 'clip_skip',
    'cfg': 'cfg_scale', 'cfg scale': 'cfg_scale', 'cfg_scale': 'cfg_scale',
    'variation seed': 'subseed', 'variation_seed': 'subseed',
    'variation seed strength': 'subseed_strength', 'variation_seed_strength': 'subseed_strength',
    'hires upscaler': 'hires_upscaler', 'hires steps': 'hires_steps',
    'hires denoising strength': 'hires_denoising', 'hires upscale strength': 'hires_denoising',
    'face restoration': 'face_restoration', 'refiner switch at': 'refiner_switch_at'
}

FIELD_NUMERIC_INT = {'steps','seed','subseed','clip_skip','width','height','hires_steps'}
FIELD_NUMERIC_FLOAT = {'cfg_scale','subseed_strength','denoising_strength','hires_denoising','refiner_switch_at'}


def _sha256(data: bytes) -> str:
    h = hashlib.sha256(); h.update(data); return h.hexdigest()


def _find_candidate_prompt_string(obj: Any) -> Optional[str]:
    """Find a plausible raw Automatic1111 parameter block inside a metadata JSON object."""
    candidates: List[str] = []
    def walk(o: Any):
        if isinstance(o, str):
            t = o.strip()
            if 'Steps:' in t and ('Sampler:' in t or t.count(':') >= 5):
                candidates.append(t)
        elif isinstance(o, list):
            for i in o: walk(i)
        elif isinstance(o, dict):
            for v in o.values(): walk(v)
    walk(obj)
    return candidates[0] if candidates else None


def _split_prompt_sections(raw: str) -> Tuple[str,str,str]:
    lines = raw.replace('\r','').split('\n')
    neg_idx = -1
    for i,l in enumerate(lines):
        if NEG_HEADER.match(l): neg_idx = i; break
    if neg_idx == -1:
        pos_lines = lines; rest_lines: List[str] = []
    else:
        pos_lines = lines[:neg_idx]; rest_lines = lines[neg_idx:]
    negative_block = ''
    params_segment = ''
    if rest_lines:
        neg_acc = []
        first = rest_lines[0]
        m = NEG_HEADER.match(first)
        if m: neg_acc.append(m.group(1))
        i = 1
        while i < len(rest_lines):
            l = rest_lines[i]
            if l.strip()=='' : i += 1; break
            if l.count(':') >= 3 and ', ' in l: break
            neg_acc.append(l); i += 1
        negative_block = '\n'.join(neg_acc)
        param_lines = []
        while i < len(rest_lines):
            l = rest_lines[i]
            param_lines.append(l)
            if l.count(':') >= 3 and not l.rstrip().endswith(','): break
            i += 1
        params_segment = ' '.join(param_lines).strip()
    positive_block = '\n'.join(pos_lines).strip()
    return positive_block, negative_block.strip(), params_segment


def _parse_param_segment(seg: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not seg: return result
    tokens = [t.strip() for t in seg.split(',') if t.strip()]
    for tok in tokens:
        m = KV_TOKEN.match(tok)
        if not m: continue
        key_raw, value_raw = m.group(1).strip(), m.group(2).strip()
        key_norm = key_raw.lower().replace('-', ' ').replace('__',' ').strip()
        key_norm = re.sub(r'\s+', ' ', key_norm)
        key_norm = key_norm.replace(' ', '_')
        key = _ALIAS.get(key_norm, key_norm)
        v: Any = value_raw
        if key in FIELD_NUMERIC_INT and re.fullmatch(r'\d+', value_raw): v = int(value_raw)
        elif key in FIELD_NUMERIC_FLOAT and re.fullmatch(r'\d+\.?\d*', value_raw): v = float(value_raw)
        elif key in ('tiling',): v = 1 if value_raw.lower() in ('true','1','yes') else 0
        result[key] = v
        if key == 'size' and isinstance(v, str):
            sm = SIZE_RX.match(v)
            if sm:
                result['width'] = int(sm.group(1)); result['height'] = int(sm.group(2)); result['size_raw'] = v
    if 'width' in result and 'height' in result and 'size_raw' not in result:
        result['size_raw'] = f"{result['width']}x{result['height']}"
    return result


def _extract_loras(text: str, context: str) -> Tuple[List[Dict[str,Any]], str]:
    loras: List[Dict[str, Any]] = []
    def _norm(name: str) -> str:
        name = re.sub(r'\.(safetensors|pt|ckpt)$','',name, flags=re.I)
        return name.strip().replace(' ','_')
    def add(name, weight, ctx):
        try: w = float(weight)
        except Exception: w = 1.0
        loras.append({'name': _norm(name), 'weight': w, 'context': ctx})
    # Full / lyco / parenthetical tags
    for rx in (LORA_FULL, LYCO_FULL, PAREN_TAG):
        for m in rx.finditer(text):
            if rx is PAREN_TAG:
                add(m.group(2), m.group(3), context)
            else:
                add(m.group(1), m.group(2), context)
    for m in LORA_SHORT.finditer(text): add(m.group(1), 1.0, context)
    for m in LORA_PLAIN.finditer(text): add(m.group(1), m.group(2), context)
    cleaned = re.sub(r'</?(lora|lyco):[^>]+>', '', text, flags=re.IGNORECASE)
    cleaned = re.sub(r'<lora:[^>]+>', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<lyco:[^>]+>', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\blora:[^\s]+:[0-9]*\.?[0-9]+', '', cleaned, flags=re.IGNORECASE)
    cleaned = PAREN_TAG.sub('', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return loras, cleaned


def _canonical_hash(model_dict: Dict[str, Any], lora_list: List[Dict[str,Any]]) -> str:
    obj = {k:v for k,v in model_dict.items() if v is not None}
    obj['lora_list'] = lora_list
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',',':'))
    return _sha256(blob.encode('utf-8'))


def parse_metadata_block(raw_text: str) -> Optional[Dict[str, Any]]:
    if not raw_text or raw_text.strip()=='' : return None
    pos, neg, param_seg = _split_prompt_sections(raw_text)
    kv = _parse_param_segment(param_seg)
    loras_p, clean_pos = _extract_loras(pos, 'positive')
    loras_n, clean_neg = _extract_loras(neg, 'negative')
    lora_list: List[Dict[str, Any]] = []
    for idx, item in enumerate(loras_p + loras_n):
        item['position_index'] = idx; lora_list.append(item)
    record = {
        'model_name': kv.get('model_name'),
        'model_hash_short': kv.get('model_hash_short'),
        'vae': kv.get('vae'),
        'vae_hash': kv.get('vae_hash'),
        'refiner_model': kv.get('refiner_model'),
        'refiner_switch_at': kv.get('refiner_switch_at'),
        'steps': kv.get('steps'),
        'sampler': kv.get('sampler'),
        'scheduler': kv.get('scheduler'),
        'cfg_scale': kv.get('cfg_scale'),
        'seed': kv.get('seed'),
        'subseed': kv.get('subseed'),
        'subseed_strength': kv.get('subseed_strength'),
        'clip_skip': kv.get('clip_skip'),
        'denoising_strength': kv.get('denoising_strength'),
        'tiling': kv.get('tiling'),
        'face_restoration': kv.get('face_restoration'),
        'width': kv.get('width'),
        'height': kv.get('height'),
        'size_raw': kv.get('size_raw'),
        'hires_upscaler': kv.get('hires_upscaler'),
        'hires_steps': kv.get('hires_steps'),
        'hires_denoising': kv.get('hires_denoising'),
        'raw_positive': pos,
        'raw_negative': neg,
        'clean_positive': clean_pos,
        'clean_negative': clean_neg,
        'lora_count': len(lora_list),
    }
    meta_hash = _canonical_hash(record, lora_list)
    record['metadata_hash'] = meta_hash
    record['lora_list'] = lora_list
    return record


def _derive_raw_text(metadata_json: str) -> Optional[str]:
    if not metadata_json: return None
    text = metadata_json.strip()
    if not text: return None
    if text.startswith('{') and text.endswith('}'):
        try: obj = json.loads(text)
        except Exception: return text
        cand = _find_candidate_prompt_string(obj)
        return cand or text
    return text


def extract_models(conn: sqlite3.Connection, limit: Optional[int] = None) -> Dict[str, Any]:
    """Extract prompt/model metadata for rows in `files`.

    Returns summary dict (processed/new/updated/skipped).
    """
    cur = conn.cursor()
    q = "SELECT id, metadata_json, last_extracted_hash FROM files"
    if limit: q += f" LIMIT {int(limit)}"
    rows = cur.execute(q).fetchall()
    new_models = updated = skipped = 0
    start_ts = time.time()
    for fid, metadata_json, last_hash in rows:
        raw_text = _derive_raw_text(metadata_json or '')
        if not raw_text:
            cur.execute("UPDATE files SET no_metadata=1 WHERE id=?", (fid,)); skipped += 1; continue
        parsed = parse_metadata_block(raw_text)
        if not parsed:
            cur.execute("UPDATE files SET no_metadata=1 WHERE id=?", (fid,)); skipped += 1; continue
        meta_hash = parsed['metadata_hash']
        if last_hash and last_hash == meta_hash:
            skipped += 1; continue
        model_cols = [
            'file_id','model_name','model_hash_short','vae','vae_hash','refiner_model','refiner_switch_at','model_hash_full','model_hash_auto_v3','hash_type','display_hash','steps','sampler','scheduler','cfg_scale','seed','subseed','subseed_strength','clip_skip','denoising_strength','tiling','face_restoration','width','height','size_raw','hires_upscaler','hires_steps','hires_denoising','raw_positive','raw_negative','clean_positive','clean_negative','lora_count','metadata_hash','extraction_time_ms'
        ]
        values = [
            fid, parsed.get('model_name'), parsed.get('model_hash_short'), parsed.get('vae'), parsed.get('vae_hash'), parsed.get('refiner_model'), parsed.get('refiner_switch_at'), None, None, None, None, parsed.get('steps'), parsed.get('sampler'), parsed.get('scheduler'), parsed.get('cfg_scale'), parsed.get('seed'), parsed.get('subseed'), parsed.get('subseed_strength'), parsed.get('clip_skip'), parsed.get('denoising_strength'), parsed.get('tiling'), parsed.get('face_restoration'), parsed.get('width'), parsed.get('height'), parsed.get('size_raw'), parsed.get('hires_upscaler'), parsed.get('hires_steps'), parsed.get('hires_denoising'), parsed.get('raw_positive'), parsed.get('raw_negative'), parsed.get('clean_positive'), parsed.get('clean_negative'), parsed.get('lora_count'), parsed.get('metadata_hash'), int((time.time()-start_ts)*1000)
        ]
        placeholders = ','.join(['?']*len(values))
        update_clause = ','.join([f"{c}=excluded.{c}" for c in model_cols[1:]])
        cur.execute(f"INSERT INTO models ({','.join(model_cols)}) VALUES ({placeholders}) ON CONFLICT(file_id) DO UPDATE SET {update_clause}", values)
        cur.execute("DELETE FROM lora_usages WHERE file_id=?", (fid,))
        if parsed['lora_list']:
            cur.executemany(
                "INSERT OR IGNORE INTO lora_usages (file_id,lora_name,weight,context,position_index) VALUES (?,?,?,?,?)",
                [(fid, l['name'], l['weight'], l['context'], l['position_index']) for l in parsed['lora_list']]
            )
        cur.execute("UPDATE files SET last_extracted_hash=?, last_extracted_at=CURRENT_TIMESTAMP, has_lora=?, extraction_version=1, no_metadata=0 WHERE id=?", (meta_hash, 1 if parsed['lora_count'] else 0, fid))
        if last_hash: updated += 1
        else: new_models += 1
    conn.commit()
    return {'processed': len(rows), 'new': new_models, 'updated': updated, 'skipped': skipped}


__all__ = ['extract_models', 'parse_metadata_block']
