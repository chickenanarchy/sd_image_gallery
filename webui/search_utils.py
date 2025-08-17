"""Search query building utilities.

Centralizes construction of WHERE clauses for metadata searches so logic
is consistent across endpoints. Supports optional FTS5 usage and special
syntaxes:
    - LEN<op><number> : compares LENGTH(metadata_json)
    - {}              : metadata_json = '{}'
If FTS is enabled (files_fts virtual table present) then queries search across
metadata_json and file path columns (path, path_norm). Basic terms are wrapped
in quotes for phrase search unless they already look like an advanced FTS
expression (boolean ops, parentheses, NEAR, wildcard, quotes).

NOTE: The UI currently presents operator choices AND / OR / NOT where NOT
is treated as a unary modifier logically equivalent to AND NOT <clause>.
To avoid misunderstanding we expose NOT as AND NOT here. (Future work:
support full precedence & grouping.)
"""
from __future__ import annotations

from typing import List, Tuple, Sequence
import re

LEN_PATTERN = re.compile(r"^LEN\s*([<>]=?|==?)\s*(\d+)\s*$", re.IGNORECASE)
BOOL_TOKEN_RE = re.compile(r"\b(AND|OR|NOT|NEAR/\d+)\b", re.IGNORECASE)

ALLOWED_LOGICS = {"AND", "OR", "NOT"}

class SearchBuildError(ValueError):
    pass

def _build_single_clause(term: str, has_fts: bool) -> Tuple[str, List]:
    term = term.strip()
    if not term:
        raise SearchBuildError("Empty term")
    m = LEN_PATTERN.match(term)
    if m:
        op, num = m.group(1), int(m.group(2))
        if op == '==':
            op = '='
        if op not in {"<", "<=", ">", ">=", "=", "=="}:  # redundant safety
            raise SearchBuildError("Invalid LEN operator")
        return f"LENGTH(metadata_json) {op} ?", [num]
    if term == '{}':
        return "metadata_json = ?", ['{}']
    if has_fts:
        advanced = bool(BOOL_TOKEN_RE.search(term) or any(ch in term for ch in ['*', '"', '(', ')']))
        fts_query = term if advanced else f'"{term}"'
        # files_fts MATCH searches across all FTS columns: metadata_json, path, path_norm
        return "rowid IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)", [fts_query]
    # Fallback LIKE across metadata and file_path
    return "(metadata_json LIKE ? OR file_path LIKE ?)", [f"%{term}%", f"%{term}%"]

def build_where(search: str, logics: Sequence[str], values: Sequence[str], has_fts: bool) -> Tuple[str, List]:
    """Return (where_sql, params) WITHOUT the leading 'WHERE'.

    logics[i] corresponds to values[i]; a missing logic defaults to AND.
    A logic of NOT is treated as AND NOT.
    """
    clauses: List[Tuple[str, List]] = []  # list of (sql, params)
    clause_ops: List[str] = []  # preceding operator for each clause after first

    def add_clause(op: str | None, term: str):
        sql, params = _build_single_clause(term, has_fts)
        clauses.append((sql, params))
        if op:
            clause_ops.append(op)

    if search:
        add_clause(None, search)

    for i, value in enumerate(values):
        if not value.strip():
            continue
        logic = 'AND'
        if i < len(logics):
            cand = logics[i].upper()
            if cand in ALLOWED_LOGICS:
                logic = cand
        add_clause(logic, value)

    if not clauses:
        return "", []

    # Construct SQL
    where_parts = []
    params: List = []
    for idx, (sql, p) in enumerate(clauses):
        if idx == 0:
            where_parts.append(f"({sql})")
        else:
            op = clause_ops[idx - 1]
            if op == 'NOT':
                where_parts.append(f"AND NOT ({sql})")  # unary NOT as AND NOT
            else:
                where_parts.append(f"{op} ({sql})")
        params.extend(p)
    return ' '.join(where_parts), params
