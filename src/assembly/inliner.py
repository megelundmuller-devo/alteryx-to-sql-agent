"""Fragment inliner — collapses a chunk's SQL fragments into a single SELECT statement.

For each chunk's linear fragment chain, three rules are applied in order:

1. Passthrough elimination  — a fragment that is purely ``SELECT * FROM [ref]``
   (no WHERE / ORDER BY / GROUP BY / window functions) is replaced by substituting
   its FROM reference into every other fragment that references it, then dropping it.
   Never eliminates the chunk's primary output (chunk_output_name).

2. WHERE hoisting  — when the outermost unprocessed fragment is
   ``SELECT * FROM [prev] WHERE cond`` with no aggregation or ordering, the WHERE
   is hoisted into the previous fragment and the outer fragment is removed.

3. Subquery nesting  — for any remaining consecutive pair that cannot be merged
   by the above rules, the inner fragment is wrapped as a parenthesised subquery
   inside the outer's FROM clause.

Fragments without a chunk_id (e.g. test fixtures) are never touched.
Chains that contain a stub fragment are not collapsed.
Secondaries (filter_false, join _L/_R anti-joins) are kept as separate temp tables;
if they reference a chain-intermediate the chain is not collapsed beyond passthrough
elimination to avoid referencing a temp table that no longer exists.
"""

from __future__ import annotations

import re

from parsing.models import CTEFragment

# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
_WINDOW_RE = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_ORDER_BY_RE = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)

# Matches a pure passthrough: SELECT * FROM <anything> with nothing else
_PASSTHROUGH_RE = re.compile(
    r"^SELECT\s+\*\s*\nFROM\s+(\S[^\n]*?)\s*$",
    re.IGNORECASE | re.DOTALL,
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _no_comments(sql: str) -> str:
    """Remove SQL line comments and return stripped body (for analysis only)."""
    return re.sub(r"--[^\n]*", "", sql).strip()


def _is_passthrough(body: str) -> bool:
    """True if body is SELECT * FROM <ref> with no other clauses."""
    if _GROUP_BY_RE.search(body) or _WINDOW_RE.search(body):
        return False
    if _WHERE_RE.search(body) or _ORDER_BY_RE.search(body):
        return False
    return bool(_PASSTHROUGH_RE.match(body))


def _get_from_ref(body: str) -> str:
    """Return the FROM reference string from a passthrough body."""
    m = _PASSTHROUGH_RE.match(body)
    return m.group(1).strip() if m else ""


def _has_agg_stop(body: str) -> bool:
    """True if body contains constructs that prevent WHERE hoisting."""
    return bool(
        _GROUP_BY_RE.search(body)
        or _WINDOW_RE.search(body)
        or _ORDER_BY_RE.search(body)
    )


def _is_where_filter(body: str, prev_name: str) -> bool:
    """True if body is exactly SELECT * FROM [prev_name] WHERE <cond>."""
    if _GROUP_BY_RE.search(body) or _WINDOW_RE.search(body) or _ORDER_BY_RE.search(body):
        return False
    pattern = re.compile(
        r"^SELECT\s+\*\s*\nFROM\s+\["
        + re.escape(prev_name)
        + r"\]\s*\nWHERE\s+(.+?)\s*$",
        re.IGNORECASE | re.DOTALL,
    )
    return bool(pattern.match(body))


def _get_where(body: str) -> str:
    """Extract the WHERE condition string from a WHERE-filter body."""
    m = re.search(r"\bWHERE\s+(.+?)\s*$", body, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else ""


def _append_where(sql: str, cond: str) -> str:
    """Add WHERE/AND clause to sql."""
    if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        return sql.rstrip() + f"\nAND ({cond})"
    return sql.rstrip() + f"\nWHERE {cond}"


def _sub_ref(sql: str, old_name: str, new_ref: str) -> str:
    """Replace all [old_name] occurrences with new_ref."""
    return sql.replace(f"[{old_name}]", new_ref)


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.splitlines())


# ---------------------------------------------------------------------------
# Chain reconstruction
# ---------------------------------------------------------------------------


def _find_from_dep(sql: str, candidates: set[str]) -> str | None:
    """Return the first FROM [name] in sql where name is in candidates."""
    for m in re.finditer(r"\bFROM\s+\[([^\]]+)\]", sql, re.IGNORECASE):
        if m.group(1) in candidates:
            return m.group(1)
    return None


def _build_chain(
    frags: list[CTEFragment], output_name: str
) -> tuple[list[CTEFragment], list[CTEFragment]]:
    """Walk dependency chain backwards from primary to determine chain and secondaries."""
    by_name = {f.name: f for f in frags}
    if output_name not in by_name:
        return list(frags), []

    chain_names: list[str] = []
    current: str | None = output_name
    all_names = set(by_name.keys())
    visited: set[str] = set()

    while current and current not in visited:
        visited.add(current)
        chain_names.append(current)
        frag = by_name.get(current)
        if frag is None:
            break
        current = _find_from_dep(frag.sql, all_names - {current})

    chain_names.reverse()
    chain = [by_name[n] for n in chain_names if n in by_name]
    chain_set = {f.name for f in chain}
    secondaries = [f for f in frags if f.name not in chain_set]
    return chain, secondaries


# ---------------------------------------------------------------------------
# Phase 1: passthrough elimination
# ---------------------------------------------------------------------------


def _eliminate_passthroughs(
    frags: list[CTEFragment], output_name: str
) -> list[CTEFragment]:
    """Remove pure SELECT * FROM [ref] fragments by substituting their ref everywhere."""
    frags = list(frags)
    changed = True
    while changed:
        changed = False
        for i, frag in enumerate(frags):
            if frag.name == output_name:
                continue  # never eliminate the primary output
            body = _no_comments(frag.sql)
            if not _is_passthrough(body):
                continue
            from_ref = _get_from_ref(body)
            if not from_ref:
                continue
            # Substitute [this_name] → from_ref in all other fragments
            frags = [
                (f.model_copy(update={"sql": _sub_ref(f.sql, frag.name, from_ref)})
                 if f.name != frag.name else f)
                for f in frags
            ]
            frags = [f for f in frags if f.name != frag.name]
            changed = True
            break
    return frags


# ---------------------------------------------------------------------------
# Phase 2: chain collapse (WHERE hoisting + subquery nesting)
# ---------------------------------------------------------------------------


def _collapse_chain(chain: list[CTEFragment]) -> CTEFragment:
    """Collapse a linear chain into one fragment via WHERE hoisting and subquery nesting."""
    if len(chain) == 1:
        return chain[0]

    current_sql = chain[0].sql
    current_name = chain[0].name

    for step, frag in enumerate(chain[1:], start=1):
        current_body = _no_comments(current_sql)
        outer_body = _no_comments(frag.sql)

        if _is_where_filter(outer_body, current_name) and not _has_agg_stop(current_body):
            # Hoist the WHERE into the current SQL
            current_sql = _append_where(current_sql, _get_where(outer_body))
        else:
            # Subquery nest
            indented = _indent(current_sql.strip(), "    ")
            current_sql = _sub_ref(frag.sql, current_name, f"(\n{indented}\n) AS _s{step}")

        current_name = frag.name

    return CTEFragment(
        name=chain[-1].name,
        sql=current_sql,
        source_tool_ids=[tid for f in chain for tid in f.source_tool_ids],
        is_stub=any(f.is_stub for f in chain),
        chunk_id=chain[-1].chunk_id,
        chunk_output_name=chain[-1].chunk_output_name,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _collapse_group(frags: list[CTEFragment]) -> list[CTEFragment]:
    """Collapse one chunk's fragment list into as few statements as possible."""
    if len(frags) <= 1:
        return list(frags)
    if any(f.is_stub for f in frags):
        return list(frags)

    output_name = frags[0].chunk_output_name
    if not output_name:
        return list(frags)

    # Phase 1: eliminate passthrough fragments (safe for both chain and secondaries)
    frags = _eliminate_passthroughs(frags, output_name)
    if len(frags) <= 1:
        return list(frags)

    chain, secondaries = _build_chain(frags, output_name)
    if len(chain) <= 1:
        return list(chain) + list(secondaries)

    # If secondaries reference chain intermediates, collapsing would drop temp tables
    # they depend on — skip phase 2 in that case.
    intermediate_names = {f.name for f in chain[:-1]}
    if any(
        f"[{name}]" in sec.sql
        for sec in secondaries
        for name in intermediate_names
    ):
        return list(chain) + list(secondaries)

    # Phase 2: full collapse
    collapsed = _collapse_chain(chain)
    return [collapsed] + list(secondaries)


def collapse_fragments(fragments: list[CTEFragment]) -> list[CTEFragment]:
    """Collapse each chunk's fragment chain into a single SELECT statement.

    Fragments without a chunk_id are passed through unchanged.
    """
    if not fragments:
        return fragments

    result: list[CTEFragment] = []
    current_id: int | None = None
    current_group: list[CTEFragment] = []

    for frag in fragments:
        if frag.chunk_id is None:
            if current_group:
                result.extend(_collapse_group(current_group))
                current_group = []
                current_id = None
            result.append(frag)
        elif frag.chunk_id != current_id:
            if current_group:
                result.extend(_collapse_group(current_group))
            current_group = [frag]
            current_id = frag.chunk_id
        else:
            current_group.append(frag)

    if current_group:
        result.extend(_collapse_group(current_group))

    return result
