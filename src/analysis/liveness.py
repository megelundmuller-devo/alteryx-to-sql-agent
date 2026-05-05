"""Column liveness analysis — post-translation SELECT-widening pass.

After all CTEs are translated (Phase 3), this module:

1. Extracts ``[ColumnName]`` references from each CTE's SQL.
2. Compares them against the inferred input schemas in ``ctx.cte_schema``.
3. For any column that is referenced but absent from the input schema:
   a. Walks backwards through ``ctx.cte_inputs`` to find the nearest upstream
      SELECT CTE that *dropped* the column (i.e. the column existed in that
      SELECT's own input but was not forwarded).
   b. Injects the missing column into that SELECT's SQL so it flows forward.
   c. Recursively looks *through* ``SELECT *`` CTEs (filter, sort, sample, etc.)
      that pass all columns unchanged — so a drop buried two or three hops back
      is still found and fixed.
4. Updates ``ctx.cte_schema`` to reflect the widened projection.
5. Returns the (possibly modified) fragment list and a list of warning strings
   for gaps that could not be fixed deterministically (passed to the LLM pass).

Only SELECT-shaped CTEs are widened.  The heuristic is:
  • The SQL does NOT contain ``SELECT *`` (pass-through / stub forms already
    expose all columns).
  • The SQL ends with ``FROM [some_cte]`` (single-input transform).
  • The column is provably available in that upstream CTE's schema.

Entry point
-----------
    from analysis.liveness import run_liveness_pass

    fragments, gap_warnings = run_liveness_pass(fragments, ctx)
"""

from __future__ import annotations

import re

from parsing.models import CTEFragment, FieldSchema
from translators.context import TranslationContext

# Matches any [Identifier] — column refs AND bracketed CTE names.
_BRACKET_RE = re.compile(r"\[([^\]]+)\]")

# Column names that are internal to our generated SQL and should be ignored.
_INTERNAL_COLS = {"_rn", "_stub", "_macro_stub", "_sort_order", "_deduped"}

# Strips "AS [alias]" so that alias names on the right side of a rename are not
# mistakenly treated as upstream column references.  Applied before bracket scanning.
_AS_ALIAS_RE = re.compile(r"\bAS\s+\[[^\]]+\]", re.IGNORECASE)

# Strips -- single-line comments so bracketed table names in comment text (e.g.
# "-- Output destination: [target_table]") are not treated as column references.
_COMMENT_RE = re.compile(r"--[^\n]*")


def _extract_col_refs(sql: str, known_ctes: set[str]) -> set[str]:
    """Return column names referenced in *sql*, excluding CTE names and internals.

    Comments are stripped first, then ``AS [alias]`` pairs, so that neither
    comment text nor alias names on the right-hand side of a rename are counted
    as missing upstream column references.
    """
    sql_no_comments = _COMMENT_RE.sub("", sql)
    sql_no_aliases = _AS_ALIAS_RE.sub("", sql_no_comments)
    refs: set[str] = set()
    for m in _BRACKET_RE.finditer(sql_no_aliases):
        name = m.group(1)
        if not name:
            continue
        if name in _INTERNAL_COLS:
            continue
        if name in known_ctes:
            continue
        refs.add(name)
    return refs


def _has_select_star(sql: str) -> bool:
    """Return True when the CTE uses SELECT * (already exposes all columns)."""
    return bool(re.search(r"\bSELECT\s+\*", sql, re.IGNORECASE))


def _has_group_by(sql: str) -> bool:
    """Return True when the CTE aggregates via GROUP BY.

    GROUP BY CTEs have a schema determined entirely by their configured aggregate
    expressions.  Injecting additional columns would produce invalid T-SQL (the
    column would be neither grouped nor aggregated), so the liveness pass treats
    them as opaque boundaries.
    """
    return bool(re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE))


def _widen_select_sql(sql: str, cols_to_add: list[str]) -> str:
    """Inject *cols_to_add* into a deterministic SELECT CTE body.

    The SELECT CTEs produced by our translators have the form::

        SELECT
            [col1],
            [col2] AS [alias]
        FROM [upstream]

    or the single-line form::

        SELECT *
        FROM [upstream]

    We inject before the ``\\nFROM [`` boundary.  If that boundary is not
    found the SQL is returned unchanged (guard for exotic / LLM-generated SQL).
    """
    boundary = re.search(r"\nFROM\s+\[", sql, re.IGNORECASE)
    if not boundary:
        return sql
    pos = boundary.start()
    additions = ",\n" + ",\n".join(f"    [{c}]" for c in sorted(cols_to_add))
    return sql[:pos] + additions + sql[pos:]


def _find_and_widen(
    col: str,
    cte_name: str,
    frag_map: dict[str, CTEFragment],
    ctx: TranslationContext,
    known_ctes: set[str],
    visited: set[str],
) -> bool:
    """Recursively search upstream to find where *col* was dropped and widen that SELECT.

    Walks backwards through the CTE chain.  SELECT * CTEs (filter, sort, sample,
    etc.) are looked *through* rather than stopped at — they pass all columns
    unchanged, so a column dropped further back still flows forward once it is
    added to the non-SELECT-star CTE that originally dropped it.

    Returns True if the column was successfully injected into some upstream SELECT
    and ``ctx.cte_schema`` + ``frag_map`` were updated accordingly.
    """
    if cte_name in visited:
        return False
    visited.add(cte_name)

    frag = frag_map.get(cte_name)
    if frag is None or frag.is_stub:
        return False

    input_names = ctx.cte_inputs.get(cte_name, [])
    if not input_names:
        return False

    if _has_select_star(frag.sql):
        # Pass-through CTE — look through it recursively.
        for inp_name in input_names:
            if _find_and_widen(col, inp_name, frag_map, ctx, known_ctes, visited):
                # Column now flows through inp_name; propagate into this CTE's schema.
                if not any(f.name == col for f in ctx.cte_schema.get(cte_name, [])):
                    col_type = next(
                        (f.alteryx_type for f in ctx.cte_schema.get(inp_name, []) if f.name == col),
                        "V_String",
                    )
                    ctx.cte_schema[cte_name] = list(ctx.cte_schema.get(cte_name, [])) + [
                        FieldSchema(name=col, alteryx_type=col_type)
                    ]
                return True
        return False

    # GROUP BY CTEs are opaque: their output schema is fixed by their aggregation
    # expressions.  Injecting a column would produce invalid T-SQL.
    if _has_group_by(frag.sql):
        return False

    # Explicit SELECT CTE.
    # Already emitting this column?  Then it already flows through — no widening needed.
    if any(f.name == col for f in ctx.cte_schema.get(cte_name, [])):
        return True

    # Rename boundary: the column appears in this CTE's SQL (e.g. as the source of a
    # rename: [col] AS [alias]) but is not forwarded under its original name.  Injecting
    # it as a plain passthrough would create a duplicate.  The downstream reference to
    # the original name is wrong; let the repair agent fix it instead.
    if f"[{col}]" in _COMMENT_RE.sub("", frag.sql):
        return False

    # Try to find the column in a direct input's schema.
    for inp_name in input_names:
        inp_schema = ctx.cte_schema.get(inp_name, [])
        if any(f.name == col for f in inp_schema):
            new_sql = _widen_select_sql(frag.sql, [col])
            frag_map[cte_name] = frag.model_copy(update={"sql": new_sql})
            col_type = next((f.alteryx_type for f in inp_schema if f.name == col), "V_String")
            ctx.cte_schema[cte_name] = list(ctx.cte_schema.get(cte_name, [])) + [
                FieldSchema(name=col, alteryx_type=col_type)
            ]
            return True

    # Column not in any direct input — recurse further upstream, then widen here too.
    for inp_name in input_names:
        if _find_and_widen(col, inp_name, frag_map, ctx, known_ctes, visited):
            # Column now flows through inp_name — widen this SELECT to pass it on.
            inp_schema = ctx.cte_schema.get(inp_name, [])
            new_sql = _widen_select_sql(frag.sql, [col])
            frag_map[cte_name] = frag.model_copy(update={"sql": new_sql})
            col_type = next((f.alteryx_type for f in inp_schema if f.name == col), "V_String")
            ctx.cte_schema[cte_name] = list(ctx.cte_schema.get(cte_name, [])) + [
                FieldSchema(name=col, alteryx_type=col_type)
            ]
            return True

    return False


def run_liveness_pass(
    fragments: list[CTEFragment],
    ctx: TranslationContext,
) -> tuple[list[CTEFragment], list[str]]:
    """Widen SELECT CTEs to include columns needed by downstream CTEs.

    Args:
        fragments: Ordered list of CTEFragments from ``translate_chunk``.
        ctx:       Shared TranslationContext; ``cte_schema`` and ``cte_inputs``
                   are read and updated in place.

    Returns:
        A tuple of:
          • updated fragment list (same order, some SQL bodies widened)
          • list of warning strings for gaps that remain unfixable
    """
    known_ctes: set[str] = {f.name for f in fragments}
    # Working copy of fragments, keyed by name for O(1) lookup and mutation.
    frag_map: dict[str, CTEFragment] = {f.name: f for f in fragments}

    gap_warnings: list[str] = []

    # Up to 3 rounds — one is usually enough with the recursive helper, but
    # diamond-shaped dependencies may need an extra pass.
    for _round in range(3):
        any_change = False

        for frag in list(frag_map.values()):
            if frag.is_stub or _has_select_star(frag.sql) or _has_group_by(frag.sql):
                continue

            input_names = ctx.cte_inputs.get(frag.name, [])
            if not input_names:
                continue

            # All columns currently available to this CTE from its direct inputs.
            available: set[str] = {
                f.name for inp in input_names for f in ctx.cte_schema.get(inp, [])
            }

            col_refs = _extract_col_refs(frag.sql, known_ctes)
            missing = col_refs - available - _INTERNAL_COLS

            if not missing:
                continue

            for col in sorted(missing):
                visited: set[str] = {frag.name}
                fixed = False
                for inp_name in input_names:
                    if _find_and_widen(col, inp_name, frag_map, ctx, known_ctes, visited):
                        fixed = True
                        any_change = True
                        break

                if not fixed:
                    gap_warnings.append(
                        f"Liveness: [{col}] referenced in {frag.name!r} but not found "
                        f"in any upstream schema — queued for LLM repair."
                    )

        if not any_change:
            break

    # Reconstruct the fragment list in original order.
    return [frag_map[f.name] for f in fragments], gap_warnings
