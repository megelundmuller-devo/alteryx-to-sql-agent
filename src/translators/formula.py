"""Translator for the Formula tool.

Formula computes one or more new or updated columns.  Each output column is
defined by a <FormulaField> element with an expression attribute.

Translation strategy
--------------------
Phase 3 (deterministic):
  For each FormulaField we try the deterministic expression converter.
  If it returns needs_llm=False we emit an explicit column list overwriting
  or appending the computed column.  If any field requires LLM we mark the
  whole fragment as is_stub=True.

Duplicate column avoidance
--------------------------
Alteryx overwrites an existing column when a Formula field shares its name.
SQL does not: SELECT *, expr AS [col] produces a duplicate if [col] already
exists upstream.  We therefore emit a fully explicit column list:

    SELECT
        [upstream_col1],
        expr AS [overwritten_col],   -- replaced in-place
        [upstream_col3],
        new_expr AS [brand_new_col]  -- appended
    FROM [upstream]

If the upstream schema is unknown (ctx.cte_schema has no entry) we fall back
to SELECT * and emit a warning — the duplicate may remain in that case.

Forward-reference chaining
--------------------------
SQL also disallows referencing an alias defined in the same SELECT list.
When expression N references a column alias defined by an earlier field in
the same Formula tool, we split the fields into separate chained CTEs:

    WITH
    cte_formula_50_s1 AS (SELECT [a], expr1 AS [Date], [c] FROM upstream),
    cte_formula_50    AS (SELECT [a], [Date], [c],
                                 expr2 AS [Weekday]    -- safe: [Date] is upstream here
                          FROM cte_formula_50_s1),

Fields are accumulated greedily into the current batch until a forward
reference is detected, then the batch is flushed to an intermediate CTE.
"""

from __future__ import annotations

import re

from llm.expression_agent import convert_expression_llm
from parsing.models import CTEFragment, FieldSchema, ToolNode
from translators.context import TranslationContext
from translators.expressions import convert_expression, looks_like_sql, needs_llm_translation

# 4-tuple: (output_col_name, sql_expression, optional_comment, is_stub)
_Triple = tuple[str, str, str, bool]


def _references_any(expr: str, col_names: set[str]) -> bool:
    """Return True if expr contains a bracketed reference to any name in col_names."""
    for col in col_names:
        if re.search(r"\[" + re.escape(col) + r"\]", expr, re.IGNORECASE):
            return True
    return False


def _build_select(
    batch: list[_Triple],
    source_schema: list[FieldSchema],
    source: str,
) -> tuple[str, list[FieldSchema]]:
    """Build a SELECT body and output schema for one formula batch.

    If source_schema is non-empty, emits an explicit column list so that
    overwritten columns are replaced in-place and no duplicates appear.
    Falls back to SELECT * when schema is unknown.

    Returns (sql_body, output_schema).  output_schema is [] on fallback.
    """
    # last-wins for duplicate output column names within the same batch
    batch_map: dict[str, tuple[str, str, str]] = {}  # lower_col → (col, expr, comment)
    for col, expr, comment, _ in batch:
        batch_map[col.lower()] = (col, expr, comment)

    if not source_schema:
        lines = ["    *"]
        seen: set[str] = set()
        for col, expr, comment, _ in batch:
            key = col.lower()
            if key not in seen:
                suffix = f"  -- {comment}" if comment else ""
                lines.append(f"    {expr} AS [{col}]{suffix}")
                seen.add(key)
        return "SELECT\n" + ",\n".join(lines) + f"\nFROM [{source}]", []

    source_col_lower = {f.name.lower() for f in source_schema}
    lines: list[str] = []
    output_schema: list[FieldSchema] = []

    for field in source_schema:
        key = field.name.lower()
        if key in batch_map:
            col, expr, comment = batch_map[key]
            suffix = f"  -- {comment}" if comment else ""
            lines.append(f"    {expr} AS [{col}]{suffix}")
            output_schema.append(
                FieldSchema(
                    name=col,
                    alteryx_type=field.alteryx_type,
                    size=field.size,
                    source=field.source,
                )
            )
        else:
            lines.append(f"    [{field.name}]")
            output_schema.append(field)

    seen_new: set[str] = set()
    for col, _, _, _ in batch:
        key = col.lower()
        if key not in source_col_lower and key not in seen_new:
            actual_col, expr, comment = batch_map[key]
            suffix = f"  -- {comment}" if comment else ""
            lines.append(f"    {expr} AS [{actual_col}]{suffix}")
            output_schema.append(FieldSchema(name=actual_col, alteryx_type="V_WString"))
            seen_new.add(key)

    return "SELECT\n" + ",\n".join(lines) + f"\nFROM [{source}]", output_schema


def translate_formula(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment | list[CTEFragment]:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    formula_fields = cfg.get("FormulaFields", {}).get("FormulaField", [])
    if isinstance(formula_fields, dict):
        formula_fields = [formula_fields]

    if not formula_fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (formula): no FormulaField elements found — pass-through."
        )
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    # ------------------------------------------------------------------
    # Build 4-tuples (col, expr_only, comment, stub) for each field.
    # Keeping expr_only separate from "AS [col]" avoids false-positive
    # forward-reference detection on the alias itself.
    # ------------------------------------------------------------------
    triples: list[_Triple] = []
    llm_fields: list[str] = []

    for ff in formula_fields:
        col = ff.get("field", ff.get("name", ""))
        expr = ff.get("expression", ff.get("Expression", "")).strip()

        if not col or not expr:
            continue

        if needs_llm_translation(expr):
            sql_expr = convert_expression_llm(expr)
            llm_failed = (
                sql_expr.startswith("-- LLM")
                or "MANUAL REVIEW" in sql_expr
                or not looks_like_sql(sql_expr)
            )
            if llm_failed:
                llm_fields.append(f"  [{col}] = {expr!r}")
                todo = sql_expr.splitlines()[0]
                triples.append((col, "NULL", f"TODO: {todo}", True))
            else:
                triples.append((col, sql_expr, "", False))
        else:
            sql_expr = convert_expression(expr, ctx.engine_vars)
            triples.append((col, sql_expr, "", False))

    if not triples:
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    if llm_fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (formula): {len(llm_fields)} field(s) require LLM translation "
            f"— stub emitted:\n" + "\n".join(llm_fields)
        )

    # ------------------------------------------------------------------
    # Group triples into batches so no batch references its own aliases.
    # ------------------------------------------------------------------
    batches: list[list[_Triple]] = []
    current_batch: list[_Triple] = []
    current_batch_cols: set[str] = set()

    for col, expr, comment, stub in triples:
        if _references_any(expr, current_batch_cols):
            batches.append(current_batch)
            current_batch = [(col, expr, comment, stub)]
            current_batch_cols = {col}
        else:
            current_batch.append((col, expr, comment, stub))
            current_batch_cols.add(col)

    batches.append(current_batch)

    # ------------------------------------------------------------------
    # Resolve upstream schema once; warn and fall back to SELECT * if missing.
    # ------------------------------------------------------------------
    running_schema = ctx.cte_schema.get(upstream, [])
    if not running_schema:
        ctx.warnings.append(
            f"Tool {node.tool_id} (formula): upstream schema unknown — "
            f"falling back to SELECT * (duplicate column names may occur)."
        )

    # ------------------------------------------------------------------
    # Emit one CTEFragment per batch, threading the schema forward.
    # Intermediate CTEs are pre-registered in ctx.cte_schema so the next
    # batch sees their correct column list (matching the join translator).
    # ------------------------------------------------------------------
    fragments: list[CTEFragment] = []
    n_batches = len(batches)
    source = upstream

    for idx, batch in enumerate(batches):
        is_last = idx == n_batches - 1
        name = cte_name if is_last else f"{cte_name}_s{idx + 1}"
        batch_stub = any(stub for _, _, _, stub in batch)
        sql, running_schema = _build_select(batch, running_schema, source)
        fragments.append(
            CTEFragment(name=name, sql=sql, source_tool_ids=[node.tool_id], is_stub=batch_stub)
        )
        if not is_last:
            ctx.cte_schema[name] = running_schema
        source = name

    return fragments[0] if n_batches == 1 else fragments
