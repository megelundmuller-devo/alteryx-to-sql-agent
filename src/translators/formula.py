"""Translator for the Formula tool.

Formula computes one or more new or updated columns.  Each output column is
defined by a <FormulaField> element with an expression attribute.

Translation strategy
--------------------
Phase 3 (deterministic):
  For each FormulaField we try the deterministic expression converter.
  If it returns needs_llm=False we emit:
      SELECT *, <expr> AS [col]
  chained over the upstream CTE.

  If any field requires LLM we mark the whole fragment as is_stub=True and
  add a warning — Phase 4 will replace the stub with an LLM-generated CTE.

Multiple fields — forward-reference chaining
--------------------------------------------
SQL does not allow a SELECT expression to reference an alias defined in the
same SELECT list.  When expression N references a column alias defined by an
earlier field in the same Formula tool, we split the fields into separate
chained CTEs:

    WITH
    cte_formula_50_s1 AS (SELECT *, <expr1> AS [col1] FROM upstream),
    cte_formula_50    AS (SELECT *, <expr2> AS [col2] FROM cte_formula_50_s1),

Fields are accumulated greedily into the current batch until a forward
reference is detected, at which point the batch is flushed to an intermediate
CTE and a new batch starts.
"""

from __future__ import annotations

import re

from llm.expression_agent import convert_expression_llm
from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext
from translators.expressions import convert_expression, looks_like_sql, needs_llm_translation


def _references_any(expr: str, col_names: set[str]) -> bool:
    """Return True if expr contains a bracketed reference to any name in col_names."""
    for col in col_names:
        if re.search(r"\[" + re.escape(col) + r"\]", expr, re.IGNORECASE):
            return True
    return False


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
    # Build (col, line, is_stub) triples for each field
    # ------------------------------------------------------------------
    triples: list[tuple[str, str, bool]] = []
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
                triples.append((col, f"    NULL AS [{col}]  -- TODO: {todo}", True))
            else:
                triples.append((col, f"    {sql_expr} AS [{col}]", False))
        else:
            sql_expr = convert_expression(expr, ctx.engine_vars)
            triples.append((col, f"    {sql_expr} AS [{col}]", False))

    if not triples:
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    if llm_fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (formula): {len(llm_fields)} field(s) require LLM translation "
            f"— stub emitted:\n" + "\n".join(llm_fields)
        )

    # ------------------------------------------------------------------
    # Group triples into batches so that no batch references its own aliases.
    # Each batch becomes one CTE; batches are chained in order.
    # ------------------------------------------------------------------
    batches: list[list[tuple[str, str, bool]]] = []
    current_batch: list[tuple[str, str, bool]] = []
    current_batch_cols: set[str] = set()

    for col, line, stub in triples:
        # The *raw* Alteryx expression (in `line`) already has [col] brackets;
        # check if this field's SQL expression references any alias from the
        # current (not-yet-flushed) batch.
        if _references_any(line, current_batch_cols):
            batches.append(current_batch)
            current_batch = [(col, line, stub)]
            current_batch_cols = {col}
        else:
            current_batch.append((col, line, stub))
            current_batch_cols.add(col)

    batches.append(current_batch)

    # ------------------------------------------------------------------
    # Emit one CTEFragment per batch
    # ------------------------------------------------------------------
    fragments: list[CTEFragment] = []
    n_batches = len(batches)
    source = upstream

    for idx, batch in enumerate(batches):
        is_last = idx == n_batches - 1
        name = cte_name if is_last else f"{cte_name}_s{idx + 1}"
        batch_stub = any(stub for _, _, stub in batch)
        cols_sql = ",\n".join(line for _, line, _ in batch)
        sql = f"SELECT\n    *,\n{cols_sql}\nFROM [{source}]"
        fragments.append(
            CTEFragment(name=name, sql=sql, source_tool_ids=[node.tool_id], is_stub=batch_stub)
        )
        source = name

    return fragments[0] if n_batches == 1 else fragments
