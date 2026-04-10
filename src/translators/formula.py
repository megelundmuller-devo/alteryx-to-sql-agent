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

Multiple fields
---------------
We chain them as sub-selects rather than a single SELECT with multiple
computed columns.  This keeps the generated SQL predictable regardless of
expression complexity:

    WITH
    _step1 AS (SELECT *, <expr1> AS [col1] FROM upstream),
    _step2 AS (SELECT *, <expr2> AS [col2] FROM _step1),
    ...

We implement this by returning a single CTEFragment whose SQL body already
references the chained CTEs inline — the assembler wraps it in one WITH block.

Actually for simplicity we flatten to one SELECT with all non-LLM columns
and flag the whole thing for LLM if any column needs it.
"""

from __future__ import annotations

from llm.expression_agent import convert_expression_llm
from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext
from translators.expressions import convert_expression, needs_llm_translation


def translate_formula(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
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
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    computed_cols: list[str] = []
    has_llm_field = False
    llm_fields: list[str] = []

    for ff in formula_fields:
        col = ff.get("field", ff.get("name", ""))
        expr = ff.get("expression", ff.get("Expression", "")).strip()

        if not col or not expr:
            continue

        if needs_llm_translation(expr):
            sql_expr = convert_expression_llm(expr)
            if sql_expr.startswith("-- LLM") or "MANUAL REVIEW" in sql_expr:
                # LLM failed or flagged for manual review
                has_llm_field = True
                llm_fields.append(f"  [{col}] = {expr!r}")
                computed_cols.append(
                    f"    NULL AS [{col}]  -- TODO: {sql_expr.splitlines()[0]}"
                )
            else:
                computed_cols.append(f"    {sql_expr} AS [{col}]")
        else:
            sql_expr = convert_expression(expr)
            computed_cols.append(f"    {sql_expr} AS [{col}]")

    if not computed_cols:
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    cols_sql = ",\n".join(computed_cols)
    sql = f"SELECT\n    *,\n{cols_sql}\nFROM [{upstream}]"

    if has_llm_field:
        ctx.warnings.append(
            f"Tool {node.tool_id} (formula): {len(llm_fields)} field(s) require LLM translation "
            f"— stub emitted:\n" + "\n".join(llm_fields)
        )
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
