"""Translator for the Multi-Row Formula tool.

Multi-Row Formula computes a column using values from the current row and
adjacent rows (previous/next).  This maps to LAG() / LEAD() window functions
in T-SQL.

Because the expression language is complex (row[-1][field] syntax, IF/THEN),
this translator always flags for LLM translation and emits a labelled stub.
The stub includes the raw Alteryx expression as a comment so the LLM agent
has full context.

Config structure:
    <NumRows>1</NumRows>          -- number of rows to look back/forward
    <FormulaField field="col" expression="..." />
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_multirow(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    formula_field = cfg.get("FormulaField", {})
    if isinstance(formula_field, list):
        formula_field = formula_field[0] if formula_field else {}

    col = formula_field.get("field", "unknown_col")
    expr = formula_field.get("expression", "").strip()
    num_rows = cfg.get("NumRows", "1")

    ctx.warnings.append(
        f"Tool {node.tool_id} (multirow): Multi-Row Formula requires LLM translation. "
        f"Column: [{col}], Rows: {num_rows}, Expression: {expr!r:.200}"
    )

    sql = (
        f"-- TODO: translate Multi-Row Formula\n"
        f"-- Column: [{col}]\n"
        f"-- Rows offset: {num_rows}\n"
        f"-- Alteryx expression: {expr}\n"
        f"-- Likely maps to LAG([col], {num_rows}) OVER (ORDER BY ...) or LEAD(...)\n"
        f"SELECT\n"
        f"    *,\n"
        f"    NULL AS [{col}]  -- REPLACE with LAG/LEAD expression\n"
        f"FROM [{upstream}]"
    )

    return CTEFragment(
        name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
    )
