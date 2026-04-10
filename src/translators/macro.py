"""Translator for Macro tool nodes.

Macro tools call external .yxmc files.  In Phase 3/4 we cannot inline them
automatically (that is Phase 5's macro_handler.py job).  Here we emit a clearly
labelled stub CTE so the overall pipeline does not break, and attach a warning
directing the user to review the macro manually or wait for macro expansion.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_macro(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    macro_path = node.macro_path or "UNKNOWN"
    upstream_comment = (
        f"-- Reads from: {', '.join(input_ctes)}"
        if input_ctes
        else "-- No upstream CTEs"
    )

    ctx.warnings.append(
        f"Tool {node.tool_id} (macro): references '{macro_path}'. "
        "Macro expansion is not yet implemented — stub CTE emitted. Review manually."
    )

    sql = (
        f"-- TODO: expand macro '{macro_path}'\n"
        f"{upstream_comment}\n"
        f"SELECT TOP 0 1 AS _macro_stub  -- replace with macro logic"
    )

    return CTEFragment(
        name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
    )
