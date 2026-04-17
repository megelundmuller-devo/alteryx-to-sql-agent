"""Translator for Macro tool nodes.

Macro tools call external .yxmc files.  This module branches on whether the
macro is a known Alteryx standard macro (found in standard_macros/) or a
user-created custom macro.

Standard macros → looked up in STANDARD_MACRO_REGISTRY; each has a
deterministic T-SQL translation (or a clearly-labelled stub when no SQL
equivalent exists).

Custom macros → currently emit a stub directing the user to Phase 5 macro
expansion (macro_handler.py), which will inline the .yxmc CTE chain.
"""

from __future__ import annotations

from pathlib import Path

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext
from translators.standard_macros import STANDARD_MACRO_REGISTRY


def translate_macro(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    if node.is_standard_macro:
        return _translate_standard(node, cte_name, input_ctes, ctx)
    return _translate_custom(node, cte_name, input_ctes, ctx)


def _translate_standard(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    macro_name = Path(node.macro_path or "").name
    translator = STANDARD_MACRO_REGISTRY.get(macro_name)
    if translator is not None:
        return translator(node, cte_name, input_ctes, ctx)

    # Standard macro present on disk but not yet in the registry
    ctx.warnings.append(
        f"Tool {node.tool_id} (standard macro '{macro_name}'): "
        "no translator registered — stub emitted. Add to STANDARD_MACRO_REGISTRY."
    )
    sql = (
        f"-- Standard macro '{macro_name}' — translator not yet registered\n"
        f"SELECT TOP 0 1 AS _macro_stub"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)


def _translate_custom(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    macro_path = node.macro_path or "UNKNOWN"
    upstream_comment = (
        f"-- Reads from: {', '.join(input_ctes)}" if input_ctes else "-- No upstream CTEs"
    )
    ctx.warnings.append(
        f"Tool {node.tool_id} (custom macro): references '{macro_path}'. "
        "Custom macro expansion is not yet implemented — stub CTE emitted. Review manually."
    )
    sql = (
        f"-- TODO: expand custom macro '{macro_path}'\n"
        f"{upstream_comment}\n"
        f"SELECT TOP 0 1 AS _macro_stub  -- replace with macro logic"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)
