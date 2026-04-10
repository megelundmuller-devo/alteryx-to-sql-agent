"""Translator for the Union tool.

Alteryx Union stacks multiple input streams vertically. Two modes:
* Auto-config (default): columns matched by position — UNION ALL
* Manual config:          columns matched by name — UNION ALL with explicit lists

Translation strategy
--------------------
UNION ALL is always the safe default because Alteryx Union preserves
duplicate rows unless the user has explicitly configured deduplication.
We emit UNION ALL and add a comment prompting the user to switch to
UNION if deduplication is needed.

The chunk's input_cte_names contains one entry per upstream branch.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_union(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    if not input_ctes:
        ctx.warnings.append(
            f"Tool {node.tool_id} (union): no input CTEs found — generating stub."
        )
        return CTEFragment(
            name=cte_name,
            sql="SELECT TOP 0 1 AS _stub  -- union: no inputs",
            source_tool_ids=[node.tool_id],
            is_stub=True,
        )

    if len(input_ctes) == 1:
        # Degenerate union of one stream — just pass through
        sql = f"SELECT *\nFROM [{input_ctes[0]}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    branches = "\nUNION ALL\n".join(f"SELECT * FROM [{cte}]" for cte in input_ctes)
    sql = (
        f"-- UNION ALL preserves duplicates. Change to UNION to deduplicate.\n"
        f"{branches}"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
