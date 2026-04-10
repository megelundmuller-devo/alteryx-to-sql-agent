"""Translator for the Append Fields tool.

Append Fields repeats (broadcasts) every row from a "Source" (small) stream
across every row of a "Target" (large) stream.  In SQL this is a CROSS JOIN
when the source produces one row, or a JOIN with no condition (Cartesian
product) when it produces multiple rows.

Alteryx AppendFields inputs:
    Left  = Target (the many-row stream)
    Right = Source (the broadcast stream)

Config: <SourceType> may be "Source" or similar.  We always emit CROSS JOIN.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_append(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    if len(input_ctes) < 2:
        ctx.warnings.append(
            f"Tool {node.tool_id} (append_fields): expected 2 inputs (Target + Source), "
            f"got {len(input_ctes)}. Generating stub."
        )
        return CTEFragment(
            name=cte_name,
            sql="SELECT TOP 0 1 AS _stub  -- append_fields: insufficient inputs",
            source_tool_ids=[node.tool_id],
            is_stub=True,
        )

    target_cte = input_ctes[0]
    source_cte = input_ctes[1]

    ctx.warnings.append(
        f"Tool {node.tool_id} (append_fields): CROSS JOIN emitted. "
        "If source has more than one row this produces a Cartesian product. "
        "Verify that the source stream always produces exactly one row."
    )

    sql = (
        f"-- Append Fields: CROSS JOIN target with source\n"
        f"-- Target: [{target_cte}], Source: [{source_cte}]\n"
        f"SELECT\n"
        f"    T.*,\n"
        f"    S.*\n"
        f"FROM [{target_cte}] AS T\n"
        f"CROSS JOIN [{source_cte}] AS S"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
