"""Translator for the RecordID tool.

RecordID appends a sequential integer row number to each record, starting
at a configurable value (default 1).

T-SQL mapping:
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + (StartValue - 1)

Config:
    <StartValue>1</StartValue>
    <FieldName>RecordID</FieldName>
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_record_id(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    field_name = cfg.get("FieldName", "RecordID")
    start_value = cfg.get("StartValue", "1")

    try:
        start_int = int(str(start_value).strip())
    except (ValueError, TypeError):
        start_int = 1
        ctx.warnings.append(
            f"Tool {node.tool_id} (record_id): could not parse "
            f"StartValue '{start_value}' — defaulting to 1."
        )

    if start_int == 1:
        id_expr = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))"
    else:
        id_expr = f"ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {start_int - 1}"

    sql = f"SELECT\n    *,\n    {id_expr} AS [{field_name}]\nFROM [{upstream}]"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
