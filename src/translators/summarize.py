"""Translator for the Summarize tool.

Summarize performs GROUP BY aggregation.  Each <SummarizeField> has an
`action` attribute that maps to a SQL aggregate function or a GROUP BY key.

Supported actions (Alteryx → T-SQL)
-------------------------------------
GroupBy    → GROUP BY column
Sum        → SUM([col])
Count      → COUNT([col])
CountDistinct → COUNT(DISTINCT [col])
Min        → MIN([col])
Max        → MAX([col])
Avg        → AVG(CAST([col] AS FLOAT))  — cast avoids integer division
First      → MIN([col])  + warning (Alteryx "First" is non-deterministic)
Last       → MAX([col])  + warning
Concat     → STRING_AGG([col], ', ')   (T-SQL 2017+)
ConcatDistinct → STRING_AGG(DISTINCT [col], ', ')  — not valid in T-SQL;
              we emit STRING_AGG with a warning to deduplicate manually
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext

_ACTION_MAP: dict[str, str] = {
    "Sum": "SUM",
    "Count": "COUNT",
    "CountDistinct": "COUNT(DISTINCT {col})",  # special-cased below
    "Min": "MIN",
    "Max": "MAX",
}


def translate_summarize(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    fields = cfg.get("SummarizeFields", {}).get("SummarizeField", [])
    if isinstance(fields, dict):
        fields = [fields]

    if not fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (summarize): no SummarizeField elements — pass-through."
        )
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    group_by_cols: list[str] = []
    select_cols: list[str] = []

    for f in fields:
        col = f.get("field", "")
        action = f.get("action", "GroupBy")
        rename = f.get("rename", col)

        if action == "GroupBy":
            group_by_cols.append(f"[{col}]")
            alias = f"[{rename}]" if rename != col else f"[{col}]"
            select_cols.append(
                f"    {alias}" if rename == col else f"    [{col}] AS {alias}"
            )
        elif action == "Sum":
            select_cols.append(f"    SUM([{col}]) AS [{rename}]")
        elif action == "Count":
            select_cols.append(f"    COUNT([{col}]) AS [{rename}]")
        elif action == "CountDistinct":
            select_cols.append(f"    COUNT(DISTINCT [{col}]) AS [{rename}]")
        elif action == "Min":
            select_cols.append(f"    MIN([{col}]) AS [{rename}]")
        elif action == "Max":
            select_cols.append(f"    MAX([{col}]) AS [{rename}]")
        elif action == "Avg":
            select_cols.append(f"    AVG(CAST([{col}] AS FLOAT)) AS [{rename}]")
        elif action == "First":
            ctx.warnings.append(
                f"Tool {node.tool_id} (summarize): 'First' action on [{col}] is non-deterministic "
                "in SQL. Using MIN() as approximation — verify this is acceptable."
            )
            select_cols.append(
                f"    MIN([{col}]) AS [{rename}]  -- was: First (non-deterministic)"
            )
        elif action == "Last":
            ctx.warnings.append(
                f"Tool {node.tool_id} (summarize): 'Last' action on [{col}] is non-deterministic "
                "in SQL. Using MAX() as approximation — verify this is acceptable."
            )
            select_cols.append(
                f"    MAX([{col}]) AS [{rename}]  -- was: Last (non-deterministic)"
            )
        elif action == "Concat":
            select_cols.append(f"    STRING_AGG([{col}], ', ') AS [{rename}]")
        elif action == "ConcatDistinct":
            ctx.warnings.append(
                f"Tool {node.tool_id} (summarize): 'ConcatDistinct' on [{col}] — "
                "T-SQL STRING_AGG does not support DISTINCT. Review manually."
            )
            select_cols.append(
                f"    STRING_AGG([{col}], ', ') AS [{rename}]"
                f"  -- was: ConcatDistinct (not valid in T-SQL)"
            )
        else:
            ctx.warnings.append(
                f"Tool {node.tool_id} (summarize): unknown action '{action}' on [{col}] — skipped."
            )

    if not select_cols:
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    cols_sql = ",\n".join(select_cols)
    if group_by_cols:
        group_sql = ", ".join(group_by_cols)
        sql = f"SELECT\n{cols_sql}\nFROM [{upstream}]\nGROUP BY {group_sql}"
    else:
        # No group-by means aggregate over the whole table
        sql = f"SELECT\n{cols_sql}\nFROM [{upstream}]"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
