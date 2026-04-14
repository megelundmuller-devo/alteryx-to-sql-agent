"""Translator for the Summarize tool.

Summarize performs GROUP BY aggregation.  Each <SummarizeField> has an
`action` attribute that maps to a SQL aggregate function or a GROUP BY key.

Supported actions (Alteryx → T-SQL)
-------------------------------------
GroupBy        → GROUP BY column
Sum            → SUM([col])
Count          → COUNT([col])
CountDistinct  → COUNT(DISTINCT [col])
Min            → MIN([col])
Max            → MAX([col])
Avg            → AVG(CAST([col] AS FLOAT))  — cast avoids integer division
First          → MIN([col])  + warning (Alteryx "First" is non-deterministic)
Last           → MAX([col])  + warning
Concat         → STUFF(…FOR XML PATH(''), TYPE) — SQL Server 2016-compatible
ConcatDistinct → STUFF(…DISTINCT…FOR XML PATH(''), TYPE) — deduplicates before concat
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


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

    # First pass: collect group-by column names — needed to build correlated subqueries
    # for Concat/ConcatDistinct when a GroupBy is also present.
    group_by_cols: list[str] = [
        f.get("field", "") for f in fields if f.get("action", "GroupBy") == "GroupBy"
    ]
    has_concat = any(f.get("action") in ("Concat", "ConcatDistinct") for f in fields)

    # When Concat and GroupBy coexist we alias the outer table so the inner
    # FOR XML PATH subquery can correlate back to the current group.
    use_outer_alias = has_concat and bool(group_by_cols)

    select_cols: list[str] = []

    for f in fields:
        col = f.get("field", "")
        action = f.get("action", "GroupBy")
        rename = f.get("rename", col)

        if action == "GroupBy":
            col_expr = f"[_outer].[{col}]" if use_outer_alias else f"[{col}]"
            if rename != col:
                select_cols.append(f"    {col_expr} AS [{rename}]")
            else:
                select_cols.append(f"    {col_expr}")
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
            select_cols.append(
                _concat_xml_path(col, rename, upstream, group_by_cols, distinct=False)
            )
        elif action == "ConcatDistinct":
            select_cols.append(
                _concat_xml_path(col, rename, upstream, group_by_cols, distinct=True)
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
        if use_outer_alias:
            group_sql = ", ".join(f"[_outer].[{c}]" for c in group_by_cols)
            sql = f"SELECT\n{cols_sql}\nFROM [{upstream}] AS [_outer]\nGROUP BY {group_sql}"
        else:
            group_sql = ", ".join(f"[{c}]" for c in group_by_cols)
            sql = f"SELECT\n{cols_sql}\nFROM [{upstream}]\nGROUP BY {group_sql}"
    else:
        sql = f"SELECT\n{cols_sql}\nFROM [{upstream}]"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def _concat_xml_path(
    col: str,
    rename: str,
    upstream: str,
    group_by_cols: list[str],
    *,
    distinct: bool,
) -> str:
    """Build a STUFF(…FOR XML PATH) concatenation — SQL Server 2016-compatible.

    When *group_by_cols* is non-empty the inner SELECT is correlated to the
    outer alias ``[_outer]`` so each group receives its own concatenated value.
    ``DISTINCT`` inside the subquery deduplicates values before concatenation.
    """
    distinct_kw = "DISTINCT " if distinct else ""
    if group_by_cols:
        value_expr = f"', ' + ISNULL(CAST([_sub].[{col}] AS NVARCHAR(MAX)), '')"
        where = " AND ".join(f"[_sub].[{g}] = [_outer].[{g}]" for g in group_by_cols)
        inner = (
            f"SELECT {distinct_kw}{value_expr}\n"
            f"            FROM [{upstream}] AS [_sub]\n"
            f"            WHERE {where}\n"
            f"            FOR XML PATH(''), TYPE"
        )
    else:
        value_expr = f"', ' + ISNULL(CAST([{col}] AS NVARCHAR(MAX)), '')"
        inner = (
            f"SELECT {distinct_kw}{value_expr}\n"
            f"            FROM [{upstream}]\n"
            f"            FOR XML PATH(''), TYPE"
        )
    return (
        f"    STUFF((\n"
        f"        {inner}\n"
        f"    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS [{rename}]"
    )
