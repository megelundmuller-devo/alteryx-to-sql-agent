"""Translator for the Sort tool.

Sort produces an ordered result set.  In SQL, ORDER BY is only valid in the
outermost query (or inside ROW_NUMBER / OFFSET-FETCH).  Since this CTE will
almost certainly be consumed by a downstream tool, we emit a SELECT with an
ORDER BY inside a subquery pattern that MSSQL accepts:

    SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY ...) AS _rn FROM upstream) t
    ORDER BY _rn

This preserves order as a materialised column that downstream tools can sort on
if needed.  A simpler SELECT * ... ORDER BY is emitted with a warning that it
may be ignored by MSSQL if wrapped in a CTE.

Alteryx SortInfo structure:
    <SortInfo>
        <Field field="ColName" order="Ascending" />
        <Field field="ColName" order="Descending" />
    </SortInfo>
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_sort(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    sort_info = cfg.get("SortInfo", {})
    fields = sort_info.get("Field", [])
    if isinstance(fields, dict):
        fields = [fields]

    if not fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (sort): no sort fields found — pass-through."
        )
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    order_parts: list[str] = []
    for f in fields:
        col = f.get("field", "")
        order = f.get("order", "Ascending")
        direction = "DESC" if order.lower() in ("descending", "desc") else "ASC"
        order_parts.append(f"[{col}] {direction}")

    order_clause = ", ".join(order_parts)

    ctx.warnings.append(
        f"Tool {node.tool_id} (sort): ORDER BY inside a CTE may be ignored by MSSQL unless "
        "combined with TOP or OFFSET-FETCH. Verify the sort is applied at the right level."
    )

    # Emit using ROW_NUMBER so order is preserved as a column
    sql = (
        f"SELECT *\n"
        f"FROM (\n"
        f"    SELECT\n"
        f"        *,\n"
        f"        ROW_NUMBER() OVER (ORDER BY {order_clause}) AS _sort_order\n"
        f"    FROM [{upstream}]\n"
        f") AS _sorted\n"
        f"ORDER BY _sort_order"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
