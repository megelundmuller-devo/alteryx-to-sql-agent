"""Translator for the Unique tool.

Unique deduplicates rows.  It has two output anchors:
* Unique — first occurrence of each unique key combination
* Duplicate — all subsequent occurrences

Translation strategy
--------------------
We implement this using ROW_NUMBER() OVER (PARTITION BY <key_cols> ORDER BY (SELECT NULL)):
    Unique    → WHERE _rn = 1
    Duplicate → WHERE _rn > 1

Unique key config lives in:
    <UniqueFields>
        <Field field="ColName" />
    </UniqueFields>

If no key fields are found we emit a DISTINCT SELECT which deduplicates all
columns — this matches Alteryx's behaviour when no key is specified.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_unique(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    fields = cfg.get("UniqueFields", {}).get("Field", [])
    if isinstance(fields, dict):
        fields = [fields]

    if not fields:
        # No key fields — deduplicate all columns
        sql = f"SELECT DISTINCT *\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    partition_cols = ", ".join(f"[{f.get('field', '')}]" for f in fields)

    sql = (
        f"-- Unique anchor: first occurrence per key\n"
        f"-- For duplicates anchor: change WHERE _rn = 1 to WHERE _rn > 1\n"
        f"SELECT *\n"
        f"FROM (\n"
        f"    SELECT\n"
        f"        *,\n"
        f"        ROW_NUMBER() OVER (\n"
        f"            PARTITION BY {partition_cols}\n"
        f"            ORDER BY (SELECT NULL)\n"
        f"        ) AS _rn\n"
        f"    FROM [{upstream}]\n"
        f") AS _deduped\n"
        f"WHERE _rn = 1"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
