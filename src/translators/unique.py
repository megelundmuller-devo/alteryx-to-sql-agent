"""Translator for the Unique tool.

Unique deduplicates rows.  It has two output anchors:
* Unique — first occurrence of each unique key combination
* Duplicate — all subsequent occurrences

Translation strategy
--------------------
We implement this using ROW_NUMBER() OVER (PARTITION BY <key_cols> ORDER BY ...):
    Unique    → WHERE _rn = 1
    Duplicate → WHERE _rn > 1

If the upstream CTE was produced by a Sort tool (indicated by a _sort_order column
in cte_schema), we use ORDER BY _sort_order so the sort intent is preserved.
Otherwise we fall back to ORDER BY (SELECT NULL) — non-deterministic but valid T-SQL.

RecordID → Unique pattern
--------------------------
When a RecordID tool feeds directly into a Unique tool, assigning IDs before
deduplication causes PARTITION BY to include a unique-per-row value, defeating
deduplication.  This translator detects that pattern via the DAG and reorders:
dedup first (against the pre-RecordID data), then assign RecordID to the
surviving rows so the IDs reflect post-dedup row positions.

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

    # Detect RecordID → Unique pattern: deduplicate against pre-RecordID data,
    # then assign RecordID to surviving rows so IDs reflect post-dedup positions.
    predecessors = ctx.dag.predecessors(node.tool_id)
    record_id_pred = next((p for p in predecessors if p.tool_type == "record_id"), None)

    if record_id_pred is not None:
        return _translate_unique_after_record_id(
            node=node,
            cte_name=cte_name,
            rid_cte=upstream,
            rid_node=record_id_pred,
            fields=fields,
            ctx=ctx,
        )

    partition_cols = ", ".join(f"[{f.get('field', '')}]" for f in fields)
    upstream_cols = {fs.name for fs in ctx.cte_schema.get(upstream, [])}
    order_by = "_sort_order" if "_sort_order" in upstream_cols else "(SELECT NULL)"

    sql = (
        f"-- Unique anchor: first occurrence per key\n"
        f"-- For duplicates anchor: change WHERE _rn = 1 to WHERE _rn > 1\n"
        f"SELECT *\n"
        f"FROM (\n"
        f"    SELECT\n"
        f"        *,\n"
        f"        ROW_NUMBER() OVER (\n"
        f"            PARTITION BY {partition_cols}\n"
        f"            ORDER BY {order_by}\n"
        f"        ) AS _rn\n"
        f"    FROM [{upstream}]\n"
        f") AS _deduped\n"
        f"WHERE _rn = 1"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def _translate_unique_after_record_id(
    node: ToolNode,
    cte_name: str,
    rid_cte: str,
    rid_node: ToolNode,
    fields: list[dict],
    ctx: TranslationContext,
) -> CTEFragment:
    """Generate a combined dedup+RecordID CTE that skips the intermediate RecordID CTE.

    Reads directly from the pre-RecordID upstream so that partition columns are
    evaluated before any sequential ID is assigned.
    """
    rid_cfg = rid_node.config
    rid_field = rid_cfg.get("FieldName", "RecordID")

    try:
        start_int = int(str(rid_cfg.get("StartValue", "1")).strip())
    except (ValueError, TypeError):
        start_int = 1

    if start_int == 1:
        id_expr = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))"
    else:
        id_expr = f"ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {start_int - 1}"

    # Skip the RecordID CTE — read from its own upstream instead.
    pre_rid_inputs = ctx.cte_inputs.get(rid_cte, [])
    pre_rid_upstream = pre_rid_inputs[0] if pre_rid_inputs else "-- NO_UPSTREAM"

    # Exclude the RecordID field itself from the partition — it doesn't exist in
    # the pre-RecordID data and would cause an invalid column reference.
    partition_fields = [f for f in fields if f.get("field", "") != rid_field]
    if not partition_fields:
        # All UniqueFields were the RecordID column — dedup makes no sense; assign only
        partition_fields = fields  # fall back gracefully, will produce a warning upstream

    partition_cols = ", ".join(f"[{f.get('field', '')}]" for f in partition_fields)

    pre_upstream_cols = {fs.name for fs in ctx.cte_schema.get(pre_rid_upstream, [])}
    order_by = "_sort_order" if "_sort_order" in pre_upstream_cols else "(SELECT NULL)"

    sql = (
        f"-- Unique anchor: dedup first, then assign RecordID to post-dedup rows\n"
        f"-- For duplicates anchor: change WHERE _rn = 1 to WHERE _rn > 1\n"
        f"SELECT\n"
        f"    *,\n"
        f"    {id_expr} AS [{rid_field}]\n"
        f"FROM (\n"
        f"    SELECT\n"
        f"        *,\n"
        f"        ROW_NUMBER() OVER (\n"
        f"            PARTITION BY {partition_cols}\n"
        f"            ORDER BY {order_by}\n"
        f"        ) AS _rn\n"
        f"    FROM [{pre_rid_upstream}]\n"
        f") AS _deduped\n"
        f"WHERE _rn = 1"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
