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

Column-count mismatches
-----------------------
When the arms have different column counts and all arm schemas are known,
we build an explicit SELECT per arm with ``NULL AS [col]`` for any column
absent from that arm.  This produces a valid UNION ALL whose column set is
the union of all arms' columns.

If any arm's schema is unknown we fall back to the simple ``SELECT *`` form
and emit a warning for manual review.
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
        ctx.warnings.append(f"Tool {node.tool_id} (union): no input CTEs found — generating stub.")
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

    schemas = [ctx.cte_schema.get(cte, []) for cte in input_ctes]
    all_known = all(schemas)

    if all_known:
        # Compute the union of column names, preserving order of first appearance.
        all_col_names: list[str] = []
        seen_cols: set[str] = set()
        for schema in schemas:
            for f in schema:
                if f.name not in seen_cols:
                    all_col_names.append(f.name)
                    seen_cols.add(f.name)

        counts = {len(s) for s in schemas}
        if len(counts) > 1:
            # Mismatched column counts — build NULL-padded explicit SELECT per arm.
            arms: list[str] = []
            for cte, schema in zip(input_ctes, schemas):
                schema_names = {f.name for f in schema}
                col_lines = [
                    f"    [{col}]" if col in schema_names else f"    NULL AS [{col}]"
                    for col in all_col_names
                ]
                arms.append("SELECT\n" + ",\n".join(col_lines) + f"\nFROM [{cte}]")
            sql = (
                "-- UNION ALL preserves duplicates. Change to UNION to deduplicate.\n"
                + "\nUNION ALL\n".join(arms)
            )
            ctx.warnings.append(
                f"Tool {node.tool_id} (union): UNION ALL arms had different column counts "
                f"— NULL columns added to align schemas."
            )
            return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    else:
        # Some schemas unknown — check whether the known arms already mismatch.
        known = [(cte, len(s)) for cte, s in zip(input_ctes, schemas) if s]
        if known:
            counts = {n for _, n in known}
            if len(counts) > 1:
                detail = ", ".join(f"{c} ({n} col{'s' if n != 1 else ''})" for c, n in known)
                ctx.warnings.append(
                    f"Tool {node.tool_id} (union): UNION ALL arms have different column counts "
                    f"({detail}) — schemas incomplete, cannot pad automatically. "
                    f"Review each arm's SELECT list."
                )

    branches = "\nUNION ALL\n".join(f"SELECT * FROM [{cte}]" for cte in input_ctes)
    sql = f"-- UNION ALL preserves duplicates. Change to UNION to deduplicate.\n{branches}"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
