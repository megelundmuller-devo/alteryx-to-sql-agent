"""Translator for the Sample tool.

Sample returns the first N rows of a stream.  In T-SQL this maps to SELECT TOP N.

Config structure:
    <SampleSize>100</SampleSize>
    <Mode>First</Mode>  (First / Last / Random — only First is deterministic)

* First N → SELECT TOP N *
* Last  N → not directly expressible without ORDER BY; we emit a warning
* Random  → TABLESAMPLE or NEWID() ORDER BY trick; we emit a stub
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_sample(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    n = cfg.get("SampleSize", cfg.get("N", ""))
    mode = cfg.get("Mode", "First")

    try:
        n_int = int(str(n).strip())
    except (ValueError, TypeError):
        n_int = None

    if n_int is None:
        ctx.warnings.append(
            f"Tool {node.tool_id} (sample): could not parse sample size '{n}' — generating stub."
        )
        sql = f"SELECT TOP 100 *\nFROM [{upstream}]  -- TODO: verify sample size"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    mode_lower = mode.lower() if mode else "first"

    if mode_lower in ("first", ""):
        sql = f"SELECT TOP {n_int} *\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    if mode_lower == "last":
        ctx.warnings.append(
            f"Tool {node.tool_id} (sample): 'Last N' mode requires ORDER BY to be meaningful. "
            "Stub emitted — add ORDER BY before applying TOP."
        )
        sql = (
            f"-- TODO: 'Last N' — add ORDER BY <column> DESC, then SELECT TOP {n_int}\n"
            f"SELECT TOP {n_int} *\n"
            f"FROM [{upstream}]\n"
            f"-- ORDER BY <column> DESC"
        )
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    if mode_lower == "random":
        ctx.warnings.append(
            f"Tool {node.tool_id} (sample): 'Random N' mode — using NEWID() ORDER BY trick. "
            "This is slow on large tables; consider TABLESAMPLE for better performance."
        )
        sql = f"SELECT TOP {n_int} *\nFROM [{upstream}]\nORDER BY NEWID()"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    ctx.warnings.append(
        f"Tool {node.tool_id} (sample): unknown mode '{mode}' — defaulting to TOP {n_int}."
    )
    sql = f"SELECT TOP {n_int} *\nFROM [{upstream}]"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
