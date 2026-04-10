"""Translator for unknown or unsupported tool types.

When the tool registry has no registered deterministic translator for a tool type,
this module is invoked as the fallback.  It follows a three-step cascade:

1. **Registry lookup** — if the tool registry is enabled and has a cached
   translation for this plugin, use it immediately (zero LLM cost).
2. **LLM translation** — call the chunk agent to generate a CTE body.
   If the result looks valid (not a failure stub), save it to the registry
   so future encounters are free.
3. **Hard stub** — if the LLM also fails, emit a clearly labelled stub CTE
   and add a warning so the user knows manual review is needed.
"""

from __future__ import annotations

from llm.chunk_agent import translate_chunk_llm
from parsing.models import CTEFragment, ToolNode
from registry.tool_registry import make_entry
from translators.context import TranslationContext


def translate_unknown(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream_comment = f"-- Reads from: {', '.join(input_ctes)}" if input_ctes else "-- Source node"

    # ------------------------------------------------------------------
    # Step 1 — Registry lookup
    # ------------------------------------------------------------------
    if ctx.registry is not None:
        cached = ctx.registry.lookup(node.plugin)
        if cached is not None:
            ctx.warnings.append(
                f"Tool {node.tool_id} ({node.tool_type}): used cached registry entry "
                f"for plugin '{node.plugin}'."
            )
            return CTEFragment(
                name=cte_name,
                sql=cached.sql_body,
                source_tool_ids=[node.tool_id],
            )

    # ------------------------------------------------------------------
    # Step 2 — LLM translation
    # ------------------------------------------------------------------
    sql_body = translate_chunk_llm(
        tool_type=node.tool_type,
        plugin=node.plugin,
        config=node.config,
        input_ctes=input_ctes,
    )

    llm_failed = sql_body.startswith("-- LLM translation failed")

    if not llm_failed:
        ctx.warnings.append(
            f"Tool {node.tool_id} ({node.tool_type}): no deterministic translator — "
            f"LLM-generated CTE for plugin '{node.plugin}'."
        )
        if ctx.registry is not None:
            entry = make_entry(
                plugin=node.plugin,
                tool_type=node.tool_type,
                description=f"LLM-translated {node.tool_type} tool",
                sql_body=sql_body,
                config=node.config,
            )
            ctx.registry.save(entry)
        return CTEFragment(name=cte_name, sql=sql_body, source_tool_ids=[node.tool_id])

    # ------------------------------------------------------------------
    # Step 3 — Hard stub
    # ------------------------------------------------------------------
    ctx.warnings.append(
        f"Tool {node.tool_id} ({node.tool_type}): no translator and LLM failed — "
        f"plugin: '{node.plugin}'. Stub CTE emitted, review manually."
    )
    sql = (
        f"-- UNKNOWN TOOL TYPE: {node.tool_type}\n"
        f"-- Plugin: {node.plugin}\n"
        f"{upstream_comment}\n"
        f"SELECT TOP 0 1 AS _unknown_stub"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)
