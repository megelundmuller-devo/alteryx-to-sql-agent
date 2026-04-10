"""Macro handler — Phase 5.

Attempts to inline macro workflows (.yxmc files) into the main SQL output.
If a .yxmc file is available on disk it is parsed, chunked, and translated
recursively.  If it is not available a clearly labelled stub is returned.

Strategy
--------
1. Locate the .yxmc file relative to the source .yxmd file's directory.
2. If found: parse → build_dag → chunk_dag → translate each chunk → return
   the fragment list for inlining.
3. If not found: return a single stub CTEFragment with a warning.

Recursive macros (macros calling other macros) are handled up to
MAX_MACRO_DEPTH levels deep; beyond that a stub is returned.
"""

from __future__ import annotations

from pathlib import Path

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext

MAX_MACRO_DEPTH: int = 3  # prevent infinite recursion


def expand_macro(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
    source_dir: Path,
    depth: int = 0,
) -> list[CTEFragment]:
    """Try to inline a macro; fall back to stub if the .yxmc is unavailable.

    Args:
        node:       The macro ToolNode.
        cte_name:   The output CTE name for this macro's result.
        input_ctes: CTE names providing input data to the macro.
        ctx:        Shared TranslationContext.
        source_dir: Directory of the parent .yxmd file.
        depth:      Current recursion depth (to enforce MAX_MACRO_DEPTH).

    Returns:
        List of CTEFragments — either the inlined macro CTEs or a single stub.
    """
    macro_path = node.macro_path
    if not macro_path:
        ctx.warnings.append(
            f"Tool {node.tool_id} (macro): no macro_path set — stub emitted."
        )
        return [_stub(node, cte_name, "no macro_path")]

    if depth >= MAX_MACRO_DEPTH:
        ctx.warnings.append(
            f"Tool {node.tool_id} (macro): max recursion depth "
            f"({MAX_MACRO_DEPTH}) reached for '{macro_path}' — stub emitted."
        )
        return [_stub(node, cte_name, f"max depth {MAX_MACRO_DEPTH} reached")]

    # Resolve path relative to the source directory
    candidate = (source_dir / macro_path).resolve()
    if not candidate.exists():
        # Try just the filename in the same directory
        candidate = (source_dir / Path(macro_path).name).resolve()

    if not candidate.exists():
        ctx.warnings.append(
            f"Tool {node.tool_id} (macro): .yxmc file not found at '{macro_path}' "
            f"(searched relative to '{source_dir}') — stub emitted."
        )
        return [_stub(node, cte_name, f"file not found: {macro_path}")]

    # Inline the macro
    try:
        from chunking.chunker import chunk_dag
        from parsing.dag import build_dag
        from parsing.parser import parse_workflow
        from translators import translate_chunk

        macro_workflow = parse_workflow(candidate)
        macro_dag = build_dag(macro_workflow)
        macro_chunks = chunk_dag(macro_dag)

        # Use a nested TranslationContext so warnings bubble up
        # but chain_cte is isolated to the macro scope.
        macro_ctx = TranslationContext(dag=macro_dag, warnings=ctx.warnings)

        # Build chain_cte for the macro's chunks
        for chunk in macro_chunks:
            for n in chunk.nodes:
                macro_ctx.chain_cte[n.tool_id] = chunk.output_cte_name

        all_fragments: list[CTEFragment] = []
        for chunk in macro_chunks:
            frags = translate_chunk(chunk, macro_ctx)
            all_fragments.extend(frags)

        # Rename the last fragment's output to match our expected cte_name
        if all_fragments:
            last = all_fragments[-1]
            all_fragments[-1] = CTEFragment(
                name=cte_name,
                sql=last.sql,
                source_tool_ids=last.source_tool_ids,
                is_stub=last.is_stub,
            )

        ctx.warnings.append(
            f"Tool {node.tool_id} (macro): successfully inlined '{candidate.name}' "
            f"({len(all_fragments)} CTEs)."
        )
        return all_fragments

    except Exception as exc:  # noqa: BLE001
        ctx.warnings.append(
            f"Tool {node.tool_id} (macro): failed to inline "
            f"'{macro_path}': {exc!s:.120} — stub emitted."
        )
        return [_stub(node, cte_name, str(exc)[:200])]


def _stub(node: ToolNode, cte_name: str, reason: str) -> CTEFragment:
    sql = (
        f"-- TODO: expand macro '{node.macro_path}'\n"
        f"-- Reason stub was emitted: {reason}\n"
        f"SELECT TOP 0 1 AS _macro_stub"
    )
    return CTEFragment(
        name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
    )
