"""Workflow documentation generator.

Renders the _docs.md output file for a converted workflow.  The file has two
parts:

1. LLM-generated narrative  — an overview of what the workflow does in plain
   English, produced by llm.doc_agent.generate_workflow_summary.

2. Deterministic index  — generated directly from the pipeline objects:
   - Workflow stats (tool count, CTE count, stub count)
   - Data flow: source tools → processing steps → sink tools (from the DAG)
   - CTE index: every CTE mapped to the Alteryx tool(s) it came from
   - Warnings: manual review items from the translator

The LLM receives a structured description of the workflow built from the same
pipeline objects, so it has full context without needing the raw XML.

Entry point:
    from doc_gen.doc_writer import generate_docs

    markdown = generate_docs(workflow_path, dag, chunks, all_fragments, warnings)
"""

from __future__ import annotations

from pathlib import Path

from chunking.chunker import Chunk
from llm.doc_agent import generate_workflow_summary
from parsing.dag import AlteryxDAG
from parsing.models import CTEFragment


def _build_llm_prompt(
    workflow_name: str,
    dag: AlteryxDAG,
    chunks: list[Chunk],
    all_fragments: list[CTEFragment],
    warnings: list[str],
) -> str:
    """Build a structured prompt describing the workflow for the LLM."""
    lines: list[str] = [f"Workflow: {workflow_name}", ""]

    sources = dag.source_nodes()
    sinks = dag.sink_nodes()

    lines.append(f"Sources ({len(sources)}):")
    for n in sources:
        ann = f" — {n.annotation}" if n.annotation else ""
        lines.append(f"  - {n.tool_type}{ann}")

    lines.append(f"\nSinks ({len(sinks)}):")
    for n in sinks:
        ann = f" — {n.annotation}" if n.annotation else ""
        lines.append(f"  - {n.tool_type}{ann}")

    lines.append(f"\nProcessing steps ({len(chunks)} chunk(s), {dag.node_count()} tools):")
    for chunk in chunks:
        tool_label = " → ".join(
            f"{n.tool_type}({n.annotation})" if n.annotation else n.tool_type for n in chunk.nodes
        )
        stub_flag = (
            " [STUB — needs manual review]"
            if any(f.is_stub for f in all_fragments if f.name == chunk.output_cte_name)
            else ""
        )
        lines.append(f"  - [{chunk.output_cte_name}] {tool_label}{stub_flag}")

    stubs = [f for f in all_fragments if f.is_stub]
    lines.append(f"\nTotal CTEs: {len(all_fragments)}  Stubs: {len(stubs)}")

    if warnings:
        lines.append(f"\nWarnings ({len(warnings)}):")
        for w in warnings:
            lines.append(f"  - {w}")
    else:
        lines.append("\nWarnings: none")

    lines.append("\nWrite the documentation.")
    return "\n".join(lines)


def _render_markdown(
    workflow_path: Path,
    dag: AlteryxDAG,
    chunks: list[Chunk],
    all_fragments: list[CTEFragment],
    warnings: list[str],
    narrative: str,
) -> str:
    """Render the full _docs.md Markdown string."""
    stubs = [f for f in all_fragments if f.is_stub]
    lines: list[str] = []

    # ── Header ───────────────────────────────────────────────────────────────
    lines += [
        f"# {workflow_path.stem}",
        "",
        f"Source: `{workflow_path.name}`  ",
        f"Tools: {dag.node_count()} | "
        f"Chunks: {len(chunks)} | "
        f"CTEs: {len(all_fragments)} | "
        f"Stubs: {len(stubs)}",
        "",
    ]

    # ── Overview (LLM narrative or fallback) ─────────────────────────────────
    lines.append("## Overview")
    lines.append("")
    if narrative:
        lines.append(narrative)
    else:
        lines.append("_Documentation generation unavailable. See sections below._")
    lines.append("")

    # ── Data flow ─────────────────────────────────────────────────────────────
    lines.append("## Data Flow")
    lines.append("")
    lines.append("**Sources**")
    lines.append("")
    for n in dag.source_nodes():
        ann = f" — {n.annotation}" if n.annotation else ""
        lines.append(f"- `{n.tool_type}` (tool {n.tool_id}){ann}")
    lines.append("")
    lines.append("**Processing** (topological order)")
    lines.append("")
    for chunk in chunks:
        tool_label = " → ".join(
            f"`{n.tool_type}`" + (f" _{n.annotation}_" if n.annotation else "") for n in chunk.nodes
        )
        reads = (
            f"  reads: {', '.join(f'`{c}`' for c in chunk.input_cte_names)}"
            if chunk.input_cte_names
            else ""
        )
        lines.append(f"- `{chunk.output_cte_name}`: {tool_label}{reads}")
    lines.append("")
    lines.append("**Sinks**")
    lines.append("")
    for n in dag.sink_nodes():
        ann = f" — {n.annotation}" if n.annotation else ""
        lines.append(f"- `{n.tool_type}` (tool {n.tool_id}){ann}")
    lines.append("")

    # ── CTE index ────────────────────────────────────────────────────────────
    lines.append("## CTEs")
    lines.append("")
    for frag in all_fragments:
        stub_flag = " ⚠ stub" if frag.is_stub else ""
        lines.append(f"- `{frag.name}`{stub_flag}")
    lines.append("")

    # ── Warnings ──────────────────────────────────────────────────────────────
    if warnings:
        lines.append("## Warnings")
        lines.append("")
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines)


def generate_docs(
    workflow_path: Path,
    dag: AlteryxDAG,
    chunks: list[Chunk],
    all_fragments: list[CTEFragment],
    warnings: list[str],
) -> str:
    """Generate the full _docs.md content for a converted workflow.

    Calls the LLM to produce a narrative overview, then combines it with a
    deterministic index of the data flow, CTEs, and warnings.

    Args:
        workflow_path: Path to the source .yxmd file (stem used as the title).
        dag:           The AlteryxDAG built in Phase 1.
        chunks:        Ordered list of Chunk objects from Phase 2.
        all_fragments: Ordered list of CTEFragments from Phase 3.
        warnings:      Translation warnings collected in TranslationContext.

    Returns:
        Complete Markdown string ready to write to <stem>_docs.md.
    """
    prompt = _build_llm_prompt(workflow_path.stem, dag, chunks, all_fragments, warnings)
    narrative = generate_workflow_summary(prompt)
    return _render_markdown(workflow_path, dag, chunks, all_fragments, warnings, narrative)
