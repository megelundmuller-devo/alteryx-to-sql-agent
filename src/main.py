"""CLI entry point — runs the full pipeline on one or more .yxmd files.

Usage (single file):
    uv run python src/main.py examples/my_workflow.yxmd
    uv run python src/main.py examples/my_workflow.yxmd --output-dir output/
    uv run python src/main.py examples/my_workflow.yxmd --dry-run
    uv run python src/main.py examples/my_workflow.yxmd --no-docs
    uv run python src/main.py examples/my_workflow.yxmd --ai-enhanced

Usage (batch — all .yxmd files in a directory):
    uv run python src/main.py examples/
    uv run python src/main.py examples/ --output-dir output/

Other flags:
    uv run python src/main.py examples/my_workflow.yxmd --no-registry
    uv run python src/main.py --show-registry
    uv run python src/main.py --clear-registry

Output files written to the output directory (default: same dir as input):
    <name>.sql           — generated T-SQL stored procedure
    <name>_docs.md       — human-readable workflow documentation
    <name>_enhanced.sql  — AI-simplified version (only with --ai-enhanced)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# Ensure src/ is importable when running as `python src/main.py`
sys.path.insert(0, str(Path(__file__).parent))

from analysis.liveness import run_liveness_pass
from assembly.source_simplifier import simplify_stub_sources
from llm.sql_enhancer import enhance_sql
from analysis.llm_validator import repair_fragments
from assembly.cte_builder import build_sql
from chunking.chunker import chunk_dag
from doc_gen.doc_writer import generate_docs
from parsing.dag import build_dag
from parsing.parser import parse_workflow
from registry.tool_registry import ToolRegistry, default_registry
from translators import translate_chunk
from translators.context import TranslationContext

console = Console()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Alteryx .yxmd workflow(s) to T-SQL stored procedures.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "workflow",
        type=Path,
        nargs="?",
        help="Path to a .yxmd file or a directory containing .yxmd files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directory to write output files (default: same directory as input)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and chunk only — print summary without translating or writing files",
    )
    parser.add_argument(
        "--no-docs",
        action="store_true",
        help="Skip writing the _docs.md file — produce the .sql file only",
    )
    parser.add_argument(
        "--no-registry",
        action="store_true",
        help="Disable the tool registry — unknown tools always go to the LLM",
    )
    parser.add_argument(
        "--clear-registry",
        action="store_true",
        help="Wipe the tool registry before running",
    )
    parser.add_argument(
        "--show-registry",
        action="store_true",
        help="Print all learned registry entries and exit",
    )
    parser.add_argument(
        "--ai-enhanced",
        action="store_true",
        help=(
            "After writing the .sql file, send it to Gemini Flash in one shot "
            "and write a simplified version to <name>_enhanced.sql"
        ),
    )
    return parser.parse_args()


def _process_one(
    workflow_path: Path,
    output_dir: Path,
    args: argparse.Namespace,
    registry: ToolRegistry | None,
) -> list[str]:
    """Run the full pipeline for a single .yxmd file.

    Returns the list of warning strings emitted during translation.
    Raises on any unrecoverable error so the batch caller can catch and continue.
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        # Phase 1 — Parse
        task = progress.add_task("Parsing workflow...", total=None)
        parsed = parse_workflow(workflow_path)
        dag = build_dag(parsed)
        progress.remove_task(task)

        console.print(f"  [bold]Parsed[/bold] {workflow_path.name}")
        console.print(
            f"    {dag.node_count()} tools  •  {dag.edge_count()} connections  •  "
            f"{len(dag.source_nodes())} source(s)  •  {len(dag.sink_nodes())} sink(s)"
        )

        # Phase 2 — Chunk
        task = progress.add_task("Chunking DAG...", total=None)
        chunks = chunk_dag(dag)
        progress.remove_task(task)

        console.print(
            f"    [bold]Chunked[/bold] → {len(chunks)} chunk(s) from {dag.node_count()} tools"
        )

        if args.dry_run:
            _print_chunk_table(chunks)
            console.print("\n[yellow]--dry-run:[/yellow] stopping before translation.")
            return []

        # Phase 3 — Translate
        task = progress.add_task("Translating chunks...", total=None)
        ctx = TranslationContext(dag=dag, registry=None if args.no_registry else registry)
        all_fragments = []
        for chunk in chunks:
            all_fragments.extend(translate_chunk(chunk, ctx))
        progress.remove_task(task)

        stubs = [f for f in all_fragments if f.is_stub]
        stub_label = (
            f"[yellow]{len(stubs)} stub(s) require review[/yellow]"
            if stubs
            else "[green]0 stubs[/green]"
        )
        console.print(f"    [bold]Translated[/bold] → {len(all_fragments)} CTE(s)  •  {stub_label}")

        # Phase 3b — Liveness pass (deterministic SELECT widening)
        task = progress.add_task("Running liveness pass...", total=None)
        all_fragments, gap_warnings = run_liveness_pass(all_fragments, ctx)
        ctx.warnings.extend(gap_warnings)
        progress.remove_task(task)
        if gap_warnings:
            console.print(
                f"    [bold]Liveness[/bold] → "
                f"[yellow]{len(gap_warnings)} gap(s) passed to LLM repair[/yellow]"
            )

        # Phase 3c — Column validation (flags remaining unresolvable references)
        task = progress.add_task("Validating column references...", total=None)
        all_fragments = repair_fragments(all_fragments, ctx)
        progress.remove_task(task)
        flagged = [f for f in all_fragments if f.llm_repair_notes]
        if flagged:
            console.print(
                f"    [bold]Validation[/bold] → "
                f"[yellow]{len(flagged)} CTE(s) flagged[/yellow] with unresolvable column references  "
                f"[dim](marked ⚠ in docs — review before production)[/dim]"
            )

        # Phase 5 — Assemble SQL
        task = progress.add_task("Assembling SQL...", total=None)
        source_ids = {n.tool_id for n in dag.source_nodes()}
        sink_ids = {n.tool_id for n in dag.sink_nodes()}
        sql = build_sql(
            all_fragments,
            workflow_name=workflow_path.name,
            source_ids=source_ids,
            sink_ids=sink_ids,
            engine_vars=ctx.engine_vars,
        )
        progress.remove_task(task)

        stem = workflow_path.stem
        sql_path = output_dir / f"{stem}.sql"
        sql = simplify_stub_sources(sql)
        sql_path.write_text(sql, encoding="utf-8")
        console.print(f"    [bold]SQL written to[/bold]  {sql_path}")

        # AI-enhanced simplification — one Gemini Flash call, full file in one shot
        if args.ai_enhanced:
            enhance_task = progress.add_task("Enhancing SQL with Gemini Flash...", total=None)
            try:
                enhanced_sql = enhance_sql(sql)
                enhanced_path = output_dir / f"{stem}_enhanced.sql"
                enhanced_path.write_text(enhanced_sql, encoding="utf-8")
                console.print(f"    [bold]Enhanced SQL written to[/bold]  {enhanced_path}")
            except Exception as exc:  # noqa: BLE001
                console.print(f"    [yellow]⚠  AI enhancement failed:[/yellow] {exc}")
            finally:
                progress.remove_task(enhance_task)

        # Workflow documentation
        if not args.no_docs:
            doc_task = progress.add_task("Generating documentation...", total=None)
            docs_md = generate_docs(workflow_path, dag, chunks, all_fragments, ctx.warnings)
            progress.remove_task(doc_task)

            docs_path = output_dir / f"{stem}_docs.md"
            docs_path.write_text(docs_md, encoding="utf-8")
            console.print(f"    [bold]Docs written to[/bold] {docs_path}")

    return ctx.warnings


def main() -> None:
    args = _parse_args()

    registry = default_registry()

    # Registry-only commands (exit early, no workflow needed)
    if args.show_registry:
        _show_registry(registry)
        return

    if args.clear_registry:
        registry.clear()
        console.print(f"[green]Registry cleared:[/green] {registry.path}")

    if args.workflow is None:
        console.print("[bold red]ERROR:[/bold red] workflow path is required.")
        sys.exit(1)

    workflow_arg: Path = args.workflow.resolve()

    if not workflow_arg.exists():
        console.print(f"[bold red]ERROR:[/bold red] path not found: {workflow_arg}")
        sys.exit(1)

    # Collect files to process
    if workflow_arg.is_dir():
        files = sorted(workflow_arg.glob("*.yxmd"))
        if not files:
            console.print(f"[yellow]No .yxmd files found in {workflow_arg}[/yellow]")
            sys.exit(1)
        default_output = workflow_arg
    else:
        files = [workflow_arg]
        default_output = workflow_arg.parent

    output_dir: Path = (args.output_dir or default_output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if len(files) > 1:
        console.print(
            f"[bold]Batch:[/bold] {len(files)} workflow(s) in {workflow_arg.name}/  →  {output_dir}"
        )

    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    for workflow_path in files:
        if len(files) > 1:
            console.print(f"\n[cyan]▶[/cyan] {workflow_path.name}")

        try:
            warnings = _process_one(workflow_path, output_dir, args, registry)
            succeeded.append(workflow_path.name)
            if warnings:
                console.print(f"    [yellow]{len(warnings)} warning(s):[/yellow]")
                for w in warnings:
                    console.print(f"      [yellow]⚠[/yellow]  {escape(w)}")
        except Exception as exc:  # noqa: BLE001
            failed.append((workflow_path.name, str(exc)))
            console.print(f"  [bold red]ERROR:[/bold red] {exc}")

    # Batch summary
    if len(files) > 1:
        console.print(
            f"\n[bold]Batch complete:[/bold] "
            f"[green]{len(succeeded)} succeeded[/green]"
            + (f"  [red]{len(failed)} failed[/red]" if failed else "")
        )
        for name, err in failed:
            console.print(f"  [red]✗[/red]  {name}: {err}")


def _show_registry(registry: ToolRegistry) -> None:
    """Print all tool registry entries as a rich table."""
    entries = registry.all_entries()
    if not entries:
        console.print(f"Registry is empty. ({registry.path})")
        return

    table = Table(show_header=True, header_style="bold", title=f"Tool Registry ({registry.path})")
    table.add_column("Plugin", style="cyan", no_wrap=False)
    table.add_column("Tool type")
    table.add_column("Learned at")
    table.add_column("Config hash")

    for e in entries:
        table.add_row(e.plugin, e.tool_type, e.learned_at[:19], e.example_config_hash)

    console.print(table)
    console.print(f"{len(entries)} entry(ies).")


def _print_chunk_table(chunks: list) -> None:
    """Render the chunk list as a rich table for --dry-run output."""
    table = Table(show_header=True, header_style="bold")
    table.add_column("CTE name", style="cyan")
    table.add_column("Tool(s)")
    table.add_column("Reads from")

    for chunk in chunks:
        tool_types = " → ".join(n.tool_type for n in chunk.nodes)
        inputs = ", ".join(chunk.input_cte_names) if chunk.input_cte_names else "—"
        table.add_row(chunk.output_cte_name, tool_types, inputs)

    console.print(table)


if __name__ == "__main__":
    main()
