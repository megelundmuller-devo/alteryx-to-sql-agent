# Phase 6 — Orchestration, CLI & Documentation

## Purpose

Phase 6 wires together the full conversion pipeline (Phases 1–5) behind a command-line interface with progress feedback, and generates the companion `_docs.md` documentation file using an LLM-backed doc agent fed directly from the pipeline data.

---

## Modules Written

### `src/main.py`

The CLI entry point.  Run with:

```bash
uv run python src/main.py <workflow.yxmd> [options]
```

**Arguments:**

| Argument | Type | Default | Description |
|---|---|---|---|
| `workflow` | positional | — | Path to the `.yxmd` file to convert |
| `--output-dir DIR` | option | input file's directory | Directory to write output files |
| `--dry-run` | flag | off | Parse + chunk only; print summary table without translating |
| `--no-docs` | flag | off | Skip writing `_docs.md`; produce `.sql` file only |

**Pipeline phases executed:**

| Phase | Step | Output |
|---|---|---|
| 1 | Parse `.yxmd` → `ToolNode` list + `AlteryxDAG` | Tool/connection counts printed |
| 2 | `chunk_dag` → `list[Chunk]` | Chunk count printed; table shown on `--dry-run` |
| 3 | `translate_chunk` for each chunk → `list[CTEFragment]` | CTE + stub counts printed |
| 5 | `build_sql` → T-SQL string written to `<stem>.sql` | Path printed |
| 6 | `generate_docs` → Markdown written to `<stem>_docs.md` (unless `--no-docs`) | Path printed |

**Progress output** uses `rich.progress` with a spinner and elapsed time column.  Each phase spins while running and disappears when done, so only the completion lines remain visible in the terminal.

**`--dry-run` output** renders the chunk list as a `rich.table.Table` with three columns: CTE name, tool type chain, and upstream inputs.

**Warnings** are printed after all progress activity completes, each prefixed with a yellow `⚠` symbol.

---

### `src/llm/doc_agent.py`

```python
def generate_workflow_summary(prompt: str) -> str
```

Calls `Agent('google-vertex:gemini-2.5-pro', output_type=str)` with a structured prompt built by `doc_writer.py`.  Returns the LLM's Markdown narrative, or an empty string on any failure.  The caller (`_render_markdown`) substitutes a fallback line when the string is empty.

---

### `src/doc_writer.py`

```python
def generate_docs(
    workflow_path: Path,
    dag: AlteryxDAG,
    chunks: list[Chunk],
    all_fragments: list[CTEFragment],
    warnings: list[str],
) -> str
```

Generates the full `_docs.md` content.  Receives the pipeline objects directly from `main.py` — the same DAG, chunks, and fragments already produced — so no re-parsing or re-processing is needed.

**Internal flow:**

1. `_build_llm_prompt` — constructs a structured text description of the workflow (sources, sinks, processing steps with annotations, stub count, warnings) and passes it to `generate_workflow_summary`.
2. `_render_markdown` — assembles the final Markdown document from four sections:

| Section | How produced |
|---|---|
| **Header** | Deterministic: workflow name, tool/CTE/stub counts |
| **Overview** | LLM-generated narrative (or fallback if LLM unavailable) |
| **Data Flow** | Deterministic: sources → processing chunks → sinks from the DAG |
| **CTEs** | Deterministic: indexed list of all CTEs with stub flags |
| **Warnings** | Deterministic: manual review items from `TranslationContext.warnings` |

---

### `src/assembly/macro_handler.py`

Handles macro tool nodes encountered during translation.  External `.yxmc` macro files always produce a stub `CTEFragment` with a warning — macro expansion is not implemented because:

- `.yxmc` files are specific to each Alteryx installation and cannot be assumed to be present at conversion time.
- Macros may reference organisation-specific plugins with no SQL equivalent.

A `MAX_MACRO_DEPTH = 3` constant is defined for future use if inline expansion is added.

---

## Design Decisions

### Why does doc generation receive the pipeline objects, not a summary string?

The doc writer receives the `AlteryxDAG`, `list[Chunk]`, and `list[CTEFragment]` directly from `main.py` — the same objects already in memory.  This means:
- The LLM prompt contains accurate, structured data (tool annotations, source/sink topology, stub flags) rather than a string the translator happened to emit.
- The deterministic sections (Data Flow, CTE index) are always correct regardless of LLM availability.
- Adding richer context to the prompt in future (e.g. column schemas from `ToolNode.output_schema`) requires no interface changes.

### Why is the LLM prompt built in `doc_writer.py` rather than in `doc_agent.py`?

`doc_agent.py` is kept thin: it knows only about the LLM agent, not about pipeline objects.  `doc_writer.py` owns the prompt construction because it has the full context.  This separation makes each module independently testable — the agent can be mocked with a fixed string, and the prompt builder can be tested without any LLM.

### Why return empty string from `generate_workflow_summary` on failure?

The doc writer must always produce a usable file even when the LLM is unavailable (no credentials, network error, quota exhaustion).  An empty string triggers a one-line fallback message in `_render_markdown`, keeping the rest of the document intact.  The file is still useful — it contains the full CTE index and warnings.

### Why `--no-docs` rather than always writing the file?

For scripted or automated runs the `_docs.md` file may be discarded.  `--no-docs` skips the LLM call entirely, reducing cost and latency for batch conversions.

### Why `rich` for output rather than plain `print`?

The pipeline takes 10–30 seconds for large workflows when LLM calls are involved.  `rich`'s spinner + elapsed time makes latency visible.  `transient=True` keeps the final output clean.

### Why always stub external macros?

Confirmed with the project owner: macros from external `.yxmc` files should always emit stubs.  The tool cannot make assumptions about macro availability, and silently skipping a macro would corrupt the output SQL.

---

## Test Coverage

**`tests/test_main.py`** — 4 tests

| Test | Behaviour verified |
|---|---|
| `test_main_dry_run` | Prints parse + chunk output, writes no files |
| `test_main_full_pipeline` | Writes `.sql` and `_docs.md` with correct content |
| `test_main_no_docs` | Writes `.sql` only, no `_docs.md` |
| `test_main_missing_file` | Exits with code 1 and error message |

**`tests/test_doc_writer.py`** — 13 tests (LLM mocked throughout)

| Test class | Behaviour verified |
|---|---|
| `TestGenerateDocs` | Header, Overview section, LLM narrative included, fallback on empty narrative, Data Flow section, sources/sinks from annotations, CTE index, stub flagging, Warnings section, no Warnings when empty, stats line |
| `TestDocAgent` | Empty string returned on exception, `result.output` stripped and returned on success |

---

## Example Usage

```bash
# Convert a workflow, write output next to the input file
uv run python src/main.py examples/BI_Aggregate\ Daily\ Simple_LDB-01.yxmd

# Write output to a specific directory
uv run python src/main.py workflow.yxmd --output-dir output/

# Preview chunks without translating
uv run python src/main.py workflow.yxmd --dry-run

# Produce SQL only, skip the docs file (no LLM doc call)
uv run python src/main.py workflow.yxmd --output-dir output/ --no-docs
```

**Example terminal output (full run):**

```
Parsed BI_Aggregate Daily Simple_LDB-01.yxmd
  51 tools  •  53 connections  •  6 source(s)  •  2 sink(s)
Chunked → 42 chunk(s) from 51 tools
Translated → 51 CTE(s)  •  2 stub(s) require review
SQL written to   output/BI_Aggregate Daily Simple_LDB-01.sql
Docs written to  output/BI_Aggregate Daily Simple_LDB-01_docs.md

3 warning(s):
  ⚠  Tool 31 (macro): references 'Cleanse.yxmc'. Stub CTE emitted.
  ⚠  Tool 52 (macro): references 'Cleanse.yxmc'. Stub CTE emitted.
  ⚠  Tool 49 (db_file_output): writes to [Aggregated_Daily_simple] ...
```
