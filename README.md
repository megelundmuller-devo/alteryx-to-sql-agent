# Alteryx to SQL

Convert Alteryx Designer workflows (`.yxmd`) to readable T-SQL CTE chains.
Each Alteryx tool becomes one or more named CTEs; the full workflow becomes a
single, runnable SQL script.  Unknown tool types are handled by an LLM
(Gemini on Vertex AI) and the result is cached in a local registry so the same
plugin is never translated twice.

---

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) — used for all package management and script execution
- A Google Cloud project with Vertex AI enabled
- `gcloud` CLI authenticated with application-default credentials

---

## Installation

```bash
git clone <repo-url>
cd alteryx-to-sql-agent

# Install all dependencies
uv sync
```

---

## Configuration

Create a `.env` file in the project root (copy the example below):

```bash
VERTEX_PROJECT=my-gcp-project
VERTEX_LOCATION=europe-west1
VERTEX_MODEL=google-vertex:gemini-2.5-pro
LLM_MAX_RETRIES=3
```

Authenticate with Google Cloud:

```bash
gcloud auth application-default login
```

---

## Usage

### Convert a workflow

```bash
uv run python src/main.py workflow.yxmd
```

Writes two files next to the input by default:

| File | Contents |
|---|---|
| `workflow.sql` | Generated T-SQL CTE script |
| `workflow_docs.md` | Human-readable documentation of the workflow and each CTE |

### Specify an output directory

```bash
uv run python src/main.py workflow.yxmd --output-dir output/
```

### Inspect without translating

```bash
uv run python src/main.py workflow.yxmd --dry-run
```

Prints a table of chunks (CTE groups) and the data flow between them.
No SQL is generated and no files are written.

### Skip documentation

```bash
uv run python src/main.py workflow.yxmd --no-docs
```

Writes `workflow.sql` only — skips the LLM documentation step.

---

## Tool Registry

Unknown Alteryx tool types are translated by the LLM once, then cached in a
registry at `~/.alteryx_to_sql/tool_registry.json`.  Subsequent runs return
the cached SQL with zero API cost.

```bash
# Inspect what has been learned
uv run python src/main.py --show-registry

# Wipe the registry and start fresh
uv run python src/main.py --clear-registry

# Disable the registry for one run (always calls the LLM)
uv run python src/main.py workflow.yxmd --no-registry

# Use a project-local registry instead of the user-global one
TOOL_REGISTRY_PATH=./my_registry.json uv run python src/main.py workflow.yxmd
```

---

## Development

```bash
# Run all tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Format
uv run ruff format src/ tests/
```

Or use the Makefile shortcuts:

```bash
make install   # uv sync
make test      # uv run pytest
make lint      # ruff check
make format    # ruff format
```

---

## Project Structure

```
src/
  main.py                # CLI entry point
  doc_writer.py          # Workflow documentation generator
  tool_registry.py       # Persistent registry for learned tool translations
  parsing/
    parser.py            # .yxmd XML → ToolNode + Connection models
    dag.py               # networkx DAG construction and traversal
    models.py            # All Pydantic domain models
  chunking/
    chunker.py           # DAG → ordered list of translation chunks
  translators/
    __init__.py          # Translator registry and dispatch
    filter.py            # FilterData tool
    formula.py           # Formula tool
    join.py              # Join tool
    union.py             # Union tool
    select.py            # Select tool
    summarize.py         # Summarize tool
    sort.py              # Sort tool
    sample.py            # Sample tool
    multirow.py          # Multi-Row Formula tool
    append.py            # Append Fields tool
    find_replace.py      # Find Replace tool
    input_output.py      # Input Data / Output Data tools
    unknown.py           # LLM fallback + registry cascade
    context.py           # TranslationContext (shared state across translators)
  assembly/
    cte_builder.py       # CTEFragment list → final SQL string
    macro_handler.py     # Macro stub generation
  llm/
    chunk_agent.py       # pydantic-ai agent for unknown tool translation
    expression_agent.py  # pydantic-ai agent for formula/filter expressions
    doc_agent.py         # pydantic-ai agent for workflow narrative
    prompts.py           # System prompts and few-shot examples
    settings.py          # LLM configuration from environment
tests/
  fixtures/              # Sample .yxmd XML and expected SQL
  parsing/               # Parser and DAG tests
  chunking/              # Chunker tests
  translators/           # Translator tests (LLM paths mocked)
  assembly/              # CTE builder tests
  test_tool_registry.py  # Registry unit tests (thread-safety included)
  test_doc_writer.py     # Documentation generator tests
  test_main.py           # CLI integration tests
docs/
  PROJECT_PLAN.md        # Master implementation checklist
  phase3_translation.md  # Deterministic translator details
  phase4_llm.md          # LLM integration (pydantic-ai + Vertex AI)
  phase5_assembly.md     # CTE assembly and macro handling
  phase6_cli.md          # CLI, progress output, doc generation
  phase7_registry.md     # Tool registry design and thread safety
```

---

## Documentation

- [Project Plan](docs/PROJECT_PLAN.md)
- [Translation](docs/phase3_translation.md)
- [LLM Integration](docs/phase4_llm.md)
- [SQL Assembly](docs/phase5_assembly.md)
- [CLI & Documentation](docs/phase6_cli.md)
- [Tool Registry](docs/phase7_registry.md)
