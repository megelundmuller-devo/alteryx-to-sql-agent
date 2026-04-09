# Alteryx-to-MSSQL Conversion Agent — Project Plan

## 1. Task Understanding

Alteryx workflows are stored as `.yxmd` XML files. Each file encodes a directed acyclic graph (DAG) where **nodes** are processing tools (filters, joins, formulas, etc.) and **edges** are data connections between them. Our goal is to:

1. Parse a `.yxmd` file into a structured DAG representation.
2. Chunk the DAG into translatable units.
3. Send each chunk to a **pydantic-ai** agent backed by **Google Vertex AI** with a structured prompt.
4. Assemble the LLM responses into a single, executable **T-SQL (MSSQL)** script of chained CTEs.
5. Generate a companion Markdown document describing both the original Alteryx workflow and the produced SQL.

The primary output per run is two files:
- `<name>.sql` — the generated T-SQL CTE chain
- `<name>_docs.md` — human-readable workflow + SQL documentation

---

## 2. Architecture

```
.yxmd file
    │
    ▼
┌─────────────────┐
│  XML Parser     │  src/parser.py
│  (ElementTree)  │  → reads raw XML, extracts tool nodes + connection wires
└────────┬────────┘
         │  list[ToolNode], list[Connection]
         ▼
┌─────────────────┐
│  DAG Builder    │  src/dag.py
│  (networkx)     │  → builds directed graph, validates, topo-sorts
└────────┬────────┘
         │  networkx.DiGraph (annotated with ToolNode data)
         ▼
┌─────────────────┐
│  Chunker        │  src/chunker.py
│                 │  → splits DAG into independently translatable Chunks
└────────┬────────┘
         │  list[Chunk]
         ▼
┌──────────────────────────────────┐
│  Translator Pipeline             │
│  ┌────────────────────────────┐  │
│  │  Tool Registry             │  │  src/translators/__init__.py
│  │  (tool_type → translator)  │  │  src/translators/<tool>.py
│  └────────────────────────────┘  │
│  ┌────────────────────────────┐  │
│  │  Prompt Builder            │  │  src/prompts.py
│  │  (chunk → agent prompt)    │  │
│  └────────────────────────────┘  │
│  ┌────────────────────────────┐  │
│  │  pydantic-ai Agent         │  │  (configured in each translator
│  │  (Vertex AI / Gemini)      │  │   that needs LLM assistance)
│  └────────────────────────────┘  │
└────────┬─────────────────────────┘
         │  list[CTEFragment]
         ▼
┌─────────────────┐       ┌─────────────────────┐
│  CTE Assembler  │       │  Doc Agent          │  src/doc_agent.py
│  src/cte_       │       │  (pydantic-ai)      │  → WorkflowDoc model
│  builder.py     │       │                     │
└────────┬────────┘       └──────────┬──────────┘
         │                           │
         ▼                           ▼
    <name>.sql               <name>_docs.md
```

---

## 3. Key Data Structures (Pydantic Models — all in `src/models.py`)

All domain objects are `pydantic.BaseModel` with `frozen=True` unless noted.

### ToolNode
```python
class ToolNode(BaseModel):
    model_config = ConfigDict(frozen=True)
    tool_id: int
    plugin: str          # e.g. "AlteryxBasePluginsGui.Filter.Filter"
    tool_type: str       # normalised short key, e.g. "filter"
    config: dict[str, Any]
    annotation: str
    position: tuple[int, int]
```

### Connection
```python
class Connection(BaseModel):
    model_config = ConfigDict(frozen=True)
    origin_id: int
    origin_anchor: str   # "Output", "True", "False", "Left", "Right"
    dest_id: int
    dest_anchor: str     # "Input", "Left", "Right"
```

### Chunk
```python
class Chunk(BaseModel):
    model_config = ConfigDict(frozen=True)
    nodes: list[ToolNode]
    edges: list[Connection]
    input_cte_names: list[str]
    output_cte_name: str
```

### CTEFragment
```python
class CTEFragment(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    sql: str             # body only: the part inside "name AS ( <sql> )"
    source_tool_ids: list[int]
    is_stub: bool = False
```

### ConversionResult
```python
class ConversionResult(BaseModel):
    sql: str                     # full assembled T-SQL script
    cte_fragments: list[CTEFragment]
    warnings: list[str]          # unresolved macros, unsupported tools, etc.
    workflow_doc: WorkflowDoc    # used to write _docs.md
```

### WorkflowDoc (output of doc_agent)
```python
class WorkflowDoc(BaseModel):
    workflow_summary: str
    alteryx_steps: list[AlterxStepDoc]
    sql_steps: list[SQLStepDoc]
    notes: list[str]             # caveats, manual review items
```

---

## 4. Module Checklist

### Phase 1 — Parsing & Graph **[DONE]**

- [x] **`src/models.py`** — Pydantic models: `ToolNode`, `Connection`, `Chunk`, `CTEFragment`, `ConversionResult`, `WorkflowDoc`, `AlteryxStepDoc`, `SQLStepDoc`
- [x] **`src/parser.py`** — `.yxmd` XML → `list[ToolNode]`, `list[Connection]`
  - Use `xml.etree.ElementTree` (stdlib)
  - Walk `<Nodes><Node>` elements; extract `ToolID`, `Plugin`, `Configuration`
  - Walk `<Connections><Connection>` elements; extract `Origin`/`Destination` anchor pairs
  - Normalise plugin string to a short `tool_type` key (e.g. `"filter"`, `"join"`)
- [x] **`src/dag.py`** — Build & validate DAG using `networkx.DiGraph`
  - Topological sort to determine CTE ordering
  - Detect source nodes (in-degree 0) and sink nodes (out-degree 0)
  - Flag multi-input tools (joins, unions) for special handling
  - Expose `get_linear_chains()` → consecutive single-I/O tool chains for chunking

### Phase 2 — Chunking **[DONE]**

- [x] **`src/chunking/chunker.py`** — DAG → `list[Chunk]`
  - Merge consecutive single-input/single-output tools into one chunk (reduces LLM calls)
  - Force chunk boundary before any multi-input tool (join/union) and after any branching tool (filter True/False outputs connected)
  - Assign stable CTE names: `cte_{tool_type}_{tool_id}`
  - Attach `input_cte_names` so translators know what to `SELECT FROM`

### Phase 3 — Translation Layer

- [ ] **`src/translators/__init__.py`** — Registry: `dict[str, TranslatorFn]`; `translate_chunk(chunk) -> CTEFragment`
- [ ] **`src/translators/input_output.py`** — `DbFileInput`, `DbFileOutput`, `OdbcInput`, `OdbcOutput`
  - Input → `SELECT * FROM [schema].[table]`
  - Output → no CTE (annotated in docs only); sink detection handled by assembler
- [ ] **`src/translators/filter.py`** — `Filter`
  - Deterministic for simple equality/range; pydantic-ai agent for complex Alteryx expressions
  - True branch CTE: `WHERE <expr>` ; False branch CTE: `WHERE NOT (<expr>)`
- [ ] **`src/translators/join.py`** — `Join`
  - Left / Right / Inner / Full joins mapped from Alteryx join config to T-SQL `JOIN ... ON`
- [ ] **`src/translators/union.py`** — `Union`
  - `UNION ALL` (default) or `UNION` when dedup flag set
- [ ] **`src/translators/select.py`** — `Select` (column chooser / renamer)
  - Emits `SELECT [col1], [col2] AS [alias], ...`
- [ ] **`src/translators/formula.py`** — `Formula` (computed columns)
  - pydantic-ai agent translates Alteryx expression syntax → T-SQL expression
  - See expression mapping table in Section 6
- [ ] **`src/translators/summarize.py`** — `Summarize`
  - `GroupBy` → `GROUP BY [col]`
  - `Sum`, `Count`, `Avg`, `Min`, `Max`, `CountDistinct` → aggregate functions
- [ ] **`src/translators/sort.py`** — `Sort`
  - Materialised as `ROW_NUMBER() OVER (ORDER BY ...)` CTE when not at sink; `ORDER BY` in final SELECT at sink
- [ ] **`src/translators/sample.py`** — `Sample`
  - `SELECT TOP N` for fixed-N; `WHERE RN <= N` via window CTE for percentage/conditional
- [ ] **`src/translators/multirow.py`** — `MultiRowFormula`
  - Window functions: `LAG`, `LEAD`, `ROW_NUMBER`, `RANK` as needed
- [ ] **`src/translators/append.py`** — `AppendFields`
  - `CROSS JOIN` (Cartesian); warn if record counts could cause explosion
- [ ] **`src/translators/find_replace.py`** — `FindReplace`
  - `REPLACE([col], find, replace)` or `CASE WHEN` for conditional replacement
- [ ] **`src/translators/unknown.py`** — AI-assisted fallback for unrecognised tools
  - First, invoke a pydantic-ai agent with the tool's full config dict, annotation, input schema, and input CTE name — ask it to infer intent and produce a T-SQL CTE body
  - If the agent succeeds, emit the generated CTE and mark `is_stub=False`
  - If the agent fails or returns low-confidence output, fall back to a commented stub CTE
  - Always add an entry to `ConversionResult.warnings` describing the unknown tool and what was done
  - Check the tool registry (see Phase 9) before calling the LLM — cache hit skips the agent call

### Phase 4 — LLM Integration (pydantic-ai)

- [ ] **`src/prompts.py`** — All system prompts and few-shot examples
  - `SYSTEM_PROMPT`: role, T-SQL rules, CTE conventions, MSSQL-specific constraints
  - `EXPRESSION_SYSTEM_PROMPT`: Alteryx → T-SQL expression translation with mapping table
  - `DOC_SYSTEM_PROMPT`: workflow documentation generation instructions
  - Few-shot examples for Formula, Filter, and MultiRowFormula translation
- [ ] **`src/translators/formula.py`** (pydantic-ai agent portion)
  - `Agent(model=settings.vertex_model, result_type=FormulaResult, system_prompt=EXPRESSION_SYSTEM_PROMPT)`
  - `FormulaResult(BaseModel)`: `expression: str`, `explanation: str`
- [ ] **`src/doc_agent.py`** — Workflow documentation agent
  - `Agent(model=settings.vertex_model, result_type=WorkflowDoc, system_prompt=DOC_SYSTEM_PROMPT)`
  - Receives full DAG summary + all CTEFragments as context
  - Returns validated `WorkflowDoc` Pydantic model
- [ ] **`src/settings.py`** — Pydantic `BaseSettings` reading env vars
  - `vertex_project`, `vertex_location`, `vertex_model`, `llm_max_retries`

### Phase 5 — Assembly & Output

- [ ] **`src/cte_builder.py`** — `list[CTEFragment]` → final T-SQL string
  - Deduplicates CTE names (appends `_2`, `_3` on conflict)
  - Emits `WITH [cte1] AS (\n  ...\n), [cte2] AS (\n  ...\n)\nSELECT * FROM [<sink_cte>];`
  - Optionally wraps in `INSERT INTO [schema].[table]` if output tool is present
  - Header comment block: source file, timestamp, tool count, warnings summary

### Phase 6 — Orchestration & CLI

- [ ] **`src/main.py`** — CLI entry point
  - `argparse`: `--input workflow.yxmd --output-dir ./output [--model google-vertex:gemini-2.0-flash] [--dry-run] [--no-docs]`
  - `--dry-run`: parse + chunk + print chunk summary, no LLM calls
  - `--no-docs`: skip doc_agent, produce SQL only
  - Wires full pipeline; writes `<stem>.sql` and `<stem>_docs.md` to output dir
  - Progress output via `rich`
- [ ] **`src/macro_handler.py`** — Macro expansion
  - Inline expansion when `.yxmc` file is on disk (recursive parse)
  - Stub CTE generation with warnings when file is missing

### Phase 7 — Tests & Fixtures

- [ ] **`tests/fixtures/`** — Sample `.yxmd` XML snippets (one per tool type) + expected SQL
- [ ] **`tests/test_parser.py`** — XML parsing, tool type normalisation, edge cases
- [ ] **`tests/test_dag.py`** — DAG construction, topo-sort, cycle detection, source/sink detection
- [ ] **`tests/test_chunker.py`** — Chunk boundary rules (branch, join, linear merge)
- [ ] **`tests/test_translators.py`** — All deterministic translators (no LLM); mock for LLM-assisted ones
- [ ] **`tests/test_cte_builder.py`** — CTE assembly, name deduplication, header block
- [ ] **`tests/test_integration.py`** — End-to-end with mocked pydantic-ai agent

### Phase 9 — Tool Registry Learning

The goal is that an unknown tool is sent to the LLM **at most once** across all runs. After a successful AI-assisted translation in `translators/unknown.py`, the result is saved to a local JSON registry file. On the next run, the registry is checked before any LLM call — a cache hit produces the CTE immediately with no API cost.

- [ ] **`src/tool_registry.py`** — Persistent JSON registry for learned tool translations
  - Registry file location: `~/.alteryx_to_sql/tool_registry.json` (user-global) or `./tool_registry.json` (project-local, configurable via env)
  - Entry structure per plugin string:
    ```json
    {
      "plugin": "com.example.CustomAggregator.CustomAggregator",
      "tool_type": "custom_aggregator",
      "description": "LLM-inferred description of what this tool does",
      "sql_pattern": "SELECT {group_cols}, COUNT(*) AS [Count] FROM {input} GROUP BY {group_cols}",
      "confidence": "high",
      "learned_at": "2026-04-09T12:00:00Z",
      "example_config_hash": "abc123"
    }
    ```
  - `ToolRegistry.lookup(plugin: str) -> RegistryEntry | None`
  - `ToolRegistry.save(plugin: str, entry: RegistryEntry) -> None`
  - Thread-safe file writes (file lock or atomic rename)
- [ ] **`src/models.py`** — Add `RegistryEntry` Pydantic model
- [ ] **Integration in `src/translators/unknown.py`** — Check registry before LLM call; save on success
- [ ] **CLI flags in `src/main.py`**
  - `--no-registry`: disable registry lookup and saving for this run
  - `--clear-registry`: wipe the registry file before running
  - `--show-registry`: print all learned entries and exit
- [ ] **`tests/test_tool_registry.py`** — Lookup, save, concurrent write safety

### Phase 8 — Workflow Documentation

- [ ] **`src/doc_agent.py`** — pydantic-ai agent producing `WorkflowDoc`
  - Receives: workflow name, list of `AlteryxStepDoc` (tool type + annotation + config summary), list of `SQLStepDoc` (CTE name + SQL body + source tool IDs)
  - Produces: `WorkflowDoc` with human-readable summary sections
- [ ] **`src/doc_writer.py`** — Renders `WorkflowDoc` → Markdown string
  - Section 1: Workflow Overview (purpose, input sources, output targets)
  - Section 2: Alteryx Step-by-Step (numbered list matching tool order)
  - Section 3: Generated SQL Walkthrough (CTE-by-CTE explanation)
  - Section 4: Notes & Manual Review Items (from `warnings`)
- [ ] **Integration in `src/main.py`** — Wire doc_agent + doc_writer; write `<stem>_docs.md`
- [ ] **`tests/test_doc_agent.py`** — Mock agent; test doc_writer Markdown rendering

---

## 5. Dependencies

```toml
[project]
name = "alteryx-to-sql-agent"
version = "0.1.0"
description = "Converts Alteryx .yxmd workflows into T-SQL CTE chains using pydantic-ai and Vertex AI."
requires-python = ">=3.12"
dependencies = [
    "networkx>=3.3",
    "pydantic>=2.7",
    "pydantic-ai[google-vertex]>=0.0.14",
    "pydantic-settings>=2.3",
    "rich>=13.7",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "pytest-mock>=3.14",
    "pytest-asyncio>=0.23",
    "ruff>=0.4",
    "pre-commit>=3.7",
]
```

Install with:
```bash
uv add networkx pydantic "pydantic-ai[google-vertex]" pydantic-settings rich
uv add --dev pytest pytest-mock pytest-asyncio ruff pre-commit
```

---

## 6. Handling Custom Macros and Advanced Tools

### Custom Macros (`.yxmc` files)

Macros appear as tool nodes whose `Plugin` path points to a `.yxmc` file, or as `MacroInput`/`MacroOutput` plugin types.

**Two-pass strategy (in `src/macro_handler.py`):**

1. **Inline expansion (preferred):** If the `.yxmc` file is on disk, recursively parse it as a sub-DAG. Namespace all CTE names with `macro_<filename>_`. Map macro input anchors to the parent's upstream CTEs.

2. **Black-box stub (fallback):** Emit a commented stub and add a warning:
   ```sql
   -- WARNING: Macro '[name]' could not be resolved. Manual implementation required.
   -- Inputs: [anchor names]  |  Outputs: [anchor names]
   [macro_name] AS (
       SELECT * FROM [upstream_cte]  -- PLACEHOLDER
   )
   ```

### Iterative / Batch Macros

No direct T-SQL equivalent for loop constructs. Strategy: generate the loop body as a CTE chain + `-- REPLACE WITH: WHILE loop or recursive CTE` stub comment. Flag in warnings.

### Unsupported GUI-Only Tools

`ReportingTools`, `Predictive` (R), `PythonTool`, `RunCommand` → stub CTE + warning. Never silently drop them.

### Expression Language Mapping

The system prompt in `src/prompts.py` includes this table for the LLM:

| Alteryx Expression              | T-SQL (MSSQL) Equivalent                         |
|---------------------------------|---------------------------------------------------|
| `IIF(cond, a, b)`               | `CASE WHEN cond THEN a ELSE b END`                |
| `IF cond THEN a ELSE b ENDIF`   | `CASE WHEN cond THEN a ELSE b END`                |
| `DateTimeAdd(dt, n, 'days')`    | `DATEADD(day, n, dt)`                             |
| `DateTimeDiff(a, b, 'days')`    | `DATEDIFF(day, b, a)`                             |
| `DateTimeToday()`               | `CAST(GETDATE() AS DATE)`                         |
| `DateTimeNow()`                 | `GETDATE()`                                       |
| `REGEX_Match(str, pat)`         | `CASE WHEN str LIKE pat THEN 1 ELSE 0 END`        |
| `ToString(x)`                   | `CAST(x AS NVARCHAR(MAX))`                        |
| `ToNumber(x)`                   | `TRY_CAST(x AS FLOAT)`                            |
| `ToInteger(x)`                  | `TRY_CAST(x AS INT)`                              |
| `Left(str, n)`                  | `LEFT([str], n)`                                  |
| `Right(str, n)`                 | `RIGHT([str], n)`                                 |
| `PadLeft(str, n, ch)`           | `RIGHT(REPLICATE(ch, n) + [str], n)`              |
| `PadRight(str, n, ch)`          | `LEFT([str] + REPLICATE(ch, n), n)`               |
| `Contains(str, sub)`            | `CHARINDEX(sub, str) > 0`                         |
| `StartsWith(str, sub)`          | `str LIKE sub + '%'`                              |
| `Trim(str)`                     | `LTRIM(RTRIM([str]))`                             |
| `TrimLeft(str)`                 | `LTRIM([str])`                                    |
| `TrimRight(str)`                | `RTRIM([str])`                                    |
| `Uppercase(str)`                | `UPPER([str])`                                    |
| `Lowercase(str)`                | `LOWER([str])`                                    |
| `Length(str)`                   | `LEN([str])`                                      |
| `[Null]`                        | `NULL`                                            |
| `IsNull(x)`                     | `x IS NULL`                                       |
| `IsEmpty(str)`                  | `(str IS NULL OR str = '')`                       |
| `%`  (mod operator)             | `%`  *(same in T-SQL)*                            |
| `//` (integer divide)           | `/`  *(T-SQL integer division when both INT)*     |

---

## 7. Configuration

All settings read from environment / `.env` file via `src/settings.py` (`pydantic-settings`):

```env
VERTEX_PROJECT=my-gcp-project
VERTEX_LOCATION=us-central1
VERTEX_MODEL=google-vertex:gemini-2.0-flash
LLM_MAX_RETRIES=3
```

Authentication: `gcloud auth application-default login` (Application Default Credentials).

---

## 8. Open Questions / Future Work

- **Schema inference:** Alteryx stores inferred column schemas in `<RecordInfo>` elements — extract these to provide column-level context to the LLM, reducing hallucinated column names.
- **Multi-output workflows:** Generate one CTE chain per output tool, separated by `GO` in the output script.
- **Incremental translation for large workflows:** For 200+ tool workflows, prioritise deterministic translators first; only invoke LLM for tools that cannot be handled deterministically to stay within context limits.
- **SQL validation:** Optionally run output through `sqlfluff --dialect tsql` or `SET NOEXEC ON` against a real MSSQL instance to catch syntax errors.
- **Round-trip testing:** Compare row counts from Alteryx outputs vs SQL outputs on sample data.
