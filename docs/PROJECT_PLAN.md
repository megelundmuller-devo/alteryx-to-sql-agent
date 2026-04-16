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

### Phase 3 — Translation Layer **[DONE]**

- [x] **`src/translators/__init__.py`** — Registry + `translate_chunk(chunk, ctx) -> list[CTEFragment]`
- [x] **`src/translators/context.py`** — `TranslationContext` dataclass (dag, chain_cte, warnings)
- [x] **`src/translators/expressions.py`** — Deterministic Alteryx→T-SQL expression converter
- [x] **`src/translators/input_output.py`** — DbFileInput (parse connection string), TextInput (VALUES), DbFileOutput (stub)
- [x] **`src/translators/filter.py`** — WHERE from expression; True+False fragments for branching filters
- [x] **`src/translators/join.py`** — INNER JOIN with parsed JoinInfo keys; Left/Right variants as comments
- [x] **`src/translators/union.py`** — UNION ALL (default preserves duplicates)
- [x] **`src/translators/select.py`** — SelectFields: rename/drop columns; Unknown pass-through
- [x] **`src/translators/formula.py`** — Per-column expression converter; stub for LLM-needed fields
- [x] **`src/translators/summarize.py`** — GroupBy/Sum/Count/Avg/Min/Max/CountDistinct/String_Agg
- [x] **`src/translators/sort.py`** — ROW_NUMBER() OVER (ORDER BY) materialised sort
- [x] **`src/translators/sample.py`** — SELECT TOP N; NEWID() for random
- [x] **`src/translators/record_id.py`** — ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
- [x] **`src/translators/multirow.py`** — Always stub; flags for LLM (LAG/LEAD patterns)
- [x] **`src/translators/append.py`** — CROSS JOIN of Target × Source
- [x] **`src/translators/find_replace.py`** — REPLACE(); stub for regex replacements
- [x] **`src/translators/macro.py`** — Stub CTE; expansion in Phase 5
- [x] **`src/translators/unknown.py`** — Stub CTE with tool type + plugin in comment

### Phase 4 — LLM Integration (pydantic-ai) **[DONE]**

- [x] **`src/llm/settings.py`** — `LLMSettings(BaseSettings)` reading VERTEX_PROJECT/LOCATION/MODEL, LLM_MAX_RETRIES
- [x] **`src/llm/prompts.py`** — EXPRESSION_SYSTEM_PROMPT, CHUNK_SYSTEM_PROMPT, DOC_SYSTEM_PROMPT + few-shot examples
- [x] **`src/llm/expression_agent.py`** — `convert_expression_llm(expr) -> str` using `Agent('google-vertex:gemini-2.5-pro', output_type=str)` with few-shot `message_history` to enforce bare T-SQL output
- [x] **`src/llm/chunk_agent.py`** — `translate_chunk_llm(tool_type, plugin, config, input_ctes) -> str`
- [x] **`src/llm/doc_agent.py`** — `generate_workflow_summary(name, steps, warnings) -> str`

### Phase 5 — Assembly & Output **[DONE]**

- [x] **`src/assembly/cte_builder.py`** — `build_sql(fragments, workflow_name) -> str`
  - Header comment with source file, timestamp, stub count
  - Stub banner inside each stub CTE block
  - Trailing `SELECT * FROM [last_cte];`
- [x] **`src/assembly/macro_handler.py`** — `expand_macro(...)` with recursive inlining up to MAX_MACRO_DEPTH=3

### Phase 6 — Orchestration, CLI & Documentation **[DONE]**

- [x] **`src/main.py`** — CLI entry point
  - `argparse`: positional `workflow` + `--output-dir` + `--dry-run` + `--no-docs`
  - `--dry-run`: parse + chunk + print rich table, no translation or LLM calls
  - `--no-docs`: produce `.sql` only, skip `_docs.md`
  - Progress output via `rich` spinner + elapsed time for each phase
  - Wires full pipeline (Phases 1→2→3→5→doc); writes `<stem>.sql` and `<stem>_docs.md`
- [x] **`src/assembly/macro_handler.py`** — Macro handler
  - External `.yxmc` files always produce a stub CTE + warning (intentional — macros are org-specific)
- [x] **`src/llm/doc_agent.py`** — `generate_workflow_summary(prompt) -> str`
  - `Agent('google-vertex:gemini-2.5-pro', output_type=str)` singleton
  - Returns empty string on failure (caller renders fallback)
- [x] **`src/doc_writer.py`** — `generate_docs(workflow_path, dag, chunks, fragments, warnings) -> str`
  - Builds structured LLM prompt from the DAG, chunks, and CTEFragments (receives the pipeline tree directly)
  - Calls `generate_workflow_summary` for the narrative Overview section
  - Deterministically renders: header stats, Data Flow (sources → steps → sinks), CTE index, Warnings
  - Graceful fallback if LLM unavailable
- [x] **`tests/test_doc_writer.py`** — 13 tests; LLM mocked with `unittest.mock.patch`

### Phase 7 — Tool Registry Learning **[DONE]**

The goal is that an unknown tool is sent to the LLM **at most once** across all runs. After a successful AI-assisted translation in `translators/unknown.py`, the result is saved to a local JSON registry file. On the next run, the registry is checked before any LLM call — a cache hit produces the CTE immediately with no API cost.

- [x] **`src/tool_registry.py`** — `ToolRegistry` + `make_entry()` + `default_registry()`
  - Location: `~/.alteryx_to_sql/tool_registry.json` (or `TOOL_REGISTRY_PATH` env var)
  - `lookup(plugin) -> RegistryEntry | None`; `save(entry)`; `all_entries()`; `clear()`
  - Thread-safe via per-path module-level lock; atomic writes via `tempfile.mkstemp` + `os.replace`
- [x] **`src/parsing/models.py`** — Added `RegistryEntry` Pydantic model
- [x] **`src/translators/context.py`** — Added `registry: ToolRegistry | None = None` field
- [x] **`src/translators/unknown.py`** — Three-step cascade: registry lookup → LLM → hard stub; saves to registry on LLM success
- [x] **`src/llm/chunk_agent.py`** — Fixed `result_type` → `output_type`, `result.data` → `result.output`
- [x] **CLI flags in `src/main.py`**
  - `--no-registry`: pass `registry=None` to context (disables lookup and saving)
  - `--clear-registry`: wipe the registry file before translating
  - `--show-registry`: print all learned entries as a rich table and exit
- [x] **`tests/test_tool_registry.py`** — 14 tests: lookup, save, persistence, overwrite, all_entries, clear, concurrent writes, make_entry

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
    "pydantic-ai>=1.78.0",      # no [google-vertex] extra — it does not exist
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
uv add networkx pydantic pydantic-ai pydantic-settings rich
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
VERTEX_LOCATION=europe-west1
VERTEX_MODEL=google-vertex:gemini-2.5-pro
LLM_MAX_RETRIES=3
```

Authentication: `gcloud auth application-default login` (Application Default Credentials).

---

### Phase 8 — Batch CLI **[DONE]**

- [x] **`src/main.py`** — `workflow` argument now accepts a file **or a directory**
  - Directory mode: globs `*.yxmd`, processes each file in sorted order
  - Per-file errors caught and logged without aborting the batch
  - Batch summary line: `N succeeded / M failed`

### Phase 9 — MSSQL 2016 Compliance **[DONE]**

- [x] **`src/translators/summarize.py`** — Replace `STRING_AGG` (SQL Server 2017+) with
  `STUFF(…FOR XML PATH(''), TYPE).value(…)` for `Concat` and `ConcatDistinct` actions
  - No-GroupBy: non-correlated full-table subquery
  - GroupBy + Concat: correlated subquery aliased as `[_outer]`/`[_sub]`
- [x] **`src/assembly/cte_builder.py`** — Wrap output in `CREATE PROCEDURE [dbo].[<name>] AS BEGIN … END; GO`
  - Procedure name derived from workflow filename (non-alphanumeric → `_`)
  - `SET NOCOUNT ON` at top of body

---

### Phase 10 — Translation Fixes **[DONE]**

Fix systematic translation errors identified during real-workflow runs.  All
fixes must be deterministic (no new LLM calls) except where explicitly noted.

#### 10a — Alteryx function mappings (deterministic)

Add missing Alteryx→T-SQL mappings to `src/translators/expressions.py` and to
`src/llm/prompts.py` (EXPRESSION_SYSTEM_PROMPT) so neither the deterministic
translator nor the LLM emits them:

- [x] `ToDate(x)`               → `CAST(x AS DATE)`
- [x] `IsInteger(x)`            → `TRY_CAST(x AS INT) IS NOT NULL`
- [x] `DateTimeFirstOfMonth()`  → `DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)`
- [x] `TitleCase(x)`            → `UPPER(LEFT(x,1)) + LOWER(SUBSTRING(x,2,LEN(x)))`
                                   *(first-word only; document limitation)*

#### 10b — Alteryx engine variables (deterministic)

- [x] **`src/translators/expressions.py`** — Detect `[Engine.*]` references with a
  regex; replace each with a named T-SQL variable (`@WorkflowFileName`, etc.)
- [x] **`src/assembly/cte_builder.py`** — Collect all engine-variable names emitted
  during translation and prepend `DECLARE @<name> NVARCHAR(255) = N'<replace me>';`
  lines inside `BEGIN`, before `SET NOCOUNT ON`, so the user can fill them in once

#### 10c — Schema tracking via `<RecordInfo>` metadata (deterministic)

Root cause of two separate warning classes:
- JOIN `SELECT L.*, R.*` → duplicate column names in every downstream CTE
- Select pass-through + explicit columns → columns referenced downstream but not defined

Alteryx embeds exact field lists (`<RecordInfo>`) on every connection in the `.yxmd`
XML.  Extract this metadata in Phase 1 and propagate it through the pipeline so
translators can emit explicit column lists instead of `*`.

- [x] **`src/parsing/models.py`** — `FieldSchema(name, alteryx_type)` already existed;
  `ToolNode.output_schema: list[FieldSchema]` populated by the parser from `<MetaInfo>`
- [x] **`src/translators/context.py`** — Added `cte_schema: dict[str, list[FieldSchema]]`
  and `engine_vars: set[str]` to `TranslationContext`
- [x] **`src/translators/__init__.py`** — After each translator, registers
  `ctx.cte_schema[frag.name] = node.output_schema` for all output fragments
- [x] **`src/translators/join.py`** — When both input schemas are known, emit explicit
  `SELECT L.[col], …, R.[col], …` de-duplicating clashing names by appending `_R` to
  the right-side column only (left-side columns keep their original names), e.g.
  `L.[id], R.[id] AS [id_R]` — instead of `SELECT L.*, R.*`
- [x] **`src/translators/select.py`** — Resolve pass-through using the upstream schema;
  emit the full explicit column list rather than a partial pass-through

LLM fallback: if a connection's `<RecordInfo>` is absent (e.g. after a macro stub),
fall back to the existing `SELECT L.*, R.*` behaviour and emit the existing warning.

---

### Phase 11 — Schema Inference (deterministic) **[DONE]**

`<MetaInfo><RecordInfo>` is only written by Alteryx for tools that were
previewed/run in the Designer.  Most transform nodes (Join, Formula, Filter…)
have no MetaInfo, so `ToolNode.output_schema` is always empty and
`ctx.cte_schema` never gets populated → every join falls back to `L.*, R.*`.

Fix: derive the output schema for every tool type deterministically from its
config + input schemas, with no LLM involvement.

- [x] **`src/translators/schema_inference.py`** — `infer_output_schema(node, input_cte_names, ctx)` for: `select`, `filter`, `formula`, `join`, `append_fields`, `union`, `summarize`, `sort`, `unique`, `sample`, `record_id`, `find_replace`, `multirow_formula`
- [x] **`src/translators/context.py`** — add `cte_inputs: dict[str, list[str]]` so the liveness pass can walk back through the CTE chain
- [x] **`src/translators/__init__.py`** — call `infer_output_schema` after each node; populate `ctx.cte_inputs`; use inferred schema when `node.output_schema` is empty

### Phase 12 — Anchor-Aware Join Input Ordering (deterministic) **[DONE]**

`chunk.input_cte_names` is built from `dag.in_edges()` in NetworkX insertion
order, which is not guaranteed to match anchor order.  `translate_join` assumes
`input_ctes[0]` = Left and `input_ctes[1]` = Right, but this is sometimes
backwards, causing join keys to be assigned to the wrong CTE.

- [x] **`src/chunking/chunker.py`** — sort external in-edges by `(dest_anchor, order or 0)` before building `input_cte_names`; this guarantees Left < Right for joins and preserves declared order for unions

### Phase 13 — Liveness Analysis: SELECT Column Widening (deterministic) **[DONE]**

When a SELECT tool drops a column that a downstream tool still needs, those
downstream references fail at runtime.  A backward liveness pass can detect
and fix this automatically.

- [x] **`src/analysis/__init__.py`** — package init
- [x] **`src/analysis/liveness.py`** — `run_liveness_pass(fragments, ctx)`:  extract `[ColName]` references from each CTE's SQL; walk backwards through `ctx.cte_inputs`; widen any SELECT CTE that dropped a needed column; return updated fragment list + unfixable-gap warnings
- [x] **`src/main.py`** — call `run_liveness_pass` after `translate_chunk` loop, before assembly

### Phase 14 — UNION ALL Column-Count Validation (deterministic) **[DONE]**

- [x] **`src/translators/union.py`** — after schema inference is available, compare column counts of all input CTEs; emit a warning and mark the CTE as a stub when counts differ

### Phase 15 — LLM Repair Pass **[DONE]**

For gaps that the liveness pass cannot fix deterministically (LLM-hallucinated
column names, cross-file CTE references, macro stub outputs), invoke a targeted
LLM call to repair the broken CTE body.

- [x] **`src/parsing/models.py`** — add `llm_repaired: bool = False` and `llm_repair_notes: str = ""` to `CTEFragment`
- [x] **`src/llm/prompts.py`** — add `CTE_REPAIR_SYSTEM_PROMPT`
- [x] **`src/analysis/llm_validator.py`** — `repair_fragments(fragments, ctx)`: scan each non-stub CTE for column refs not in any input schema; send to LLM with available-column context; update fragment with repaired SQL + `llm_repaired=True`
- [x] **`src/doc_gen/doc_writer.py`** — mark LLM-repaired CTEs with a distinct ⚙ symbol and list repaired columns in the Warnings section
- [x] **`src/main.py`** — call `repair_fragments` after liveness pass, before assembly

---

## 8. Open Questions / Future Work

- **Schema inference:** Alteryx stores inferred column schemas in `<RecordInfo>` elements — extract these to provide column-level context to the LLM, reducing hallucinated column names.
- **Multi-output workflows:** Generate one CTE chain per output tool, separated by `GO` in the output script.
- **Incremental translation for large workflows:** For 200+ tool workflows, prioritise deterministic translators first; only invoke LLM for tools that cannot be handled deterministically to stay within context limits.
- **SQL validation:** Optionally run output through `sqlfluff --dialect tsql` or `SET NOEXEC ON` against a real MSSQL instance to catch syntax errors.
- **Round-trip testing:** Compare row counts from Alteryx outputs vs SQL outputs on sample data.
