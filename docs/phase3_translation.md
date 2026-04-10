# Phase 3 — Deterministic Translation Layer

## Purpose

Phase 3 converts `Chunk` objects (from Phase 2) into `CTEFragment` objects — the SQL body for each named CTE block.  The primary goal is to handle as many tool types as possible **without an LLM call**, reserving LLM use for genuinely complex expressions and unknown tools.

---

## Modules Written

### `src/translators/context.py`

```python
@dataclass
class TranslationContext:
    dag: AlteryxDAG
    chain_cte: dict[int, str]  # tool_id → chunk output CTE name
    warnings: list[str]        # mutable; appended to during translation
```

Shared state threaded through all translator calls.  Translators may append to `warnings` but must not mutate anything else.

---

### `src/translators/expressions.py`

Deterministic Alteryx expression → T-SQL expression converter.

**Two public functions:**

```python
def needs_llm_translation(expression: str) -> bool
def convert_expression(expression: str) -> str
```

**`needs_llm_translation`** scans for patterns that cannot be converted deterministically:
- `IF...THEN...ENDIF` constructs
- `REGEX_*` functions
- `SWITCH`, `IIF` (flagged even though IIF exists in T-SQL — Alteryx's IIF has different semantics)
- Date/time arithmetic (`DATETIMEADD`, `DATETIMEDIFF`, `DATETIMEFORMAT`)
- `FINDSTRING` (0-based vs 1-based index difference)

**`convert_expression`** handles (in order):
1. Compound function patterns first (CONTAINS, STARTSWITH, ISNULL, etc.) — must run before quote conversion because they match double-quoted string arguments
2. Double-quoted string literals → single-quoted (`"val"` → `'val'`)
3. `[Null]` → `NULL`
4. `True`/`False` → `1`/`0`
5. Simple function renames (LENGTH→LEN, UPPERCASE→UPPER, TRIM→LTRIM(RTRIM(…)), etc.)

**Important ordering bug fixed during development:** Compound patterns must be applied before the global double-quote → single-quote conversion because they explicitly match `"val"` inside function calls (e.g. `CONTAINS([col], "val")`).

**Other translation bugs discovered and fixed:**
- **DbFileInput**: The connection string lives in `cfg["File"]["_text"]` (format `alias|||query`), not in `cfg["Connection"]` as initially assumed. Fixed by checking all known key locations in priority order.
- **Join**: `<JoinInfo>` elements use the attribute name `connection=` (not `side=`) to identify Left vs Right. Field names within each `<JoinInfo>` use `field=` (not `name=`). Fixed by checking both attribute names with fallbacks.
- **Filter Simple mode**: Alteryx Filter has two modes — `Simple` (structured `<Field>/<Operator>/<Operand>` config) and `Custom` (raw `<Expression>`). The initial implementation only handled Custom mode, causing all Simple-mode filters to stub. Fixed by adding `_simple_condition()` dispatched via `_get_expression()` based on `cfg["Mode"]`.

---

### `src/translators/__init__.py`

Registry and public entry point.

**Public API:**

```python
def translate_chunk(chunk: Chunk, ctx: TranslationContext) -> list[CTEFragment]
```

**Registry:**

```python
_REGISTRY: dict[str, TranslatorFn] = {
    "db_file_input": translate_db_file_input,
    "text_input":    translate_text_input,
    "db_file_output": translate_db_file_output,
    "select":        translate_select,
    "filter":        translate_filter,
    "formula":       translate_formula,
    "summarize":     translate_summarize,
    "sort":          translate_sort,
    "unique":        translate_unique,
    "sample":        translate_sample,
    "record_id":     translate_record_id,
    "multirow_formula": translate_multirow,
    "join":          translate_join,
    "union":         translate_union,
    "append_fields": translate_append,
    "find_replace":  translate_find_replace,
    "macro":         translate_macro,
    # fallback: translate_unknown
}
```

**Multi-node chunk wiring:** Intermediate tools in a merged chain get temporary CTE names (`cte_{tool_type}_{tool_id}`).  Only the last tool uses the chunk's `output_cte_name`.

---

### Individual translator modules

| Module | Tool type(s) | Strategy |
|---|---|---|
| `input_output.py` | db_file_input, text_input, db_file_output | DbFileInput: parses `alias\|\|\|query` from `cfg["File"]["_text"]` or `cfg["Connection"]`; TextInput: builds VALUES CTE; DbFileOutput: extracts destination table from `alias\|\|\|TableName`, emits INSERT comment with pass-through SELECT (stub only if table cannot be determined) |
| `select.py` | select | Parse SelectFields; rename/drop columns; handle Unknown pass-through |
| `filter.py` | filter | WHERE clause via expression converter; stub for complex expressions; emits True+False fragments when both anchors connected |
| `formula.py` | formula | Per-column expression converter; stub columns for LLM-needed fields |
| `summarize.py` | summarize | GROUP BY + SUM/COUNT/AVG/MIN/MAX/COUNT(DISTINCT); STRING_AGG for concat; warns on First/Last |
| `join.py` | join | INNER JOIN on parsed JoinInfo keys; comments for Left/Right anti-join variants |
| `union.py` | union | UNION ALL (preserves duplicates by default) |
| `unique.py` | unique | ROW_NUMBER() OVER (PARTITION BY) WHERE _rn=1; DISTINCT fallback if no keys |
| `sort.py` | sort | ROW_NUMBER() OVER (ORDER BY) to materialise sort order |
| `sample.py` | sample | SELECT TOP N; NEWID() for random; stub for Last N |
| `record_id.py` | record_id | ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) with configurable start |
| `multirow.py` | multirow_formula | Always stub — flags for LLM (LAG/LEAD patterns) |
| `append.py` | append_fields | CROSS JOIN of Target × Source |
| `find_replace.py` | find_replace | REPLACE(); stub for regex replacements |
| `macro.py` | macro | Stub CTE; expansion handled in Phase 5 |
| `unknown.py` | (fallback) | Stub CTE with tool type and plugin name |

---

## Stub / Warning Protocol

Any tool that cannot be fully translated emits:
1. A `CTEFragment` with `is_stub=True`
2. One or more entries in `ctx.warnings`

The assembler (Phase 5) annotates stubs visually.  All warnings bubble up to `ConversionResult.warnings`.

---

## Design Decisions

### Why stub + warning rather than raising an exception?

The pipeline must produce a complete SQL script even when individual tools are ambiguous.  A stub keeps the CTE chain intact, lets the user see the full picture, and makes manual review straightforward — all stubs are clearly labelled.

### Why UNION ALL by default?

Alteryx Union preserves duplicate rows unless the user explicitly configures deduplication.  UNION (with dedup) would silently change semantics.

### Why ROW_NUMBER() for Sort?

`ORDER BY` inside a CTE is only valid in MSSQL when combined with `TOP` or `OFFSET-FETCH`.  Materialising the sort as `_sort_order` makes the intent explicit and lets downstream tools reference it.

### Why apply compound expression patterns before quote conversion?

Compound patterns match `CONTAINS([col], "val")` where the second argument is a double-quoted string.  If quote conversion runs first, the argument becomes `'val'` and the pattern no longer matches.

---

## Test Coverage

64 tests — all passing (`uv run pytest tests/translators/ tests/assembly/`).

| Class | Tests |
|---|---|
| `TestNeedsLlm` | IF, regex, IIF, simple comparison, CONTAINS, arithmetic, DATETIMEADD |
| `TestConvertExpression` | Quotes, Null, booleans, CONTAINS, STARTSWITH, ISNULL, ISNOTNULL, TRIM, LENGTH, UPPERCASE, LOWERCASE, DATETIMENOW, DATETIMETODAY, TONUMBER, TOSTRING, passthrough arithmetic, passthrough column refs |
| `TestInputOutputTranslators` | No connection → stub; connection with query; TextInput with data; TextInput empty; DbFileOutput passthrough |
| `TestSelectTranslator` | No fields → stub; explicit fields with rename and drop |
| `TestFilterTranslator` | Simple expression; no expression → stub; complex → stub |
| `TestFormulaTranslator` | Simple formula; LLM expression → stub |
| `TestSummarizeTranslator` | GroupBy+Sum; COUNT DISTINCT; First/Last warn |
| `TestJoinTranslator` | Join with keys; insufficient inputs → stub |
| `TestUnionTranslator` | UNION ALL multi; single input passthrough |
| `TestUniqueTranslator` | With keys → ROW_NUMBER; no keys → DISTINCT |
| `TestSortTranslator` | ASC/DESC |
| `TestSampleTranslator` | First N; Random |
| `TestRecordIdTranslator` | Default start; non-default start |
| `TestUnknownTranslator` | Emits stub + warning |
| `TestTranslateChunk` | Single node; output_cte_name in fragments; real example all chunks |
| `TestBuildSql` | Empty; single; multiple; stub annotation; header; stub count; indentation; semicolon |

---

## How to Extend

**Adding a new deterministic tool translator:**
1. Create `src/translators/<tool>.py` with function `translate_<tool>(node, cte_name, input_ctes, ctx) -> CTEFragment`
2. Add it to `_REGISTRY` in `src/translators/__init__.py`

**Adding new expression patterns:**
- Simple renames → add to `_FUNCTION_RENAMES` in `expressions.py`
- Compound (with captured groups) → add to `_COMPOUND_PATTERNS`
- Patterns requiring LLM → add to `_LLM_PATTERNS`
