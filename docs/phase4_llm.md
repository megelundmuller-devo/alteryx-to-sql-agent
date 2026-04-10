# Phase 4 — LLM Integration

## Purpose

Phase 4 provides LLM-backed translation for the cases Phase 3 cannot handle deterministically:
- Complex Alteryx expressions (`IF...THEN`, `REGEX_*`, `DATETIMEADD`, etc.)
- Entirely unknown tool types
- Workflow documentation generation (used by Phase 8)

All agents use **pydantic-ai** with the **Google Vertex AI** backend (`google-vertex:gemini-2.5-pro`).

---

## Modules Written

### `src/llm/settings.py`

Pydantic-settings class reading Vertex AI credentials from `.env`.

```python
class LLMSettings(BaseSettings):
    vertex_project:  str   # VERTEX_PROJECT
    vertex_location: str   # VERTEX_LOCATION
    model_id:        str   # VERTEX_MODEL (default: google-vertex:gemini-2.5-pro)
    llm_max_retries: int   # LLM_MAX_RETRIES (default: 3)

def get_settings() -> LLMSettings: ...  # cached singleton
```

Credentials are loaded from `.env` at the project root via `pydantic-settings`.  The `get_settings()` function is `@lru_cache`'d so the file is only read once per process.

---

### `src/llm/prompts.py`

All prompt templates in one place, independent of agent code.

| Prompt | Used by | Purpose |
|---|---|---|
| `EXPRESSION_SYSTEM_PROMPT` | expression_agent | Converts single Alteryx expressions to T-SQL |
| `EXPRESSION_FEW_SHOT` | (reference) | Three-shot examples for expression conversion |
| `CHUNK_SYSTEM_PROMPT` | chunk_agent | Translates a whole unknown tool config into a CTE body |
| `DOC_SYSTEM_PROMPT` | doc_agent | Generates Markdown workflow documentation |

The expression prompt is the most detailed — it lists all Alteryx→T-SQL function mappings and explicitly instructs the model to output MSSQL dialect only (square brackets, `NVARCHAR`, `TRY_CAST`, `ISNULL`, `GETDATE()`, etc.).

---

### `src/llm/expression_agent.py`

```python
def convert_expression_llm(alteryx_expression: str) -> str
```

- Creates an `Agent('google-vertex:gemini-2.5-pro', output_type=str)` singleton.
- Sets `GOOGLE_CLOUD_PROJECT` / `GOOGLE_CLOUD_LOCATION` env vars from settings (Vertex AI backend reads these).
- Builds a few-shot message history from `EXPRESSION_FEW_SHOT` (via `_build_history()`) and passes it as `message_history=` on every `run_sync` call. This ensures the model returns bare T-SQL rather than markdown prose.
- Calls `agent.run_sync(prompt, message_history=...)` — synchronous since the pipeline is sequential.
- Returns `result.output.strip()`.
- On any exception: returns a commented stub (`-- LLM conversion failed: ...`), never raises.

Used by `formula.py` and `filter.py` when the expression needs LLM.

---

### `src/llm/chunk_agent.py`

```python
def translate_chunk_llm(
    tool_type: str,
    plugin: str,
    config: dict,
    input_ctes: list[str],
) -> str
```

Sends the full tool configuration (as JSON, truncated to 3000 chars) to the LLM and asks for a CTE body.  Used by `unknown.py` in Phase 9 (Tool Registry Learning) when the registry has no deterministic translator.

---

### `src/llm/doc_agent.py`

```python
def generate_workflow_summary(
    workflow_name: str,
    tool_descriptions: list[str],
    warnings: list[str],
) -> str
```

Generates Markdown documentation for the converted workflow.  Called by Phase 8.  Returns a plain-text fallback if LLM call fails.

---

## Agent Pattern

All three agents follow the same pattern:

```python
_agent: Agent | None = None  # module-level singleton

def _make_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,          # "google-vertex:gemini-2.5-pro"
        output_type=str,            # pydantic-ai >=1.78 renamed result_type → output_type
        system_prompt=PROMPT,
        retries=settings.llm_max_retries,
    )

def _get_agent() -> Agent:
    global _agent
    if _agent is None:
        _agent = _make_agent()
    return _agent
```

The singleton is created on first use (lazy init) so importing the module does not trigger credential loading or network activity.

---

## Design Decisions

### Why `google-vertex:gemini-2.5-pro` string model ID?

pydantic-ai 1.78.0 does not expose a `VertexAIModel` class — `pydantic_ai.models.vertexai` does not exist.  The string-based model ID is the documented and confirmed-working API.  `gemini-2.0-flash` returns 404 in `europe-west1` for this project; `gemini-2.5-pro` is confirmed accessible.

### Why `output_type` not `result_type`?

pydantic-ai 1.78.0 renamed the parameter from `result_type` to `output_type`.  Using `result_type` raises `Unknown keyword arguments: result_type` at agent creation time.

### Why pass few-shot examples via `message_history`?

Without examples, `gemini-2.5-pro` returns markdown-formatted responses with explanations rather than bare SQL expressions.  Passing `EXPRESSION_FEW_SHOT` as a synthetic message history on every call teaches the model the expected output format without needing to repeat the instruction in the system prompt.

### Why synchronous (`run_sync`) rather than async?

The overall pipeline is a sequential, single-threaded batch process.  There is no async event loop to integrate with.  `run_sync()` is the correct call for synchronous contexts.

### Why module-level singletons?

Creating an `Agent` object involves loading settings and constructing the HTTP client.  Doing this once per module call rather than once per expression call avoids repeated credential resolution overhead in workflows with many formula tools.

### Why never raise in the LLM functions?

The pipeline must produce output even when the LLM is unavailable (network issues, quota exhaustion, bad credentials).  All LLM functions catch exceptions and return stub SQL with a clear `-- LLM conversion failed` comment.

### Why separate expression and chunk agents?

Different prompts, different few-shot examples, and different result contracts.  Keeping them separate makes each prompt easy to tune independently.

---

## Credential Setup

The `.env` file must contain:

```
VERTEX_PROJECT=your-gcp-project-id
VERTEX_LOCATION=europe-west1
VERTEX_MODEL=google-vertex:gemini-2.5-pro
LLM_MAX_RETRIES=3
```

Authentication uses Application Default Credentials (ADC).  Run `gcloud auth application-default login` if not already configured.  The `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_LOCATION` environment variables are set automatically from settings at agent creation time.

---

## Tests

LLM agent tests are not included in the standard test suite because they require live Vertex AI credentials and incur cost.  The agents are covered indirectly through the translator tests (which emit stubs when LLM would be needed) and can be manually tested with:

```python
from llm.expression_agent import convert_expression_llm
print(convert_expression_llm("IF [x] > 0 THEN 1 ELSE 0 ENDIF"))
```
