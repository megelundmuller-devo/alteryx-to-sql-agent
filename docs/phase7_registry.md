# Phase 7 — Tool Registry Learning

## Purpose

Unknown Alteryx tool types are translated by the LLM at most once.  After a successful translation, the result is stored in a persistent JSON registry.  On subsequent runs, a registry hit returns the cached SQL immediately — no LLM call, no latency, no API cost.

---

## Modules Written

### `src/parsing/models.py` — `RegistryEntry`

```python
class RegistryEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    plugin: str               # Full plugin string — registry key
    tool_type: str            # Normalised short name, e.g. "custom_aggregator"
    description: str          # One-line description of what the tool does
    sql_body: str             # CTE body SQL learned for this tool type
    learned_at: str           # ISO-8601 datetime string
    example_config_hash: str  # 12-char SHA-256 prefix of the config used when learning
```

---

### `src/tool_registry.py`

**Public API:**

```python
class ToolRegistry:
    def __init__(self, path: Path) -> None: ...
    def lookup(self, plugin: str) -> RegistryEntry | None: ...
    def save(self, entry: RegistryEntry) -> None: ...
    def all_entries(self) -> list[RegistryEntry]: ...
    def clear(self) -> None: ...
    def path(self) -> Path: ...  # property

def make_entry(plugin, tool_type, description, sql_body, config) -> RegistryEntry: ...
def default_registry() -> ToolRegistry: ...
```

**Registry file location** (priority order):
1. `TOOL_REGISTRY_PATH` environment variable
2. `~/.alteryx_to_sql/tool_registry.json` (user-global default)

**File format** — a JSON object keyed by plugin string:

```json
{
  "Vendor.Plugin.CustomAgg": {
    "plugin": "Vendor.Plugin.CustomAgg",
    "tool_type": "custom_agg",
    "description": "LLM-translated custom_agg tool",
    "sql_body": "SELECT [Group], COUNT(*) AS [Count] FROM [upstream] GROUP BY [Group]",
    "learned_at": "2026-04-10T12:00:00+00:00",
    "example_config_hash": "a1b2c3d4e5f6"
  }
}
```

---

### `src/translators/context.py`

Added `registry: ToolRegistry | None = None` to `TranslationContext`.  Translators read from and write to the registry through this field.  `None` means the registry is disabled for this run.

---

### `src/translators/unknown.py`

Three-step cascade for any tool with no registered deterministic translator:

```
1. Registry lookup  →  hit: return cached SQL (zero LLM cost)
                   →  miss: continue to step 2
2. LLM translation  →  success: save to registry, return SQL
                    →  failure: continue to step 3
3. Hard stub        →  emit stub CTE + warning
```

---

### `src/llm/chunk_agent.py`

Fixed API issues introduced in pydantic-ai 1.78.0:
- `result_type=str` → `output_type=str`
- `result.data` → `result.output`
- Type annotations cleaned up (`Agent[None, str]` → `Agent`)

---

### CLI flags in `src/main.py`

| Flag | Behaviour |
|---|---|
| `--no-registry` | Pass `registry=None` to `TranslationContext` — disables lookup and saving for this run |
| `--clear-registry` | Call `registry.clear()` before translating — wipes all learned entries |
| `--show-registry` | Print all entries as a rich table and exit (no workflow argument needed) |

---

## Design Decisions

### Why store `sql_body` rather than a parameterised template?

A template (`SELECT {cols} FROM {input}`) would require a template engine and column inference.  The stored SQL is the actual CTE body from the first successful translation — it may need manual adjustment for tools with significantly different configs, but it is immediately usable as-is and gives the reviewer a concrete starting point.

### Why a module-level per-path lock rather than a per-instance lock?

Two `ToolRegistry` instances pointing to the same file (e.g. created in different translation threads) must coordinate.  A per-instance `threading.Lock()` only prevents races within one instance.  A module-level `dict[str, Lock]` keyed by resolved file path ensures any number of instances sharing a file share the same lock.

### Why re-read from disk on every `save`?

Inside the lock, `save` invalidates the in-process cache and re-reads the file before merging the new entry.  This means a save from instance A and a save from instance B will both see each other's previous saves — no entry is silently overwritten by a stale cache.

### Why `tempfile.mkstemp` + `os.replace` rather than writing to a fixed `.tmp` file?

`os.replace` is atomic on POSIX (rename syscall).  Using `mkstemp` gives each write a unique temp filename, so two concurrent writes don't collide on the temp file even if the lock is momentarily not held (e.g. between the mkstemp and the replace).  This is the standard pattern for safe concurrent file writes.

### Why `--show-registry` exits without requiring a workflow argument?

Registry inspection is a standalone administrative action.  Requiring a valid workflow path would make it awkward to use as a quick audit command.  Making `workflow` optional (via `nargs="?"`) lets registry-only flags work independently.

---

## Test Coverage

14 tests in `tests/test_tool_registry.py` — all passing.

| Class | Tests |
|---|---|
| `TestRegistryLookup` | Returns None on empty; returns entry after save; unknown plugin → None |
| `TestRegistrySave` | Creates file; saves multiple entries; overwrites existing; persists across instances |
| `TestRegistryAllEntries` | Empty returns []; entries sorted by plugin |
| `TestRegistryClear` | Removes all entries; clear on empty does not raise |
| `TestRegistryConcurrentWrites` | Two threads × 3 saves each → all 6 entries survive |
| `TestMakeEntry` | Correct fields; different configs → different hashes |

3 additional tests in `tests/translators/test_translators.py::TestUnknownTranslator`:

| Test | Behaviour verified |
|---|---|
| `test_unknown_emits_stub_when_llm_fails` | Hard stub path when LLM returns failure (LLM mocked) |
| `test_unknown_uses_llm_when_registry_miss` | LLM result returned when registry has no entry |
| `test_unknown_uses_registry_cache` | LLM not called when registry has a cached entry |

---

## Example Usage

```bash
# Normal run — registry enabled by default
uv run python src/main.py workflow.yxmd --output-dir output/

# Inspect what has been learned so far
uv run python src/main.py --show-registry

# Wipe the registry and start fresh
uv run python src/main.py --clear-registry workflow.yxmd

# Disable registry for this run only (always goes to LLM)
uv run python src/main.py workflow.yxmd --no-registry

# Point to a project-local registry instead of the user-global one
TOOL_REGISTRY_PATH=./my_registry.json uv run python src/main.py workflow.yxmd
```
