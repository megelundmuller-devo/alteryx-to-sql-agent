# Phase 1 — Parsing & Graph

## Purpose

Phase 1 converts a raw `.yxmd` Alteryx workflow file into two structured artefacts:

1. A flat list of `ToolNode` and `Connection` Pydantic models (via `parser.py`)
2. A validated, topo-sorted `AlteryxDAG` wrapper around a `networkx.DiGraph` (via `dag.py`)

These are the inputs to every downstream phase. Nothing after this phase touches raw XML.

---

## Modules Written

### `src/models.py`

Central Pydantic model registry. All models use `frozen=True` for value objects.

| Model | Role |
|---|---|
| `FieldSchema` | One column from a `<RecordInfo>` block (name, Alteryx type, size) |
| `ToolNode` | One Alteryx tool node — tool_id, plugin, tool_type, config dict, schema, annotation |
| `Connection` | One directed data-flow edge — origin/dest IDs, anchors, wireless flag, order |
| `ParsedWorkflow` | Raw parser output: flat node list + connection list + source filename |
| `Chunk` | Translatable DAG unit (Phase 2 output) |
| `CTEFragment` | One generated CTE body (Phase 3/4 output) |
| `AlteryxStepDoc` / `SQLStepDoc` / `WorkflowDoc` | Documentation models (Phase 8) |
| `ConversionResult` | Final pipeline output |

### `src/parser.py`

**Public API:**

```python
def parse_workflow(path: Path | str) -> ParsedWorkflow
```

Parses a `.yxmd` file and returns a `ParsedWorkflow` with all nodes (flat) and all connections.

**Key internals:**

```python
_PLUGIN_TO_TYPE: dict[str, str]          # known plugin → tool_type registry
_normalize_plugin(plugin: str) -> str    # fallback CamelCase→snake_case for unknowns
_elem_to_value(elem: ET.Element) -> dict | str  # XML element → Python dict/str
_parse_record_info(props: ET.Element) -> list[FieldSchema]
_parse_node(node_elem: ET.Element) -> ToolNode
_collect_nodes(parent: ET.Element) -> list[ToolNode]   # recursive, flattens ChildNodes
_parse_connection(conn: ET.Element) -> Connection | None
```

**Handles:**
- Standard tool nodes with a `Plugin` attribute on `<GuiSettings>`
- Macro nodes with `<EngineSettings Macro="filename.yxmc">` and no `Plugin` attribute
- `ToolContainer` nodes whose children live in `<ChildNodes>` — recursively flattened
- `RecordInfo` schema extraction (prefers `connection="Output"` MetaInfo)
- Wireless connections (`Wireless="True"`)
- Ordered connections (`name="#1"` / `name="#2"` for union input ordering)
- `_elem_to_value`: elements with multiple same-tag children collapse to a Python list;
  inline text alongside attributes is stored under `"_text"`

### `src/dag.py`

**Public API:**

```python
def build_dag(parsed: ParsedWorkflow) -> AlteryxDAG
```

**`AlteryxDAG` methods:**

```python
dag.topological_order() -> list[int]        # tool_ids in valid execution order
dag.source_nodes() -> list[ToolNode]        # in-degree 0
dag.sink_nodes() -> list[ToolNode]          # out-degree 0
dag.predecessors(tool_id) -> list[ToolNode]
dag.successors(tool_id) -> list[ToolNode]
dag.in_edges(tool_id) -> list[Connection]
dag.out_edges(tool_id) -> list[Connection]
dag.get_linear_chains() -> list[list[int]]  # maximal single-I/O chains
dag.all_nodes -> dict[int, ToolNode]
dag.graph -> nx.DiGraph
```

**Raises:**
- `ValueError` if a connection references an unknown `tool_id`
- `ValueError` if the graph contains a cycle

---

## Design Decisions

### Why flatten ToolContainers rather than preserve hierarchy?

ToolContainers are purely visual in Alteryx — they never appear in the `<Connections>` list. All connections reference child tool IDs directly. Flattening simplifies every downstream consumer: the chunker, translators, and doc agent all see a flat tool list with no special nesting logic.

The container nodes themselves are kept in `ParsedWorkflow.nodes` (with `tool_type="tool_container"`) but are excluded from the `AlteryxDAG` by `_SKIP_TYPES`.

### Why store `config` as `dict[str, Any]`?

Each Alteryx tool type has a completely different `<Configuration>` schema. Rather than writing 20 typed models upfront, Phase 1 stores the config as a generic dict. Each Phase 3 translator knows its own config shape and extracts from it accordingly. This keeps Phase 1 agnostic and extensible.

### Why `_elem_to_value` collapses single-item lists to scalars?

Most XML elements have exactly one child of a given tag (e.g., one `<FormulaField>`). Collapsing these to a scalar avoids callers always having to handle `list[…]` for the common case. Multiple same-tag siblings (e.g., multiple `<SummarizeField>`) correctly become lists.

### Why `get_linear_chains` in the DAG rather than the chunker?

The chain concept is a property of the graph topology, not of translation logic. Keeping it in `AlteryxDAG` means it can be tested independently and reused if a future phase needs topology information directly.

---

## How to Extend

**Adding a new tool type:** Add an entry to `_PLUGIN_TO_TYPE` in `parser.py`. No other Phase 1 code changes.

**Adding a new config field:** Config is stored as a raw dict — no Phase 1 changes needed. Only the relevant Phase 3 translator needs updating.

**Multi-output MetaInfo:** Currently only the primary (`connection="Output"`) schema is extracted. To support tool-specific output schemas (e.g., Filter True vs False), extend `_parse_record_info` to return a `dict[str, list[FieldSchema]]` keyed by anchor name.

---

## Example Usage

```python
from pathlib import Path
from parser import parse_workflow
from dag import build_dag

pw = parse_workflow(Path("examples/my_workflow.yxmd"))
print(f"Nodes: {len(pw.nodes)}, Connections: {len(pw.connections)}")

dag = build_dag(pw)
print(f"Sources: {[n.tool_type for n in dag.source_nodes()]}")
print(f"Sinks:   {[n.tool_type for n in dag.sink_nodes()]}")
print(f"Topo order: {dag.topological_order()}")

for chain in dag.get_linear_chains():
    types = [dag.get_node(tid).tool_type for tid in chain]
    print(f"Chain: {' → '.join(types)}")
```

---

## Test Coverage

57 tests — all passing (`uv run pytest tests/test_parser.py tests/test_dag.py`).

| File | Tests |
|---|---|
| `tests/test_parser.py` | `_normalize_plugin`, `_elem_to_value`, minimal fixture (24 cases), real example file (8 cases) |
| `tests/test_dag.py` | Minimal fixture DAG (14 cases), error cases (2 cases), real example file (8 cases) |
| `tests/fixtures/minimal_workflow.yxmd` | Synthetic fixture covering: DbFileInput with schema, Filter, Formula, Summarize, DbFileOutput (inside ToolContainer), macro node, wireless connection |
