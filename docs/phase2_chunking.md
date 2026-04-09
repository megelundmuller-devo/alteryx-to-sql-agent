# Phase 2 — Chunking

## Purpose

Phase 2 converts an `AlteryxDAG` into an ordered list of `Chunk` objects. Each chunk is an independently translatable unit — a group of tools that can be handed to the Phase 3/4 translation layer together, producing one CTE block in the output SQL.

The primary goal is to **minimise LLM calls** while keeping each chunk self-contained.

---

## Module Written

### `src/chunking/chunker.py`

**Public API:**

```python
def chunk_dag(dag: AlteryxDAG) -> list[Chunk]
```

**Key internals:**

```python
_MULTI_INPUT_TYPES: set[str]                          # tool types that always start a new chunk
_cte_name(nodes: list[ToolNode]) -> str               # stable deterministic CTE name
_is_chunk_boundary_before(node, dag) -> bool          # must a new chunk start here?
_is_chunk_boundary_after(node, dag) -> bool           # must the current chunk end here?
```

---

## Chunking Rules

### 1. Continuity check (main rule)

A node is only merged into the current pending group if **all** of the following hold:
- It has exactly one predecessor
- That predecessor is the last node currently in pending (direct connection, not just topological proximity)
- No explicit boundary rule triggers

This prevents topologically adjacent but unconnected tools (e.g. two independent source chains appearing consecutively in topo order) from being incorrectly batched.

### 2. Multi-input boundary (before)

Tools in `_MULTI_INPUT_TYPES = {"join", "union", "append_fields"}` always start a fresh chunk. A join or union semantically requires all its inputs to exist as CTEs before it can be translated, so it must be isolated from anything that precedes it. It may, however, merge with single-I/O successors that follow it.

### 3. Branching boundary (after)

Any node with **more than one connected successor** ends its chunk. Each downstream branch starts a new chunk, which allows the translator to use the anchor name (True/False, Left/Right) as a CTE name qualifier.

**Important nuance:** a Filter with only its True output connected has `out_degree=1` and is treated as single-I/O — it merges naturally with its successor. A Filter with both True and False connected has `out_degree=2` and ends its chunk. This is determined at runtime from the actual connections, not hardcoded by tool type.

---

## CTE Naming

| Case | Pattern | Example |
|---|---|---|
| Single-tool chunk | `cte_{tool_type}_{tool_id}` | `cte_join_3` |
| Multi-tool merged chain | `cte_{first_type}_{first_id}_to_{last_type}_{last_id}` | `cte_db_file_input_1_to_formula_3` |

Names are stable and deterministic — re-running the chunker on the same DAG always produces the same names.

---

## Design Decisions

### Why continuity check rather than using `get_linear_chains`?

`get_linear_chains` from Phase 1 was considered as the basis for chunking but abandoned: it operates purely on graph topology and doesn't enforce the multi-input boundary rule inline. The chunker's topo-order sweep with the continuity check produces the same linear groupings while simultaneously applying the boundary rules in a single pass.

### Why is branching based on actual `out_degree` rather than tool type?

Hardcoding `filter` as always branching was tested against the real example file and found to be wrong — several filters in the workflow have only their True output connected, making them linear tools. Using actual successor count from the DAG gives correct results in all cases.

### Why can multi-input tools merge with their successors?

A Join starts a new chunk but there's no reason to prevent a downstream single-I/O Select or Formula from merging into the same chunk. The boundary is about what comes *before* the Join (its inputs), not what comes after. Allowing downstream merging reduces LLM calls further.

---

## How to Extend

**Adding a new multi-input tool type:** Add its `tool_type` key to `_MULTI_INPUT_TYPES` in `chunker.py`. No other changes needed.

**Custom boundary rules:** Extend `_is_chunk_boundary_before` or `_is_chunk_boundary_after` with additional conditions.

**Chunk size limit:** If a merged chain grows too large for the LLM context window, add a `max_nodes_per_chunk` parameter to `chunk_dag` that forces a flush when `len(pending) >= max_nodes`.

---

## Example Usage

```python
from pathlib import Path
from parsing.parser import parse_workflow
from parsing.dag import build_dag
from chunking.chunker import chunk_dag

dag = build_dag(parse_workflow(Path("examples/my_workflow.yxmd")))
chunks = chunk_dag(dag)

print(f"{len(chunks)} chunks from {dag.node_count()} tools")
for c in chunks:
    types = [n.tool_type for n in c.nodes]
    print(f"  [{c.output_cte_name}] {' → '.join(types)}")
    if c.input_cte_names:
        print(f"    reads from: {c.input_cte_names}")
```

---

## Test Coverage

26 tests — all passing (`uv run pytest tests/chunking/`).

| Class | Tests |
|---|---|
| `TestCteName` | Single-node naming, multi-node naming |
| `TestChunkDagMinimal` | Sequential IDs, full node coverage, unique CTE names, filter isolation, source has no inputs, sink has inputs, internal edges, topological ordering across chunks |
| `TestChunkBoundaryRules` | Join isolation, filter with 1 output merges, filter with 2 outputs ends chunk, linear chain merges, union isolation |
| `TestChunkDagExample` | Real file: produces chunks, fewer chunks than nodes, full coverage, no duplicates, unique names, topological ordering, joins start their chunk, multi-output filters end their chunk |

**Discovered and fixed during testing:**
1. Bug: topological ordering alone was used to decide merging — unconnected tools in the same topo position were incorrectly batched. Fixed with the continuity check.
2. Bug: filter was hardcoded as always branching — but filters with only one connected output are linear. Fixed by using actual successor count.
