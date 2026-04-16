"""Splits an AlteryxDAG into a list of Chunks for translation.

Entry point: chunk_dag(dag) -> list[Chunk]

Chunking strategy
-----------------
The goal is to minimise LLM calls while ensuring each chunk is independently
translatable. The rules are:

1. **Continuity** — a node is only merged into the current pending group if its
   single predecessor is exactly the last node in that group. Topological order
   alone is not sufficient; the nodes must be directly connected.

2. **Multi-input boundary** — Join, Union and similar tools that semantically
   consume more than one upstream stream always start a fresh chunk, even when
   they happen to have only one connection in a given workflow.

3. **Branching boundary** — any node whose output fans out to two or more
   successors ends its chunk so each downstream branch starts fresh (e.g. a
   Filter with both True and False outputs connected).

4. **CTE naming** — stable, deterministic names:
   ``cte_{tool_type}_{tool_id}`` for single-tool chunks and
   ``cte_{first_type}_{first_id}_to_{last_type}_{last_id}`` for merged chains.

Each Chunk carries:
- ``nodes`` — the ToolNodes in topological order
- ``edges`` — internal connections (between nodes in this chunk only)
- ``input_cte_names`` — CTE names produced by preceding chunks this chunk reads from
- ``output_cte_name`` — the CTE name this chunk will produce
"""

from parsing.dag import AlteryxDAG
from parsing.models import Chunk, Connection, ToolNode

# Tool types that semantically require multiple inputs — always start a new chunk
# even if only one upstream connection is present in the workflow.
_MULTI_INPUT_TYPES = {"join", "union", "append_fields"}


def _cte_name(nodes: list[ToolNode]) -> str:
    """Generate a stable CTE name for a list of nodes."""
    if len(nodes) == 1:
        n = nodes[0]
        return f"cte_{n.tool_type}_{n.tool_id}"
    first, last = nodes[0], nodes[-1]
    return f"cte_{first.tool_type}_{first.tool_id}_to_{last.tool_type}_{last.tool_id}"


def _is_chunk_boundary_before(node: ToolNode, dag: AlteryxDAG) -> bool:
    """Return True if this node must start a fresh chunk regardless of predecessor.

    Only covers semantic rules (tool type). Topological continuity (whether the
    predecessor is actually the last node in pending) is checked separately in
    chunk_dag to avoid merging unconnected lineages.
    """
    return node.tool_type in _MULTI_INPUT_TYPES


def _is_chunk_boundary_after(node: ToolNode, dag: AlteryxDAG) -> bool:
    """Return True if this node's chunk must end here.

    Triggered when the node fans out to more than one downstream tool — each
    branch needs its own chunk so anchor names (True/False, Left/Right) can be
    used unambiguously in CTE references.
    """
    return len(dag.successors(node.tool_id)) > 1


def chunk_dag(dag: AlteryxDAG) -> list[Chunk]:
    """Split an AlteryxDAG into an ordered list of Chunks.

    Args:
        dag: A validated, topo-sorted AlteryxDAG from parsing.dag.build_dag.

    Returns:
        List of Chunk objects in topological order, ready for translation.
    """
    topo = dag.topological_order()

    raw_chains: list[list[ToolNode]] = []
    pending: list[ToolNode] = []

    for tool_id in topo:
        node = dag.get_node(tool_id)

        # Decide whether this node continues the current pending group.
        # Conditions for continuation (ALL must hold):
        #   • pending is non-empty
        #   • this node has exactly one predecessor
        #   • that predecessor is the last node in pending (direct connection)
        #   • no explicit multi-input boundary rule applies to this node
        if pending:
            preds = dag.predecessors(node.tool_id)
            is_continuation = (
                len(preds) == 1
                and preds[0].tool_id == pending[-1].tool_id
                and not _is_chunk_boundary_before(node, dag)
            )
        else:
            is_continuation = False

        if not is_continuation:
            if pending:
                raw_chains.append(pending)
            pending = [node]
        else:
            pending.append(node)

        # Flush after this node if it fans out to multiple successors
        if _is_chunk_boundary_after(node, dag):
            raw_chains.append(pending)
            pending = []

    if pending:
        raw_chains.append(pending)

    # Assign CTE names — every tool_id maps to exactly one chain's CTE name
    chain_cte: dict[int, str] = {}
    named_chains: list[tuple[list[ToolNode], str]] = []
    for chain in raw_chains:
        name = _cte_name(chain)
        for n in chain:
            chain_cte[n.tool_id] = name
        named_chains.append((chain, name))

    # Build Chunk objects
    chunks: list[Chunk] = []
    for chunk_id, (nodes, output_cte_name) in enumerate(named_chains):
        node_ids = {n.tool_id for n in nodes}

        # Internal edges: both endpoints inside this chunk
        internal_edges: list[Connection] = [
            conn
            for n in nodes
            for conn in dag.in_edges(n.tool_id)
            if conn.origin_id in node_ids
        ]

        # Input CTE names: upstream CTEs that feed into this chunk.
        # Sort by (dest_anchor, order) so that Join inputs are always
        # Left-first, Right-second (alphabetical on anchor name), and Union
        # inputs preserve the declared connection order from the workflow.
        seen: set[str] = set()
        input_cte_names: list[str] = []
        for n in nodes:
            external = sorted(
                (conn for conn in dag.in_edges(n.tool_id) if conn.origin_id not in node_ids),
                key=lambda c: (c.dest_anchor or "", c.order or 0),
            )
            for conn in external:
                upstream_cte = chain_cte.get(conn.origin_id)
                if upstream_cte and upstream_cte not in seen:
                    input_cte_names.append(upstream_cte)
                    seen.add(upstream_cte)

        chunks.append(
            Chunk(
                chunk_id=chunk_id,
                nodes=nodes,
                edges=internal_edges,
                input_cte_names=input_cte_names,
                output_cte_name=output_cte_name,
            )
        )

    return chunks
