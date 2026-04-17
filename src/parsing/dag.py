"""Builds and interrogates the Alteryx workflow DAG.

Entry point: build_dag(parsed_workflow) -> AlteryxDAG

The DAG is a networkx.DiGraph where each node key is a tool_id (int) and
each edge carries the Connection model as its data. ToolContainer nodes are
excluded because they are purely visual groupings in Alteryx and never appear
in the connection list.

Key operations provided by AlteryxDAG:
- topological_order() — deterministic execution order for CTE generation
- source_nodes() / sink_nodes() — entry/exit points of the workflow
- get_linear_chains() — maximal consecutive chains of single-I/O tools,
  used by the chunker to merge tools into fewer LLM calls
- predecessors() / successors() — typed adjacency lookup
"""

import networkx as nx

from parsing.models import Connection, ParsedWorkflow, ToolNode

# Tool types that are purely visual and must be excluded from the DAG
_SKIP_TYPES = {"tool_container", "comment", "browse"}


class AlteryxDAG:
    """Wrapper around a networkx DiGraph that holds workflow graph state."""

    def __init__(
        self,
        graph: nx.DiGraph,
        nodes: dict[int, ToolNode],
        connections: list[Connection],
    ) -> None:
        self._graph = graph
        self._nodes = nodes  # tool_id → ToolNode (DAG nodes only; no containers)
        self._connections = connections

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    @property
    def all_nodes(self) -> dict[int, ToolNode]:
        return self._nodes

    def get_node(self, tool_id: int) -> ToolNode:
        return self._nodes[tool_id]

    def node_count(self) -> int:
        return len(self._graph)

    def edge_count(self) -> int:
        return self._graph.number_of_edges()

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------

    def topological_order(self) -> list[int]:
        """Return tool_ids in a valid topological execution order."""
        return list(nx.topological_sort(self._graph))

    def source_nodes(self) -> list[ToolNode]:
        """Nodes with no incoming edges — the workflow's data sources."""
        return [
            self._nodes[n] for n in self._graph.nodes if self._graph.in_degree(n) == 0
        ]

    def sink_nodes(self) -> list[ToolNode]:
        """Nodes with no outgoing edges — the workflow's data outputs."""
        return [
            self._nodes[n] for n in self._graph.nodes if self._graph.out_degree(n) == 0
        ]

    def predecessors(self, tool_id: int) -> list[ToolNode]:
        return [self._nodes[p] for p in self._graph.predecessors(tool_id)]

    def successors(self, tool_id: int) -> list[ToolNode]:
        return [self._nodes[s] for s in self._graph.successors(tool_id)]

    def in_edges(self, tool_id: int) -> list[Connection]:
        """Return all Connection objects arriving at tool_id.

        Multiple connections between the same origin and destination (e.g. a
        Join emitting both J and R to the same Union) are all returned.
        """
        return [
            conn
            for _, _, data in self._graph.in_edges(tool_id, data=True)
            for conn in data["connections"]
        ]

    def out_edges(self, tool_id: int) -> list[Connection]:
        """Return all Connection objects leaving tool_id.

        Multiple connections between the same pair of nodes are all returned.
        """
        return [
            conn
            for _, _, data in self._graph.out_edges(tool_id, data=True)
            for conn in data["connections"]
        ]

    # ------------------------------------------------------------------
    # Linear chain detection (used by chunker)
    # ------------------------------------------------------------------

    def get_linear_chains(self) -> list[list[int]]:
        """Return maximal chains of tool_ids where each internal node has
        exactly one predecessor and one successor.

        A chain starts at any node that is NOT a simple pass-through:
        - source node (in_degree == 0)
        - multi-input node (in_degree > 1)
        - node whose predecessor has multiple outputs (out_degree > 1)

        The chain continues as long as each subsequent node has exactly
        one input and the current node has exactly one output.
        """
        visited: set[int] = set()
        chains: list[list[int]] = []

        for node_id in nx.topological_sort(self._graph):
            if node_id in visited:
                continue

            preds = list(self._graph.predecessors(node_id))
            is_chain_start = (
                len(preds) != 1  # source or multi-input node
                or self._graph.out_degree(preds[0]) != 1  # predecessor branches
            )

            if not is_chain_start:
                # This node belongs to a chain already started from its predecessor
                continue

            chain = [node_id]
            visited.add(node_id)

            current = node_id
            while True:
                succs = list(self._graph.successors(current))
                if len(succs) != 1:
                    break  # no output or multiple outputs — end of chain
                nxt = succs[0]
                if self._graph.in_degree(nxt) != 1:
                    break  # next node has multiple inputs — it starts a new chain
                chain.append(nxt)
                visited.add(nxt)
                current = nxt

            chains.append(chain)

        return chains


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_dag(parsed: ParsedWorkflow) -> AlteryxDAG:
    """Construct an AlteryxDAG from a ParsedWorkflow.

    Args:
        parsed: Output of parser.parse_workflow().

    Returns:
        AlteryxDAG ready for chunking and translation.

    Raises:
        ValueError: If the graph contains a cycle (not a valid DAG).
        ValueError: If a connection references an unknown tool_id.
    """
    # Index nodes by tool_id; skip visual-only nodes
    node_map: dict[int, ToolNode] = {
        n.tool_id: n for n in parsed.nodes if n.tool_type not in _SKIP_TYPES
    }

    graph: nx.DiGraph = nx.DiGraph()
    graph.add_nodes_from(node_map.keys())

    # Keep only connections where both endpoints are in the DAG
    dag_connections: list[Connection] = []
    skipped_ids = {n.tool_id for n in parsed.nodes if n.tool_type in _SKIP_TYPES}

    for conn in parsed.connections:
        if conn.origin_id in skipped_ids or conn.dest_id in skipped_ids:
            continue
        if conn.origin_id not in node_map or conn.dest_id not in node_map:
            raise ValueError(
                f"Connection references unknown tool_id: "
                f"{conn.origin_id} → {conn.dest_id}"
            )
        if graph.has_edge(conn.origin_id, conn.dest_id):
            graph[conn.origin_id][conn.dest_id]["connections"].append(conn)
        else:
            graph.add_edge(conn.origin_id, conn.dest_id, connections=[conn])
        dag_connections.append(conn)

    if not nx.is_directed_acyclic_graph(graph):
        cycles = list(nx.find_cycle(graph))
        raise ValueError(f"Workflow contains a cycle: {cycles}")

    return AlteryxDAG(graph, node_map, dag_connections)
