"""CLI entry point — placeholder until Phase 6 wires the full pipeline."""

from parsing.dag import build_dag
from parsing.parser import parse_workflow


def main():
    pw = parse_workflow("examples/BI_Aggregate Daily Simple_LDB-01.yxmd")
    dag = build_dag(pw)
    print(f"Nodes in DAG: {dag.node_count()}")
    print(f"Edges:        {dag.edge_count()}")
    print()
    print("Sources:", [f"{n.tool_id}:{n.tool_type}" for n in dag.source_nodes()])
    print("Sinks:  ", [f"{n.tool_id}:{n.tool_type}" for n in dag.sink_nodes()])
    print()
    for chain in dag.get_linear_chains():
        types = [f"{tid}:{dag.get_node(tid).tool_type}" for tid in chain]
        print("Chain:", " -> ".join(types))


if __name__ == "__main__":
    main()
