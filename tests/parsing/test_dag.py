"""Tests for src/dag.py — DAG construction and analysis."""

from pathlib import Path

import pytest

from parsing.dag import AlteryxDAG, build_dag
from parsing.parser import parse_workflow

FIXTURES = Path(__file__).parent / "fixtures"
MINIMAL = FIXTURES / "minimal_workflow.yxmd"
EXAMPLE = (
    Path(__file__).parents[2] / "examples" / "BI_Aggregate Daily Simple_LDB-01.yxmd"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_minimal() -> AlteryxDAG:
    return build_dag(parse_workflow(MINIMAL))


# ---------------------------------------------------------------------------
# build_dag — minimal fixture
# ---------------------------------------------------------------------------


class TestBuildDagMinimal:
    def setup_method(self):
        self.dag = _build_minimal()

    def test_returns_alteryx_dag(self):
        assert isinstance(self.dag, AlteryxDAG)

    def test_tool_container_excluded(self):
        # ToolID 99 is a tool_container — must not be in the DAG
        assert 99 not in self.dag.all_nodes

    def test_macro_node_included(self):
        # Macro node (ToolID 31) is a real tool and must be in the DAG
        assert 31 in self.dag.all_nodes

    def test_node_count(self):
        # 5 real tools: 1, 2, 3, 4, 5 + macro 31 = 6 (container 99 excluded)
        assert self.dag.node_count() == 6

    def test_source_nodes(self):
        sources = self.dag.source_nodes()
        source_ids = {n.tool_id for n in sources}
        # Tool 1 (DbFileInput) has no predecessors
        assert 1 in source_ids

    def test_sink_nodes(self):
        sinks = self.dag.sink_nodes()
        sink_ids = {n.tool_id for n in sinks}
        # Tool 5 (DbFileOutput) has no successors
        assert 5 in sink_ids

    def test_topological_order_respects_dependencies(self):
        order = self.dag.topological_order()
        pos = {tool_id: i for i, tool_id in enumerate(order)}
        # All edges must go from lower to higher position
        for conn in self.dag._connections:
            assert pos[conn.origin_id] < pos[conn.dest_id], (
                f"Edge {conn.origin_id}→{conn.dest_id} violates topological order"
            )

    def test_source_comes_before_sink(self):
        order = self.dag.topological_order()
        assert order.index(1) < order.index(5)

    def test_predecessors(self):
        preds = self.dag.predecessors(2)
        assert len(preds) == 1
        assert preds[0].tool_id == 1

    def test_successors(self):
        succs = self.dag.successors(2)
        assert len(succs) == 1
        assert succs[0].tool_id == 3

    def test_in_edges(self):
        edges = self.dag.in_edges(2)
        assert len(edges) == 1
        assert edges[0].origin_id == 1
        assert edges[0].origin_anchor == "Output"

    def test_out_edges(self):
        edges = self.dag.out_edges(2)
        assert len(edges) == 1
        assert edges[0].dest_id == 3
        assert edges[0].origin_anchor == "True"

    def test_linear_chains(self):
        chains = self.dag.get_linear_chains()
        # The main path 1→2→3→4→5 is split by filter branching (2 has True/False outputs)
        # but in our minimal fixture, filter only has True output connected.
        # 1 is a source, so starts a chain. 2→3→4→5 continues.
        # Macro 31 is only reachable via wireless from 1, so it's its own chain start.
        all_ids_in_chains = {tid for chain in chains for tid in chain}
        # Every DAG node should appear in exactly one chain
        assert all_ids_in_chains == set(self.dag.all_nodes.keys())

    def test_each_node_in_exactly_one_chain(self):
        chains = self.dag.get_linear_chains()
        seen: list[int] = []
        for chain in chains:
            seen.extend(chain)
        assert len(seen) == len(set(seen)), "Nodes appear in more than one chain"


# ---------------------------------------------------------------------------
# build_dag — error cases
# ---------------------------------------------------------------------------


class TestBuildDagErrors:
    def test_raises_on_unknown_tool_id_in_connection(self, tmp_path):
        bad_xml = """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="0" y="0" />
      </GuiSettings>
      <Properties>
        <Configuration />
        <Annotation DisplayMode="0">
          <Name /><DefaultAnnotationText /><Left value="False" />
        </Annotation>
      </Properties>
      <EngineSettings EngineDll="x.dll" EngineDllEntryPoint="y" />
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output" />
      <Destination ToolID="999" Connection="Input" />
    </Connection>
  </Connections>
</AlteryxDocument>"""
        p = tmp_path / "bad.yxmd"
        p.write_text(bad_xml)
        with pytest.raises(ValueError, match="unknown tool_id"):
            build_dag(parse_workflow(p))

    def test_raises_on_cycle(self, tmp_path):
        cyclic_xml = """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula">
        <Position x="0" y="0" />
      </GuiSettings>
      <Properties>
        <Configuration />
        <Annotation DisplayMode="0">
          <Name /><DefaultAnnotationText /><Left value="False" />
        </Annotation>
      </Properties>
      <EngineSettings EngineDll="x.dll" EngineDllEntryPoint="y" />
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula">
        <Position x="100" y="0" />
      </GuiSettings>
      <Properties>
        <Configuration />
        <Annotation DisplayMode="0">
          <Name /><DefaultAnnotationText /><Left value="False" />
        </Annotation>
      </Properties>
      <EngineSettings EngineDll="x.dll" EngineDllEntryPoint="y" />
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output" />
      <Destination ToolID="2" Connection="Input" />
    </Connection>
    <Connection>
      <Origin ToolID="2" Connection="Output" />
      <Destination ToolID="1" Connection="Input" />
    </Connection>
  </Connections>
</AlteryxDocument>"""
        p = tmp_path / "cyclic.yxmd"
        p.write_text(cyclic_xml)
        with pytest.raises(ValueError, match="cycle"):
            build_dag(parse_workflow(p))


# ---------------------------------------------------------------------------
# build_dag — real example file
# ---------------------------------------------------------------------------


class TestBuildDagExample:
    @pytest.fixture(autouse=True)
    def setup(self):
        if not EXAMPLE.exists():
            pytest.skip("Example file not found")
        self.dag = build_dag(parse_workflow(EXAMPLE))

    def test_parses_without_error(self):
        assert self.dag.node_count() > 0

    def test_tool_containers_excluded(self):
        # Containers 58 and 73 must not appear in the DAG
        assert 58 not in self.dag.all_nodes
        assert 73 not in self.dag.all_nodes

    def test_container_children_included(self):
        # Tool 49 (DbFileOutput inside container 58) must be in the DAG
        assert 49 in self.dag.all_nodes

    def test_is_acyclic(self):
        import networkx as nx

        assert nx.is_directed_acyclic_graph(self.dag.graph)

    def test_topological_order_length(self):
        order = self.dag.topological_order()
        assert len(order) == self.dag.node_count()

    def test_topological_order_valid(self):
        order = self.dag.topological_order()
        pos = {tid: i for i, tid in enumerate(order)}
        for conn in self.dag._connections:
            assert pos[conn.origin_id] < pos[conn.dest_id]

    def test_has_source_and_sink_nodes(self):
        assert len(self.dag.source_nodes()) > 0
        assert len(self.dag.sink_nodes()) > 0

    def test_linear_chains_cover_all_nodes(self):
        chains = self.dag.get_linear_chains()
        all_ids = {tid for chain in chains for tid in chain}
        assert all_ids == set(self.dag.all_nodes.keys())

    def test_each_node_in_exactly_one_chain(self):
        chains = self.dag.get_linear_chains()
        seen: list[int] = []
        for chain in chains:
            seen.extend(chain)
        assert len(seen) == len(set(seen))
