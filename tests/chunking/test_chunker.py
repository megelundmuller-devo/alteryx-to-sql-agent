"""Tests for src/chunking/chunker.py — DAG chunking logic."""

from pathlib import Path

import pytest

from chunking.chunker import (
    _cte_name,
    chunk_dag,
)
from parsing.dag import build_dag
from parsing.models import Chunk
from parsing.parser import parse_workflow

FIXTURES = Path(__file__).parents[1] / "parsing" / "fixtures"
MINIMAL = FIXTURES / "minimal_workflow.yxmd"
EXAMPLE = (
    Path(__file__).parents[2] / "examples" / "BI_Aggregate Daily Simple_LDB-01.yxmd"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chunk_minimal() -> tuple[list[Chunk], object]:
    dag = build_dag(parse_workflow(MINIMAL))
    return chunk_dag(dag), dag


# ---------------------------------------------------------------------------
# _cte_name
# ---------------------------------------------------------------------------


class TestCteName:
    def _node(self, tool_id: int, tool_type: str):
        from parsing.models import ToolNode

        return ToolNode(
            tool_id=tool_id,
            plugin="",
            tool_type=tool_type,
            config={},
            annotation="",
            position=(0, 0),
            output_schema=[],
        )

    def test_single_node(self):
        n = self._node(5, "filter")
        assert _cte_name([n]) == "cte_filter_5"

    def test_multi_node(self):
        n1 = self._node(1, "db_file_input")
        n2 = self._node(3, "formula")
        assert _cte_name([n1, n2]) == "cte_db_file_input_1_to_formula_3"


# ---------------------------------------------------------------------------
# chunk_dag — minimal fixture
# ---------------------------------------------------------------------------


class TestChunkDagMinimal:
    def setup_method(self):
        self.chunks, self.dag = _chunk_minimal()

    def test_returns_list_of_chunks(self):
        assert isinstance(self.chunks, list)
        assert all(isinstance(c, Chunk) for c in self.chunks)

    def test_chunk_ids_sequential(self):
        ids = [c.chunk_id for c in self.chunks]
        assert ids == list(range(len(self.chunks)))

    def test_every_dag_node_in_exactly_one_chunk(self):
        dag_ids = set(self.dag.all_nodes.keys())
        chunk_ids: list[int] = []
        for c in self.chunks:
            chunk_ids.extend(n.tool_id for n in c.nodes)
        assert set(chunk_ids) == dag_ids
        assert len(chunk_ids) == len(set(chunk_ids)), "Duplicate nodes across chunks"

    def test_output_cte_names_unique(self):
        names = [c.output_cte_name for c in self.chunks]
        assert len(names) == len(set(names)), "Duplicate CTE names"

    def test_cte_names_start_with_cte(self):
        for c in self.chunks:
            assert c.output_cte_name.startswith("cte_")

    def test_filter_is_its_own_chunk(self):
        # Filter (tool 2) is a branching node — must be its own chunk
        filter_chunk = next(
            c for c in self.chunks if any(n.tool_id == 2 for n in c.nodes)
        )
        assert len(filter_chunk.nodes) == 1
        assert filter_chunk.nodes[0].tool_type == "filter"

    def test_source_chunk_has_no_input_ctes(self):
        # Tool 1 (DbFileInput) is a source — its chunk has no upstream CTEs
        source_chunk = next(
            c for c in self.chunks if any(n.tool_id == 1 for n in c.nodes)
        )
        assert source_chunk.input_cte_names == []

    def test_sink_chunk_has_input_ctes(self):
        # Tool 5 (DbFileOutput) depends on upstream — must have input CTEs
        sink_chunk = next(
            c for c in self.chunks if any(n.tool_id == 5 for n in c.nodes)
        )
        assert len(sink_chunk.input_cte_names) > 0

    def test_internal_edges_within_chunk(self):
        # For merged multi-node chains, internal edges should reference only nodes in that chunk
        for chunk in self.chunks:
            node_ids = {n.tool_id for n in chunk.nodes}
            for edge in chunk.edges:
                assert edge.origin_id in node_ids
                assert edge.dest_id in node_ids

    def test_topological_order_across_chunks(self):
        # Each chunk's input CTEs must be output CTEs of earlier chunks
        seen_ctes: set[str] = set()
        for chunk in self.chunks:
            for input_cte in chunk.input_cte_names:
                assert input_cte in seen_ctes, (
                    f"Chunk {chunk.chunk_id} references '{input_cte}' "
                    f"before it is produced. Seen so far: {seen_ctes}"
                )
            seen_ctes.add(chunk.output_cte_name)

    def test_formula_and_summarize_may_merge(self):
        # Tools 3 (formula) and 4 (summarize) are single-I/O — can be in same chunk
        # (depends on whether filter's True branch allows merging downstream)
        # At minimum, they should not violate boundary rules individually
        formula_chunk = next(
            c for c in self.chunks if any(n.tool_id == 3 for n in c.nodes)
        )
        summarize_chunk = next(
            c for c in self.chunks if any(n.tool_id == 4 for n in c.nodes)
        )
        # Both must exist in some chunk — just verify they are present
        assert formula_chunk is not None
        assert summarize_chunk is not None


# ---------------------------------------------------------------------------
# chunk_dag — boundary rule tests via synthetic DAGs
# ---------------------------------------------------------------------------


class TestChunkBoundaryRules:
    def _make_dag(self, xml: str, tmp_path):
        p = tmp_path / "wf.yxmd"
        p.write_text(xml)
        return build_dag(parse_workflow(p))

    def _wrap(self, nodes_xml: str, conns_xml: str) -> str:
        return f"""<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Nodes>{nodes_xml}</Nodes>
  <Connections>{conns_xml}</Connections>
</AlteryxDocument>"""

    def _node_xml(self, tool_id: int, plugin: str, pos: tuple = (0, 0)) -> str:
        x, y = pos
        return f"""
    <Node ToolID="{tool_id}">
      <GuiSettings Plugin="{plugin}">
        <Position x="{x}" y="{y}" />
      </GuiSettings>
      <Properties>
        <Configuration />
        <Annotation DisplayMode="0"><Name /><DefaultAnnotationText /><Left value="False" /></Annotation>
      </Properties>
      <EngineSettings EngineDll="x.dll" EngineDllEntryPoint="y" />
    </Node>"""

    def _conn_xml(
        self,
        origin_id: int,
        origin_anchor: str,
        dest_id: int,
        dest_anchor: str = "Input",
    ) -> str:
        return f"""
    <Connection>
      <Origin ToolID="{origin_id}" Connection="{origin_anchor}" />
      <Destination ToolID="{dest_id}" Connection="{dest_anchor}" />
    </Connection>"""

    def test_join_gets_its_own_chunk(self, tmp_path):
        """Join (multi-input) must be a separate chunk from its predecessors."""
        nodes = (
            self._node_xml(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(2, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(3, "AlteryxBasePluginsGui.Join.Join")
        )
        conns = self._conn_xml(1, "Output", 3, "Left") + self._conn_xml(
            2, "Output", 3, "Right"
        )
        dag = self._make_dag(self._wrap(nodes, conns), tmp_path)
        chunks = chunk_dag(dag)

        join_chunk = next(c for c in chunks if any(n.tool_id == 3 for n in c.nodes))
        assert all(n.tool_id == 3 for n in join_chunk.nodes), (
            "Join shares chunk with non-join tool"
        )

    def test_filter_one_output_merges_with_successor(self, tmp_path):
        """A filter with only one connected output (True only) is linear — merges downstream."""
        nodes = (
            self._node_xml(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(2, "AlteryxBasePluginsGui.Filter.Filter")
            + self._node_xml(3, "AlteryxBasePluginsGui.Formula.Formula")
        )
        conns = self._conn_xml(1, "Output", 2) + self._conn_xml(2, "True", 3)
        dag = self._make_dag(self._wrap(nodes, conns), tmp_path)
        chunks = chunk_dag(dag)

        # Filter with 1 output → out_degree=1 → no boundary → can merge with formula
        filter_chunk = next(c for c in chunks if any(n.tool_id == 2 for n in c.nodes))
        formula_chunk = next(c for c in chunks if any(n.tool_id == 3 for n in c.nodes))
        assert filter_chunk is formula_chunk, (
            "Single-output filter should merge with successor"
        )

    def test_filter_two_outputs_ends_its_chunk(self, tmp_path):
        """A filter with both True and False connected must end its chunk."""
        nodes = (
            self._node_xml(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(2, "AlteryxBasePluginsGui.Filter.Filter")
            + self._node_xml(3, "AlteryxBasePluginsGui.Formula.Formula")
            + self._node_xml(4, "AlteryxBasePluginsGui.Formula.Formula")
        )
        conns = (
            self._conn_xml(1, "Output", 2)
            + self._conn_xml(2, "True", 3)
            + self._conn_xml(2, "False", 4)
        )
        dag = self._make_dag(self._wrap(nodes, conns), tmp_path)
        chunks = chunk_dag(dag)

        filter_chunk = next(c for c in chunks if any(n.tool_id == 2 for n in c.nodes))
        formula_true_chunk = next(
            c for c in chunks if any(n.tool_id == 3 for n in c.nodes)
        )
        formula_false_chunk = next(
            c for c in chunks if any(n.tool_id == 4 for n in c.nodes)
        )
        assert filter_chunk is not formula_true_chunk, (
            "Filter with 2 outputs must end its chunk"
        )
        assert formula_true_chunk is not formula_false_chunk, (
            "True/False branches must be separate chunks"
        )

    def test_linear_chain_merges(self, tmp_path):
        """Three single-I/O tools in a row should merge into one chunk."""
        nodes = (
            self._node_xml(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(2, "AlteryxBasePluginsGui.Formula.Formula")
            + self._node_xml(3, "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect")
            + self._node_xml(4, "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput")
        )
        conns = (
            self._conn_xml(1, "Output", 2)
            + self._conn_xml(2, "Output", 3)
            + self._conn_xml(3, "Output", 4)
        )
        dag = self._make_dag(self._wrap(nodes, conns), tmp_path)
        chunks = chunk_dag(dag)

        # All 4 tools may end up in 1 chunk (linear chain — no boundaries)
        # At minimum, formula and select (2 and 3) must be together or in very few chunks
        tool_chunks = {n.tool_id: c.chunk_id for c in chunks for n in c.nodes}
        # Tools 2 and 3 are single-I/O with single-I/O neighbours — must be in same chunk
        assert tool_chunks[2] == tool_chunks[3], "Linear single-I/O tools not merged"

    def test_union_gets_its_own_chunk(self, tmp_path):
        """Union (multi-input) must be its own chunk."""
        nodes = (
            self._node_xml(1, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(2, "AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            + self._node_xml(3, "AlteryxBasePluginsGui.Union.Union")
        )
        conns = self._conn_xml(1, "Output", 3, "Input") + self._conn_xml(
            2, "Output", 3, "Input"
        )
        dag = self._make_dag(self._wrap(nodes, conns), tmp_path)
        chunks = chunk_dag(dag)

        union_chunk = next(c for c in chunks if any(n.tool_id == 3 for n in c.nodes))
        assert all(n.tool_id == 3 for n in union_chunk.nodes)


# ---------------------------------------------------------------------------
# chunk_dag — real example file
# ---------------------------------------------------------------------------


class TestChunkDagExample:
    @pytest.fixture(autouse=True)
    def setup(self):
        if not EXAMPLE.exists():
            pytest.skip("Example file not found")
        self.dag = build_dag(parse_workflow(EXAMPLE))
        self.chunks = chunk_dag(self.dag)

    def test_produces_chunks(self):
        assert len(self.chunks) > 0

    def test_fewer_chunks_than_nodes(self):
        # Merging should reduce the count
        assert len(self.chunks) < self.dag.node_count()

    def test_all_dag_nodes_covered(self):
        dag_ids = set(self.dag.all_nodes.keys())
        chunk_ids = {n.tool_id for c in self.chunks for n in c.nodes}
        assert chunk_ids == dag_ids

    def test_no_duplicate_nodes(self):
        seen = []
        for c in self.chunks:
            seen.extend(n.tool_id for n in c.nodes)
        assert len(seen) == len(set(seen))

    def test_output_cte_names_unique(self):
        names = [c.output_cte_name for c in self.chunks]
        assert len(names) == len(set(names))

    def test_topological_ordering_preserved(self):
        seen_ctes: set[str] = set()
        for chunk in self.chunks:
            for input_cte in chunk.input_cte_names:
                assert input_cte in seen_ctes, f"CTE '{input_cte}' used before produced"
            seen_ctes.add(chunk.output_cte_name)

    def test_joins_start_their_chunk(self):
        # A join always starts a new chunk (multi-input boundary).
        # Its single-I/O successors MAY merge into the same chunk after it.
        for chunk in self.chunks:
            if any(n.tool_type == "join" for n in chunk.nodes):
                assert chunk.nodes[0].tool_type == "join", (
                    "Join is not the first tool in its chunk — "
                    "something merged into the join from before"
                )

    def test_filters_with_multiple_outputs_end_their_chunk(self):
        # A filter with both True and False connected must end its chunk.
        # A filter with only one output connected may merge with its successor.
        for chunk in self.chunks:
            for i, node in enumerate(chunk.nodes):
                if node.tool_type == "filter":
                    succs = self.dag.successors(node.tool_id)
                    if len(succs) > 1:
                        assert i == len(chunk.nodes) - 1, (
                            f"Filter {node.tool_id} has {len(succs)} outputs "
                            f"but is not the last node in its chunk"
                        )
