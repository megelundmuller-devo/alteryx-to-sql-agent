"""Tests for doc_gen/doc_writer.py and llm/doc_agent.py."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from parsing.models import CTEFragment, Chunk, Connection, ToolNode

# ---------------------------------------------------------------------------
# Helpers — minimal in-memory objects
# ---------------------------------------------------------------------------


def _make_node(tool_id: int, tool_type: str, annotation: str = "") -> ToolNode:
    return ToolNode(
        tool_id=tool_id,
        plugin=f"Plugin.{tool_type}",
        tool_type=tool_type,
        config={},
        annotation=annotation,
        position=(0, 0),
        output_schema=[],
    )


def _make_fragment(name: str, is_stub: bool = False) -> CTEFragment:
    return CTEFragment(name=name, sql="SELECT 1", source_tool_ids=[1], is_stub=is_stub)


def _make_chunk(output_cte_name: str, nodes: list[ToolNode], inputs: list[str]) -> Chunk:
    return Chunk(
        chunk_id=1,
        nodes=nodes,
        edges=[],
        input_cte_names=inputs,
        output_cte_name=output_cte_name,
    )


def _minimal_dag():
    """Build a tiny two-node DAG: db_file_input → filter."""
    import networkx as nx

    from parsing.dag import AlteryxDAG
    from parsing.models import Connection

    src = _make_node(1, "db_file_input", "Sales table")
    flt = _make_node(2, "filter", "Active only")

    graph = nx.DiGraph()
    graph.add_nodes_from([1, 2])
    conn = Connection(origin_id=1, origin_anchor="Output", dest_id=2, dest_anchor="Input")
    graph.add_edge(1, 2, connection=conn)

    return AlteryxDAG(graph, {1: src, 2: flt}, [conn])


# ---------------------------------------------------------------------------
# doc_writer tests
# ---------------------------------------------------------------------------


class TestGenerateDocs:
    WORKFLOW_PATH = Path("/fake/my_workflow.yxmd")

    def _run(self, narrative: str = "Mocked summary."):
        from doc_gen.doc_writer import generate_docs

        dag = _minimal_dag()
        src_node = dag.get_node(1)
        flt_node = dag.get_node(2)
        chunks = [
            _make_chunk("cte_src", [src_node], []),
            _make_chunk("cte_flt", [flt_node], ["cte_src"]),
        ]
        fragments = [_make_fragment("cte_src"), _make_fragment("cte_flt", is_stub=True)]
        warnings = ["Tool 2: stub emitted"]

        with patch("doc_gen.doc_writer.generate_workflow_summary", return_value=narrative):
            return generate_docs(self.WORKFLOW_PATH, dag, chunks, fragments, warnings)

    def test_header_contains_workflow_name(self):
        md = self._run()
        assert "my_workflow" in md

    def test_overview_section_present(self):
        md = self._run()
        assert "## Overview" in md

    def test_llm_narrative_included(self):
        md = self._run(narrative="This workflow filters sales data.")
        assert "This workflow filters sales data." in md

    def test_fallback_when_llm_returns_empty(self):
        md = self._run(narrative="")
        assert "Documentation generation unavailable" in md

    def test_data_flow_section_present(self):
        md = self._run()
        assert "## Data Flow" in md
        assert "db_file_input" in md
        assert "filter" in md

    def test_sources_and_sinks_listed(self):
        md = self._run()
        assert "Sales table" in md
        assert "Active only" in md

    def test_cte_index_present(self):
        md = self._run()
        assert "## CTEs" in md
        assert "`cte_src`" in md
        assert "`cte_flt`" in md

    def test_stub_flagged_in_cte_index(self):
        md = self._run()
        assert "cte_flt` ⚠ stub" in md

    def test_warnings_section_present(self):
        md = self._run()
        assert "## Warnings" in md
        assert "stub emitted" in md

    def test_no_warnings_section_when_empty(self):
        from doc_gen.doc_writer import generate_docs

        dag = _minimal_dag()
        src = dag.get_node(1)
        chunks = [_make_chunk("cte_src", [src], [])]
        fragments = [_make_fragment("cte_src")]

        with patch("doc_gen.doc_writer.generate_workflow_summary", return_value="Summary."):
            md = generate_docs(self.WORKFLOW_PATH, dag, chunks, fragments, [])
        assert "## Warnings" not in md

    def test_stats_line_present(self):
        md = self._run()
        assert "Tools:" in md
        assert "CTEs:" in md
        assert "Stubs:" in md


# ---------------------------------------------------------------------------
# doc_agent tests
# ---------------------------------------------------------------------------


class TestDocAgent:
    def test_returns_empty_string_on_exception(self):
        from llm.doc_agent import generate_workflow_summary

        with patch("llm.doc_agent._get_agent") as mock_get:
            mock_get.return_value.run_sync.side_effect = RuntimeError("network error")
            result = generate_workflow_summary("any prompt")
        assert result == ""

    def test_returns_llm_output_on_success(self):
        from llm.doc_agent import generate_workflow_summary

        mock_result = type("R", (), {"output": "  Generated summary.  "})()
        with patch("llm.doc_agent._get_agent") as mock_get:
            mock_get.return_value.run_sync.return_value = mock_result
            result = generate_workflow_summary("prompt")
        assert result == "Generated summary."
