"""Tests for src/parser.py — .yxmd XML parsing."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from parsing.parser import _elem_to_value, _normalize_plugin, parse_workflow

FIXTURES = Path(__file__).parent / "fixtures"
MINIMAL = FIXTURES / "minimal_workflow.yxmd"
EXAMPLE = (
    Path(__file__).parents[2] / "examples" / "BI_Aggregate Daily Simple_LDB-01.yxmd"
)


# ---------------------------------------------------------------------------
# _normalize_plugin
# ---------------------------------------------------------------------------


class TestNormalizePlugin:
    def test_known_plugins(self):
        assert _normalize_plugin("AlteryxBasePluginsGui.Filter.Filter") == "filter"
        assert _normalize_plugin("AlteryxBasePluginsGui.Join.Join") == "join"
        assert _normalize_plugin("AlteryxBasePluginsGui.Formula.Formula") == "formula"
        assert (
            _normalize_plugin("AlteryxBasePluginsGui.DbFileInput.DbFileInput")
            == "db_file_input"
        )
        assert (
            _normalize_plugin("AlteryxSpatialPluginsGui.Summarize.Summarize")
            == "summarize"
        )
        assert (
            _normalize_plugin("AlteryxGuiToolkit.ToolContainer.ToolContainer")
            == "tool_container"
        )

    def test_unknown_plugin_fallback(self):
        # Unknown plugins should produce a snake_case key, not raise
        result = _normalize_plugin(
            "AlteryxBasePluginsGui.SomeFutureWidget.SomeFutureWidget"
        )
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# _elem_to_value
# ---------------------------------------------------------------------------


class TestElemToValue:
    def _parse(self, xml_str: str) -> ET.Element:
        return ET.fromstring(xml_str)

    def test_leaf_text_only(self):
        elem = self._parse("<Mode>Custom</Mode>")
        assert _elem_to_value(elem) == "Custom"

    def test_attributes_only(self):
        elem = self._parse('<Field name="OrderID" type="Int32" />')
        result = _elem_to_value(elem)
        assert isinstance(result, dict)
        assert result["name"] == "OrderID"
        assert result["type"] == "Int32"

    def test_text_and_attributes(self):
        elem = self._parse('<File FileFormat="23">mydb|||table</File>')
        result = _elem_to_value(elem)
        assert isinstance(result, dict)
        assert result["FileFormat"] == "23"
        assert result["_text"] == "mydb|||table"

    def test_single_child(self):
        elem = self._parse(
            "<FormulaFields><FormulaField expression='x' /></FormulaFields>"
        )
        result = _elem_to_value(elem)
        assert isinstance(result, dict)
        assert "FormulaField" in result
        assert isinstance(result["FormulaField"], dict)

    def test_multiple_same_tag_children_become_list(self):
        xml = """
        <SummarizeFields>
          <SummarizeField field="a" action="GroupBy" />
          <SummarizeField field="b" action="Sum" />
        </SummarizeFields>
        """
        elem = self._parse(xml)
        result = _elem_to_value(elem)
        assert isinstance(result["SummarizeField"], list)
        assert len(result["SummarizeField"]) == 2

    def test_empty_element(self):
        elem = self._parse("<Name />")
        assert _elem_to_value(elem) == ""


# ---------------------------------------------------------------------------
# parse_workflow — minimal fixture
# ---------------------------------------------------------------------------


class TestParseWorkflowMinimal:
    def setup_method(self):
        self.pw = parse_workflow(MINIMAL)

    def test_returns_parsed_workflow(self):
        assert self.pw.source_file == "minimal_workflow.yxmd"

    def test_node_count(self):
        # 6 nodes: 1 db_file_input, 1 filter, 1 formula, 1 summarize,
        #          1 tool_container, 1 db_file_output (nested), 1 macro = 7
        ids = {n.tool_id for n in self.pw.nodes}
        assert ids == {1, 2, 3, 4, 99, 5, 31}

    def test_tool_types_correct(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        assert by_id[1].tool_type == "db_file_input"
        assert by_id[2].tool_type == "filter"
        assert by_id[3].tool_type == "formula"
        assert by_id[4].tool_type == "summarize"
        assert by_id[99].tool_type == "tool_container"
        assert by_id[5].tool_type == "db_file_output"
        assert by_id[31].tool_type == "macro"

    def test_macro_node_path(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        assert by_id[31].macro_path == "Cleanse.yxmc"

    def test_connection_count(self):
        assert len(self.pw.connections) == 5

    def test_connection_anchors(self):
        # First connection: 1 Output → 2 Input
        first = next(
            c for c in self.pw.connections if c.origin_id == 1 and not c.wireless
        )
        assert first.origin_anchor == "Output"
        assert first.dest_anchor == "Input"
        assert first.dest_id == 2

    def test_filter_true_branch_connection(self):
        conn = next(c for c in self.pw.connections if c.origin_id == 2)
        assert conn.origin_anchor == "True"

    def test_wireless_connection(self):
        wireless = [c for c in self.pw.connections if c.wireless]
        assert len(wireless) == 1
        assert wireless[0].origin_id == 1
        assert wireless[0].dest_id == 31

    def test_schema_parsed_for_db_input(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        schema = by_id[1].output_schema
        assert len(schema) == 3
        names = [f.name for f in schema]
        assert "OrderID" in names
        assert "CustomerID" in names
        assert "Amount" in names
        amount = next(f for f in schema if f.name == "Amount")
        assert amount.alteryx_type == "Double"

    def test_schema_size_parsed(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        cust = next(f for f in by_id[1].output_schema if f.name == "CustomerID")
        assert cust.size == 50

    def test_annotation_extracted(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        assert "Orders" in by_id[1].annotation

    def test_position_parsed(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        assert by_id[1].position == (100, 100)
        assert by_id[3].position == (400, 100)

    def test_formula_config_parsed(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        cfg = by_id[3].config
        assert "FormulaFields" in cfg
        ff = cfg["FormulaFields"]["FormulaField"]
        assert ff["field"] == "AmountWithVAT"
        assert "1.25" in ff["expression"]

    def test_summarize_config_parsed(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        cfg = by_id[4].config
        fields = cfg["SummarizeFields"]["SummarizeField"]
        assert isinstance(fields, list)
        assert len(fields) == 2
        group_by = next(f for f in fields if f["action"] == "GroupBy")
        assert group_by["field"] == "CustomerID"

    def test_filter_config_mode(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        cfg = by_id[2].config
        assert cfg["Mode"] == "Custom"
        assert "Amount" in cfg["Expression"]

    def test_db_file_config_has_query(self):
        by_id = {n.tool_id: n for n in self.pw.nodes}
        cfg = by_id[1].config
        file_val = cfg["File"]
        assert isinstance(file_val, dict)
        assert "mydb|||select" in file_val["_text"]


# ---------------------------------------------------------------------------
# parse_workflow — real example file
# ---------------------------------------------------------------------------


class TestParseWorkflowExample:
    @pytest.fixture(autouse=True)
    def setup(self):
        if not EXAMPLE.exists():
            pytest.skip("Example file not found")
        self.pw = parse_workflow(EXAMPLE)

    def test_parses_without_error(self):
        assert len(self.pw.nodes) > 0
        assert len(self.pw.connections) > 0

    def test_expected_tool_types_present(self):
        types = {n.tool_type for n in self.pw.nodes}
        assert "db_file_input" in types
        assert "join" in types
        assert "filter" in types
        assert "formula" in types
        assert "summarize" in types
        assert "union" in types
        assert "macro" in types

    def test_macro_nodes_have_macro_path(self):
        macros = [n for n in self.pw.nodes if n.tool_type == "macro"]
        assert len(macros) > 0
        for m in macros:
            assert m.macro_path is not None
            assert m.macro_path.endswith(".yxmc")

    def test_tool_container_children_extracted(self):
        # Child nodes (e.g., DbFileOutput inside container) should be in the flat list
        ids = {n.tool_id for n in self.pw.nodes}
        # Tool 49 is inside container 58 in the example
        assert 49 in ids
        assert 58 in ids  # container itself also present

    def test_connections_reference_valid_tool_ids(self):
        node_ids = {n.tool_id for n in self.pw.nodes}
        for conn in self.pw.connections:
            assert conn.origin_id in node_ids, f"Origin {conn.origin_id} not found"
            assert conn.dest_id in node_ids, f"Dest {conn.dest_id} not found"

    def test_ordered_connections_parsed(self):
        ordered = [c for c in self.pw.connections if c.order is not None]
        assert len(ordered) > 0
        orders = {c.order for c in ordered}
        assert 1 in orders
        assert 2 in orders

    def test_wireless_connections_parsed(self):
        wireless = [c for c in self.pw.connections if c.wireless]
        assert len(wireless) > 0

    def test_db_input_schema_extracted(self):
        db_inputs = [n for n in self.pw.nodes if n.tool_type == "db_file_input"]
        with_schema = [n for n in db_inputs if n.output_schema]
        assert len(with_schema) > 0
