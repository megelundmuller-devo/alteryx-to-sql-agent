"""Integration tests for the translator registry and translate_chunk().

Tests cover:
- Each individual translator producing valid CTEFragment output
- translate_chunk() correctly wiring multi-node chunks
- translate_chunk() on real example file chunks
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chunking.chunker import chunk_dag
from parsing.dag import build_dag
from parsing.models import Chunk, CTEFragment, ToolNode
from parsing.parser import parse_workflow
from translators import translate_chunk
from translators.context import TranslationContext

FIXTURES = Path(__file__).parents[1] / "parsing" / "fixtures"
MINIMAL = FIXTURES / "minimal_workflow.yxmd"
EXAMPLE = (
    Path(__file__).parents[2] / "examples" / "BI_Aggregate Daily Simple_LDB-01.yxmd"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(tool_id: int, tool_type: str, config: dict | None = None) -> ToolNode:
    return ToolNode(
        tool_id=tool_id,
        plugin=f"AlteryxBasePluginsGui.{tool_type}.{tool_type}",
        tool_type=tool_type,
        config=config or {},
        annotation="",
        position=(0, 0),
        output_schema=[],
    )


def _single_chunk(node: ToolNode, input_ctes: list[str] | None = None) -> Chunk:
    return Chunk(
        chunk_id=0,
        nodes=[node],
        edges=[],
        input_cte_names=input_ctes or [],
        output_cte_name=f"cte_{node.tool_type}_{node.tool_id}",
    )


def _make_ctx() -> TranslationContext:
    dag = build_dag(parse_workflow(MINIMAL))
    return TranslationContext(dag=dag)


# ---------------------------------------------------------------------------
# Individual translator smoke tests
# ---------------------------------------------------------------------------


class TestInputOutputTranslators:
    def test_db_file_input_no_connection(self):
        from translators.input_output import translate_db_file_input

        node = _node(1, "db_file_input", {})
        ctx = _make_ctx()
        frag = translate_db_file_input(node, "cte_1", [], ctx)
        assert isinstance(frag, CTEFragment)
        assert frag.is_stub
        assert frag.name == "cte_1"
        assert len(ctx.warnings) > 0

    def test_db_file_input_with_query(self):
        from translators.input_output import translate_db_file_input

        cfg = {"Connection": "ODBC|DSN=MyDSN|||SELECT * FROM dbo.MyTable"}
        node = _node(1, "db_file_input", cfg)
        ctx = _make_ctx()
        frag = translate_db_file_input(node, "cte_1", [], ctx)
        assert not frag.is_stub
        assert "SELECT * FROM dbo.MyTable" in frag.sql

    def test_text_input_with_data(self):
        from translators.input_output import translate_text_input

        cfg = {
            "Fields": {"Field": [{"name": "id"}, {"name": "val"}]},
            "Data": {"R": [{"C": [{"v": "1"}, {"v": "hello"}]}]},
        }
        node = _node(2, "text_input", cfg)
        ctx = _make_ctx()
        frag = translate_text_input(node, "cte_2", [], ctx)
        assert "VALUES" in frag.sql
        assert "'1'" in frag.sql
        assert "'hello'" in frag.sql

    def test_text_input_empty(self):
        from translators.input_output import translate_text_input

        cfg = {"Fields": {"Field": [{"name": "col1"}]}, "Data": {}}
        node = _node(3, "text_input", cfg)
        ctx = _make_ctx()
        frag = translate_text_input(node, "cte_3", [], ctx)
        assert "WHERE 1 = 0" in frag.sql

    def test_db_file_output_emits_passthrough(self):
        from translators.input_output import translate_db_file_output

        node = _node(4, "db_file_output", {})
        ctx = _make_ctx()
        frag = translate_db_file_output(node, "cte_4", ["upstream_cte"], ctx)
        assert frag.is_stub
        assert "upstream_cte" in frag.sql
        assert len(ctx.warnings) > 0


class TestSelectTranslator:
    def test_passthrough_no_fields(self):
        from translators.select import translate_select

        node = _node(1, "select", {})
        ctx = _make_ctx()
        frag = translate_select(node, "cte_1", ["up"], ctx)
        assert "SELECT *" in frag.sql
        assert frag.is_stub

    def test_explicit_fields(self):
        from translators.select import translate_select

        cfg = {
            "SelectFields": {
                "SelectField": [
                    {"name": "col1", "selected": "True"},
                    {"name": "col2", "selected": "True", "rename": "col2_renamed"},
                    {"name": "col3", "selected": "False"},
                ]
            }
        }
        node = _node(1, "select", cfg)
        ctx = _make_ctx()
        frag = translate_select(node, "cte_1", ["up"], ctx)
        assert "[col1]" in frag.sql
        assert "[col2] AS [col2_renamed]" in frag.sql
        assert "col3" not in frag.sql


class TestFilterTranslator:
    def test_simple_filter(self):
        from translators.filter import translate_filter

        cfg = {"Expression": '[Status] = "Active"'}
        node = _node(1, "filter", cfg)
        ctx = _make_ctx()
        result = translate_filter(node, "cte_1", ["up"], ctx)
        assert len(result) >= 1
        assert "WHERE" in result[0].sql
        assert "'Active'" in result[0].sql  # double → single quotes

    def test_no_expression_gives_stub(self):
        from translators.filter import translate_filter

        node = _node(1, "filter", {})
        ctx = _make_ctx()
        result = translate_filter(node, "cte_1", ["up"], ctx)
        assert result[0].is_stub

    def test_complex_expression_gives_stub(self, mocker):
        from translators.filter import translate_filter

        # Mock the LLM to simulate a failed conversion so the stub path is tested
        mocker.patch(
            "translators.filter.convert_expression_llm",
            return_value="-- LLM conversion failed: mocked\nNULL  -- MANUAL REVIEW REQUIRED",
        )
        cfg = {"Expression": "IF [x] > 0 THEN True ELSE False ENDIF"}
        node = _node(1, "filter", cfg)
        ctx = _make_ctx()
        result = translate_filter(node, "cte_1", ["up"], ctx)
        assert result[0].is_stub
        assert len(ctx.warnings) > 0


class TestFormulaTranslator:
    def test_simple_formula(self):
        from translators.formula import translate_formula

        cfg = {
            "FormulaFields": {
                "FormulaField": [
                    {"field": "new_col", "expression": "[A] + [B]"},
                ]
            }
        }
        node = _node(1, "formula", cfg)
        ctx = _make_ctx()
        frag = translate_formula(node, "cte_1", ["up"], ctx)
        assert "[A] + [B] AS [new_col]" in frag.sql
        assert not frag.is_stub

    def test_llm_expression_is_stub(self, mocker):
        from translators.formula import translate_formula

        # Mock the LLM to simulate a failed conversion so the stub path is tested
        mocker.patch(
            "translators.formula.convert_expression_llm",
            return_value="-- LLM conversion failed: mocked\nNULL  -- MANUAL REVIEW REQUIRED",
        )
        cfg = {
            "FormulaFields": {
                "FormulaField": [
                    {"field": "flag", "expression": "IF [x] > 0 THEN 1 ELSE 0 ENDIF"},
                ]
            }
        }
        node = _node(1, "formula", cfg)
        ctx = _make_ctx()
        frag = translate_formula(node, "cte_1", ["up"], ctx)
        assert frag.is_stub
        assert len(ctx.warnings) > 0


class TestSummarizeTranslator:
    def test_group_by_sum(self):
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {"field": "Category", "action": "GroupBy", "rename": "Category"},
                    {"field": "Amount", "action": "Sum", "rename": "TotalAmount"},
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        ctx = _make_ctx()
        frag = translate_summarize(node, "cte_1", ["up"], ctx)
        assert "GROUP BY" in frag.sql
        assert "SUM([Amount])" in frag.sql
        assert "[Category]" in frag.sql

    def test_count_distinct(self):
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {
                        "field": "UserId",
                        "action": "CountDistinct",
                        "rename": "UniqueUsers",
                    },
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        ctx = _make_ctx()
        frag = translate_summarize(node, "cte_1", ["up"], ctx)
        assert "COUNT(DISTINCT [UserId])" in frag.sql

    def test_first_last_warn(self):
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {"field": "Col", "action": "First", "rename": "FirstCol"},
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        ctx = _make_ctx()
        frag = translate_summarize(node, "cte_1", ["up"], ctx)
        assert "MIN([Col])" in frag.sql
        assert len(ctx.warnings) > 0

    def test_concat_no_groupby_uses_xml_path(self):
        """Concat without GroupBy: full-table STUFF…FOR XML PATH, no STRING_AGG."""
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {"field": "Tag", "action": "Concat", "rename": "AllTags"},
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        frag = translate_summarize(node, "cte_1", ["up"], _make_ctx())
        assert "STRING_AGG" not in frag.sql
        assert "FOR XML PATH" in frag.sql
        assert "STUFF" in frag.sql
        assert "[AllTags]" in frag.sql
        # Non-correlated — no alias needed
        assert "[_outer]" not in frag.sql
        assert "[_sub]" not in frag.sql

    def test_concat_with_groupby_uses_correlated_xml_path(self):
        """Concat with GroupBy: correlated STUFF…FOR XML PATH per group."""
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {"field": "Category", "action": "GroupBy", "rename": "Category"},
                    {"field": "Tag", "action": "Concat", "rename": "Tags"},
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        frag = translate_summarize(node, "cte_1", ["up"], _make_ctx())
        assert "STRING_AGG" not in frag.sql
        assert "FOR XML PATH" in frag.sql
        assert "[_outer]" in frag.sql
        assert "[_sub]" in frag.sql
        # Correlation condition must reference the group-by column
        assert "[_sub].[Category] = [_outer].[Category]" in frag.sql
        assert "GROUP BY [_outer].[Category]" in frag.sql
        assert "[Tags]" in frag.sql

    def test_concat_distinct_uses_distinct_keyword(self):
        """ConcatDistinct produces DISTINCT inside the XML PATH subquery."""
        from translators.summarize import translate_summarize

        cfg = {
            "SummarizeFields": {
                "SummarizeField": [
                    {"field": "Region", "action": "GroupBy", "rename": "Region"},
                    {"field": "Code", "action": "ConcatDistinct", "rename": "Codes"},
                ]
            }
        }
        node = _node(1, "summarize", cfg)
        frag = translate_summarize(node, "cte_1", ["src"], _make_ctx())
        assert "STRING_AGG" not in frag.sql
        assert "DISTINCT" in frag.sql
        assert "FOR XML PATH" in frag.sql
        assert "[Codes]" in frag.sql


class TestJoinTranslator:
    def test_join_with_keys(self):
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "user_id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        frag = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)
        assert "INNER JOIN" in frag.sql
        assert "L.[id] = R.[user_id]" in frag.sql

    def test_join_insufficient_inputs(self):
        from translators.join import translate_join

        node = _node(1, "join", {})
        ctx = _make_ctx()
        frag = translate_join(node, "cte_1", ["only_one"], ctx)
        assert frag.is_stub


class TestUnionTranslator:
    def test_union_all(self):
        from translators.union import translate_union

        node = _node(1, "union", {})
        ctx = _make_ctx()
        frag = translate_union(node, "cte_1", ["a", "b", "c"], ctx)
        assert "UNION ALL" in frag.sql
        assert "[a]" in frag.sql and "[b]" in frag.sql and "[c]" in frag.sql

    def test_single_input_passthrough(self):
        from translators.union import translate_union

        node = _node(1, "union", {})
        ctx = _make_ctx()
        frag = translate_union(node, "cte_1", ["single"], ctx)
        assert "UNION" not in frag.sql
        assert "[single]" in frag.sql


class TestUniqueTranslator:
    def test_unique_with_keys(self):
        from translators.unique import translate_unique

        cfg = {"UniqueFields": {"Field": [{"field": "customer_id"}]}}
        node = _node(1, "unique", cfg)
        ctx = _make_ctx()
        frag = translate_unique(node, "cte_1", ["up"], ctx)
        assert "ROW_NUMBER()" in frag.sql
        assert "PARTITION BY [customer_id]" in frag.sql
        assert "WHERE _rn = 1" in frag.sql

    def test_unique_no_keys_uses_distinct(self):
        from translators.unique import translate_unique

        node = _node(1, "unique", {})
        ctx = _make_ctx()
        frag = translate_unique(node, "cte_1", ["up"], ctx)
        assert "DISTINCT" in frag.sql


class TestSortTranslator:
    def test_sort_asc_desc(self):
        from translators.sort import translate_sort

        cfg = {
            "SortInfo": {
                "Field": [
                    {"field": "Date", "order": "Descending"},
                    {"field": "Name", "order": "Ascending"},
                ]
            }
        }
        node = _node(1, "sort", cfg)
        ctx = _make_ctx()
        frag = translate_sort(node, "cte_1", ["up"], ctx)
        assert "ROW_NUMBER()" in frag.sql
        assert "[Date] DESC" in frag.sql
        assert "[Name] ASC" in frag.sql


class TestSampleTranslator:
    def test_first_n(self):
        from translators.sample import translate_sample

        node = _node(1, "sample", {"SampleSize": "50", "Mode": "First"})
        ctx = _make_ctx()
        frag = translate_sample(node, "cte_1", ["up"], ctx)
        assert "SELECT TOP 50" in frag.sql
        assert not frag.is_stub

    def test_random(self):
        from translators.sample import translate_sample

        node = _node(1, "sample", {"SampleSize": "10", "Mode": "Random"})
        ctx = _make_ctx()
        frag = translate_sample(node, "cte_1", ["up"], ctx)
        assert "NEWID()" in frag.sql


class TestRecordIdTranslator:
    def test_default_start(self):
        from translators.record_id import translate_record_id

        node = _node(1, "record_id", {"FieldName": "ID", "StartValue": "1"})
        ctx = _make_ctx()
        frag = translate_record_id(node, "cte_1", ["up"], ctx)
        assert "ROW_NUMBER() OVER" in frag.sql
        assert "[ID]" in frag.sql

    def test_nondefault_start(self):
        from translators.record_id import translate_record_id

        node = _node(1, "record_id", {"FieldName": "ID", "StartValue": "100"})
        ctx = _make_ctx()
        frag = translate_record_id(node, "cte_1", ["up"], ctx)
        assert "+ 99" in frag.sql


class TestUnknownTranslator:
    def test_unknown_emits_stub_when_llm_fails(self, mocker):
        from translators.unknown import translate_unknown

        # Mock the LLM to simulate failure so the hard-stub path is exercised
        mocker.patch(
            "translators.unknown.translate_chunk_llm",
            return_value="-- LLM translation failed: mocked\nSELECT TOP 0 1 AS _stub",
        )
        node = _node(1, "some_custom_tool", {})
        ctx = _make_ctx()
        frag = translate_unknown(node, "cte_1", ["up"], ctx)
        assert frag.is_stub
        assert len(ctx.warnings) > 0

    def test_unknown_uses_llm_when_registry_miss(self, mocker):
        from translators.unknown import translate_unknown

        mocker.patch(
            "translators.unknown.translate_chunk_llm",
            return_value="SELECT * FROM [up]  -- LLM translated",
        )
        node = _node(1, "some_custom_tool", {})
        ctx = _make_ctx()
        frag = translate_unknown(node, "cte_1", ["up"], ctx)
        assert not frag.is_stub
        assert "LLM translated" in frag.sql

    def test_unknown_uses_registry_cache(self, tmp_path, mocker):
        from registry.tool_registry import ToolRegistry, make_entry
        from translators.unknown import translate_unknown

        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(
            make_entry("Plugin.Custom", "some_custom_tool", "desc", "SELECT 42 AS _cached", {})
        )
        llm_mock = mocker.patch("translators.unknown.translate_chunk_llm")

        node = _node(1, "some_custom_tool", {})
        node = node.model_copy(update={"plugin": "Plugin.Custom"})
        ctx = _make_ctx()
        ctx.registry = reg
        frag = translate_unknown(node, "cte_1", ["up"], ctx)

        llm_mock.assert_not_called()
        assert "SELECT 42" in frag.sql
        assert not frag.is_stub


# ---------------------------------------------------------------------------
# translate_chunk() integration
# ---------------------------------------------------------------------------


class TestTranslateChunk:
    def test_single_node_chunk(self):
        dag = build_dag(parse_workflow(MINIMAL))
        ctx = TranslationContext(dag=dag)
        chunks = chunk_dag(dag)
        for chunk in chunks:
            frags = translate_chunk(chunk, ctx)
            assert isinstance(frags, list)
            assert len(frags) >= 1
            for f in frags:
                assert isinstance(f, CTEFragment)
                assert f.name

    def test_output_cte_name_in_fragments(self):
        dag = build_dag(parse_workflow(MINIMAL))
        ctx = TranslationContext(dag=dag)
        chunks = chunk_dag(dag)
        for chunk in chunks:
            frags = translate_chunk(chunk, ctx)
            # The chunk's output_cte_name must appear in some fragment
            names = {f.name for f in frags}
            assert chunk.output_cte_name in names, (
                f"chunk {chunk.chunk_id} output_cte_name '{chunk.output_cte_name}' "
                f"not found in fragment names {names}"
            )

    @pytest.mark.skipif(not EXAMPLE.exists(), reason="Example file not present")
    def test_real_example_all_chunks_translate(self):
        dag = build_dag(parse_workflow(EXAMPLE))
        ctx = TranslationContext(dag=dag)
        chunks = chunk_dag(dag)
        all_frags: list[CTEFragment] = []
        for chunk in chunks:
            frags = translate_chunk(chunk, ctx)
            all_frags.extend(frags)
        # Every fragment must have a non-empty name and sql
        for f in all_frags:
            assert f.name
            assert f.sql
