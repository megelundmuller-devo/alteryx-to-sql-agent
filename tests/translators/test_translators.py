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
from parsing.models import Chunk, CTEFragment, FieldSchema, ToolNode
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

    def test_unknown_passthrough_expands_with_schema(self):
        """Unknown pass-through + explicit cols uses upstream schema to add remaining cols."""
        from translators.select import translate_select

        cfg = {
            "SelectFields": {
                "SelectField": [
                    {"name": "id", "selected": "True"},
                    {"name": "name", "selected": "True", "rename": "full_name"},
                    {"Unknown": "True"},
                ]
            }
        }
        node = _node(1, "select", cfg)
        ctx = _make_ctx()
        # Upstream schema has three columns; id and name are handled explicitly
        ctx.cte_schema["up"] = [
            FieldSchema(name="id", alteryx_type="Int32"),
            FieldSchema(name="name", alteryx_type="V_String"),
            FieldSchema(name="created_at", alteryx_type="DateTime"),
        ]
        frag = translate_select(node, "cte_1", ["up"], ctx)
        assert "[id]" in frag.sql
        assert "[name] AS [full_name]" in frag.sql
        # Pass-through column not explicitly handled must appear
        assert "[created_at]" in frag.sql
        assert "SELECT *" not in frag.sql

    def test_unknown_passthrough_only_gives_star(self):
        """Pure pass-through (no explicit columns) emits SELECT *."""
        from translators.select import translate_select

        cfg = {
            "SelectFields": {
                "SelectField": [
                    {"Unknown": "True"},
                ]
            }
        }
        node = _node(1, "select", cfg)
        ctx = _make_ctx()
        frag = translate_select(node, "cte_1", ["up"], ctx)
        assert "SELECT *" in frag.sql


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
    # translate_join always returns list[CTEFragment]; first element is the J (inner) CTE.

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
        frags = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)
        j = frags[0]
        assert j.name == "cte_1"
        assert "INNER JOIN" in j.sql
        assert "L.[id] = R.[user_id]" in j.sql

    def test_join_insufficient_inputs(self):
        from translators.join import translate_join

        node = _node(1, "join", {})
        ctx = _make_ctx()
        frags = translate_join(node, "cte_1", ["only_one"], ctx)
        assert frags[0].is_stub

    def test_join_schema_explicit_columns_with_right_prefix(self):
        """When both CTE schemas are known, join emits explicit cols; clash gets Right_ prefix."""
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        ctx.cte_schema["left_cte"] = [
            FieldSchema(name="id", alteryx_type="Int32"),
            FieldSchema(name="name", alteryx_type="V_String"),
        ]
        ctx.cte_schema["right_cte"] = [
            FieldSchema(name="id", alteryx_type="Int32"),
            FieldSchema(name="score", alteryx_type="Double"),
        ]
        j = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)[0]
        assert "L.[id]" in j.sql
        assert "L.[name]" in j.sql
        assert "R.[id] AS [Right_id]" in j.sql
        assert "R.[score]" in j.sql
        assert "R.[score] AS" not in j.sql

    def test_join_schema_custom_prefix_from_config(self):
        """RenameRightInput in config overrides the default Right_ prefix."""
        from translators.join import translate_join

        cfg = {
            "RenameRightInput": "R_",
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ],
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        ctx.cte_schema["left_cte"] = [FieldSchema(name="id", alteryx_type="Int32")]
        ctx.cte_schema["right_cte"] = [FieldSchema(name="id", alteryx_type="Int32")]
        j = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)[0]
        assert "R.[id] AS [R_id]" in j.sql

    def test_join_schema_no_clash_no_rename(self):
        """When right columns don't clash with left, no renaming is applied."""
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "order_id"}]},
                {"side": "Right", "Field": [{"name": "order_id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        ctx.cte_schema["left_cte"] = [
            FieldSchema(name="order_id", alteryx_type="Int32"),
            FieldSchema(name="amount", alteryx_type="Double"),
        ]
        ctx.cte_schema["right_cte"] = [
            FieldSchema(name="order_id", alteryx_type="Int32"),
            FieldSchema(name="product", alteryx_type="V_String"),
        ]
        j = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)[0]
        assert "R.[product]" in j.sql
        assert "R.[product] AS" not in j.sql
        assert "R.[order_id] AS [Right_order_id]" in j.sql

    def test_join_falls_back_to_star_when_schema_missing(self):
        """Without schema info, fall back to L.*, R.*."""
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        j = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)[0]
        assert "L.*" in j.sql
        assert "R.*" in j.sql

    def test_join_l_anchor_emits_left_anti_join(self):
        """L anchor → LEFT JOIN … WHERE R.key IS NULL, only left columns selected."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        ctx.cte_schema["left_cte"] = [
            FieldSchema(name="id", alteryx_type="Int32"),
            FieldSchema(name="val", alteryx_type="V_String"),
        ]
        ctx.cte_schema["right_cte"] = [FieldSchema(name="id", alteryx_type="Int32")]

        # Simulate the L anchor being connected downstream
        l_conn = Connection(origin_id=1, origin_anchor="Left", dest_id=99, dest_anchor="Input")
        ctx.dag._graph.add_node(1)
        ctx.dag._graph.add_node(99)
        ctx.dag._graph.add_edge(1, 99, connections=[l_conn])

        frags = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)
        names = [f.name for f in frags]
        assert "cte_1" in names       # J always present
        assert "cte_1_L" in names     # L anchor emitted

        l_frag = next(f for f in frags if f.name == "cte_1_L")
        assert "LEFT JOIN" in l_frag.sql
        assert "WHERE R.[id] IS NULL" in l_frag.sql
        # SELECT clause contains only left columns; R.[id] only appears in ON/WHERE
        select_part = l_frag.sql.split("FROM")[0]
        assert "L.[id]" in select_part
        assert "L.[val]" in select_part
        assert "R.[" not in select_part

        # Schema tracked as left-only columns
        assert [f.name for f in ctx.cte_schema["cte_1_L"]] == ["id", "val"]

    def test_join_r_anchor_emits_right_anti_join(self):
        """R anchor → RIGHT JOIN … WHERE L.key IS NULL, only right columns selected."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        ctx = _make_ctx()
        ctx.cte_schema["left_cte"] = [FieldSchema(name="id", alteryx_type="Int32")]
        ctx.cte_schema["right_cte"] = [
            FieldSchema(name="id", alteryx_type="Int32"),
            FieldSchema(name="score", alteryx_type="Double"),
        ]

        r_conn = Connection(origin_id=1, origin_anchor="Right", dest_id=99, dest_anchor="Input")
        ctx.dag._graph.add_node(1)
        ctx.dag._graph.add_node(99)
        ctx.dag._graph.add_edge(1, 99, connections=[r_conn])

        frags = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)
        names = [f.name for f in frags]
        assert "cte_1" in names
        assert "cte_1_R" in names

        r_frag = next(f for f in frags if f.name == "cte_1_R")
        assert "RIGHT JOIN" in r_frag.sql
        assert "WHERE L.[id] IS NULL" in r_frag.sql
        assert "R.[id]" in r_frag.sql
        assert "R.[score]" in r_frag.sql

        assert [f.name for f in ctx.cte_schema["cte_1_R"]] == ["id", "score"]

    def test_join_all_three_anchors(self):
        """All three anchors connected → three CTEFragments produced."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "k"}]},
                {"side": "Right", "Field": [{"name": "k"}]},
            ]
        }
        node = _node(5, "join", cfg)
        ctx = _make_ctx()

        for anchor, dest in [("Join", 10), ("Left", 11), ("Right", 12)]:
            conn = Connection(origin_id=5, origin_anchor=anchor, dest_id=dest, dest_anchor="Input")
            ctx.dag._graph.add_node(5)
            ctx.dag._graph.add_node(dest)
            ctx.dag._graph.add_edge(5, dest, connections=[conn])

        frags = translate_join(node, "cte_5", ["lc", "rc"], ctx)
        assert len(frags) == 3
        names = {f.name for f in frags}
        assert names == {"cte_5", "cte_5_L", "cte_5_R"}

        j = next(f for f in frags if f.name == "cte_5")
        assert "INNER JOIN" in j.sql
        l_frag = next(f for f in frags if f.name == "cte_5_L")
        assert "LEFT JOIN" in l_frag.sql
        r_frag = next(f for f in frags if f.name == "cte_5_R")
        assert "RIGHT JOIN" in r_frag.sql

    def test_join_jl_same_union_collapses_to_left_join(self):
        """J+L both feeding the same Union → J becomes LEFT JOIN, L is a passthrough alias."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(1, "join", cfg)
        union_node = _node(10, "union", {})
        ctx = _make_ctx()
        ctx.dag._nodes[1] = node
        ctx.dag._nodes[10] = union_node

        j_conn = Connection(origin_id=1, origin_anchor="Join", dest_id=10, dest_anchor="Input")
        l_conn = Connection(origin_id=1, origin_anchor="Left", dest_id=10, dest_anchor="Input2")
        ctx.dag._graph.add_node(1)
        ctx.dag._graph.add_node(10)
        ctx.dag._graph.add_edge(1, 10, connections=[j_conn, l_conn])

        frags = translate_join(node, "cte_1", ["left_cte", "right_cte"], ctx)

        names = [f.name for f in frags]
        assert "cte_1" in names
        assert "cte_1_L" not in names          # L fragment must NOT be emitted

        j = next(f for f in frags if f.name == "cte_1")
        assert "LEFT JOIN" in j.sql            # J upgraded to LEFT JOIN
        assert "WHERE" not in j.sql            # no anti-join filter

        assert ctx.cte_passthrough.get("cte_1_L") == "cte_1"  # passthrough registered

    def test_join_jr_same_union_collapses_to_right_join(self):
        """J+R both feeding the same Union → J becomes RIGHT JOIN, R is a passthrough alias."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(2, "join", cfg)
        union_node = _node(20, "union", {})
        ctx = _make_ctx()
        ctx.dag._nodes[2] = node
        ctx.dag._nodes[20] = union_node

        j_conn = Connection(origin_id=2, origin_anchor="Join", dest_id=20, dest_anchor="Input")
        r_conn = Connection(origin_id=2, origin_anchor="Right", dest_id=20, dest_anchor="Input2")
        ctx.dag._graph.add_node(2)
        ctx.dag._graph.add_node(20)
        ctx.dag._graph.add_edge(2, 20, connections=[j_conn, r_conn])

        frags = translate_join(node, "cte_2", ["left_cte", "right_cte"], ctx)

        names = [f.name for f in frags]
        assert "cte_2_R" not in names
        j = next(f for f in frags if f.name == "cte_2")
        assert "RIGHT JOIN" in j.sql
        assert ctx.cte_passthrough.get("cte_2_R") == "cte_2"

    def test_join_jlr_same_union_collapses_to_full_outer_join(self):
        """J+L+R all feeding the same Union → J becomes FULL OUTER JOIN."""
        from parsing.models import Connection
        from translators.join import translate_join

        cfg = {
            "JoinInfo": [
                {"side": "Left", "Field": [{"name": "id"}]},
                {"side": "Right", "Field": [{"name": "id"}]},
            ]
        }
        node = _node(3, "join", cfg)
        union_node = _node(30, "union", {})
        ctx = _make_ctx()
        ctx.dag._nodes[3] = node
        ctx.dag._nodes[30] = union_node

        conns = [
            Connection(origin_id=3, origin_anchor="Join", dest_id=30, dest_anchor="Input"),
            Connection(origin_id=3, origin_anchor="Left", dest_id=30, dest_anchor="Input2"),
            Connection(origin_id=3, origin_anchor="Right", dest_id=30, dest_anchor="Input3"),
        ]
        ctx.dag._graph.add_node(3)
        ctx.dag._graph.add_node(30)
        ctx.dag._graph.add_edge(3, 30, connections=conns)

        frags = translate_join(node, "cte_3", ["left_cte", "right_cte"], ctx)

        names = [f.name for f in frags]
        assert len(frags) == 1                 # only J fragment
        assert "cte_3_L" not in names
        assert "cte_3_R" not in names
        j = next(f for f in frags if f.name == "cte_3")
        assert "FULL OUTER JOIN" in j.sql
        assert ctx.cte_passthrough.get("cte_3_L") == "cte_3"
        assert ctx.cte_passthrough.get("cte_3_R") == "cte_3"


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

    def test_union_resolves_join_passthrough(self):
        """Union([J, L→J]) collapses to SELECT * FROM [J] — no UNION in output."""
        from translators.union import translate_union

        node = _node(10, "union", {})
        ctx = _make_ctx()
        ctx.cte_passthrough["cte_join_L"] = "cte_join"

        frag = translate_union(node, "cte_union", ["cte_join", "cte_join_L"], ctx)

        assert "UNION" not in frag.sql
        assert "[cte_join]" in frag.sql        # resolves to single LEFT JOIN CTE

    def test_union_partial_passthrough_not_deduplicated(self):
        """When only some inputs are passthroughs the non-aliased ones are preserved."""
        from translators.union import translate_union

        node = _node(11, "union", {})
        ctx = _make_ctx()
        ctx.cte_passthrough["cte_a_L"] = "cte_a"

        frag = translate_union(node, "cte_union", ["cte_a", "cte_a_L", "cte_b"], ctx)

        # cte_a_L resolves to cte_a → deduplicated → effective inputs: [cte_a, cte_b]
        assert "UNION ALL" in frag.sql
        assert "[cte_a]" in frag.sql
        assert "[cte_b]" in frag.sql
        # cte_a_L should not appear — it was resolved away
        assert "cte_a_L" not in frag.sql


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
