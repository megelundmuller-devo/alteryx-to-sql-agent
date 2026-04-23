"""Tests for src/assembly/cte_builder.py."""

from assembly.cte_builder import build_sql
from parsing.models import CTEFragment


def _frag(name: str, sql: str, is_stub: bool = False, source_tool_ids: list[int] | None = None) -> CTEFragment:
    return CTEFragment(name=name, sql=sql, source_tool_ids=source_tool_ids or [], is_stub=is_stub)


class TestBuildSql:
    def test_empty_fragments(self):
        result = build_sql([])
        assert "_empty" in result

    def test_single_fragment(self):
        frag = _frag("temp_source", "SELECT 1 AS col")
        result = build_sql([frag], workflow_name="test.yxmd")
        assert "SELECT * INTO #temp_source" in result
        assert "SELECT 1 AS col" in result
        assert "SELECT * FROM #temp_source" in result

    def test_multiple_fragments_linear_chain_uses_ctes(self):
        """A → B → C linear chain: A and B become CTEs, C is the temp table."""
        frags = [
            _frag("temp_a", "SELECT * FROM raw"),
            _frag("temp_b", "SELECT * FROM [temp_a]"),
            _frag("temp_c", "SELECT * FROM [temp_b]"),
        ]
        result = build_sql(frags, workflow_name="multi.yxmd")
        # A and B are CTEs (ref-count == 1), C is the materialised temp table
        assert "[temp_a] AS (" in result
        assert "[temp_b] AS (" in result
        assert "SELECT * INTO #temp_c" in result
        # CTE references are NOT rewritten to #name inside the WITH block
        assert "FROM [temp_a]" in result
        assert "FROM [temp_b]" in result
        # Final SELECT targets the materialised temp table
        assert "SELECT * FROM #temp_c" in result

    def test_fan_out_materialises_shared_node(self):
        """When two fragments both reference the same upstream, it must be a temp table."""
        frags = [
            _frag("base", "SELECT * FROM raw"),
            _frag("branch1", "SELECT [a] FROM [base]"),
            _frag("branch2", "SELECT [b] FROM [base]"),
        ]
        result = build_sql(frags, workflow_name="fanout.yxmd")
        # base is referenced by both branch1 and branch2 → must materialise
        assert "SELECT * INTO #base" in result
        assert "#base" in result  # branch1 and branch2 reference #base

    def test_stub_annotation(self):
        frags = [
            _frag("temp_good", "SELECT 1"),
            _frag("temp_stub", "SELECT TOP 0 1 AS _stub", is_stub=True),
        ]
        result = build_sql(frags, workflow_name="stub_test.yxmd")
        assert "STUB" in result.upper()

    def test_stub_always_materialised(self):
        """A stub fragment is always a temp table, never a CTE."""
        frags = [
            _frag("temp_inp", "SELECT 1 AS x"),
            _frag("temp_stub", "SELECT TOP 0 1 AS _stub", is_stub=True),
            _frag("temp_out", "SELECT * FROM [temp_stub]"),
        ]
        result = build_sql(frags, workflow_name="stub.yxmd")
        assert "SELECT * INTO #temp_stub" in result

    def test_header_contains_workflow_name(self):
        result = build_sql(
            [_frag("temp_a", "SELECT 1")], workflow_name="my_workflow.yxmd"
        )
        assert "my_workflow.yxmd" in result

    def test_stub_count_in_header(self):
        frags = [
            _frag("temp_a", "SELECT 1"),
            _frag("temp_b", "SELECT 2", is_stub=True),
            _frag("temp_c", "SELECT 3", is_stub=True),
        ]
        result = build_sql(frags, workflow_name="x.yxmd")
        assert "2" in result  # stub count

    def test_indentation_applied(self):
        frag = _frag("temp_a", "SELECT\n    *\nFROM raw")
        result = build_sql([frag])
        body_lines = [line for line in result.splitlines() if "FROM raw" in line]
        assert body_lines, "Expected 'FROM raw' inside temp table body"
        for line in body_lines:
            assert line.startswith("    "), f"Body line not indented: {line!r}"

    def test_script_ends_with_go(self):
        frag = _frag("temp_a", "SELECT 1")
        result = build_sql([frag])
        assert result.rstrip().endswith("GO")

    def test_stored_procedure_structure(self):
        frag = _frag("temp_a", "SELECT 1")
        result = build_sql([frag], workflow_name="my_workflow.yxmd")
        assert "CREATE PROCEDURE [dbo].[my_workflow]" in result
        assert "AS" in result
        assert "BEGIN" in result
        assert "SET NOCOUNT ON" in result
        assert "END;" in result
        assert result.rstrip().endswith("GO")

    def test_proc_name_sanitised(self):
        """Spaces and hyphens in the workflow name become underscores in the proc name."""
        frag = _frag("temp_a", "SELECT 1")
        result = build_sql([frag], workflow_name="BI Report - Daily (2024).yxmd")
        assert "CREATE PROCEDURE [dbo].[BI_Report_Daily_2024]" in result

    def test_chunk_grouping_preserved_across_section_boundary(self):
        """Linear chain: input → select → output appear in order.

        With CTE grouping the chain becomes two CTEs + one temp table, but
        the topological order of their definitions must still be preserved.
        """
        frags = [
            _frag("temp_input_1", "SELECT * FROM [dbo].[raw]", source_tool_ids=[1]),
            _frag("temp_select_2", "SELECT [a], [b] FROM [temp_input_1]", source_tool_ids=[2]),
            _frag("temp_output_3", "SELECT * FROM [temp_select_2]", source_tool_ids=[3]),
        ]
        result = build_sql(
            frags,
            workflow_name="test.yxmd",
            source_ids={1},
            sink_ids={3},
        )
        # input and select are CTEs (ref-count == 1); output is the temp table
        pos_input = result.index("[temp_input_1]")
        pos_select = result.index("[temp_select_2]")
        pos_output = result.index("#temp_output_3")
        assert pos_input < pos_select < pos_output, (
            "CTE definitions or temp table are out of order"
        )

    def test_leaf_secondary_materialised(self):
        """A fragment referenced by nobody (a leaf side-output) is always a temp table."""
        frags = [
            _frag("src", "SELECT * FROM raw"),
            _frag("main_out", "SELECT [a] FROM [src]"),
            _frag("side_out", "SELECT [b] FROM [src]"),
        ]
        result = build_sql(frags, workflow_name="leaf.yxmd")
        # src is referenced by main_out AND side_out → fan-out → temp table
        assert "SELECT * INTO #src" in result
        assert "SELECT * INTO #main_out" in result
        assert "SELECT * INTO #side_out" in result

    def test_independent_chains_not_mixed_into_same_with_block(self):
        """Two independent CTE chains must produce separate WITH blocks.

        chain_a: src_a → filter_a → mat_a
        chain_b: src_b → select_b → mat_b

        filter_a must only appear in mat_a's WITH block, not in mat_b's.
        If filter_a were in mat_b's WITH block it would be out of scope when
        mat_a (a later temp table) tries to reference it.
        """
        frags = [
            _frag("src_a", "SELECT * FROM [dbo].[a]"),
            _frag("filter_a", "SELECT * FROM [src_a] WHERE x = 1"),
            _frag("src_b", "SELECT * FROM [dbo].[b]"),
            _frag("select_b", "SELECT col FROM [src_b]"),
            # mat_b is next in list but does NOT reference filter_a or src_a
            _frag("mat_b", "SELECT * FROM [select_b]"),
            _frag("mat_a", "SELECT * FROM [filter_a]"),
        ]
        result = build_sql(frags, workflow_name="chains.yxmd")

        # Each chain's WITH block must be separate: find the positions
        idx_filter_a = result.index("[filter_a] AS")
        idx_mat_a = result.index("#mat_a")
        idx_select_b = result.index("[select_b] AS")
        idx_mat_b = result.index("#mat_b")

        # filter_a must appear in the same WITH block as mat_a (before it)
        assert idx_filter_a < idx_mat_a
        # select_b must appear in the same WITH block as mat_b (before it)
        assert idx_select_b < idx_mat_b

        # The two WITH blocks must be separate: filter_a must NOT appear between
        # select_b's WITH header and mat_b's SELECT INTO
        with_b_start = result.index("WITH\n", result.index("[select_b] AS"))
        into_b = result.index("SELECT * INTO #mat_b")
        between_b = result[with_b_start:into_b]
        assert "[filter_a]" not in between_b, (
            "filter_a leaked into mat_b's WITH block"
        )

    def test_cte_refs_not_rewritten_to_hash(self):
        """Within a WITH block, CTE names must stay as [name], not be rewritten to #name."""
        frags = [
            _frag("step1", "SELECT 1 AS x"),
            _frag("step2", "SELECT x FROM [step1]"),
        ]
        result = build_sql(frags, workflow_name="ref.yxmd")
        # step1 is a CTE; its reference in step2 stays as [step1]
        assert "FROM [step1]" in result
        assert "FROM #step1" not in result
