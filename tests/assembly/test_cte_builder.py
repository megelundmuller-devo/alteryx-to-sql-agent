"""Tests for src/assembly/cte_builder.py."""

from assembly.cte_builder import build_sql
from parsing.models import CTEFragment


def _frag(name: str, sql: str, is_stub: bool = False) -> CTEFragment:
    return CTEFragment(name=name, sql=sql, source_tool_ids=[], is_stub=is_stub)


class TestBuildSql:
    def test_empty_fragments(self):
        result = build_sql([])
        assert "_empty" in result

    def test_single_fragment(self):
        frag = _frag("cte_source", "SELECT 1 AS col")
        result = build_sql([frag], workflow_name="test.yxmd")
        assert "WITH" in result
        assert "[cte_source]" in result
        assert "SELECT 1 AS col" in result
        assert "SELECT * FROM [cte_source]" in result

    def test_multiple_fragments(self):
        frags = [
            _frag("cte_a", "SELECT * FROM raw"),
            _frag("cte_b", "SELECT * FROM [cte_a]"),
            _frag("cte_c", "SELECT * FROM [cte_b]"),
        ]
        result = build_sql(frags, workflow_name="multi.yxmd")
        assert "[cte_a]" in result
        assert "[cte_b]" in result
        assert "[cte_c]" in result
        # Last CTE is the terminal SELECT target
        assert "SELECT * FROM [cte_c]" in result

    def test_stub_annotation(self):
        frags = [
            _frag("cte_good", "SELECT 1"),
            _frag("cte_stub", "SELECT TOP 0 1 AS _stub", is_stub=True),
        ]
        result = build_sql(frags, workflow_name="stub_test.yxmd")
        assert "STUB" in result.upper()

    def test_header_contains_workflow_name(self):
        result = build_sql(
            [_frag("cte_a", "SELECT 1")], workflow_name="my_workflow.yxmd"
        )
        assert "my_workflow.yxmd" in result

    def test_stub_count_in_header(self):
        frags = [
            _frag("cte_a", "SELECT 1"),
            _frag("cte_b", "SELECT 2", is_stub=True),
            _frag("cte_c", "SELECT 3", is_stub=True),
        ]
        result = build_sql(frags, workflow_name="x.yxmd")
        assert "2" in result  # stub count

    def test_indentation_applied(self):
        frag = _frag("cte_a", "SELECT\n    *\nFROM raw")
        result = build_sql([frag])
        # The body of the CTE should be indented inside the WITH block.
        # "FROM raw" only appears inside the CTE body, so it must be indented.
        body_lines = [line for line in result.splitlines() if "FROM raw" in line]
        assert body_lines, "Expected 'FROM raw' inside CTE body"
        for line in body_lines:
            assert line.startswith("    "), f"Body line not indented: {line!r}"

    def test_script_ends_with_semicolon(self):
        frag = _frag("cte_a", "SELECT 1")
        result = build_sql([frag])
        assert result.rstrip().endswith(";")
