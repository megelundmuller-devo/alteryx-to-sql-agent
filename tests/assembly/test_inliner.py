"""Tests for src/assembly/inliner.py."""

import pytest
from parsing.models import CTEFragment
from assembly.inliner import collapse_fragments


def _frag(
    name: str,
    sql: str,
    *,
    chunk_id: int | None = None,
    chunk_output_name: str | None = None,
    is_stub: bool = False,
) -> CTEFragment:
    return CTEFragment(
        name=name,
        sql=sql,
        source_tool_ids=[],
        chunk_id=chunk_id,
        chunk_output_name=chunk_output_name,
        is_stub=is_stub,
    )


def _chain(*frags: CTEFragment) -> list[CTEFragment]:
    """Convenience: assign chunk_id=1 and chunk_output_name = last frag's name."""
    output = frags[-1].name
    return [
        f.model_copy(update={"chunk_id": 1, "chunk_output_name": output})
        for f in frags
    ]


class TestNoChunkId:
    def test_no_chunk_id_passes_through_unchanged(self):
        frags = [
            _frag("a", "SELECT * FROM [dbo].[t]"),
            _frag("b", "SELECT * FROM [a]"),
        ]
        result = collapse_fragments(frags)
        assert len(result) == 2

    def test_mixed_chunk_id_and_none(self):
        frags = [
            _frag("a", "SELECT * FROM [dbo].[t]"),
            _frag("b", "SELECT * FROM [a]", chunk_id=1, chunk_output_name="b"),
        ]
        result = collapse_fragments(frags)
        assert len(result) == 2  # 'a' is unchunked, 'b' is single-fragment chunk


class TestPassthroughElimination:
    def test_select_star_input_eliminated(self):
        frags = _chain(
            _frag("inp", "SELECT *\nFROM [dbo].[raw]"),
            _frag("sel", "SELECT\n    [a],\n    [b]\nFROM [inp]"),
        )
        result = collapse_fragments(frags)
        assert len(result) == 1
        assert result[0].name == "sel"
        assert "[dbo].[raw]" in result[0].sql
        assert "[inp]" not in result[0].sql

    def test_passthrough_chain_all_collapsed(self):
        frags = _chain(
            _frag("a", "SELECT *\nFROM [dbo].[raw]"),
            _frag("b", "SELECT *\nFROM [a]"),
            _frag("c", "SELECT *\nFROM [b]"),
        )
        result = collapse_fragments(frags)
        # b and a are passthroughs; c (primary) stays but references raw directly
        assert len(result) == 1
        assert result[0].name == "c"
        assert "[dbo].[raw]" in result[0].sql

    def test_primary_output_never_eliminated(self):
        """Even if the only fragment is a passthrough, it is kept as the output."""
        frags = [_frag("out", "SELECT *\nFROM [dbo].[t]", chunk_id=1, chunk_output_name="out")]
        result = collapse_fragments(frags)
        assert len(result) == 1
        assert result[0].name == "out"


class TestWhereHoisting:
    def test_filter_hoisted_onto_select(self):
        frags = _chain(
            _frag("inp", "SELECT *\nFROM [dbo].[raw]"),
            _frag("sel", "SELECT\n    [a],\n    [b]\nFROM [dbo].[raw]"),
            _frag("flt", "SELECT *\nFROM [sel]\nWHERE [a] > 0"),
        )
        # After passthrough of inp: sel has FROM [dbo].[raw]
        # Then filter hoisted onto sel
        result = collapse_fragments(frags)
        assert len(result) == 1
        sql = result[0].sql
        assert "WHERE [a] > 0" in sql
        assert "UNION" not in sql
        assert "[sel]" not in sql

    def test_filter_on_plain_input(self):
        frags = _chain(
            _frag("inp", "SELECT *\nFROM [dbo].[raw]"),
            _frag("flt", "SELECT *\nFROM [inp]\nWHERE [x] > 5"),
        )
        result = collapse_fragments(frags)
        assert len(result) == 1
        sql = result[0].sql
        assert "FROM [dbo].[raw]" in sql
        assert "WHERE [x] > 5" in sql

    def test_where_not_hoisted_past_group_by(self):
        """WHERE must not be hoisted onto a GROUP BY fragment."""
        frags = _chain(
            _frag("grp", "SELECT [a], COUNT(*) AS [n]\nFROM [src]\nGROUP BY [a]"),
            _frag("flt", "SELECT *\nFROM [grp]\nWHERE [n] > 1"),
        )
        result = collapse_fragments(frags)
        # Cannot hoist — flt must wrap grp in a subquery
        assert len(result) == 1
        sql = result[0].sql
        assert "GROUP BY" in sql
        assert "WHERE [n] > 1" in sql
        # Should be subquery-nested, not flat
        assert "(\n" in sql

    def test_where_not_hoisted_past_window_fn(self):
        frags = _chain(
            _frag("win", "SELECT *, ROW_NUMBER() OVER (ORDER BY [x]) AS _rn\nFROM [src]"),
            _frag("flt", "SELECT *\nFROM [win]\nWHERE _rn = 1"),
        )
        result = collapse_fragments(frags)
        assert len(result) == 1
        sql = result[0].sql
        assert "ROW_NUMBER()" in sql
        assert "WHERE _rn = 1" in sql
        assert "(\n" in sql  # subquery nested


class TestSubqueryNesting:
    def test_formula_on_input_nested(self):
        frags = _chain(
            _frag("inp", "SELECT *\nFROM [dbo].[raw]"),
            _frag("frm", "SELECT\n    [a],\n    [a]*2 AS [b]\nFROM [inp]"),
            _frag("sel", "SELECT\n    [a],\n    [b]\nFROM [frm]"),
        )
        result = collapse_fragments(frags)
        assert len(result) == 1
        sql = result[0].sql
        # inp eliminated (passthrough), frm nested into sel
        assert "[inp]" not in sql
        assert "[frm]" not in sql
        assert "[dbo].[raw]" in sql
        assert "[a]*2 AS [b]" in sql
        assert "(\n" in sql  # subquery present

    def test_secondary_not_in_chain(self):
        """filter_false is a secondary; chain collapses without it."""
        inp = _frag("inp", "SELECT *\nFROM [dbo].[raw]")
        true_f = _frag("out", "SELECT *\nFROM [inp]\nWHERE [x] > 0")
        false_f = _frag("out_false", "SELECT *\nFROM [inp]\nWHERE NOT ([x] > 0)")
        # Both secondaries reference inp (which is a passthrough) — passthrough elim substitutes
        frags = [
            inp.model_copy(update={"chunk_id": 1, "chunk_output_name": "out"}),
            true_f.model_copy(update={"chunk_id": 1, "chunk_output_name": "out"}),
            false_f.model_copy(update={"chunk_id": 1, "chunk_output_name": "out"}),
        ]
        result = collapse_fragments(frags)
        # inp eliminated; true and false both get FROM [dbo].[raw]
        assert len(result) == 2
        names = {f.name for f in result}
        assert "out" in names
        assert "out_false" in names
        for f in result:
            assert "[dbo].[raw]" in f.sql
            assert "[inp]" not in f.sql


class TestStubs:
    def test_stub_chain_not_collapsed(self):
        frags = _chain(
            _frag("inp", "SELECT *\nFROM [dbo].[raw]"),
            _frag("stub", "SELECT TOP 0 1 AS _stub", is_stub=True),
        )
        result = collapse_fragments(frags)
        assert len(result) == 2  # not collapsed


class TestMultipleChunks:
    def test_two_independent_chunks(self):
        chunk1 = [
            _frag("a1", "SELECT *\nFROM [dbo].[t1]", chunk_id=1, chunk_output_name="a2"),
            _frag("a2", "SELECT [x]\nFROM [a1]", chunk_id=1, chunk_output_name="a2"),
        ]
        chunk2 = [
            _frag("b1", "SELECT *\nFROM [dbo].[t2]", chunk_id=2, chunk_output_name="b2"),
            _frag("b2", "SELECT [y]\nFROM [b1]", chunk_id=2, chunk_output_name="b2"),
        ]
        result = collapse_fragments(chunk1 + chunk2)
        assert len(result) == 2
        assert result[0].name == "a2"
        assert result[1].name == "b2"
