"""Deterministic post-processing pass that removes redundant subquery wrappers
from simple stub source temp tables.

The cte_builder always emits:

    SELECT * INTO #name FROM (
        -- !!! STUB — requires manual review !!!
        SELECT [col1], [col2], ...
        FROM SomeTable
    ) AS [_src];

When the inner query is a plain STUB SELECT … FROM <table> (no WHERE, JOIN,
GROUP BY, or subquery), the outer wrapper is unnecessary.  This pass collapses
it to:

    -- !!! STUB — requires manual review !!!
    SELECT
        [col1],
        [col2]
    INTO #name
    FROM SomeTable;

A second pattern handled here is an outer SELECT with an explicit column list
wrapping the same kind of simple stub — produced either by the AI enhancer or
by future pipeline changes:

    SELECT
        [col1], [col2]
    INTO #name
    FROM (
        -- !!! STUB...
        SELECT [col1], [col2], [col3], [col4]
        FROM SomeTable
    ) AS [_src];

Both collapse to the same unwrapped form.  If the inner query is complex
(contains WHERE / JOIN / GROUP BY / subquery keywords) the pattern does not
match and the block is left unchanged.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Pattern A — outer is "SELECT *" (current cte_builder default output)
# ---------------------------------------------------------------------------
# Matches:
#     <indent>SELECT * INTO #name FROM (
#     <inner_indent>-- !!! STUB...
#     <inner_indent>SELECT
#     <col_indent>[col1],
#     ...
#     <inner_indent>FROM TableName
#     <indent>) AS [_src];
_PATTERN_A = re.compile(
    r"(?P<indent>[ \t]+)SELECT \* INTO (?P<temp>#\w+) FROM \(\n"
    r"(?P<stub>[ \t]+-- !!! STUB[^\n]*)\n"
    r"[ \t]+SELECT\n"
    r"(?P<inner_cols>(?:[ \t]+[^\n]+\n)*?)"
    r"[ \t]+FROM (?P<table>[A-Za-z_]\w*)[ \t]*\n"
    r"[ \t]+\) AS \[_src\];",
)

# ---------------------------------------------------------------------------
# Pattern B — outer has an explicit column list (AI-enhanced or future output)
# ---------------------------------------------------------------------------
# Matches:
#     <indent>SELECT
#     <col_indent>[col1],
#     <col_indent>[col2]
#     <indent>INTO #name
#     <indent>FROM (
#     <inner_indent>-- !!! STUB...
#     <inner_indent>SELECT
#     <inner_col_indent>[more], [cols]
#     <inner_indent>FROM TableName
#     <indent>) AS [_src];
_PATTERN_B = re.compile(
    r"(?P<indent>[ \t]+)SELECT\n"
    r"(?P<outer_cols>(?:[ \t]+[^\n]+,\n)*[ \t]+[^\n]+)\n"
    r"[ \t]+INTO (?P<temp>#\w+)\n"
    r"[ \t]+FROM \(\n"
    r"(?P<stub>[ \t]+-- !!! STUB[^\n]*)\n"
    r"[ \t]+SELECT\n"
    r"(?:[ \t]+[^\n]+\n)*?"
    r"[ \t]+FROM (?P<table>[A-Za-z_]\w*)[ \t]*\n"
    r"[ \t]+\) AS \[_src\];",
)

# Inner query is only safe to unwrap when it has no these keywords after SELECT
_COMPLEX_INNER = re.compile(r"\b(WHERE|JOIN|GROUP\s+BY|HAVING|ORDER\s+BY|UNION)\b", re.IGNORECASE)


def _build_unwrapped(indent: str, stub_line: str, col_lines_raw: str, temp: str, table: str) -> str:
    """Build the simplified SELECT ... INTO #temp FROM table; block."""
    col_lines = [
        f"{indent}    {line.strip()}"
        for line in col_lines_raw.splitlines()
        if line.strip()
    ]
    return "\n".join([
        f"{indent}{stub_line}",
        f"{indent}SELECT",
        *col_lines,
        f"{indent}INTO {temp}",
        f"{indent}FROM {table};",
    ])


def _replace_a(m: re.Match) -> str:
    inner_block = m.group("inner_cols")
    if _COMPLEX_INNER.search(inner_block):
        return m.group(0)  # leave complex inner queries unchanged
    return _build_unwrapped(
        indent=m.group("indent"),
        stub_line=m.group("stub").strip(),
        col_lines_raw=inner_block,
        temp=m.group("temp"),
        table=m.group("table"),
    )


def _replace_b(m: re.Match) -> str:
    # Use outer column list — it's already the filtered/correct set
    outer_cols = m.group("outer_cols")
    if _COMPLEX_INNER.search(m.group(0)):
        return m.group(0)
    return _build_unwrapped(
        indent=m.group("indent"),
        stub_line=m.group("stub").strip(),
        col_lines_raw=outer_cols,
        temp=m.group("temp"),
        table=m.group("table"),
    )


def simplify_stub_sources(sql: str) -> str:
    """Remove subquery wrappers from simple stub source temp tables.

    Safe to call on any generated SQL — only matches the specific patterns
    described in the module docstring; leaves everything else unchanged.
    """
    sql = _PATTERN_A.sub(_replace_a, sql)
    sql = _PATTERN_B.sub(_replace_b, sql)
    return sql
