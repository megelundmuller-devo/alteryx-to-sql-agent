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

Table references can be simple names, bracketed names, or qualified names
(e.g. SomeTable, [SomeTable], [dbo].[SomeTable]).

This module uses a line-by-line state-machine parser instead of a single
large regex, which avoids catastrophic backtracking on large SQL files.
"""

from __future__ import annotations

import re

# Matches: <indent>SELECT * INTO #name FROM (
_SELECT_STAR_INTO_RE = re.compile(
    r"^(?P<indent>[ \t]+)SELECT \* INTO (?P<temp>#\w+) FROM \($"
)

# Matches: <indent>-- !!! STUB...
_STUB_RE = re.compile(r"^[ \t]+-- !!! STUB")

# Matches: <indent>SELECT  (bare SELECT, no columns on same line)
_SELECT_KW_RE = re.compile(r"^[ \t]+SELECT$", re.IGNORECASE)

# Matches a FROM line with a T-SQL table reference.
# Accepts: simple, [bracketed], schema.table, [schema].[table], etc.
_FROM_TABLE_RE = re.compile(
    r"^[ \t]+FROM (?P<table>(?:\[[^\]]*\]|\w+)(?:\.(?:\[[^\]]*\]|\w+))*)[ \t]*$",
    re.IGNORECASE,
)

# Matches: <indent>FROM  (any FROM, including complex ones like JOIN subqueries)
_FROM_ANY_RE = re.compile(r"^[ \t]+FROM\b", re.IGNORECASE)

# Matches: <indent>) AS [_src];
_CLOSING_RE = re.compile(r"^[ \t]+\) AS \[_src\];")

# Inner query is only safe to unwrap when it contains none of these keywords.
_COMPLEX_INNER = re.compile(
    r"\b(WHERE|JOIN|GROUP\s+BY|HAVING|ORDER\s+BY|UNION)\b", re.IGNORECASE
)


def _build_simplified(
    indent: str, stub_line: str, col_lines: list[str], temp: str, table: str
) -> str:
    col_parts = [f"{indent}    {ln.strip()}" for ln in col_lines if ln.strip()]
    return "\n".join(
        [
            f"{indent}{stub_line}",
            f"{indent}SELECT",
            *col_parts,
            f"{indent}INTO {temp}",
            f"{indent}FROM {table};",
        ]
    )


def _try_simplify(
    lines: list[str], start: int, m: re.Match
) -> tuple[str | None, int]:
    """Try to parse and simplify a SELECT * INTO #name FROM ( block.

    Returns (simplified_text, next_line_index) on success, or (None, start)
    when the block does not match the expected shape.
    """
    indent = m.group("indent")
    temp = m.group("temp")
    i = start + 1
    n = len(lines)

    if i >= n or not _STUB_RE.match(lines[i]):
        return None, start
    stub_line = lines[i].strip()
    i += 1

    if i >= n or not _SELECT_KW_RE.match(lines[i]):
        return None, start
    i += 1

    col_lines: list[str] = []
    while i < n:
        line = lines[i]

        from_m = _FROM_TABLE_RE.match(line)
        if from_m:
            table = from_m.group("table")
            i += 1
            if i >= n or not _CLOSING_RE.match(lines[i]):
                return None, start
            if _COMPLEX_INNER.search("\n".join(col_lines)):
                return None, start
            return _build_simplified(indent, stub_line, col_lines, temp, table), i + 1

        if _FROM_ANY_RE.match(line):
            # FROM with a complex reference (brackets, subquery, etc.) — don't simplify.
            return None, start

        col_lines.append(line)
        i += 1

    return None, start


def simplify_stub_sources(sql: str) -> str:
    """Remove subquery wrappers from simple stub source temp tables.

    Safe to call on any generated SQL — only matches the specific pattern
    described in the module docstring; leaves everything else unchanged.
    """
    lines = sql.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _SELECT_STAR_INTO_RE.match(line)
        if m:
            simplified, end_i = _try_simplify(lines, i, m)
            if simplified is not None:
                out.append(simplified)
                i = end_i
                continue
        out.append(line)
        i += 1
    return "\n".join(out)
