"""Translators for source and sink tools.

Covered tool types
------------------
* db_file_input  — DbFileInput (reads from a file or database via ODBC connection string)
* text_input     — TextInput   (inline rows defined in the workflow XML)
* db_file_output — DbFileOutput (write/append to a table or file — becomes a terminal comment)

DbFileInput connection string format
-------------------------------------
Alteryx stores the connection as a pipe-delimited string:

    "ODBC|connection_string|||SELECT * FROM table"
    "file:///C:/path/to/file.csv"

We parse these on a best-effort basis to produce a useful SQL stub.
If the connection string cannot be decoded we emit a labelled stub CTE.
"""

from __future__ import annotations

import re

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext

# Matches a simple "SELECT * FROM <target>" with no WHERE/JOIN/GROUP/etc.
# Used to decide whether a SELECT * can be safely expanded to explicit columns.
_SIMPLE_SELECT_STAR_RE = re.compile(
    r"^\s*SELECT\s+\*\s+FROM\s+(.*?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SQL_CLAUSE_RE = re.compile(
    r"\b(WHERE|JOIN|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|INTERSECT|EXCEPT|SUBQUERY)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# DbFileInput
# ---------------------------------------------------------------------------

_ODBC_PATTERN = re.compile(r"[Ss][Qq][Ll]\s*=\s*(.+)", re.DOTALL)
_SELECT_PATTERN = re.compile(r"(SELECT\s.+)", re.DOTALL | re.IGNORECASE)


def _extract_query(connection_string: str) -> str | None:
    """Try to extract a SELECT statement from an Alteryx connection string."""
    # Format: "ODBC|DSN=foo;...|||SELECT * FROM dbo.MyTable"
    if "|||" in connection_string:
        query_part = connection_string.split("|||", 1)[1].strip()
        if query_part.upper().startswith("SELECT"):
            return query_part

    # Sometimes embedded as Sql= key
    m = _ODBC_PATTERN.search(connection_string)
    if m:
        return m.group(1).strip()

    # Raw SQL in the connection itself
    m = _SELECT_PATTERN.search(connection_string)
    if m:
        return m.group(1).strip()

    return None


def _connection_label(connection_string: str) -> str:
    """Return a short human-readable label for a connection string."""
    if connection_string.lower().startswith("file://"):
        return connection_string.split("/")[-1]
    if "DSN=" in connection_string:
        m = re.search(r"DSN=([^;]+)", connection_string)
        if m:
            return m.group(1)
    if "|||" in connection_string:
        return connection_string.split("|||")[0].split("|")[-1][:60]
    return connection_string[:60]


def translate_db_file_input(
    node: ToolNode,
    cte_name: str,
    _input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Translate a DbFileInput node into a CTE."""
    cfg = node.config

    # The connection string lives at different keys depending on the workflow version:
    #   <Connection>...</Connection>          older format
    #   <DbConnection>...</DbConnection>      some versions
    #   <File FileFormat="23">alias|||SQL</File>  most common in .yxmd files
    file_val = cfg.get("File", "")
    file_text = file_val.get("_text", "") if isinstance(file_val, dict) else str(file_val or "")
    conn_str: str = (
        cfg.get("Connection", "")
        or cfg.get("ConnectionString", "")
        or cfg.get("DbConnection", {}).get("_text", "")
        or file_text
        or ""
    )

    if not conn_str and isinstance(cfg.get("DbConnection"), str):
        conn_str = cfg["DbConnection"]

    query = _extract_query(conn_str) if conn_str else None

    if query:
        sql = query.rstrip(";")
        # If the source query is a plain SELECT * and we have the output schema from
        # <MetaInfo><RecordInfo>, expand to explicit columns.  This makes downstream
        # schema tracking reliable and the generated SQL self-documenting.
        if node.output_schema:
            m = _SIMPLE_SELECT_STAR_RE.match(sql)
            if m:
                from_target = m.group(1).strip()
                if not _SQL_CLAUSE_RE.search(from_target):
                    cols = ",\n".join(f"    [{f.name}]" for f in node.output_schema)
                    sql = f"SELECT\n{cols}\nFROM {from_target}"
    else:
        label = _connection_label(conn_str) if conn_str else "UNKNOWN_SOURCE"
        ctx.warnings.append(
            f"Tool {node.tool_id} ({node.tool_type}): could not parse connection string — "
            f"generated stub CTE. Review manually. Connection: {conn_str!r:.120}"
        )
        annotation = node.annotation or label
        sql = (
            f"-- TODO: replace with actual query\n"
            f"-- Source: {annotation}\n"
            f"-- Connection: {label}\n"
            f"SELECT TOP 0 1 AS _stub"
        )
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


# ---------------------------------------------------------------------------
# TextInput
# ---------------------------------------------------------------------------


def translate_text_input(
    node: ToolNode,
    cte_name: str,
    _input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Translate a TextInput node into a T-SQL VALUES CTE.

    Alteryx TextInput config structure (as produced by _elem_to_value):

        Fields.Field  — list[dict] with a "name" key per column
        Data.r        — list[dict] per data row  (key is lowercase "r")
          .c          — list[str] or str per cell (key is lowercase "c")

    Single-row Data yields a dict for "r" (not a list); single-column rows
    yield a scalar string for "c" (not a list).  Both are normalised below.

    Empty cells (<c />) are emitted as NULL; all non-empty values are emitted
    as N'...' string literals (NVARCHAR-compatible).  If a row has fewer cells
    than declared columns the trailing slots are padded with NULL.
    """
    cfg = node.config
    fields_cfg = cfg.get("Fields", {})
    data_cfg = cfg.get("Data", {})

    # Normalise fields to a list
    field_entries = fields_cfg.get("Field", [])
    if isinstance(field_entries, dict):
        field_entries = [field_entries]

    col_names = [f.get("name", f"col_{i}") for i, f in enumerate(field_entries)]

    if not col_names:
        ctx.warnings.append(
            f"Tool {node.tool_id} (text_input): no field definitions found — generating stub."
        )
        return CTEFragment(
            name=cte_name,
            sql="SELECT TOP 0 1 AS _stub  -- TextInput: no field definitions",
            source_tool_ids=[node.tool_id],
            is_stub=True,
        )

    col_list = ", ".join(f"[{c}]" for c in col_names)

    # Normalise rows to a list.
    # _elem_to_value preserves XML tag case, so rows live under lowercase "r".
    # A single <r> element becomes a dict; multiple become a list.
    row_entries = data_cfg.get("r", data_cfg.get("R", []))
    if isinstance(row_entries, dict):
        row_entries = [row_entries]

    if not row_entries:
        # Empty TextInput — typed empty result set
        nul_cols = ", ".join(f"NULL AS [{c}]" for c in col_names)
        sql = f"SELECT {nul_cols} WHERE 1 = 0"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    value_rows: list[str] = []
    for row in row_entries:
        # Cells live under lowercase "c".  A single-column row gives a scalar
        # string instead of a list, so we normalise to list[str | dict] here.
        cells = row.get("c", row.get("C", []))
        if isinstance(cells, str):
            cells = [cells]
        elif isinstance(cells, dict):
            # Old XML format: <c v="..." /> — treat as a single cell dict
            cells = [cells]

        vals: list[str] = []
        for cell in cells:
            # _elem_to_value yields plain strings for text-content elements.
            # Older formats used attribute dicts {v: "..."} — handle both.
            v: str = cell if isinstance(cell, str) else cell.get("v", "")
            if not v:
                vals.append("NULL")
            else:
                escaped = str(v).replace("'", "''")
                vals.append(f"N'{escaped}'")

        # Pad short rows so every row has the same column count.
        while len(vals) < len(col_names):
            vals.append("NULL")

        value_rows.append(f"    ({', '.join(vals[: len(col_names)])})")

    rows_sql = ",\n".join(value_rows)
    sql = f"SELECT {col_list}\nFROM (VALUES\n{rows_sql}\n) AS _t ({col_list})"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


# ---------------------------------------------------------------------------
# DbFileOutput  (sink — does not produce a CTE, emits a comment stub)
# ---------------------------------------------------------------------------


def _extract_output_table(cfg: dict) -> str | None:
    """Extract the destination table name from a DbFileOutput config.

    Alteryx stores the destination as: alias|||TableName  in <File _text>.
    Returns the table name string, or None if it cannot be determined.
    """
    file_val = cfg.get("File", "")
    file_text = file_val.get("_text", "") if isinstance(file_val, dict) else str(file_val or "")
    if "|||" in file_text:
        table = file_text.split("|||", 1)[1].strip()
        if table:
            return table
    return None


def translate_db_file_output(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Translate a DbFileOutput node.

    Emits the INSERT INTO statement as a SQL comment above a pass-through SELECT,
    so the CTE chain stays valid while making the intent clear.  If the target
    table can be extracted from the config it is used directly; otherwise a stub
    is emitted.
    """
    cfg = node.config
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"

    target_table = _extract_output_table(cfg)
    output_option = cfg.get("FormatSpecificOptions", {}).get("OutputOption", "Overwrite")

    if target_table:
        ctx.warnings.append(
            f"Tool {node.tool_id} (db_file_output): writes to [{target_table}] "
            f"(mode: {output_option}). Replace the trailing SELECT in the final script "
            f"with: INSERT INTO [{target_table}] SELECT * FROM [{upstream}]"
        )
        sql = (
            f"-- Output destination: [{target_table}] (mode: {output_option})\n"
            f"-- Replace trailing SELECT with:\n"
            f"--   INSERT INTO [{target_table}] SELECT * FROM [{upstream}]\n"
            f"SELECT * FROM [{upstream}]"
        )
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    # Could not determine target — fall back to stub
    file_val = cfg.get("File", "")
    file_text = file_val.get("_text", "") if isinstance(file_val, dict) else str(file_val or "")
    ctx.warnings.append(
        f"Tool {node.tool_id} (db_file_output): could not extract target table "
        f"from connection '{file_text[:80]}'. Stub emitted."
    )

    sql = (
        f"-- TODO: replace with INSERT INTO <destination> SELECT * FROM [{upstream}]\n"
        f"SELECT * FROM [{upstream}]"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)
