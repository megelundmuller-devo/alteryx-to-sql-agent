"""Translator for the TextToColumns tool.

Alteryx TextToColumns splits one string field into N new fields using a
delimiter, appending the new columns to the existing record.

Config keys read
----------------
  Field      — source column name
  RootName   — prefix for the generated output columns (default = Field value)
  Delimeters — nested dict with "value" key holding the delimiter string
  NumFields  — nested dict with "value" key holding the column count as string

Translation strategy
--------------------
T-SQL has no built-in string-split that adds individual columns.  We use a
chain of CROSS APPLY blocks computing CHARINDEX positions, then extract each
token with SUBSTRING.  The output is the upstream schema plus N new columns
named ``{RootName}1``, ``{RootName}2``, …, ``{RootName}N``.

For N columns, N-1 CROSS APPLY blocks are emitted (P1 … P(N-1)), each finding
the next occurrence of the delimiter starting after the previous match.

Example (N=3, delimiter=',', field='[Decoded]'):
    CROSS APPLY (VALUES(CHARINDEX(',', [Decoded]))) AS P1(P)
    CROSS APPLY (VALUES(CASE WHEN P1.P > 0 THEN CHARINDEX(',', [Decoded], P1.P + 1)
                             ELSE 0 END)) AS P2(P)
    Decoded1 = first token, Decoded2 = middle token, Decoded3 = remainder

Limitations
-----------
* When the Alteryx delimiter contains multiple characters it is treated as a
  single literal T-SQL string (e.g. '|' splits on the pipe character).
  Alteryx itself treats each character in the delimiter string as a separate
  separator — if the delimiter is multi-char and each character is intended to
  be a separate separator, a manual rewrite using STRING_SPLIT + pivoting is
  needed.
"""

from __future__ import annotations

from parsing.models import CTEFragment, FieldSchema, ToolNode
from translators.context import TranslationContext


def _get_str_cfg(cfg: dict, key: str, default: str = "") -> str:
    raw = cfg.get(key, {})
    if isinstance(raw, dict):
        return raw.get("value", default)
    return str(raw) if raw else default


def translate_text_to_columns(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    field = cfg.get("Field", "")
    root_name = cfg.get("RootName", "") or field
    delim = _get_str_cfg(cfg, "Delimeters", ",")
    num_str = _get_str_cfg(cfg, "NumFields", "2")

    try:
        num_cols = max(1, int(num_str))
    except (ValueError, TypeError):
        num_cols = 2

    if not field:
        ctx.warnings.append(
            f"Tool {node.tool_id} (text_to_columns): no Field in config — generating stub."
        )
        return CTEFragment(
            name=cte_name,
            sql=f"SELECT *\nFROM [{upstream}]  -- TODO: TextToColumns stub (no Field in config)",
            source_tool_ids=[node.tool_id],
            is_stub=True,
        )

    if len(delim) > 1:
        ctx.warnings.append(
            f"Tool {node.tool_id} (text_to_columns): delimiter {delim!r} has multiple "
            f"characters — treated as a single literal string. Alteryx splits on each "
            f"character individually; verify the output."
        )

    schema = ctx.cte_schema.get(upstream, [])
    delim_sql = "'" + delim.replace("'", "''") + "'"
    delim_len = len(delim)
    field_sql = f"[{field}]"

    output_col_names = [f"{root_name}{i}" for i in range(1, num_cols + 1)]

    # --- SELECT clause ---
    if schema:
        select_cols = [f"    [{f.name}]" for f in schema]
    else:
        select_cols = ["    *"]

    # Trivial case: only one output column — just alias the source field.
    if num_cols == 1:
        select_cols.append(f"    {field_sql} AS [{output_col_names[0]}]")
        sql = "SELECT\n" + ",\n".join(select_cols) + f"\nFROM [{upstream}]"
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    # --- Build split column expressions ---
    # Column 1: from start to first delimiter (or the whole string if no delimiter)
    split_exprs: list[str] = [
        f"    CAST(\n"
        f"        CASE\n"
        f"            WHEN P1.P > 0 THEN SUBSTRING({field_sql}, 1, P1.P - 1)\n"
        f"            ELSE {field_sql}\n"
        f"        END\n"
        f"    AS NVARCHAR(MAX)) AS [{output_col_names[0]}]"
    ]

    # Columns 2 through N-1: between consecutive delimiter positions
    for k in range(2, num_cols):
        prev_alias = f"P{k - 1}"
        curr_alias = f"P{k}"
        split_exprs.append(
            f"    CAST(\n"
            f"        CASE\n"
            f"            WHEN {prev_alias}.P > 0\n"
            f"            THEN SUBSTRING({field_sql}, {prev_alias}.P + {delim_len},\n"
            f"                           ISNULL(NULLIF({curr_alias}.P, 0),\n"
            f"                                  LEN({field_sql}) + 1) - {prev_alias}.P - {delim_len})\n"
            f"            ELSE NULL\n"
            f"        END\n"
            f"    AS NVARCHAR(MAX)) AS [{output_col_names[k - 1]}]"
        )

    # Column N: from last delimiter to end of string (or NULL if last delimiter not found)
    last_alias = f"P{num_cols - 1}"
    split_exprs.append(
        f"    CAST(\n"
        f"        CASE\n"
        f"            WHEN {last_alias}.P > 0\n"
        f"            THEN SUBSTRING({field_sql}, {last_alias}.P + {delim_len}, LEN({field_sql}))\n"
        f"            ELSE NULL\n"
        f"        END\n"
        f"    AS NVARCHAR(MAX)) AS [{output_col_names[-1]}]"
    )

    all_select = select_cols + split_exprs
    select_clause = "SELECT\n" + ",\n".join(all_select)

    # --- CROSS APPLY blocks for N-1 delimiter positions ---
    cross_applies: list[str] = []

    # P1: first occurrence
    cross_applies.append(
        f"CROSS APPLY (VALUES(CHARINDEX({delim_sql}, {field_sql}))) AS P1(P)"
    )

    # P2 … P(N-1): each starts searching after the previous match
    for k in range(2, num_cols):
        prev = f"P{k - 1}"
        curr = f"P{k}"
        cross_applies.append(
            f"CROSS APPLY (VALUES(\n"
            f"    CASE WHEN {prev}.P > 0\n"
            f"         THEN CHARINDEX({delim_sql}, {field_sql}, {prev}.P + {delim_len})\n"
            f"         ELSE 0\n"
            f"    END\n"
            f")) AS {curr}(P)"
        )

    from_clause = f"FROM [{upstream}]\n" + "\n".join(cross_applies)
    sql = f"{select_clause}\n{from_clause}"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
