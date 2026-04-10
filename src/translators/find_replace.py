"""Translator for the Find Replace tool.

Find Replace substitutes values in one or more columns.  Simple string
substitution maps directly to T-SQL REPLACE().  Regex-based replacement
requires LLM translation.

Config structure:
    <FindReplaceFields>
        <FindReplaceField field="col" find="old" replace="new" matchCase="True" />
    </FindReplaceFields>
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_find_replace(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    fields = cfg.get("FindReplaceFields", {}).get("FindReplaceField", [])
    if isinstance(fields, dict):
        fields = [fields]

    if not fields:
        ctx.warnings.append(
            f"Tool {node.tool_id} (find_replace): no FindReplaceField elements — pass-through."
        )
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    select_cols: list[str] = []
    replaced_cols: set[str] = set()
    is_stub = False

    for f in fields:
        col = f.get("field", "")
        find = f.get("find", "")
        replace = f.get("replace", "")
        is_regex = f.get("isRegEx", "False") == "True"

        if not col:
            continue

        if is_regex:
            ctx.warnings.append(
                f"Tool {node.tool_id} (find_replace): regex replacement on [{col}] — "
                "T-SQL does not support native regex REPLACE. Stub emitted."
            )
            select_cols.append(
                f"    [{col}]  -- TODO: regex replace '{find}' with '{replace}'"
            )
            is_stub = True
        else:
            find_esc = find.replace("'", "''")
            replace_esc = replace.replace("'", "''")
            select_cols.append(
                f"    REPLACE([{col}], '{find_esc}', '{replace_esc}') AS [{col}]"
            )
        replaced_cols.add(col)

    cols_sql = ",\n".join(select_cols)
    # Passthrough all other columns
    sql = (
        f"SELECT\n"
        f"    * EXCEPT ({', '.join(f'[{c}]' for c in sorted(replaced_cols))}),\n"
        f"{cols_sql}\n"
        f"FROM [{upstream}]"
    )

    # NOTE: EXCEPT is not valid in MSSQL SELECT column list — warn and use subquery
    ctx.warnings.append(
        f"Tool {node.tool_id} (find_replace): T-SQL does not support 'SELECT * EXCEPT'. "
        "Manually list all pass-through columns or use a subquery to exclude replaced columns."
    )

    # Fall back to listing replaced cols explicitly; passthrough others as *
    passthrough_sql = "SELECT\n    *"
    for fc in select_cols:
        passthrough_sql += f",\n{fc}"
    passthrough_sql += f"\nFROM [{upstream}]"

    return CTEFragment(
        name=cte_name,
        sql=passthrough_sql,
        source_tool_ids=[node.tool_id],
        is_stub=is_stub,
    )
