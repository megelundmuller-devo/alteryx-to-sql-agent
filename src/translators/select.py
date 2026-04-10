"""Translator for the AlteryxSelect (Select) tool.

AlteryxSelect lets users rename, reorder, retype, and drop columns.
The <SelectFields> / <Field> XML structure encodes this declaratively.

Translation strategy
--------------------
* Fields marked Unknown="True" are pass-through columns from upstream — we
  expand them to SELECT * if they are the only instruction, or add them as
  columns explicitly if we know the schema from the upstream node.
* Renamed fields: [OldName] AS [NewName]
* Dropped fields: omitted from SELECT
* If the config is completely missing we fall back to SELECT * with a warning.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_select(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    # <SelectFields> or <Configuration><SelectFields>
    fields_cfg = cfg.get("SelectFields", {}) or cfg.get("Fields", {})

    field_entries = fields_cfg.get("SelectField", []) or fields_cfg.get("Field", [])
    if isinstance(field_entries, dict):
        field_entries = [field_entries]

    if not field_entries:
        ctx.warnings.append(
            f"Tool {node.tool_id} (select): no field definitions found — falling back to SELECT *."
        )
        sql = f"SELECT *\nFROM [{upstream}]"
        return CTEFragment(
            name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True
        )

    selected: list[str] = []
    has_unknown_passthrough = False

    for f in field_entries:
        # A field is excluded if selected="False" or Selected="False"
        selected_attr = f.get("selected", f.get("Selected", "True"))
        if selected_attr == "False":
            continue

        name = f.get("name", f.get("field", ""))
        rename = f.get("rename", f.get("Rename", ""))
        is_unknown = f.get("Unknown", "False") == "True"

        if is_unknown or name == "*Unknown":
            # Pass-through for columns not explicitly listed.
            # Alteryx represents this either via Unknown="True" attribute
            # or via a field named "*Unknown".
            has_unknown_passthrough = True
            continue

        if not name:
            continue

        if rename and rename != name:
            selected.append(f"    [{name}] AS [{rename}]")
        else:
            selected.append(f"    [{name}]")

    if has_unknown_passthrough and not selected:
        # Pure pass-through — nothing renamed or dropped
        sql = f"SELECT *\nFROM [{upstream}]"
    elif has_unknown_passthrough:
        # Mix: explicit columns + wildcard for the rest is not expressible in SQL.
        # We emit the explicit columns only and add a warning.
        ctx.warnings.append(
            f"Tool {node.tool_id} (select): Unknown pass-through combined with explicit columns. "
            "Only the explicitly listed columns are emitted. Review manually."
        )
        cols = ",\n".join(selected)
        sql = f"SELECT\n{cols}\nFROM [{upstream}]"
    elif selected:
        cols = ",\n".join(selected)
        sql = f"SELECT\n{cols}\nFROM [{upstream}]"
    else:
        # All columns dropped — shouldn't happen but be safe
        ctx.warnings.append(
            f"Tool {node.tool_id} (select): all columns appear to be deselected — "
            "falling back to SELECT *."
        )
        sql = f"SELECT *\nFROM [{upstream}]"

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])
