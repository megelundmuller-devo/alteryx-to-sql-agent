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
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    selected: list[str] = []
    # Track upstream column names that are explicitly handled (renamed or kept).
    # Used to determine which columns to add from the pass-through schema.
    explicit_names: set[str] = set()
    # Track output column names to detect duplicates (e.g. rename target already exists).
    output_names: set[str] = set()
    # Track source column names that were explicitly dropped (selected="False").
    # These must NOT appear in the *Unknown passthrough expansion.
    dropped_names: set[str] = set()
    has_unknown_passthrough = False

    for f in field_entries:
        # A field is excluded if selected="False" or Selected="False"
        selected_attr = f.get("selected", f.get("Selected", "True"))
        name = f.get("name", f.get("field", ""))
        if selected_attr == "False":
            # Record the drop so the *Unknown passthrough cannot re-introduce it.
            if name and name != "*Unknown":
                dropped_names.add(name)
            continue

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

        output_col = rename if (rename and rename != name) else name

        # Skip if this output name already exists — duplicate rename target would
        # cause an ambiguous column error in MSSQL.
        if output_col in output_names:
            ctx.warnings.append(
                f"Tool {node.tool_id} (select): duplicate output column [{output_col}] "
                f"(from source [{name}]) — skipping to avoid MSSQL ambiguity."
            )
            continue

        output_names.add(output_col)
        explicit_names.add(name)
        if rename and rename != name:
            selected.append(f"    [{name}] AS [{rename}]")
        else:
            selected.append(f"    [{name}]")

    if has_unknown_passthrough and not selected and not dropped_names:
        # Pure pass-through — nothing renamed, nothing dropped.
        sql = f"SELECT *\nFROM [{upstream}]"
    elif has_unknown_passthrough:
        # Mix: explicit columns + pass-through for the rest, minus dropped columns.
        # Resolve using the upstream CTE schema when available.
        incoming = ctx.cte_schema.get(upstream, [])
        if incoming:
            # Append pass-through columns that are not already handled explicitly
            # and were not explicitly dropped, preserving upstream schema order.
            for field_meta in incoming:
                if (
                    field_meta.name not in explicit_names
                    and field_meta.name not in output_names
                    and field_meta.name not in dropped_names
                ):
                    selected.append(f"    [{field_meta.name}]")
            cols = ",\n".join(selected)
            sql = f"SELECT\n{cols}\nFROM [{upstream}]"
        else:
            # Schema not available — emit explicit columns only and warn.
            if selected:
                ctx.warnings.append(
                    f"Tool {node.tool_id} (select): Unknown pass-through combined with explicit "
                    "columns but upstream schema unknown. Only explicitly listed columns are "
                    "emitted; dropped columns cannot be verified. Review manually."
                )
            cols = ",\n".join(selected) if selected else "*"
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
