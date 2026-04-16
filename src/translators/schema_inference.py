"""Schema inference — derives the output FieldSchema list for each tool type.

When Alteryx has not persisted <MetaInfo><RecordInfo> for a node (common for all
non-source tools in a workflow that was never fully run in Designer),
ToolNode.output_schema is empty.  This module fills that gap by deriving the
output schema deterministically from:

  * The tool's configuration (same config dict used by the translators)
  * The input schemas already tracked in TranslationContext.cte_schema

Entry point
-----------
    from translators.schema_inference import infer_output_schema

    schema = infer_output_schema(node, input_cte_names, ctx)
    ctx.cte_schema[cte_name] = schema

Supported tool types
--------------------
select, filter, formula, multirow_formula, join, append_fields, union,
summarize, sort, unique, sample, record_id, find_replace.

Unknown / macro / stub tools return [] — schema propagation stops there and
downstream joins fall back to L.*, R.* as before.
"""

from __future__ import annotations

from parsing.models import FieldSchema, ToolNode
from translators.context import TranslationContext


def infer_output_schema(
    node: ToolNode,
    input_cte_names: list[str],
    ctx: TranslationContext,
) -> list[FieldSchema]:
    """Return the inferred output schema for a translated node.

    Uses ``node.output_schema`` if non-empty (parser already populated it from
    ``<MetaInfo>``).  Otherwise falls back to tool-type-specific inference from
    the node's config and the input schemas in *ctx*.
    """
    if node.output_schema:
        return list(node.output_schema)

    schemas = [ctx.cte_schema.get(name, []) for name in input_cte_names]
    primary = schemas[0] if schemas else []
    cfg = node.config
    t = node.tool_type

    if t == "select":
        upstream = input_cte_names[0] if input_cte_names else ""
        return _infer_select(cfg, primary, ctx, upstream)
    if t in ("filter", "sort", "sample", "find_replace"):
        return list(primary)
    if t == "formula":
        return _infer_formula(cfg, primary)
    if t == "multirow_formula":
        return _infer_multirow_formula(cfg, primary)
    if t in ("join", "append_fields"):
        return _infer_join(cfg, schemas)
    if t == "union":
        # All arms should share the same schema; return first arm's.
        return list(primary)
    if t == "summarize":
        return _infer_summarize(cfg, primary)
    if t == "unique":
        # _rn is an internal column stripped by the outer WHERE; output = input.
        return list(primary)
    if t == "record_id":
        return _infer_record_id(cfg, primary)

    # macro, unknown, text_box, db_file_output, etc. — schema unknown.
    return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fs(name: str, alteryx_type: str = "V_String", size: int | None = None) -> FieldSchema:
    return FieldSchema(name=name, alteryx_type=alteryx_type, size=size)


def _infer_select(
    cfg: dict,
    input_schema: list[FieldSchema],
    ctx: TranslationContext,
    upstream_name: str,
) -> list[FieldSchema]:
    fields_cfg = cfg.get("SelectFields", {}) or cfg.get("Fields", {})
    entries = fields_cfg.get("SelectField", []) or fields_cfg.get("Field", [])
    if isinstance(entries, dict):
        entries = [entries]

    if not entries:
        return list(input_schema)

    input_map: dict[str, FieldSchema] = {f.name: f for f in input_schema}
    result: list[FieldSchema] = []
    explicit_names: set[str] = set()
    output_names: set[str] = set()
    dropped_names: set[str] = set()
    has_passthrough = False

    for e in entries:
        selected = e.get("selected", e.get("Selected", "True"))
        name = e.get("name", e.get("field", ""))
        if selected == "False":
            if name and name != "*Unknown":
                dropped_names.add(name)
            continue
        rename = e.get("rename", e.get("Rename", "")) or name
        is_unknown = e.get("Unknown", "False") == "True" or name == "*Unknown"

        if is_unknown:
            has_passthrough = True
            continue

        if not name:
            continue

        output_col = rename or name
        if output_col in output_names:
            continue  # skip duplicate rename targets (matches translate_select behaviour)
        output_names.add(output_col)
        explicit_names.add(name)
        orig = input_map.get(name)
        result.append(
            _fs(rename, orig.alteryx_type if orig else "V_String", orig.size if orig else None)
        )

    if has_passthrough:
        # Append all input columns not already handled and not explicitly dropped.
        for f in input_schema:
            if (
                f.name not in explicit_names
                and f.name not in output_names
                and f.name not in dropped_names
            ):
                result.append(f)

    return result


def _infer_formula(cfg: dict, input_schema: list[FieldSchema]) -> list[FieldSchema]:
    entries = cfg.get("FormulaFields", {}).get("FormulaField", [])
    if isinstance(entries, dict):
        entries = [entries]

    result = list(input_schema)
    input_names = {f.name for f in input_schema}

    for e in entries:
        col = e.get("field", e.get("name", ""))
        if not col or col in input_names:
            # Overwriting an existing column — name stays; type unknown from expr.
            continue
        result.append(_fs(col))

    return result


def _infer_multirow_formula(cfg: dict, input_schema: list[FieldSchema]) -> list[FieldSchema]:
    entries = cfg.get("FormulaFields", {}).get("FormulaField", [])
    if isinstance(entries, dict):
        entries = [entries]

    result = list(input_schema)
    input_names = {f.name for f in input_schema}

    for e in entries:
        col = e.get("field", e.get("name", ""))
        if col and col not in input_names:
            result.append(_fs(col))

    return result


def _infer_join(cfg: dict, schemas: list[list[FieldSchema]]) -> list[FieldSchema]:
    left = schemas[0] if len(schemas) > 0 else []
    right = schemas[1] if len(schemas) > 1 else []

    if not left and not right:
        return []

    right_prefix: str = cfg.get("RenameRightInput", "Right_")
    left_names = {f.name for f in left}

    result = list(left)
    for f in right:
        if f.name in left_names:
            result.append(_fs(f"{right_prefix}{f.name}", f.alteryx_type, f.size))
        else:
            result.append(f)

    return result


def _infer_summarize(cfg: dict, input_schema: list[FieldSchema]) -> list[FieldSchema]:
    entries = cfg.get("SummarizeFields", {}).get("SummarizeField", [])
    if isinstance(entries, dict):
        entries = [entries]

    input_map = {f.name: f for f in input_schema}
    result: list[FieldSchema] = []

    for e in entries:
        col = e.get("field", "")
        action = e.get("action", "GroupBy")
        rename = e.get("rename", "") or col
        if not rename:
            continue

        if action == "GroupBy":
            orig = input_map.get(col)
            result.append(
                _fs(rename, orig.alteryx_type if orig else "V_String", orig.size if orig else None)
            )
        elif action in ("Count", "CountDistinct"):
            result.append(_fs(rename, "Int64"))
        elif action in ("Sum", "Avg", "Min", "Max", "First", "Last"):
            orig = input_map.get(col)
            result.append(_fs(rename, orig.alteryx_type if orig else "Double"))
        else:
            # Concat, ConcatDistinct → string
            result.append(_fs(rename, "V_String"))

    return result


def _infer_record_id(cfg: dict, input_schema: list[FieldSchema]) -> list[FieldSchema]:
    field_name = cfg.get("FieldName", "RecordID")
    return list(input_schema) + [_fs(field_name, "Int64")]
