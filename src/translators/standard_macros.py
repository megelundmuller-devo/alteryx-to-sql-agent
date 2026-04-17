"""Deterministic SQL translators for Alteryx standard (built-in) macros.

Each function translates one specific standard macro into a fixed T-SQL CTE body.
The registry maps macro filename (e.g. 'CountRecords.yxmc') to its translator.

Macros that have no meaningful SQL equivalent (spatial tools, reporting macros,
statistical operations without T-SQL counterparts) emit a labelled stub CTE so
the pipeline does not break and the user receives a clear review message.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext

TranslatorFn = Callable[
    [ToolNode, str, list[str], TranslationContext],
    "CTEFragment | list[CTEFragment]",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upstream(input_ctes: list[str]) -> str:
    return input_ctes[0] if input_ctes else "__no_input__"


def _stub(
    node: ToolNode,
    cte_name: str,
    reason: str,
    ctx: TranslationContext,
) -> CTEFragment:
    macro_name = Path(node.macro_path or "unknown").name
    msg = f"Tool {node.tool_id} ({macro_name}): {reason}"
    ctx.warnings.append(msg)
    sql = f"-- Standard macro '{macro_name}': {reason}\nSELECT TOP 0 1 AS _macro_stub"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)


def _str_config(node: ToolNode, key: str, default: str = "") -> str:
    val = node.config.get(key, default)
    return str(val).strip() if val else default


# ---------------------------------------------------------------------------
# Translatable macros
# ---------------------------------------------------------------------------


def translate_count_records(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """CountRecords — returns a single row [Count] with the number of input rows.

    COUNT(*) always returns 0 for empty inputs, matching Alteryx behaviour.
    """
    src = _upstream(input_ctes)
    sql = f"SELECT COUNT(*) AS [Count]\nFROM [{src}]"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def translate_datetime_now(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """DateTimeNow — generates a single row containing the current server timestamp."""
    sql = "SELECT GETDATE() AS [DateTimeNow]"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def translate_random_records(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """RandomRecords — selects N rows at random using NEWID() ordering.

    The macro exposes a single question 'N' for the record count (default 100).
    """
    src = _upstream(input_ctes)
    n_raw = _str_config(node, "N", "100")
    try:
        n = int(n_raw)
    except ValueError:
        n = 100
        ctx.warnings.append(
            f"Tool {node.tool_id} (RandomRecords.yxmc): "
            f"could not parse N='{n_raw}', defaulting to 100."
        )
    sql = f"SELECT TOP {n} *\nFROM [{src}]\nORDER BY NEWID()"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def translate_weighted_avg(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """WeightedAvg — SUM(value * weight) / SUM(weight), optionally grouped.

    Reads macro parameters from node.config:
      Value           — numeric value field
      Weight          — numeric weight field
      OutputFieldName — output column name (default 'WeightedAverage')
      GroupFields     — comma- or space-separated grouping fields (optional)
    """
    src = _upstream(input_ctes)
    value_field = _str_config(node, "Value")
    weight_field = _str_config(node, "Weight")
    output_field = _str_config(node, "OutputFieldName", "WeightedAverage")
    group_raw = _str_config(node, "GroupFields", "")

    if not value_field or not weight_field:
        return _stub(
            node,
            cte_name,
            "missing Value or Weight parameter — configure the macro call and re-run",
            ctx,
        )

    group_fields = [f.strip() for f in group_raw.replace(",", " ").split() if f.strip()]
    select_groups = "".join(f"[{f}],\n    " for f in group_fields)
    group_by = "\nGROUP BY " + ", ".join(f"[{f}]" for f in group_fields) if group_fields else ""

    sql = (
        f"SELECT\n"
        f"    {select_groups}"
        f"SUM(CAST([{value_field}] AS FLOAT) * CAST([{weight_field}] AS FLOAT))\n"
        f"        / NULLIF(SUM(CAST([{weight_field}] AS FLOAT)), 0) AS [{output_field}]\n"
        f"FROM [{src}]"
        f"{group_by}"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def translate_select_records(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """SelectRecords — filters to specific record numbers or ranges.

    The macro accepts a 'Ranges' parameter such as '1-5\\n10\\n20+'.
    This translator emits a ROW_NUMBER-based CTE when a simple contiguous
    range is detected, and stubs otherwise due to the dynamic range-parsing
    logic that the macro performs internally.
    """
    src = _upstream(input_ctes)
    ranges_raw = _str_config(node, "Ranges", "")

    # Attempt to handle a simple single range like '1-100' or '-50' or '10+'
    ranges_raw = ranges_raw.strip()
    start: int | None = None
    end: int | None = None
    simple = False

    if ranges_raw:
        parts = ranges_raw.split()
        if len(parts) == 1:
            r = parts[0]
            if r.startswith("-") and r[1:].isdigit():
                start, end, simple = 1, int(r[1:]), True
            elif r.endswith("+") and r[:-1].isdigit():
                start, simple = int(r[:-1]), True
            elif "-" in r:
                lr = r.split("-", 1)
                if lr[0].isdigit() and lr[1].isdigit():
                    start, end, simple = int(lr[0]), int(lr[1]), True
            elif r.isdigit():
                start, end, simple = int(r), int(r), True

    if not simple:
        return _stub(
            node,
            cte_name,
            "complex or multi-range 'Ranges' parameter — translate manually using ROW_NUMBER()",
            ctx,
        )

    rn_cte = f"{cte_name}__rn"
    where = f"[__rn] >= {start}" if start else ""
    if end:
        where = (f"[__rn] BETWEEN {start} AND {end}") if start else f"[__rn] <= {end}"

    sql = (
        f"-- SelectRecords: rows {ranges_raw!r}\n"
        f"WITH {rn_cte} AS (\n"
        f"    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS [__rn]\n"
        f"    FROM [{src}]\n"
        f")\n"
        f"SELECT * FROM [{rn_cte}] WHERE {where}"
    )
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


# ---------------------------------------------------------------------------
# Non-translatable macros — spatial / analytical / reporting
# ---------------------------------------------------------------------------

_NO_SQL_REASON = "no T-SQL equivalent exists — review manually"


def _make_no_sql_stub(label: str) -> TranslatorFn:
    def _translate(
        node: ToolNode,
        cte_name: str,
        input_ctes: list[str],
        ctx: TranslationContext,
    ) -> CTEFragment:
        return _stub(node, cte_name, f"{label}: {_NO_SQL_REASON}", ctx)

    _translate.__name__ = f"translate_{label.lower().replace(' ', '_')}"
    return _translate


def translate_cleanse(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Cleanse — configurable data cleaning applied to selected fields.

    Reads the macro's checkbox configuration from node.config["Value"] (a list
    of {name, _text} dicts) and applies per-field SQL transformations.

    Supported: null replacement, trim, tabs/linebreaks, all-whitespace removal,
    case conversion, remove-null-rows WHERE clause.
    Not translatable to static SQL: remove null columns, remove letters/numbers/
    punctuation, title case.  These emit warnings but do not block translation.
    """
    src = _upstream(input_ctes)

    # --- Parse checkbox flags from config ---
    flags = _parse_cleanse_flags(node)

    remove_null_rows: bool = flags.get("Check Box (135)", False)
    remove_null_cols: bool = flags.get("Check Box (136)", False)
    replace_null_str: bool = flags.get("Check Box (84)", True)
    replace_null_num: bool = flags.get("Check Box (117)", True)
    trim_ws: bool = flags.get("Check Box (15)", True)
    tabs_linebreaks: bool = flags.get("Check Box (109)", False)
    all_ws: bool = flags.get("Check Box (122)", False)
    remove_letters: bool = flags.get("Check Box (53)", False)
    remove_numbers: bool = flags.get("Check Box (58)", False)
    remove_punct: bool = flags.get("Check Box (70)", False)
    modify_case: bool = flags.get("Check Box (77)", False)
    case_type: str = flags.get("Drop Down (81)", "upper").lower()

    # --- Unsupported operations → warnings ---
    if remove_null_cols:
        ctx.warnings.append(
            f"Tool {node.tool_id} (Cleanse): 'Remove null columns' cannot be expressed "
            "as static T-SQL — remove manually after inspecting data."
        )
    for label, active in (
        ("Remove letters", remove_letters),
        ("Remove numbers", remove_numbers),
        ("Remove punctuation", remove_punct),
    ):
        if active:
            ctx.warnings.append(
                f"Tool {node.tool_id} (Cleanse): '{label}' has no direct T-SQL equivalent "
                "— approximated with a comment; implement via a custom scalar UDF if needed."
            )
    if modify_case and case_type == "title":
        ctx.warnings.append(
            f"Tool {node.tool_id} (Cleanse): 'Title Case' has no T-SQL built-in — "
            "converted to UPPER() as a placeholder; replace with a scalar UDF."
        )

    # --- Determine fields to cleanse ---
    selected_raw: str = flags.get("List Box (11)", "")
    if selected_raw:
        selected_fields = [
            f.strip().strip('"') for f in selected_raw.split(",") if f.strip().strip('"')
        ]
    else:
        selected_fields = []  # empty = all fields

    input_schema = ctx.cte_schema.get(src, [])

    _STRING_TYPES = {"String", "WString", "V_String", "V_WString"}
    _NUMERIC_TYPES = {"Byte", "Int16", "Int32", "Int64", "FixedDecimal", "Float", "Double"}

    def _is_string(field_name: str) -> bool:
        for f in input_schema:
            if f.name == field_name:
                return f.alteryx_type in _STRING_TYPES
        return True  # unknown field → assume string

    def _is_numeric(field_name: str) -> bool:
        for f in input_schema:
            if f.name == field_name:
                return f.alteryx_type in _NUMERIC_TYPES
        return False

    def _apply(col: str) -> str:
        """Build the SQL expression for one column given the active flags."""
        is_str = _is_string(col)
        is_num = _is_numeric(col)
        expr = f"[{col}]"

        if is_str:
            if all_ws:
                expr = (
                    f"REPLACE(REPLACE(REPLACE(REPLACE({expr}, ' ', ''), "
                    f"CHAR(9), ''), CHAR(10), ''), CHAR(13), '')"
                )
            elif tabs_linebreaks:
                expr = (
                    f"REPLACE(REPLACE(REPLACE({expr}, CHAR(9), ' '), CHAR(10), ' '), CHAR(13), ' ')"
                )
            if trim_ws and not all_ws:
                expr = f"LTRIM(RTRIM({expr}))"
            if modify_case:
                fn = "LOWER" if case_type == "lower" else "UPPER"
                expr = f"{fn}({expr})"
            if remove_letters or remove_numbers or remove_punct:
                expr = f"/* TODO: remove chars — see Cleanse warning */ {expr}"
            if replace_null_str:
                expr = f"ISNULL({expr}, '')"
        elif is_num:
            if replace_null_num:
                expr = f"ISNULL({expr}, 0)"

        return f"{expr} AS [{col}]"

    # --- Build column list ---
    if input_schema:
        all_cols = [f.name for f in input_schema]
        target_cols = set(selected_fields) if selected_fields else set(all_cols)
        col_exprs = [_apply(col) if col in target_cols else f"[{col}]" for col in all_cols]
    else:
        # No schema available — emit a generic note and pass through
        ctx.warnings.append(
            f"Tool {node.tool_id} (Cleanse): no upstream schema found — "
            "emitting SELECT * with a comment; re-run after schema is resolved."
        )
        sql = (
            f"-- Cleanse: schema unknown — add per-column transformations manually\n"
            f"SELECT *\nFROM [{src}]"
        )
        return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id], is_stub=True)

    select_body = ",\n    ".join(col_exprs)

    # --- WHERE clause for null row removal ---
    where_clause = ""
    if remove_null_rows and all_cols:
        null_checks = " AND ".join(f"[{c}] IS NULL" for c in all_cols)
        where_clause = f"\nWHERE NOT ({null_checks})"

    sql = f"SELECT\n    {select_body}\nFROM [{src}]{where_clause}"
    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def _parse_cleanse_flags(node: ToolNode) -> dict[str, bool | str]:
    """Extract Cleanse checkbox flags from node.config into a flat dict.

    The calling workflow stores each checkbox as:
      <Value name="Check Box (N)">True/False</Value>
    These are parsed by _elem_to_value into a list under config["Value"].
    """
    raw = node.config.get("Value", [])
    if isinstance(raw, dict):
        raw = [raw]

    result: dict[str, bool | str] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "")
        text = (entry.get("_text") or "").strip()
        if not name:
            continue
        if text in ("True", "False"):
            result[name] = text == "True"
        else:
            result[name] = text
    return result


translate_date_filter = _make_no_sql_stub("Date_Filter")
translate_base64_encoder = _make_no_sql_stub("Base64_Encoder")
translate_footer_macro = _make_no_sql_stub("FooterMacro")
translate_header_macro = _make_no_sql_stub("HeaderMacro")
translate_heatmap = _make_no_sql_stub("HeatMap")
translate_imputation = _make_no_sql_stub("Imputation")
translate_legend_builder = _make_no_sql_stub("Legend_Builder")
translate_legend_splitter = _make_no_sql_stub("Legend_Splitter")
translate_multifield_binning = _make_no_sql_stub("MultiFieldBinning")
translate_pearson = _make_no_sql_stub("PearsonCorrCoeff")
translate_spearman = _make_no_sql_stub("SpearmanCorrCoeff")
translate_pie_wedge = _make_no_sql_stub("PieWedgeTradeArea")
translate_pipe_to_table = _make_no_sql_stub("Pipe_to_Table")
translate_google_analytics = _make_no_sql_stub("Google_Analytics")

# ---------------------------------------------------------------------------
# Registry: macro filename → translator function
# ---------------------------------------------------------------------------

STANDARD_MACRO_REGISTRY: dict[str, TranslatorFn] = {
    "CountRecords.yxmc": translate_count_records,
    "DateTimeNow.yxmc": translate_datetime_now,
    "RandomRecords.yxmc": translate_random_records,
    "WeightedAvg.yxmc": translate_weighted_avg,
    "SelectRecords.yxmc": translate_select_records,
    # Non-translatable — emit labelled stubs
    "Cleanse.yxmc": translate_cleanse,
    "Date_Filter.yxmc": translate_date_filter,
    "Base64_Encoder.yxmc": translate_base64_encoder,
    "FooterMacro.yxmc": translate_footer_macro,
    "HeaderMacro.yxmc": translate_header_macro,
    "HeatMap.yxmc": translate_heatmap,
    "Imputation.yxmc": translate_imputation,
    "Imputation_v2.yxmc": translate_imputation,
    "Imputation_v3.yxmc": translate_imputation,
    "Legend_Builder.yxmc": translate_legend_builder,
    "Legend_Splitter.yxmc": translate_legend_splitter,
    "MultiFieldBinning.yxmc": translate_multifield_binning,
    "MultiFieldBinning_v2.yxmc": translate_multifield_binning,
    "PearsonCorrCoeff.yxmc": translate_pearson,
    "SpearmanCorrCoeff.yxmc": translate_spearman,
    "PieWedgeTradeArea.yxmc": translate_pie_wedge,
    "Pipe_to_Table.yxmc": translate_pipe_to_table,
    "Google_Analytics.yxmc": translate_google_analytics,
    "Google_Analytics_v5.yxmc": translate_google_analytics,
    "Google_Analytics_v6.yxmc": translate_google_analytics,
}
