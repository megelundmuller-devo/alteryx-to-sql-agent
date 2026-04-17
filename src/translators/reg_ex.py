"""Translator for the RegEx tool.

Alteryx RegEx supports four methods:
  Replace  — replace all matches of the expression in a field
  Match    — add a boolean field indicating whether the field matches
  Parse    — split field into named output columns using capture groups
  Token    — extract the Nth token

Translation strategy
--------------------
T-SQL has no native regex engine.  We translate deterministically where the
pattern is simple enough:

* Unicode escape replacement: pattern ``(\\uXXXX)`` with a replacement string
  → ``REPLACE([Field], '\\uXXXX', N'replacement')``
  These appear when Alteryx workflows normalise URL/JSON-encoded Unicode chars.

All other patterns produce a documented stub.  The upstream schema is always
propagated so the stub SELECT does not break downstream schema inference.
"""

from __future__ import annotations

import re

from parsing.models import CTEFragment, FieldSchema, ToolNode
from translators.context import TranslationContext

# Matches the Alteryx regex pattern for a single Unicode escape group: (\uXXXX)
# In the parsed config, two backslashes are stored as-is from XML.
_UNICODE_ESCAPE_RE = re.compile(r"^\(\\\\u([0-9a-fA-F]{4})\)$")


def _get_regex_value(cfg: dict) -> str:
    """Extract the regex pattern string from the config dict."""
    raw = cfg.get("RegExExpression", {})
    if isinstance(raw, dict):
        return raw.get("value", "")
    return str(raw)


def _get_replace_expr(cfg: dict) -> str:
    """Extract the replacement string from <Replace expression='...'/>."""
    raw = cfg.get("Replace", {})
    if isinstance(raw, dict):
        return raw.get("expression", "")
    return str(raw)


def _select_with_replacement(
    schema: list[FieldSchema],
    target_field: str,
    replacement_expr: str,
    upstream: str,
) -> str:
    """Build a SELECT that replaces one column and passes the rest through."""
    if not schema:
        return (
            f"SELECT\n"
            f"    {replacement_expr} AS [{target_field}],\n"
            f"    *  -- schema unknown; may include duplicate [{target_field}]\n"
            f"FROM [{upstream}]"
        )
    cols: list[str] = []
    for f in schema:
        if f.name == target_field:
            cols.append(f"    {replacement_expr} AS [{f.name}]")
        else:
            cols.append(f"    [{f.name}]")
    return "SELECT\n" + ",\n".join(cols) + f"\nFROM [{upstream}]"


def translate_reg_ex(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Translate a RegEx node into a CTEFragment."""
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    field = cfg.get("Field", "")
    method = cfg.get("Method", "Replace")
    pattern = _get_regex_value(cfg)

    schema = ctx.cte_schema.get(upstream, [])

    if method == "Replace" and field and pattern:
        replace_expr = _get_replace_expr(cfg)

        # Deterministic path: Unicode escape replacement  (\\uXXXX) → char
        m = _UNICODE_ESCAPE_RE.match(pattern)
        if m:
            hex_code = m.group(1)
            # The data contains the literal 7-char sequence \uXXXX (not the char).
            # T-SQL REPLACE finds it as a plain string constant.
            search_str = f"\\u{hex_code}"
            if replace_expr:
                replacement_sql = f"REPLACE([{field}], '{search_str}', N'{replace_expr}')"
            else:
                replacement_sql = f"REPLACE([{field}], '{search_str}', N'')"
            sql = _select_with_replacement(schema, field, replacement_sql, upstream)
            return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])

    # Fallback: emit a pass-through stub with the Alteryx expression documented.
    ctx.warnings.append(
        f"Tool {node.tool_id} (reg_ex): cannot deterministically translate "
        f"Method={method!r} pattern={pattern!r} — generating pass-through stub."
    )
    if schema:
        col_list = ",\n".join(f"    [{f.name}]" for f in schema)
        stub_sql = (
            f"-- TODO: translate RegEx tool\n"
            f"-- Method={method!r}  Field={field!r}  Pattern={pattern!r}\n"
            f"SELECT\n{col_list}\nFROM [{upstream}]"
        )
    else:
        stub_sql = (
            f"-- TODO: translate RegEx tool\n"
            f"-- Method={method!r}  Field={field!r}  Pattern={pattern!r}\n"
            f"SELECT *\nFROM [{upstream}]"
        )
    return CTEFragment(name=cte_name, sql=stub_sql, source_tool_ids=[node.tool_id], is_stub=True)
