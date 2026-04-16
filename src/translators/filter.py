"""Translator for the Filter tool.

Filter splits one stream into True (rows matching condition) and False (rows
not matching).  In SQL this maps to a WHERE clause applied once per branch.

Translation strategy
--------------------
Alteryx Filter has two modes stored in <Mode>:

Simple mode  — condition built from <Simple><Field>, <Operator>, <Operand>.
    Deterministically converted for all standard comparison operators.

Custom mode  — raw Alteryx expression in <Expression>.
    Passed through the expression converter; complex expressions flagged for LLM.

Branching
---------
* If only the True output is connected (out_degree == 1) → one CTE with WHERE.
* If both True and False outputs are connected (out_degree == 2) → two CTEs:
      - `cte_name`        → WHERE <condition>
      - `cte_name_false`  → WHERE NOT (<condition>)
"""

from __future__ import annotations

from llm.expression_agent import convert_expression_llm
from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext
from translators.expressions import convert_expression, looks_like_sql, needs_llm_translation

# Alteryx Simple-mode operator → T-SQL fragment
# Operators that need the operand quoted are handled inline.
_SIMPLE_OPS: dict[str, str] = {
    "=": "=",
    "!=": "!=",
    "<>": "<>",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
    "IsNull": "IS NULL",
    "IsNotNull": "IS NOT NULL",
    "IsEmpty": "IS NULL",  # Alteryx "IsEmpty" covers both NULL and ""
    "IsNotEmpty": "IS NOT NULL",
    "Contains": "LIKE",
    "DoesNotContain": "NOT LIKE",
    "StartsWith": "LIKE",
    "EndsWith": "LIKE",
    "Between": "BETWEEN",
}


def _simple_condition(simple: dict) -> str | None:
    """Build a T-SQL WHERE condition from a Simple-mode filter config dict.

    Returns None if the config is unrecognised.
    """
    field = simple.get("Field", "")
    operator = simple.get("Operator", "")
    operands = simple.get("Operands", {})
    operand = operands.get("Operand", "")

    if not field or not operator:
        return None

    col = f"[{field}]"
    op = operator.strip()

    if op in ("IsNull", "IsEmpty"):
        return f"{col} IS NULL"
    if op in ("IsNotNull", "IsNotEmpty"):
        return f"{col} IS NOT NULL"

    # Quote the operand unless it looks purely numeric
    def _quote(val: str) -> str:
        try:
            float(val)
            return val
        except (ValueError, TypeError):
            return "'" + str(val).replace("'", "''") + "'"

    if op in ("Contains", "DoesNotContain"):
        like_op = "NOT LIKE" if op == "DoesNotContain" else "LIKE"
        val = str(operand).replace("'", "''")
        return f"{col} {like_op} '%{val}%'"

    if op == "StartsWith":
        val = str(operand).replace("'", "''")
        return f"{col} LIKE '{val}%'"

    if op == "EndsWith":
        val = str(operand).replace("'", "''")
        return f"{col} LIKE '%{val}'"

    if op == "Between":
        start = operands.get("StartDate", operand)
        end = operands.get("EndDate", operand)
        return f"{col} BETWEEN {_quote(str(start))} AND {_quote(str(end))}"

    if op in _SIMPLE_OPS:
        return f"{col} {op} {_quote(str(operand))}"

    return None


def _get_expression(cfg: dict, engine_vars: set[str] | None = None) -> tuple[str, bool]:
    """Return (sql_condition_or_empty, is_stub) from the filter config.

    Handles both Simple and Custom modes.
    """
    mode = cfg.get("Mode", "Custom")

    if mode == "Simple":
        simple = cfg.get("Simple", {})
        condition = _simple_condition(simple)
        if condition:
            return condition, False
        # Unrecognised simple mode config — stub
        return "", True

    # Custom mode: raw expression in <Expression>
    expression = cfg.get("Expression", "").strip()
    if not expression:
        return "", True

    if needs_llm_translation(expression):
        sql_expr = convert_expression_llm(expression)
        llm_failed = (
            sql_expr.startswith("-- LLM")
            or "MANUAL REVIEW" in sql_expr
            or not looks_like_sql(sql_expr)
        )
        if llm_failed:
            return expression, True  # LLM failed or returned prose — caller emits stub
        return sql_expr, False

    return convert_expression(expression, engine_vars), False


def translate_filter(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> list[CTEFragment]:
    """Return one or two CTEFragments depending on branching."""
    upstream = input_ctes[0] if input_ctes else "-- NO_UPSTREAM"
    cfg = node.config

    condition, is_stub = _get_expression(cfg, ctx.engine_vars)

    if not condition:
        ctx.warnings.append(
            f"Tool {node.tool_id} (filter): could not parse filter condition "
            f"(Mode={cfg.get('Mode')!r}) — generating pass-through stub."
        )
        stub_sql = f"SELECT *\nFROM [{upstream}]  -- TODO: add WHERE condition"
        return [
            CTEFragment(name=cte_name, sql=stub_sql, source_tool_ids=[node.tool_id], is_stub=True)
        ]

    if is_stub:
        # condition holds the raw Alteryx expression; needs LLM
        ctx.warnings.append(
            f"Tool {node.tool_id} (filter): expression requires LLM translation — "
            f"stub emitted. Expression: {condition!r:.120}"
        )
        true_sql = (
            f"-- TODO: translate filter expression\n"
            f"-- Alteryx: {condition}\n"
            f"SELECT *\nFROM [{upstream}]\nWHERE 1 = 1  -- REPLACE THIS"
        )
        true_frag = CTEFragment(
            name=cte_name, sql=true_sql, source_tool_ids=[node.tool_id], is_stub=True
        )
    else:
        true_sql = f"SELECT *\nFROM [{upstream}]\nWHERE {condition}"
        true_frag = CTEFragment(name=cte_name, sql=true_sql, source_tool_ids=[node.tool_id])

    # Check if the False output is connected
    successors = ctx.dag.successors(node.tool_id)
    has_false_branch = any(
        conn.origin_anchor == "False"
        for conn in ctx.dag.graph.edges(node.tool_id, data="conn")
        if conn[2] is not None
    )

    if not has_false_branch or len(successors) < 2:
        return [true_frag]

    # Emit False branch
    false_cte_name = f"{cte_name}_false"
    if is_stub:
        false_sql = (
            f"-- TODO: translate filter expression (FALSE branch)\n"
            f"-- Alteryx: {condition}\n"
            f"SELECT *\nFROM [{upstream}]\nWHERE 1 = 0  -- REPLACE THIS"
        )
        false_frag = CTEFragment(
            name=false_cte_name, sql=false_sql, source_tool_ids=[node.tool_id], is_stub=True
        )
    else:
        false_sql = f"SELECT *\nFROM [{upstream}]\nWHERE NOT ({condition})"
        false_frag = CTEFragment(name=false_cte_name, sql=false_sql, source_tool_ids=[node.tool_id])

    return [true_frag, false_frag]
