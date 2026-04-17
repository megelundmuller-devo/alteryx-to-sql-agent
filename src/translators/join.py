"""Translator for the Join tool.

Alteryx Join merges two streams on one or more key fields, emitting up to three
output anchors:
  Join  (J) — matched rows          → INNER JOIN
  Left  (L) — unmatched left rows   → LEFT JOIN … WHERE R.key IS NULL
  Right (R) — unmatched right rows  → RIGHT JOIN … WHERE L.key IS NULL

Translation strategy
--------------------
* Join keys are read from <JoinInfo side="Left"> / <JoinInfo side="Right">.
* The DAG is queried for which of the three anchors actually have downstream
  consumers.  A CTE is emitted for each connected anchor:
    - J anchor → canonical chunk output CTE  (name = cte_name)
    - L anchor → cte_name + "_L"
    - R anchor → cte_name + "_R"
  The J CTE is always emitted (it is the chunk's primary output).
* When schema is available, explicit column lists are emitted; otherwise L.*/R.*.
* ctx.cte_schema is set for L and R CTEs inside this translator so that
  translate_chunk does not overwrite them with the full-join schema.
"""

from __future__ import annotations

from parsing.models import CTEFragment, FieldSchema, ToolNode
from translators.context import TranslationContext


def translate_join(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> list[CTEFragment]:
    """Translate a Join node into one to three CTEFragments (J, L, R)."""
    if len(input_ctes) < 2:
        ctx.warnings.append(
            f"Tool {node.tool_id} (join): expected 2 input CTEs, got {len(input_ctes)}. "
            "Generating stub."
        )
        return [
            CTEFragment(
                name=cte_name,
                sql="SELECT TOP 0 1 AS _stub  -- join: insufficient inputs",
                source_tool_ids=[node.tool_id],
                is_stub=True,
            )
        ]

    left_cte = input_ctes[0]
    right_cte = input_ctes[1]
    cfg = node.config

    left_keys = _parse_join_keys(cfg, "Left")
    right_keys = _parse_join_keys(cfg, "Right")

    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        ctx.warnings.append(
            f"Tool {node.tool_id} (join): could not parse join keys — generating stub. "
            f"Left keys: {left_keys}, Right keys: {right_keys}"
        )
        return [
            CTEFragment(
                name=cte_name,
                sql=(
                    f"-- TODO: translate Join keys\n"
                    f"SELECT L.*, R.*\n"
                    f"FROM [{left_cte}] AS L\n"
                    f"INNER JOIN [{right_cte}] AS R ON 1 = 1  -- REPLACE join condition"
                ),
                source_tool_ids=[node.tool_id],
                is_stub=True,
            )
        ]

    on_parts = [f"L.[{lk}] = R.[{rk}]" for lk, rk in zip(left_keys, right_keys)]
    on_clause = "\n    AND ".join(on_parts)

    # Determine which output anchors have downstream consumers.
    connected = {conn.origin_anchor for conn in ctx.dag.out_edges(node.tool_id)}
    has_l = "Left" in connected
    has_r = "Right" in connected

    left_schema = ctx.cte_schema.get(left_cte, [])
    right_schema = ctx.cte_schema.get(right_cte, [])

    fragments: list[CTEFragment] = []

    # --- J anchor: INNER JOIN (matched rows) ---
    j_select = _build_select_join(left_schema, right_schema, cfg)
    j_sql = (
        f"{j_select}\n"
        f"FROM [{left_cte}] AS L\n"
        f"INNER JOIN [{right_cte}] AS R\n"
        f"    ON {on_clause}"
    )
    fragments.append(CTEFragment(name=cte_name, sql=j_sql, source_tool_ids=[node.tool_id]))

    # --- L anchor: unmatched left rows ---
    if has_l:
        l_cte_name = f"{cte_name}_L"
        l_select = _build_select_left(left_schema)
        l_sql = (
            f"{l_select}\n"
            f"FROM [{left_cte}] AS L\n"
            f"LEFT JOIN [{right_cte}] AS R\n"
            f"    ON {on_clause}\n"
            f"WHERE R.[{right_keys[0]}] IS NULL"
        )
        fragments.append(
            CTEFragment(name=l_cte_name, sql=l_sql, source_tool_ids=[node.tool_id])
        )
        ctx.cte_schema[l_cte_name] = list(left_schema)

    # --- R anchor: unmatched right rows ---
    if has_r:
        r_cte_name = f"{cte_name}_R"
        r_select = _build_select_right(right_schema)
        r_sql = (
            f"{r_select}\n"
            f"FROM [{left_cte}] AS L\n"
            f"RIGHT JOIN [{right_cte}] AS R\n"
            f"    ON {on_clause}\n"
            f"WHERE L.[{left_keys[0]}] IS NULL"
        )
        fragments.append(
            CTEFragment(name=r_cte_name, sql=r_sql, source_tool_ids=[node.tool_id])
        )
        ctx.cte_schema[r_cte_name] = list(right_schema)

    return fragments


def _build_select_join(
    left_schema: list[FieldSchema],
    right_schema: list[FieldSchema],
    cfg: dict,
) -> str:
    """SELECT clause for the J (inner join) anchor — all columns from both sides."""
    if not left_schema or not right_schema:
        return "SELECT\n    L.*,\n    R.*"

    right_prefix: str = cfg.get("RenameRightInput", "Right_")
    left_names = {f.name for f in left_schema}
    cols: list[str] = [f"    L.[{f.name}]" for f in left_schema]
    for f in right_schema:
        if f.name in left_names:
            cols.append(f"    R.[{f.name}] AS [{right_prefix}{f.name}]")
        else:
            cols.append(f"    R.[{f.name}]")
    return "SELECT\n" + ",\n".join(cols)


def _build_select_left(left_schema: list[FieldSchema]) -> str:
    """SELECT clause for the L anchor — only left-side columns."""
    if not left_schema:
        return "SELECT\n    L.*"
    cols = [f"    L.[{f.name}]" for f in left_schema]
    return "SELECT\n" + ",\n".join(cols)


def _build_select_right(right_schema: list[FieldSchema]) -> str:
    """SELECT clause for the R anchor — only right-side columns."""
    if not right_schema:
        return "SELECT\n    R.*"
    cols = [f"    R.[{f.name}]" for f in right_schema]
    return "SELECT\n" + ",\n".join(cols)


def _parse_join_keys(cfg: dict, side: str) -> list[str]:
    """Extract field names from <JoinInfo side='Left'> or <JoinInfo side='Right'>."""
    join_info = cfg.get("JoinInfo", [])
    if isinstance(join_info, dict):
        join_info = [join_info]

    for info in join_info:
        attr = info.get("connection", info.get("side", info.get("Side", "")))
        if attr == side:
            field = info.get("Field", [])
            if isinstance(field, dict):
                field = [field]
            return [
                f.get("field", f.get("name", f.get("Name", "")))
                for f in field
                if f.get("field") or f.get("name") or f.get("Name")
            ]

    return []
