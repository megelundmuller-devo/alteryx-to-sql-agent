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
    - L anchor → cte_name + "_L"  (or a passthrough alias — see below)
    - R anchor → cte_name + "_R"  (or a passthrough alias — see below)
  The J CTE is always emitted (it is the chunk's primary output).
* When schema is available, explicit column lists are emitted; otherwise L.*/R.*.
* ctx.cte_schema is set for L and R CTEs inside this translator so that
  translate_chunk does not overwrite them with the full-join schema.

Left/right join collapsing
--------------------------
Alteryx users often simulate a LEFT JOIN by taking both J and L outputs and
feeding them into a Union tool.  When the translator detects this pattern
(J and L both flow into the same single Union, and nothing else), it:
  - Emits J as LEFT JOIN instead of INNER JOIN
  - Registers L as a passthrough alias (ctx.cte_passthrough[L] = J)
  - Does not emit an L fragment
The Union translator then resolves [J, L→J] → [J] and emits a simple
SELECT * FROM [J], giving a single LEFT JOIN in the final SQL.
The same applies to J+R → RIGHT JOIN, and J+L+R → FULL OUTER JOIN.
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

    # Detect whether L/R anchors can be absorbed into J as a wider JOIN type.
    # This collapses the Alteryx pattern "INNER JOIN + LEFT WHERE NULL + UNION"
    # into a single LEFT/RIGHT/FULL OUTER JOIN on the J anchor.
    # Safe only when J and the anchor both feed the exact same single Union tool
    # (and nothing else), so changing J's join type can't affect other consumers.
    j_ids = {c.dest_id for c in ctx.dag.out_edges(node.tool_id) if c.origin_anchor == "Join"}

    def _single_shared_union(anchor_ids: set[int]) -> bool:
        return (
            len(j_ids) == 1
            and j_ids == anchor_ids
            and ctx.dag.get_node(next(iter(j_ids))).tool_type == "union"
        )

    l_ids = {c.dest_id for c in ctx.dag.out_edges(node.tool_id) if c.origin_anchor == "Left"}
    r_ids = {c.dest_id for c in ctx.dag.out_edges(node.tool_id) if c.origin_anchor == "Right"}
    l_passthrough = has_l and _single_shared_union(l_ids)
    r_passthrough = has_r and _single_shared_union(r_ids)

    if l_passthrough and r_passthrough:
        join_type = "FULL OUTER JOIN"
    elif l_passthrough:
        join_type = "LEFT JOIN"
    elif r_passthrough:
        join_type = "RIGHT JOIN"
    else:
        join_type = "INNER JOIN"

    left_schema = ctx.cte_schema.get(left_cte, [])
    right_schema = ctx.cte_schema.get(right_cte, [])

    fragments: list[CTEFragment] = []

    # --- J anchor ---
    j_select = _build_select_join(left_schema, right_schema, cfg)
    j_sql = f"{j_select}\nFROM [{left_cte}] AS L\n{join_type} [{right_cte}] AS R\n    ON {on_clause}"
    fragments.append(CTEFragment(name=cte_name, sql=j_sql, source_tool_ids=[node.tool_id]))

    # --- L anchor: either a passthrough alias or an explicit anti-join fragment ---
    if has_l:
        l_cte_name = f"{cte_name}_L"
        if l_passthrough:
            # L is absorbed into the LEFT/FULL JOIN on J; register the alias so
            # the downstream Union can collapse [J, L] → [J].
            ctx.cte_passthrough[l_cte_name] = cte_name
        else:
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

    # --- R anchor: either a passthrough alias or an explicit anti-join fragment ---
    if has_r:
        r_cte_name = f"{cte_name}_R"
        if r_passthrough:
            ctx.cte_passthrough[r_cte_name] = cte_name
        else:
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


def parse_join_select(cfg: dict, anchor: str) -> tuple[set[str], dict[str, str]]:
    """Parse <SelectConfiguration> for a given output anchor (Join/Left/Right).

    Returns (excluded, renames) where:
      excluded — set of final column names to drop from the output
      renames  — dict mapping final_name → new_name
    Field names in SelectConfiguration are already post-prefix (e.g. "Right_EMPLOYEE").
    """
    excluded: set[str] = set()
    renames: dict[str, str] = {}

    configurations = cfg.get("SelectConfiguration", {}).get("Configuration", [])
    if isinstance(configurations, dict):
        configurations = [configurations]

    target: dict = {}
    for c in configurations:
        if c.get("outputConnection") == anchor:
            target = c
            break

    fields = target.get("SelectFields", {}).get("SelectField", [])
    if isinstance(fields, dict):
        fields = [fields]

    for f in fields:
        name = f.get("field", "")
        if not name or name == "*Unknown":
            continue
        if f.get("selected", "True") == "False":
            excluded.add(name)
        else:
            rename = f.get("rename", "")
            if rename and rename != name:
                renames[name] = rename

    return excluded, renames


def _build_select_join(
    left_schema: list[FieldSchema],
    right_schema: list[FieldSchema],
    cfg: dict,
) -> str:
    """SELECT clause for the J (inner join) anchor — columns from both sides,
    filtered and renamed according to <SelectConfiguration outputConnection="Join">."""
    if not left_schema or not right_schema:
        return "SELECT\n    L.*,\n    R.*"

    right_prefix: str = cfg.get("RenameRightInput", "Right_")
    excluded, renames = parse_join_select(cfg, "Join")
    left_names = {f.name for f in left_schema}
    cols: list[str] = []

    for f in left_schema:
        out_name = renames.get(f.name, f.name)
        if f.name in excluded or out_name in excluded:
            continue
        entry = f"    L.[{f.name}]" if out_name == f.name else f"    L.[{f.name}] AS [{out_name}]"
        cols.append(entry)

    for f in right_schema:
        prefixed = f"{right_prefix}{f.name}" if f.name in left_names else f.name
        out_name = renames.get(prefixed, prefixed)
        if prefixed in excluded or out_name in excluded:
            continue
        if f.name in left_names:
            cols.append(f"    R.[{f.name}] AS [{out_name}]")
        else:
            if out_name == f.name:
                cols.append(f"    R.[{f.name}]")
            else:
                cols.append(f"    R.[{f.name}] AS [{out_name}]")

    if not cols:
        return "SELECT\n    L.*,\n    R.*"
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
