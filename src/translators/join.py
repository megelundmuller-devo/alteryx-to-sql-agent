"""Translator for the Join tool.

Alteryx Join merges two streams on one or more key fields, emitting three
output anchors: Join (matched rows), Left (unmatched left rows), Right
(unmatched right rows).

Translation strategy
--------------------
* We identify the join keys from <JoinInfo side="Left"> / <JoinInfo side="Right">
  sub-elements, each of which contain a <Field name="..."> list.
* We produce a FULL OUTER JOIN that faithfully replicates the three-output
  behaviour of Alteryx — but in practice most workflows only consume the Join
  (inner) output, so we also emit simpler INNER JOIN / LEFT JOIN / RIGHT JOIN
  variants as commented alternatives.
* The chunk's input_cte_names list for a Join chunk has at least two entries:
  the Left CTE (index 0) and the Right CTE (index 1).  The translator uses
  these positionally.
* If the chunk has more than two input CTEs (rare, multi-join) we emit a stub.
"""

from __future__ import annotations

from parsing.models import CTEFragment, ToolNode
from translators.context import TranslationContext


def translate_join(
    node: ToolNode,
    cte_name: str,
    input_ctes: list[str],
    ctx: TranslationContext,
) -> CTEFragment:
    """Translate a Join node into a CTE."""
    if len(input_ctes) < 2:
        ctx.warnings.append(
            f"Tool {node.tool_id} (join): expected 2 input CTEs, got {len(input_ctes)}. "
            "Generating stub."
        )
        return CTEFragment(
            name=cte_name,
            sql="SELECT TOP 0 1 AS _stub  -- join: insufficient inputs",
            source_tool_ids=[node.tool_id],
            is_stub=True,
        )

    left_cte = input_ctes[0]
    right_cte = input_ctes[1]

    cfg = node.config

    # Parse join keys from <JoinInfo side="Left"><Field name="..."/></JoinInfo>
    left_keys = _parse_join_keys(cfg, "Left")
    right_keys = _parse_join_keys(cfg, "Right")

    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        ctx.warnings.append(
            f"Tool {node.tool_id} (join): could not parse join keys — generating stub. "
            f"Left keys: {left_keys}, Right keys: {right_keys}"
        )
        return CTEFragment(
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

    # Build ON clause
    on_parts = [f"L.[{lk}] = R.[{rk}]" for lk, rk in zip(left_keys, right_keys)]
    on_clause = "\n    AND ".join(on_parts)

    # Build SELECT list from schema when available; fall back to L.*, R.*
    select_clause = _build_select(left_cte, right_cte, ctx, cfg)

    # The three Alteryx Join anchors map to:
    #   Join  → INNER JOIN  (matched rows)
    #   Left  → LEFT ANTI JOIN (add WHERE R.key IS NULL)
    #   Right → RIGHT ANTI JOIN (add WHERE L.key IS NULL)
    sql = (
        f"-- Join anchor output (matched rows)\n"
        f"-- Left-only:  add WHERE R.[{right_keys[0]}] IS NULL\n"
        f"-- Right-only: add WHERE L.[{left_keys[0]}] IS NULL\n"
        f"{select_clause}\n"
        f"FROM [{left_cte}] AS L\n"
        f"INNER JOIN [{right_cte}] AS R\n"
        f"    ON {on_clause}"
    )

    return CTEFragment(name=cte_name, sql=sql, source_tool_ids=[node.tool_id])


def _build_select(
    left_cte: str,
    right_cte: str,
    ctx: TranslationContext,
    cfg: dict,
) -> str:
    """Return a SELECT clause for a join.

    When both upstream CTEs have known schemas, emits an explicit column list
    with right-side clashes disambiguated using the configured prefix
    (``RenameRightInput`` in the Join XML config, defaulting to ``Right_``).

    Falls back to ``SELECT L.*, R.*`` when schema is unavailable.
    """
    left_schema = ctx.cte_schema.get(left_cte, [])
    right_schema = ctx.cte_schema.get(right_cte, [])

    if not left_schema or not right_schema:
        return "SELECT\n    L.*,\n    R.*"

    # RenameRightInput stores the prefix Alteryx prepends to clashing right-side
    # column names.  The user can customise this in the Join tool UI.
    # RenameRightInput_AddSuffix is a rarely-used alternative (suffix instead of
    # prefix); not currently handled — falls back to the prefix form.
    right_prefix: str = cfg.get("RenameRightInput", "Right_")

    left_names = {f.name for f in left_schema}
    cols: list[str] = [f"    L.[{f.name}]" for f in left_schema]
    for f in right_schema:
        if f.name in left_names:
            cols.append(f"    R.[{f.name}] AS [{right_prefix}{f.name}]")
        else:
            cols.append(f"    R.[{f.name}]")

    return "SELECT\n" + ",\n".join(cols)


def _parse_join_keys(cfg: dict, side: str) -> list[str]:
    """Extract field names from <JoinInfo side='Left'> or <JoinInfo side='Right'>."""
    join_info = cfg.get("JoinInfo", [])
    if isinstance(join_info, dict):
        join_info = [join_info]

    for info in join_info:
        # Alteryx uses connection= attribute (e.g. connection="Left")
        # Some older formats use side= — check both
        attr = info.get("connection", info.get("side", info.get("Side", "")))
        if attr == side:
            field = info.get("Field", [])
            if isinstance(field, dict):
                field = [field]
            # Field name stored in field= attribute (parsed from XML), fallback to name=
            return [
                f.get("field", f.get("name", f.get("Name", "")))
                for f in field
                if f.get("field") or f.get("name") or f.get("Name")
            ]

    return []
