"""TranslationContext — shared state threaded through all translator calls.

Each call to translate_chunk() creates one TranslationContext that is passed
to every individual tool translator.  Translators may append to `warnings`,
`engine_vars`, and `cte_schema` but must not mutate anything else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from parsing.dag import AlteryxDAG
from parsing.models import FieldSchema

if TYPE_CHECKING:
    from registry.tool_registry import ToolRegistry


@dataclass
class TranslationContext:
    """Shared read-only DAG reference plus mutable accumulator fields."""

    dag: AlteryxDAG

    # tool_id → the output_cte_name of the chunk that contains that tool.
    # Populated by translate_chunk() before any translator is called.
    chain_cte: dict[int, str] = field(default_factory=dict)

    # Translators append human-readable strings here; bubbled up to the caller.
    warnings: list[str] = field(default_factory=list)

    # Optional tool registry for unknown tool caching. None = registry disabled.
    registry: ToolRegistry | None = None

    # Alteryx [Engine.XYZ] variable names encountered during expression conversion.
    # cte_builder uses this to emit DECLARE statements at the top of the procedure.
    engine_vars: set[str] = field(default_factory=set)

    # cte_name → output column schema of the node that produced that CTE.
    # Populated by translate_chunk() after each node is translated.
    # join.py and select.py use this to emit explicit column lists.
    cte_schema: dict[str, list[FieldSchema]] = field(default_factory=dict)

    # cte_name → list of input CTE names that feed into it.
    # Populated by translate_chunk() alongside cte_schema.
    # Used by the liveness pass to walk backwards through the CTE chain.
    cte_inputs: dict[str, list[str]] = field(default_factory=dict)

    # Passthrough aliases: when a Join's L/R anchor is absorbed into J as a
    # LEFT/RIGHT/FULL OUTER JOIN, the L or R CTE name maps to J's CTE name here.
    # Union translators resolve these before building their SQL so the extra
    # anchor fragment is never emitted and the UNION is collapsed to a single SELECT.
    cte_passthrough: dict[str, str] = field(default_factory=dict)
