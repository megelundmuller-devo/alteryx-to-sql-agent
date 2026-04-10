"""TranslationContext — shared state threaded through all translator calls.

Each call to translate_chunk() creates one TranslationContext that is passed
to every individual tool translator.  Translators may append to `warnings` but
must not mutate anything else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from parsing.dag import AlteryxDAG

if TYPE_CHECKING:
    from registry.tool_registry import ToolRegistry


@dataclass
class TranslationContext:
    """Shared read-only DAG reference plus a mutable warnings list."""

    dag: AlteryxDAG

    # tool_id → the output_cte_name of the chunk that contains that tool.
    # Populated by translate_chunk() before any translator is called.
    chain_cte: dict[int, str] = field(default_factory=dict)

    # Translators append human-readable strings here; bubbled up to the caller.
    warnings: list[str] = field(default_factory=list)

    # Optional tool registry for unknown tool caching. None = registry disabled.
    registry: ToolRegistry | None = None
