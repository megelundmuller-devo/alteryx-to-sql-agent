"""Translation layer — Phase 3.

Public API
----------
    from translators import translate_chunk

    fragments: list[CTEFragment] = translate_chunk(chunk, ctx)

translate_chunk() converts one Chunk (from Phase 2) into a list of CTEFragments.
Each fragment is the SQL body for one named CTE.  The assembler (Phase 5)
joins them into the final WITH clause.

Translator registry
-------------------
Each tool type maps to a callable with signature:

    fn(node, cte_name, input_ctes, ctx) -> CTEFragment | list[CTEFragment]

`translate_chunk` dispatches based on `node.tool_type`.  Unknown types fall
back to `translate_unknown`.

Multi-node chunks
-----------------
When a chunk contains several merged tools (e.g. DbFileInput → Formula → Select),
each intermediate tool gets a temporary name (`temp_{type}_{id}`) so the
chain can reference it.  The final tool uses the chunk's `output_cte_name`.
Only the final CTE name is visible outside the chunk.

Filter branching
----------------
If a filter node returns two CTEFragments (True + False branches), both are
appended to the output list.  The assembler must handle this correctly.
"""

from __future__ import annotations

from typing import Callable

from parsing.models import Chunk, CTEFragment, ToolNode
from translators.append import translate_append
from translators.schema_inference import infer_output_schema
from translators.context import TranslationContext
from translators.filter import translate_filter
from translators.find_replace import translate_find_replace
from translators.reg_ex import translate_reg_ex
from translators.text_to_columns import translate_text_to_columns
from translators.formula import translate_formula
from translators.input_output import (
    translate_db_file_input,
    translate_db_file_output,
    translate_text_input,
)
from translators.join import translate_join
from translators.macro import translate_macro
from translators.multirow import translate_multirow
from translators.record_id import translate_record_id
from translators.sample import translate_sample
from translators.select import translate_select
from translators.sort import translate_sort
from translators.summarize import translate_summarize
from translators.union import translate_union
from translators.unique import translate_unique
from translators.unknown import translate_unknown

# ---------------------------------------------------------------------------
# Type alias for a translator function
# ---------------------------------------------------------------------------

TranslatorFn = Callable[
    [ToolNode, str, list[str], TranslationContext],
    "CTEFragment | list[CTEFragment]",
]

# ---------------------------------------------------------------------------
# Registry — map short tool_type strings → translator callables
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, TranslatorFn] = {
    # Sources
    "db_file_input": translate_db_file_input,
    "text_input": translate_text_input,
    # Sinks
    "db_file_output": translate_db_file_output,
    # Transforms
    "select": translate_select,
    "filter": translate_filter,
    "formula": translate_formula,
    "summarize": translate_summarize,
    "sort": translate_sort,
    "unique": translate_unique,
    "sample": translate_sample,
    "record_id": translate_record_id,
    "multirow_formula": translate_multirow,
    "multi_row_formula": translate_multirow,
    # Multi-input
    "join": translate_join,
    "union": translate_union,
    "append_fields": translate_append,
    # Special
    "find_replace": translate_find_replace,
    "reg_ex": translate_reg_ex,
    "text_to_columns": translate_text_to_columns,
    "macro": translate_macro,
}


def _get_translator(tool_type: str) -> TranslatorFn:
    return _REGISTRY.get(tool_type, translate_unknown)


# ---------------------------------------------------------------------------
# Core public function
# ---------------------------------------------------------------------------


def translate_chunk(chunk: Chunk, ctx: TranslationContext) -> list[CTEFragment]:
    """Translate one Chunk into a list of CTEFragments.

    Args:
        chunk: The Chunk produced by Phase 2 chunker.
        ctx:   Shared TranslationContext (DAG reference + warnings list).

    Returns:
        Ordered list of CTEFragments.  The last fragment (or the True-branch
        fragment for a filter) always has `name == chunk.output_cte_name`.
    """
    fragments: list[CTEFragment] = []
    n_nodes = len(chunk.nodes)

    for i, node in enumerate(chunk.nodes):
        is_last = i == n_nodes - 1

        # The CTE name for this node's output:
        # - Last node in the chunk → use the chunk's canonical output_cte_name
        # - Intermediate nodes     → per-node temporary name
        if is_last:
            cte_name = chunk.output_cte_name
        else:
            cte_name = f"temp_{node.tool_type}_{node.tool_id}"

        # The input CTEs for this node:
        # - First node  → reads from chunk.input_cte_names
        # - Subsequent  → reads from the previous node's temp table name
        if i == 0:
            input_ctes = list(chunk.input_cte_names)
        else:
            prev = chunk.nodes[i - 1]
            # Previous node's name is always its per-node temp name,
            # because only the *last* node in the chunk uses output_cte_name.
            input_ctes = [f"temp_{prev.tool_type}_{prev.tool_id}"]

        translator = _get_translator(node.tool_type)
        result = translator(node, cte_name, input_ctes, ctx)

        # Infer output schema from config + input schemas when the parser
        # did not populate node.output_schema (most non-source tools).
        inferred_schema = infer_output_schema(node, input_ctes, ctx)

        if isinstance(result, list):
            for frag in result:
                # Translators (e.g. join) may pre-set ctx.cte_schema for
                # anchor-specific CTEs (L, R) with the correct per-anchor
                # schema.  Don't overwrite those with the inferred J schema.
                if frag.name not in ctx.cte_schema:
                    ctx.cte_schema[frag.name] = inferred_schema
                ctx.cte_inputs[frag.name] = list(input_ctes)
            fragments.extend(result)
        else:
            ctx.cte_schema[result.name] = inferred_schema
            ctx.cte_inputs[result.name] = list(input_ctes)
            fragments.append(result)

    return fragments
