"""LLM agent for repairing broken CTE bodies.

Called by the validation pass when a CTE references columns that do not exist
in any of its input schemas.  The agent receives the CTE name, its current SQL
body, the schemas of every input CTE, and the list of missing columns, and
returns a corrected SQL body.

The repaired SQL is still marked as a stub (is_stub=True) so reviewers know it
was auto-corrected rather than deterministically translated.
"""

from __future__ import annotations

import os

from pydantic_ai import Agent

from llm.prompts import CTE_REPAIR_SYSTEM_PROMPT
from llm.settings import get_settings
from parsing.models import FieldSchema


def _make_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=CTE_REPAIR_SYSTEM_PROMPT,
        retries=1,
    )


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text[text.index("\n") + 1 :] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[: text.rindex("```")].rstrip()
    return text.strip()


def repair_cte_llm(
    cte_name: str,
    sql_body: str,
    input_schemas: dict[str, list[FieldSchema]],
    missing_cols: set[str],
) -> str | None:
    """Ask the LLM to fix column-reference errors in a CTE body.

    Args:
        cte_name:      Name of the broken CTE (for context in the prompt).
        sql_body:      Current (broken) SQL body of the CTE.
        input_schemas: Mapping of input CTE name → list of FieldSchema.
        missing_cols:  Column names that are referenced but not found in inputs.

    Returns:
        Corrected SQL body string on success, or None on failure.
    """
    schema_lines: list[str] = []
    for inp_name, fields in input_schemas.items():
        if fields:
            cols = ", ".join(f"[{f.name}]" for f in fields)
            schema_lines.append(f"  {inp_name}: {cols}")
        else:
            schema_lines.append(f"  {inp_name}: (schema unknown)")

    missing_str = ", ".join(f"[{c}]" for c in sorted(missing_cols))
    schemas_str = "\n".join(schema_lines) if schema_lines else "  (none)"

    prompt = (
        f"CTE name: {cte_name}\n"
        f"Missing columns: {missing_str}\n\n"
        f"Available input schemas:\n{schemas_str}\n\n"
        f"Current SQL body:\n{sql_body}"
    )

    try:
        result = _make_agent().run_sync(prompt)
        repaired = _strip_fences(result.output)
        return repaired if repaired else None
    except Exception:  # noqa: BLE001
        return None
