"""LLM agent for translating an entire tool chunk when deterministic fails.

Used as a fallback for unknown tool types after the tool registry has no result.
The agent receives the tool's config dict as a JSON-formatted string and the
names of upstream CTEs, and returns the body of a SQL CTE.

Usage:
    from llm.chunk_agent import translate_chunk_llm

    sql_body = translate_chunk_llm(
        tool_type="unknown_tool",
        plugin="Vendor.Plugin.PluginName",
        config={"Key": "value"},
        input_ctes=["cte_upstream_1"],
    )
"""

from __future__ import annotations

import json
import os

from pydantic_ai import Agent

from llm.prompts import CHUNK_SYSTEM_PROMPT
from llm.settings import get_settings


def _make_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=CHUNK_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


_agent: Agent | None = None


def _get_agent() -> Agent:
    global _agent
    if _agent is None:
        _agent = _make_agent()
    return _agent


def _strip_fences(text: str) -> str:
    """Remove markdown code fences that the LLM sometimes wraps output in."""
    text = text.strip()
    if text.startswith("```"):
        text = text[text.index("\n") + 1 :] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[: text.rindex("```")].rstrip()
    return text.strip()


def translate_chunk_llm(
    tool_type: str,
    plugin: str,
    config: dict,
    input_ctes: list[str],
) -> str:
    """Ask the LLM to translate an entire tool into a CTE body.

    Returns the CTE body SQL string, or a stub comment on failure.
    A failed result starts with '-- LLM translation failed'.
    """
    config_json = json.dumps(config, indent=2, default=str)[:3000]  # truncate large configs
    upstream = ", ".join(f"[{c}]" for c in input_ctes) if input_ctes else "none"

    prompt = (
        f"Tool type: {tool_type}\n"
        f"Plugin: {plugin}\n"
        f"Upstream CTEs: {upstream}\n\n"
        f"Configuration:\n```json\n{config_json}\n```\n\n"
        "Generate the T-SQL CTE body for this tool."
    )

    try:
        result = _get_agent().run_sync(prompt)
        return _strip_fences(result.output)
    except Exception as exc:  # noqa: BLE001
        return (
            f"-- LLM translation failed: {exc!s:.200}\n"
            f"-- Tool: {tool_type} / {plugin}\n"
            f"SELECT TOP 0 1 AS _stub  -- MANUAL REVIEW REQUIRED"
        )
