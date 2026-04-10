"""LLM agent for generating workflow documentation.

Produces a Markdown narrative summary of the converted workflow, explaining what
the workflow does in plain English.  The result is embedded in the _docs.md file
alongside the deterministic CTE index rendered by doc_writer.py.

Usage:
    from llm.doc_agent import generate_workflow_summary

    narrative = generate_workflow_summary(prompt)
"""

from __future__ import annotations

import os

from pydantic_ai import Agent

from llm.prompts import DOC_SYSTEM_PROMPT
from llm.settings import get_settings


def _make_agent() -> Agent:
    settings = get_settings()
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)
    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=DOC_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


_agent: Agent | None = None


def _get_agent() -> Agent:
    global _agent
    if _agent is None:
        _agent = _make_agent()
    return _agent


def generate_workflow_summary(prompt: str) -> str:
    """Call the LLM to generate a Markdown narrative for the workflow.

    Args:
        prompt: Structured description of the workflow (built by doc_writer).

    Returns:
        Markdown string.  Returns an empty string on failure — the caller
        renders a fallback section header.
    """
    try:
        result = _get_agent().run_sync(prompt)
        return result.output.strip()
    except Exception:  # noqa: BLE001
        return ""
