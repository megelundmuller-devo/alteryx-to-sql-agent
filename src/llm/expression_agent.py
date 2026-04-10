"""LLM agent for converting complex Alteryx expressions to T-SQL.

This agent is invoked by the formula and filter translators when
`needs_llm_translation()` returns True for an expression.

Usage:
    from llm.expression_agent import convert_expression_llm

    sql_expr = convert_expression_llm("IF [Status] == 'Active' THEN 1 ELSE 0 ENDIF")
    # Returns a T-SQL expression string, or a commented stub on failure.

Architecture:
    Agent('google-vertex:gemini-2.5-pro', output_type=str)
    Vertex AI credentials are read from the environment via LLMSettings.
    The agent runs synchronously using run_sync().
    Few-shot examples from prompts.EXPRESSION_FEW_SHOT are prepended to
    every call so the model returns bare T-SQL without markdown prose.
"""

from __future__ import annotations

import os

from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, UserPromptPart

from llm.prompts import EXPRESSION_FEW_SHOT, EXPRESSION_SYSTEM_PROMPT
from llm.settings import get_settings


def _make_agent() -> Agent:
    settings = get_settings()

    # Vertex AI requires project + location to be set as environment variables
    # for the google-genai / google-vertex backend used by pydantic-ai.
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", settings.vertex_project)
    os.environ.setdefault("GOOGLE_CLOUD_LOCATION", settings.vertex_location)

    return Agent(
        settings.model_id,
        output_type=str,
        system_prompt=EXPRESSION_SYSTEM_PROMPT,
        retries=settings.llm_max_retries,
    )


def _build_history() -> list[ModelMessage]:
    """Build the few-shot message history from EXPRESSION_FEW_SHOT pairs."""
    history: list[ModelMessage] = []
    for msg in EXPRESSION_FEW_SHOT:
        if msg["role"] == "user":
            history.append(ModelRequest(parts=[UserPromptPart(content=msg["content"])]))
        else:
            history.append(ModelResponse(parts=[TextPart(content=msg["content"])]))
    return history


# Module-level singleton — created lazily on first call
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


def convert_expression_llm(alteryx_expression: str) -> str:
    """Convert an Alteryx expression to T-SQL using the LLM agent.

    Args:
        alteryx_expression: The raw Alteryx formula/expression string.

    Returns:
        A T-SQL expression string.  On failure returns a commented stub.

    Raises:
        Never raises — all exceptions are caught and returned as stubs.
    """
    try:
        agent = _get_agent()
        prompt = f"Convert: {alteryx_expression}"
        result = agent.run_sync(prompt, message_history=_build_history())
        return _strip_fences(result.output)
    except Exception as exc:  # noqa: BLE001
        return (
            f"-- LLM conversion failed: {exc!s:.200}\n"
            f"-- Original: {alteryx_expression!r:.200}\n"
            f"NULL  -- MANUAL REVIEW REQUIRED"
        )
