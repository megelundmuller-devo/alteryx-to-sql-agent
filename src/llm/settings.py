"""LLM settings loaded from environment / .env file.

Usage:
    from llm.settings import get_settings
    s = get_settings()
    print(s.vertex_project, s.vertex_location, s.model_id)
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMSettings(BaseSettings):
    """Vertex AI connection settings.  Reads from .env at the project root."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    vertex_project: str = Field(..., alias="VERTEX_PROJECT")
    vertex_location: str = Field(..., alias="VERTEX_LOCATION")
    model_id: str = Field("google-vertex:gemini-2.0-flash", alias="VERTEX_MODEL")

    # How many times the agent retries a failed LLM call before giving up
    llm_max_retries: int = Field(3, alias="LLM_MAX_RETRIES")


@lru_cache(maxsize=1)
def get_settings() -> LLMSettings:
    """Return a cached LLMSettings instance."""
    return LLMSettings()  # type: ignore[call-arg]
