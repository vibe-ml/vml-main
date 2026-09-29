"""Deep research agent built on the Deep Agents harness."""

import os

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from common.settings import Settings
from research.prompts import (
    ATTENTION_SCOUT_PROMPT,
    COORDINATOR_PROMPT,
    EVIDENCE_SCOUT_PROMPT,
)
from research.tools import build_search_tool


def _openai_api_key(settings: Settings) -> str | None:
    if settings.model_api_key is not None:
        return settings.model_api_key.get_secret_value()
    return os.environ.get("OPENAI_API_KEY") or None


def build_chat_model(settings: Settings) -> str | BaseChatModel:
    """Resolve the configured chat model.

    A base URL selects an OpenAI-compatible server, such as a local
    inference endpoint. Otherwise the model string is a provider id
    that Deep Agents resolves, for example ``anthropic:claude-sonnet-4-5``.
    """
    if settings.model_base_url:
        name = settings.model.removeprefix("openai:").strip() or "gpt-4.1-mini"
        return init_chat_model(
            name,
            model_provider="openai",
            base_url=settings.model_base_url,
            api_key=_openai_api_key(settings) or "not-set",
        )
    api_key = _openai_api_key(settings)
    if api_key and settings.model.startswith("openai:"):
        return init_chat_model(settings.model, api_key=api_key)
    return settings.model


def readiness_error(settings: Settings) -> str | None:
    """Return the configuration problem that blocks a research run."""
    if not settings.model.strip() and not settings.model_base_url:
        return "Set HORIZON_MODEL, or HORIZON_MODEL_BASE_URL for an OpenAI-compatible server."
    needs_openai_key = settings.model.startswith("openai:") and not settings.model_base_url
    if needs_openai_key and not _openai_api_key(settings):
        return "Set HORIZON_MODEL_API_KEY, or OPENAI_API_KEY, for the OpenAI model."
    if settings.tavily_api_key is None:
        return "Set HORIZON_TAVILY_API_KEY so the agent can search the web."
    return None


def build_research_agent(settings: Settings):
    """Create the coordinator and its evidence and attention scouts."""
    from deepagents import create_deep_agent

    search = build_search_tool(
        settings.tavily_api_key.get_secret_value(),
        settings.max_search_results,
    )
    return create_deep_agent(
        model=build_chat_model(settings),
        system_prompt=COORDINATOR_PROMPT,
        subagents=[
            {
                "name": "evidence-scout",
                "description": (
                    "Search scientific, patent, and specialist sources for one "
                    "technology branch. Pass one branch at a time."
                ),
                "system_prompt": EVIDENCE_SCOUT_PROMPT,
                "tools": [search],
            },
            {
                "name": "attention-scout",
                "description": (
                    "Search news, marketing, and investment sources for one "
                    "technology branch. Pass one branch at a time."
                ),
                "system_prompt": ATTENTION_SCOUT_PROMPT,
                "tools": [search],
            },
        ],
        name="horizon-research",
    )
