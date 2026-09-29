"""Web search tool used by the research subagents."""

from typing import Any

from langchain_core.tools import BaseTool, tool
from langchain_tavily import TavilySearch


def format_search_results(payload: Any) -> str:
    """Turn a Tavily payload into source blocks the agent can cite."""
    if not isinstance(payload, dict):
        return str(payload)
    results = payload.get("results") or []
    if not results:
        return f"No results for {payload.get('query', 'the query')}."
    blocks: list[str] = []
    for index, result in enumerate(results, start=1):
        title = result.get("title") or "Untitled"
        url = result.get("url") or ""
        content = (result.get("content") or "").strip()
        blocks.append(f"[{index}] {title}\nURL: {url}\nExcerpt: {content}")
    query = payload.get("query") or ""
    return f"Results for {query}:\n\n" + "\n\n".join(blocks)


def build_search_tool(api_key: str, max_results: int) -> BaseTool:
    """Build a web search tool backed by Tavily."""
    search = TavilySearch(
        max_results=max_results,
        tavily_api_key=api_key,
        search_depth="advanced",
    )

    @tool
    def web_search(query: str) -> str:
        """Search the public web and return titles, URLs, and excerpts."""
        payload = search.invoke({"query": query})
        return format_search_results(payload)

    return web_search
