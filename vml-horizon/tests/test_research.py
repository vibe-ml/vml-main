"""Search formatting and stream parsing."""

from common.settings import Settings
from research.agent import build_research_agent
from research.events import events_from_chunk
from research.tools import format_search_results


def test_format_search_results_includes_url_and_excerpt():
    text = format_search_results(
        {
            "query": "organ-on-chip",
            "results": [
                {
                    "title": "A microfluidic platform",
                    "url": "https://example.test/paper",
                    "content": "Proof of concept in a lab.",
                }
            ],
        }
    )
    assert "https://example.test/paper" in text
    assert "Proof of concept in a lab." in text


def test_format_search_results_when_empty():
    assert "No results" in format_search_results({"query": "missing", "results": []})


def test_agent_graph_compiles():
    settings = Settings(
        model="openai:gpt-4.1-mini",
        model_api_key="sk-test",
        tavily_api_key="tvly-test",
    )
    assert build_research_agent(settings) is not None


def test_events_from_chunk_reads_the_report():
    events = events_from_chunk(
        {
            "type": "updates",
            "ns": [],
            "data": {"tools": {"files": {"/final_report.md": "# Report\n\nCandidate"}}},
        }
    )
    assert events[-1]["kind"] == "report"
    assert "Candidate" in events[-1]["text"]
