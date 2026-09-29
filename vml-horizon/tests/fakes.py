"""Scripted agent for tests. It never calls a model or the network."""

from deepagents.backends.utils import create_file_data


class ScriptedAgent:
    """Yield a scout step and a finished report."""

    async def astream(self, payload, **kwargs):
        del payload, kwargs
        yield {
            "type": "updates",
            "ns": [],
            "data": {
                "model": {
                    "messages": [
                        {
                            "type": "ai",
                            "content": "Delegating the first technology branch.",
                            "tool_calls": [
                                {
                                    "name": "task",
                                    "args": {"subagent_type": "evidence-scout"},
                                    "id": "call-1",
                                }
                            ],
                        }
                    ]
                }
            },
        }
        yield {
            "type": "updates",
            "ns": ["evidence-scout"],
            "data": {
                "tools": {
                    "messages": [
                        {
                            "type": "tool",
                            "name": "web_search",
                            "content": "Results for organ-on-chip: one specialist source.",
                        }
                    ]
                }
            },
        }
        report = (
            "# Weak-signal research\n\n"
            "## Technology candidates\n\n"
            "### Organ-on-chip sensors\n\n"
            "- Classification: technology candidate\n"
        )
        yield {
            "type": "updates",
            "ns": [],
            "data": {"tools": {"files": {"/final_report.md": create_file_data(report)}}},
        }
