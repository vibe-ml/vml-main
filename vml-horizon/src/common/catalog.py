"""Repositories that make up the weak-signal system."""

from typing import TypedDict


class Component(TypedDict):
    """One repository and the job it does in the system."""

    name: str
    repo: str
    role: str


COMPONENTS: tuple[Component, ...] = (
    {
        "name": "OpenAlex crawler",
        "repo": "vml-crawler-openalex",
        "role": "Collects scientific works and the taxonomy used as publication evidence.",
    },
    {
        "name": "Social crawler",
        "repo": "vml-crawler-social",
        "role": "Collects Hacker News, Stack Exchange, and Bluesky mentions for mass attention.",
    },
    {
        "name": "Topic processor",
        "repo": "vml-processor-bertopic",
        "role": (
            "Discovers research themes in a scientific corpus. "
            "A discovery topic is not a weak signal."
        ),
    },
    {
        "name": "Inference",
        "repo": "vml-inference",
        "role": "Serves models and vector search for the rest of the system.",
    },
    {
        "name": "Pipelines",
        "repo": "vml-dagster",
        "role": "Runs the collection and processing jobs.",
    },
    {
        "name": "Methodology",
        "repo": "vml-trends",
        "role": "Holds the problem statement, glossary, and weak-signal research.",
    },
    {
        "name": "Horizon",
        "repo": "vml-horizon",
        "role": (
            "Researches an open query on the public web and presents the project to an analyst."
        ),
    },
)
