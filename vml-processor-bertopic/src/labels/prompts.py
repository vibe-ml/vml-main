"""Versioned prompt constants for work summaries and topic headlines."""

PROMPT_VERSION = "label_prompts_v1"

SUMMARY_PROMPT_TEMPLATE = """\
Write a concise English summary of the following scientific work.
Respond in English regardless of the source language of the title and abstract.

Title: {title}
Abstract: {abstract}
"""

HEADLINE_PROMPT_TEMPLATE = """\
Write one short English headline that captures the research theme of these work summaries.
Respond in English regardless of the source language of the summaries.

Summaries:
{summaries}
"""
