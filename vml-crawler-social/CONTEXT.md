# Social mentions context

Domain glossary for weak-signal analysis lives in [vml-trends/CONTEXT.md](../vml-trends/CONTEXT.md). Terms specific to this repository:

| Term | Meaning |
| --- | --- |
| Search profile | Versioned set of OpenAlex topics and their search terms |
| Term | Normalized phrase from a topic name or OpenAlex keyword |
| Channel | Searchable unit of a platform: `hn`, a Stack Exchange site, Bluesky `all` |
| Scan | One term search in one channel and month window; may split or truncate |
| Mention | Social item whose own text contains a profile term |
| Volume | All items published on a channel in a month |
| Attention | Monthly topic mentions per channel with share and coverage |
| Coverage | Whether a topic-month was fully observed: complete, truncated, partial, missing |
| Mass attention | Public interest outside science and patents; high values argue against a weak signal |
