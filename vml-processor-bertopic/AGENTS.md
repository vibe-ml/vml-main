# AGENTS.md

This project processes and embeds scientific works collected by `vml-crawler-openalex` and uses BERTopic to discover topics for weak-signal analysis.

Use the caveman skill in lite mode while working in this repository.

## Project guidance

- Before managing dependencies or running Python, read [build instructions](docs/agents/build.md).
- Before deploying the processor code location on vml.pub, read [deployment instructions](docs/agents/deploy.md).
- Before writing or changing code, read [code conventions](docs/agents/code-conventions.md).
- Before writing or changing documentation, read [documentation rules](docs/agents/documentation-rules.md).
- Before selecting input data or implementing preprocessing, embeddings, or topic modeling, read the relevant references in the [crawler documentation guide](docs/upstream/vml-crawler-openalex/INDEX.md).
- Before naming domain concepts, read the imported [domain glossary](docs/upstream/vml-crawler-openalex/CONTEXT.md).

## Project structure

- `src/`: Processor implementation. `common/` holds settings, logging, and database access; `corpus/` is the corpus selection stage; `models/` declares tables.
- `tests/`: pytest suite against the local `pwf_test` database; see [build instructions](docs/agents/build.md).
- `docs/agents/`: Instructions for work in this repository. Keep local Markdown specs and issues under `docs/agents/tasks/` when needed.
- `docs/upstream/vml-crawler-openalex/`: Preserved crawler documentation snapshot, with source provenance in `manifest.json`.

The upstream snapshot describes the crawler. Its commands, dependency requirements, paths, and task completion claims apply to that project. Keep processor decisions in this repository's own documentation; distinguish research proposals from implemented behavior.

## Agent skills

### Issue tracker

Before creating, fetching, or updating issues and specs, read
[local tracker rules](docs/agents/issue-tracker.md).
Track work in `docs/agents/tasks/`.

### Triage labels

Before triaging issues, read
[triage labels](docs/agents/triage-labels.md).

### Domain docs

Before exploring the codebase, read
[domain documentation rules](docs/agents/domain.md).
Use a single-context layout.
