# AGENTS.md, CLAUDE.md

The project goal is to gather data from OpenAlex (openalex.org) on regular bases.
We will use this data to build our own knowledge graph for Weak Signal Analysis (innovation trigger).

Use caveman skill in mode lite when working in this repository.

## Project structure and documentation

- `docs/agents/build.md`               - Package and dependency management and script running.
- `docs/agents/code-conventions.md`    - Code conventions and rules.
- `docs/agents/documentation-rules.md` - Documentation rules.
- `src/common/settings.py`             - Project settings
- `src/models/`                        - SQLAlchemy database models
- `deploy/`                            - Deployment tools and automation

## Agent skills

### Issue tracker

Use local Markdown issues under `docs/agents/tasks/`. Read
`docs/agents/issue-tracker.md` before creating or updating specs and issues.

### Triage labels

Use the five default triage roles. Read `docs/agents/triage-labels.md`
before assigning triage status.

### Domain docs

Use a single-context layout. Read `docs/agents/domain.md` before exploring the codebase.
