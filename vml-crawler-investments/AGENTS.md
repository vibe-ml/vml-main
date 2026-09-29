# AGENTS.md, CLAUDE.md

The project goal is to collect funding news for OpenAlex topics since 2020
for Weak Signal Analysis (investment multiplier). Task: vml-trends `trends-05`.

Use caveman skill in mode lite when working in this repository.

## Project structure and documentation

- `docs/agents/build.md`               - Package and dependency management and script running.
- `docs/agents/code-conventions.md`    - Code conventions and rules.
- `docs/agents/documentation-rules.md` - Documentation rules.
- `docs/collection.md`                 - Source decision, extraction, rate limits.
- `src/common/settings.py`             - Project settings
- `src/models/`                        - SQLAlchemy database models
- `src/collection/`                    - GDELT client, headline extraction, scans, Dagster job
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
