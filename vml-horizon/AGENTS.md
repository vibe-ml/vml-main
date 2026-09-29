# AGENTS.md, CLAUDE.md

The project is a Weak Signal recognition system.

Use caveman skill in mode lite when working in this repository.

## Project structure and documentation

- `docs/agents/architecture/overview.md` - Current architecture
- `docs/agents/build.md`               - Package and dependency management and script running.
- `docs/agents/code-conventions.md`    - Code conventions and rules.
- `docs/agents/documentation-rules.md` - Documentation rules.
- `src/common/settings.py`             - Project settings
- `src/research/`                      - Deep research agent
- `src/api/`                           - HTTP API
- `src/web/static/`                    - Analyst web UI
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
