"""Run the research agent and publish progress for the UI."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from common.log import logger
from common.settings import Settings, get_settings
from research.agent import build_research_agent, readiness_error
from research.events import events_from_chunk
from research.prompts import render_request

AgentFactory = Callable[[], object]


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class Run:
    """One open-query research run and the events produced so far."""

    id: str
    query: str
    exclusions: str
    status: str = "queued"
    created_at: str = field(default_factory=_now)
    error: str | None = None
    report: str = ""
    fallback: str = ""
    events: list[dict[str, str]] = field(default_factory=list)
    subscribers: list[asyncio.Queue] = field(default_factory=list)
    task: asyncio.Task | None = None


class ResearchService:
    """Start research runs and stream their events."""

    def __init__(
        self,
        settings: Settings | None = None,
        agent_factory: AgentFactory | None = None,
        ready: Callable[[], str | None] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._agent_factory = agent_factory or (lambda: build_research_agent(self._settings))
        self._ready = ready or (lambda: readiness_error(self._settings))
        self._runs: dict[str, Run] = {}

    def configuration_error(self) -> str | None:
        """Return a blocking configuration message, or none when a run can start."""
        return self._ready()

    def status(self) -> dict[str, object]:
        """Describe whether a research run can start. Never include secrets."""
        message = self.configuration_error()
        return {
            "ready": message is None,
            "model": self._settings.model,
            "model_base_url": self._settings.model_base_url or "",
            "search_configured": self._settings.tavily_api_key is not None,
            "message": message or "",
        }

    def list_runs(self) -> list[Run]:
        """Return runs newest first."""
        return sorted(self._runs.values(), key=lambda run: run.created_at, reverse=True)

    def get(self, run_id: str) -> Run | None:
        """Return one run, or none when the id is unknown."""
        return self._runs.get(run_id)

    def start(self, query: str, exclusions: str) -> Run:
        """Queue a research run. The caller must check configuration first."""
        run = Run(id=uuid.uuid4().hex, query=query.strip(), exclusions=exclusions.strip())
        self._runs[run.id] = run
        run.task = asyncio.create_task(self._execute(run))
        return run

    async def wait(self, run_id: str) -> Run:
        """Block until the run task finishes."""
        run = self._runs[run_id]
        if run.task is not None:
            await run.task
        return run

    def subscribe(self, run: Run) -> asyncio.Queue:
        """Replay events so far, then receive new ones. None marks the end."""
        queue: asyncio.Queue = asyncio.Queue()
        for event in run.events:
            queue.put_nowait(event)
        if run.status in {"completed", "failed"}:
            queue.put_nowait(None)
        else:
            run.subscribers.append(queue)
        return queue

    async def iter_events(self, run: Run) -> AsyncIterator[dict[str, str] | None]:
        """Yield live events. None means the run has finished."""
        queue = self.subscribe(run)
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
            except TimeoutError:
                yield None
                continue
            yield event
            finished = event is None or (
                event.get("kind") == "status" and event.get("status") in {"completed", "failed"}
            )
            if finished:
                return

    def _publish(self, run: Run, event: dict[str, str]) -> None:
        event.setdefault("at", _now())
        run.events.append(event)
        for queue in run.subscribers:
            queue.put_nowait(dict(event))
        if event.get("kind") == "status" and event.get("status") in {"completed", "failed"}:
            for queue in run.subscribers:
                queue.put_nowait(None)
            run.subscribers.clear()

    def _finish(self, run: Run, status: str, error: str | None = None) -> None:
        run.status = status
        run.error = error
        self._publish(
            run,
            {
                "kind": "status",
                "status": status,
                "name": "",
                "text": error or run.report,
                "scope": "coordinator",
            },
        )
        logger.info("research_finished", run_id=run.id, status=status)

    async def _execute(self, run: Run) -> None:
        run.status = "running"
        self._publish(
            run,
            {
                "kind": "status",
                "status": "running",
                "name": "",
                "text": "Research started",
                "scope": "coordinator",
            },
        )
        logger.info("research_started", run_id=run.id)
        try:
            agent = self._agent_factory()
            cutoff = datetime.now(UTC).date().isoformat()
            config = {
                "configurable": {"thread_id": run.id},
                "recursion_limit": self._settings.recursion_limit,
            }
            request = render_request(run.query, run.exclusions, cutoff)
            async for chunk in agent.astream(
                {"messages": [{"role": "user", "content": request}]},
                config=config,
                stream_mode="updates",
                subgraphs=True,
                version="v2",
            ):
                for event in events_from_chunk(chunk):
                    if event["kind"] == "report":
                        run.report = event["text"]
                    elif event["kind"] == "note" and event["scope"] == "coordinator":
                        if len(event["text"]) > len(run.fallback):
                            run.fallback = event["text"]
                    self._publish(run, event)
        except Exception as exc:
            logger.exception("research_failed", run_id=run.id)
            self._finish(run, "failed", error=str(exc))
            return
        if not run.report.strip() and len(run.fallback) >= 400:
            run.report = run.fallback
        if not run.report.strip():
            self._finish(run, "failed", error="The agent finished without a report.")
            return
        self._finish(run, "completed")
