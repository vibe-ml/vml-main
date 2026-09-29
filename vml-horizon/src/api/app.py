"""HTTP API and static analyst UI."""

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from common.catalog import COMPONENTS
from common.settings import get_settings
from research.service import ResearchService, Run

STATIC_DIR = Path(__file__).resolve().parent.parent / "web" / "static"


class RunRequest(BaseModel):
    """An analyst's open query."""

    query: str = Field(min_length=3, max_length=4000)
    exclusions: str = Field(default="", max_length=2000)


def _public_run(run: Run) -> dict[str, object]:
    return {
        "id": run.id,
        "query": run.query,
        "exclusions": run.exclusions,
        "status": run.status,
        "created_at": run.created_at,
        "error": run.error,
        "report": run.report,
        "events": run.events,
    }


def create_app(service: ResearchService | None = None) -> FastAPI:
    """Build the API and mount the analyst UI."""
    app = FastAPI(title="Horizon")
    app.state.research = service or ResearchService(settings=get_settings())

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/catalog")
    def catalog() -> dict[str, object]:
        return {"components": list(COMPONENTS)}

    @app.get("/api/status")
    def status() -> dict[str, object]:
        return app.state.research.status()

    @app.get("/api/runs")
    def list_runs() -> dict[str, object]:
        return {"runs": [_public_run(run) for run in app.state.research.list_runs()]}

    @app.post("/api/runs", status_code=202)
    async def start_run(body: RunRequest) -> dict[str, object]:
        message = app.state.research.configuration_error()
        if message:
            raise HTTPException(status_code=503, detail=message)
        run = app.state.research.start(body.query, body.exclusions)
        return _public_run(run)

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, object]:
        run = app.state.research.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Unknown research run.")
        return _public_run(run)

    @app.get("/api/runs/{run_id}/events")
    async def run_events(run_id: str) -> StreamingResponse:
        run = app.state.research.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Unknown research run.")

        async def generate():
            async for event in app.state.research.iter_events(run):
                if event is None:
                    yield ": keepalive\n\n"
                    continue
                payload = json.dumps(event, ensure_ascii=False)
                yield f"data: {payload}\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")

    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="ui")
    return app
