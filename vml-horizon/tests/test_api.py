"""Research runs and the HTTP API."""

import asyncio

from httpx import ASGITransport, AsyncClient
from tests.fakes import ScriptedAgent

from api.app import create_app
from common.settings import Settings
from research.service import ResearchService


def _service() -> ResearchService:
    settings = Settings(model="openai:gpt-4.1-mini", tavily_api_key="tvly-test")
    return ResearchService(
        settings=settings,
        agent_factory=ScriptedAgent,
        ready=lambda: None,
    )


def test_scripted_run_writes_a_report():
    service = _service()

    async def scenario():
        run = service.start("microfluidic diagnostics", "")
        return await service.wait(run.id)

    run = asyncio.run(scenario())
    assert run.status == "completed"
    assert "Organ-on-chip" in run.report
    assert any(event["text"] == "Delegate to evidence-scout" for event in run.events)


def test_api_lists_the_project_and_starts_research():
    app = create_app(_service())

    async def scenario():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            catalog = await client.get("/api/catalog")
            names = [item["repo"] for item in catalog.json()["components"]]
            assert "vml-horizon" in names
            assert "vml-crawler-openalex" in names

            blocked = create_app(ResearchService(settings=Settings()))
            async with AsyncClient(
                transport=ASGITransport(app=blocked), base_url="http://test"
            ) as blocked_client:
                refused = await blocked_client.post(
                    "/api/runs", json={"query": "microfluidic diagnostics"}
                )
            assert refused.status_code == 503

            started = await client.post(
                "/api/runs",
                json={"query": "microfluidic diagnostics", "exclusions": "consumer wearables"},
            )
            assert started.status_code == 202
            run_id = started.json()["id"]
            body = {}
            for _ in range(20):
                body = (await client.get(f"/api/runs/{run_id}")).json()
                if body["status"] == "completed":
                    break
                await asyncio.sleep(0.01)
            assert body["status"] == "completed"
            assert "technology candidate" in body["report"]

    asyncio.run(scenario())
