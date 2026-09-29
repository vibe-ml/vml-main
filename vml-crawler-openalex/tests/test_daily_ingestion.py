import os


def test_schedule_cadence():
    os.environ["OPENALEX_INGESTION_CADENCE"] = "0 1 * * *"
    import importlib

    import src.ingestion.defs

    importlib.reload(src.ingestion.defs)
    from src.ingestion.defs import defs

    schedules = list(defs.schedules or [])
    assert len(schedules) >= 2
    for s in schedules:
        assert getattr(s, "cron_schedule", None) == "0 1 * * *"
        assert getattr(s, "execution_timezone", None) == "UTC"
