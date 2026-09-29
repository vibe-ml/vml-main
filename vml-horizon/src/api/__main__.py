"""Run the Horizon web server."""

import uvicorn

from api.app import create_app
from common.settings import get_settings


def main() -> None:
    """Serve the API and the analyst UI."""
    settings = get_settings()
    uvicorn.run(create_app(), host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
