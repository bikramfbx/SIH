"""Vercel serverless entry point for the PyroSphere FastAPI application.

Vercel builds every top-level ``.py`` file under ``api/`` as a function;
helper modules live in ``api/app/`` (bundled via ``includeFiles``).
The aborted body is handled with Mangum; local Docker/dev keeps using
``uvicorn api.app.main:app`` directly.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.main import app  # noqa: E402

try:
    from mangum import Mangum

    handler = Mangum(app, lifespan="off")
except ImportError:  # not part of the Docker/runtime path; dev uses uvicorn
    handler = None


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)