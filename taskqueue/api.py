"""HTTP API exposing the queue service via FastAPI."""

from fastapi import FastAPI

from taskqueue.config import QueueConfig
from taskqueue.service import QueueService


def create_app(config: QueueConfig | None = None) -> FastAPI:
    app = FastAPI(title="durable-task-queue")
    app.state.config = config or QueueConfig()
    app.state.service = None

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app
