"""Shared fixtures: an isolated on-disk queue per test."""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from taskqueue.config import QueueConfig
from taskqueue.http_api import create_app
from taskqueue.queue import TaskQueue


@pytest.fixture
def tmp_db_path() -> str:
    d = tempfile.mkdtemp(prefix="dtq-test-")
    return os.path.join(d, "queue.db")


@pytest.fixture
def config(tmp_db_path: str) -> QueueConfig:
    return QueueConfig(
        db_path=tmp_db_path,
        visibility_timeout=0.3,
        max_attempts=3,
        retry_backoff_base=0.01,
        max_payload_bytes=1024,
        reaper_interval=3600,
    )


@pytest.fixture
def queue(config: QueueConfig):
    q = TaskQueue(config)
    yield q
    q.close()


@pytest.fixture
def client(config: QueueConfig):
    app = create_app(config, start_reaper=False)
    with TestClient(app) as c:
        yield c
    app.state.queue.close()
