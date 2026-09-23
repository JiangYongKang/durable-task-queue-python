"""Local entrypoint.

Run:
    uv run python main.py
    uv run uvicorn main:app --reload

All settings can be tweaked via CLI flags (see ``--help``) or environment
variables (``TASKQUEUE_DB_PATH``, ``TASKQUEUE_VISIBILITY_TIMEOUT``,
``TASKQUEUE_MAX_ATTEMPTS``, ``TASKQUEUE_RETRY_BACKOFF_BASE``,
``TASKQUEUE_MAX_PAYLOAD_BYTES``, ``TASKQUEUE_REAPER_INTERVAL``).
"""

from __future__ import annotations

import argparse
import logging

from taskqueue.config import config_from_env
from taskqueue.http_api import create_app


def _build_config():  # type: ignore[no-untyped-def]
    parser = argparse.ArgumentParser(description="Durable local task queue")
    parser.add_argument("--db", dest="db_path", default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--visibility-timeout", type=float, default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--backoff-base", type=float, default=None)
    parser.add_argument("--max-payload-bytes", type=int, default=None)
    parser.add_argument("--reaper-interval", type=float, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = config_from_env(
        db_path=args.db_path,
        visibility_timeout=args.visibility_timeout,
        max_attempts=args.max_attempts,
        retry_backoff_base=args.backoff_base,
        max_payload_bytes=args.max_payload_bytes,
        reaper_interval=args.reaper_interval,
    )
    return config, args.host, args.port


_config, _host, _port = _build_config()
app = create_app(_config)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=_host, port=_port)
