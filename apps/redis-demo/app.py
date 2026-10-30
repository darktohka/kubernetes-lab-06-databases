"""Lab 6 - Redis demo API (Sentinel-aware).

The app never talks to a hostname that is fixed in its configuration: it asks
Sentinel for the current master and caches that client.  When a command fails
because the master disappeared, the cached client is dropped and the next
request asks Sentinel again - which is exactly the behaviour you want to watch
during a failover.

Notes are stored as JSON strings plus a sorted set index, so /read returns the
newest notes without scanning the whole keyspace.  The same service also
demonstrates a plain cache with TTL via /cache/{key}.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import redis
from redis.sentinel import Sentinel

from fastapi import Body, FastAPI
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
)
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE = "redis-demo"
DATABASE_KIND = "redis"

PORT = int(os.getenv("PORT", "8080"))
REDIS_MODE = os.getenv("REDIS_MODE", "sentinel")
REDIS_SENTINELS_RAW = os.getenv("REDIS_SENTINELS", "redis-s-hl:26379")
REDIS_MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "lab06:")
REDIS_CACHE_TTL = int(os.getenv("REDIS_CACHE_TTL", "300"))

SOCKET_TIMEOUT = 3
MAX_NOTES = 100


def _parse_sentinels(raw: str) -> list[tuple[str, int]]:
    sentinels: list[tuple[str, int]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, _, port = entry.partition(":")
        sentinels.append((host, int(port or "26379")))
    if not sentinels:
        sentinels = [("redis-sentinel", 26379)]
    return sentinels


SENTINELS = _parse_sentinels(REDIS_SENTINELS_RAW)
NOTES_KEY = f"{REDIS_KEY_PREFIX}notes"

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
WRITES = Counter("redis_demo_writes_total", "Successful Redis writes")
WRITE_ERRORS = Counter("redis_demo_write_errors_total", "Failed Redis writes")
READS = Counter("redis_demo_reads_total", "Successful Redis reads")
RECONNECTS = Counter("redis_demo_reconnects_total", "Cached Redis clients reset")
UP = Gauge("redis_demo_up", "1 when the last database operation succeeded")

app = FastAPI(title=SERVICE, version="1.0.0")

_lock = threading.Lock()
_client: redis.Redis | None = None
_sentinel: Sentinel | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(level: str, event: str, detail: Any = "") -> None:
    print(
        json.dumps(
            {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": level,
                "service": SERVICE,
                "event": event,
                "detail": detail if isinstance(detail, str) else _jsonable(detail),
            },
            default=str,
        ),
        flush=True,
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _value_from(payload: dict | None) -> str:
    if isinstance(payload, dict):
        value = payload.get("value")
        if value is not None:
            return str(value)
    return f"hello-{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
def _build_sentinel() -> Sentinel:
    return Sentinel(
        SENTINELS,
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_TIMEOUT,
        sentinel_kwargs={
            "socket_timeout": SOCKET_TIMEOUT,
            # The operator-managed Sentinel (and the manual chart, for parity)
            # require the same password as Redis. Without this, Sentinel replies
            # NOAUTH and the app can never discover the master.
            "password": REDIS_PASSWORD or None,
        },
    )


def _get_client() -> redis.Redis:
    """Return the cached master client, asking Sentinel the first time."""
    global _client, _sentinel
    with _lock:
        if _client is None:
            if REDIS_MODE == "sentinel":
                _sentinel = _build_sentinel()
                _client = _sentinel.master_for(
                    REDIS_MASTER_NAME,
                    password=REDIS_PASSWORD or None,
                    db=REDIS_DB,
                    socket_timeout=SOCKET_TIMEOUT,
                    socket_connect_timeout=SOCKET_TIMEOUT,
                    decode_responses=True,
                )
                log(
                    "info",
                    "client_created",
                    {"mode": REDIS_MODE, "sentinels": REDIS_SENTINELS_RAW},
                )
            else:
                # Not the lab default, but useful when you point the demo at a
                # single Redis (e.g. docker run): use the first sentinel entry
                # as the direct host.
                host, port = SENTINELS[0]
                _client = redis.Redis(
                    host=host,
                    port=port,
                    password=REDIS_PASSWORD or None,
                    db=REDIS_DB,
                    socket_timeout=SOCKET_TIMEOUT,
                    socket_connect_timeout=SOCKET_TIMEOUT,
                    decode_responses=True,
                )
                log("info", "client_created", {"mode": REDIS_MODE, "host": host})
        return _client


def _reset_client(reason: str) -> None:
    global _client, _sentinel
    with _lock:
        client, _client = _client, None
        _sentinel = None
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    RECONNECTS.inc()
    log("warning", "client_reset", reason)


def _with_retry(operation, retry: bool = True):
    try:
        return operation(_get_client())
    except redis.RedisError as exc:
        _reset_client(_error(exc))
        if not retry:
            raise
        time.sleep(0.5)
        return operation(_get_client())


def _served_by(client: redis.Redis) -> str:
    try:
        info = client.client_info()
        # raddr is what older Redis versions reported; current versions only
        # expose laddr (the server side of this connection), so fall back to it.
        return str(info.get("raddr") or info.get("laddr") or "")
    except Exception:
        # Some managed Redis variants disable CLIENT INFO; not fatal.
        return ""


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def _write(client: redis.Redis, value: str) -> dict:
    note_id = uuid.uuid4().hex
    created_at = datetime.now(timezone.utc)
    member = f"note:{note_id}"
    payload = json.dumps(
        {"id": note_id, "value": value, "created_at": created_at.isoformat()},
        separators=(",", ":"),
    )
    pipe = client.pipeline(transaction=False)
    pipe.set(f"{REDIS_KEY_PREFIX}{member}", payload, ex=REDIS_CACHE_TTL)
    pipe.zadd(NOTES_KEY, {member: created_at.timestamp()})
    pipe.zremrangebyrank(NOTES_KEY, 0, -(MAX_NOTES + 1))
    pipe.expire(NOTES_KEY, REDIS_CACHE_TTL)
    pipe.execute()
    return {
        "id": note_id,
        "value": value,
        "created_at": created_at.isoformat(),
        "served_by": _served_by(client),
    }


def _read(client: redis.Redis, limit: int) -> dict:
    members = client.zrevrange(NOTES_KEY, 0, limit - 1)
    keys = [f"{REDIS_KEY_PREFIX}{member}" for member in members]
    raw_values = client.mget(keys) if keys else []
    items: list[dict] = []
    for member, raw in zip(members, raw_values):
        if raw is None:
            continue
        try:
            item = json.loads(raw)
        except (TypeError, ValueError):
            continue
        items.append(
            {
                "id": item.get("id", member),
                "value": item.get("value"),
                "created_at": item.get("created_at"),
            }
        )
    return {"items": items, "served_by": _served_by(client), "count": len(items)}


def _ping(client: redis.Redis) -> None:
    client.ping()


def _status(client: redis.Redis) -> dict:
    replication = client.info("replication")
    memory = client.info("memory")
    result = {
        "service": SERVICE,
        "database": DATABASE_KIND,
        "mode": REDIS_MODE,
        "master_name": REDIS_MASTER_NAME,
        "role": replication.get("role"),
        "connected_slaves": replication.get("connected_slaves"),
        "master_host": replication.get("master_host"),
        "master_link_status": replication.get("master_link_status"),
        "used_memory_human": memory.get("used_memory_human"),
        "maxmemory_human": memory.get("maxmemory_human"),
        "maxmemory_policy": memory.get("maxmemory_policy"),
        "key_prefix": REDIS_KEY_PREFIX,
    }
    sentinel = _sentinel
    if sentinel is not None:
        try:
            host, port = sentinel.discover_master(REDIS_MASTER_NAME)
            result["sentinel_master"] = {"host": host, "port": port}
        except Exception as exc:
            result["sentinel_master"] = None
            result["sentinel_error"] = _error(exc)
    return result


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
@app.get("/")
def root() -> dict:
    return {"service": SERVICE, "database": DATABASE_KIND, "mode": REDIS_MODE}


@app.get("/ui")
def ui() -> FileResponse:
    """A tiny browser UI on top of /read and /write."""
    return FileResponse(Path(__file__).with_name("ui.html"), media_type="text/html")


@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


@app.get("/readyz")
def readyz():
    try:
        _with_retry(_ping, retry=False)
        UP.set(1)
        return {"ready": True, "service": SERVICE, "database": DATABASE_KIND}
    except Exception as exc:
        UP.set(0)
        log("warning", "readyz_failed", _error(exc))
        return JSONResponse(
            status_code=503,
            content={"ready": False, "error": _error(exc)},
        )


@app.get("/status")
def status():
    try:
        payload = _with_retry(_status, retry=False)
        UP.set(1)
        return payload
    except Exception as exc:
        UP.set(0)
        log("error", "status_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})


@app.post("/write")
def write(payload: dict | None = Body(default=None)):
    value = _value_from(payload)
    try:
        document = _with_retry(lambda client: _write(client, value))
    except Exception as exc:
        WRITE_ERRORS.inc()
        UP.set(0)
        log("error", "write_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})
    WRITES.inc()
    UP.set(1)
    log("info", "write_ok", {"id": document["id"], "served_by": document["served_by"]})
    return JSONResponse(status_code=201, content=document)


@app.get("/read")
def read(limit: int = 10):
    limit = max(1, min(100, limit))
    try:
        result = _with_retry(lambda client: _read(client, limit))
    except Exception as exc:
        UP.set(0)
        log("error", "read_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})
    READS.inc()
    UP.set(1)
    return result


# ---------------------------------------------------------------------------
# Cache endpoints: a second, simpler use of the same Redis.
# ---------------------------------------------------------------------------
@app.get("/cache/{key}")
def cache_get(key: str):
    try:
        client = _get_client()
        value = client.get(f"{REDIS_KEY_PREFIX}cache:{key}")
        ttl = client.ttl(f"{REDIS_KEY_PREFIX}cache:{key}")
        UP.set(1)
        return {"key": key, "value": value, "hit": value is not None, "ttl": ttl}
    except Exception as exc:
        UP.set(0)
        if isinstance(exc, redis.RedisError):
            _reset_client(_error(exc))
        log("error", "cache_get_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})


@app.post("/cache/{key}")
def cache_set(key: str, payload: dict | None = Body(default=None)):
    if not isinstance(payload, dict) or payload.get("value") is None:
        return JSONResponse(
            status_code=422, content={"error": "body must be {\"value\": ...}"}
        )
    value = str(payload["value"])
    cache_key = f"{REDIS_KEY_PREFIX}cache:{key}"
    try:
        client = _get_client()
        client.set(cache_key, value, ex=REDIS_CACHE_TTL)
        ttl = client.ttl(cache_key)
        UP.set(1)
        log("info", "cache_set_ok", {"key": key})
        return {"key": key, "value": value, "ttl": ttl, "hit": False}
    except Exception as exc:
        UP.set(0)
        if isinstance(exc, redis.RedisError):
            _reset_client(_error(exc))
        log("error", "cache_set_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})


@app.get("/loop")
def loop(seconds: float = 20, interval: float = 1):
    """Write once per tick; no retry, so the failover is visible in the events."""
    seconds = max(1.0, min(60.0, float(seconds)))
    interval = max(0.2, min(5.0, float(interval)))
    deadline = time.monotonic() + seconds

    attempts = ok = errors = 0
    events: list[dict] = []
    while True:
        attempts += 1
        try:
            document = _with_retry(
                lambda client: _write(client, _value_from(None)), retry=False
            )
            ok += 1
            WRITES.inc()
            UP.set(1)
            events.append(
                {"t": datetime.now(timezone.utc).isoformat(), "ok": True, "id": document["id"]}
            )
        except Exception as exc:
            errors += 1
            WRITE_ERRORS.inc()
            UP.set(0)
            events.append(
                {
                    "t": datetime.now(timezone.utc).isoformat(),
                    "ok": False,
                    "error": _error(exc),
                }
            )
        if len(events) > 300:
            del events[:-300]
        if time.monotonic() + interval > deadline:
            break
        time.sleep(interval)

    return {
        "attempts": attempts,
        "ok": ok,
        "errors": errors,
        "events": events,
        "recovered": errors > 0 and bool(events) and events[-1]["ok"],
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
