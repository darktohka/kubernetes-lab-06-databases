"""Lab 6 - PostgreSQL demo API.

The service connects per operation to the CloudNativePG read-write Service and
creates its table lazily. There is no connection at import or startup, so a
missing database only makes /readyz return 503.

A failed transaction is retried once on a *new* connection: that connection
resolves the Service again, which is what makes a controlled failover
invisible to /write and /read.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any

import psycopg

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
SERVICE = "postgres-demo"
DATABASE_KIND = "postgresql"
APP_NAME = "lab06-postgres-demo"

PORT = int(os.getenv("PORT", "8080"))
DB_HOST = os.getenv("DB_HOST", "postgres-rw")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "lab6")
DB_USER = os.getenv("DB_USER", "app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "disable")
CONNECT_TIMEOUT = 3

MODE = "direct"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS notes (
    id bigserial primary key,
    value text not null,
    created_at timestamptz not null default now()
)
"""

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
WRITES = Counter("postgres_demo_writes_total", "Successful PostgreSQL writes")
WRITE_ERRORS = Counter("postgres_demo_write_errors_total", "Failed PostgreSQL writes")
READS = Counter("postgres_demo_reads_total", "Successful PostgreSQL reads")
RECONNECTS = Counter("postgres_demo_reconnects_total", "PostgreSQL connection resets")
UP = Gauge("postgres_demo_up", "1 when the last database operation succeeded")

app = FastAPI(title=SERVICE, version="1.0.0")

_lock = threading.Lock()
_schema_ready = False


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
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
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


def _connect() -> psycopg.Connection:
    # psycopg.connect is lazy too, but this call is made per request only.
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=CONNECT_TIMEOUT,
        application_name=APP_NAME,
        autocommit=False,
    )


def _ensure_schema(conn: psycopg.Connection) -> None:
    """CREATE TABLE once per process; a failure is retried on the next request."""
    global _schema_ready
    if _schema_ready:
        return
    with _lock:
        if _schema_ready:
            return
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
        conn.commit()
        _schema_ready = True
        log("info", "schema_ready", {"table": "notes"})


def _run(operation):
    """Open a connection, run *operation*, commit, close.  Raises on failure."""
    conn = _connect()
    try:
        _ensure_schema(conn)
        result = operation(conn)
        conn.commit()
        return result
    except BaseException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _with_retry(operation, retry: bool = True):
    """Run the operation; on a psycopg error reconnect once after 0.5 s.

    There is no long-lived client object to close for PostgreSQL, but a new
    connection is exactly what re-resolves the read-write Service and
    increments the reconnect counter.
    """
    global _schema_ready
    try:
        return _run(operation)
    except psycopg.Error as exc:
        with _lock:
            _schema_ready = False
        RECONNECTS.inc()
        log("warning", "connection_reset", _error(exc))
        if not retry:
            raise
        time.sleep(0.5)
        return _run(operation)


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def _insert(conn: psycopg.Connection, value: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO notes (value) VALUES (%s) RETURNING id, value, created_at",
            (value,),
        )
        row = cur.fetchone()
    return {
        "id": row[0],
        "value": row[1],
        "created_at": _jsonable(row[2]),
        "served_by": _server_address(conn),
    }


def _server_address(conn: psycopg.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT inet_server_addr()::text")
        row = cur.fetchone()
    return row[0] if row and row[0] else ""


def _find(conn: psycopg.Connection, limit: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, value, created_at FROM notes ORDER BY created_at DESC, id DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    items = [
        {"id": row[0], "value": row[1], "created_at": _jsonable(row[2])}
        for row in rows
    ]
    return {
        "items": items,
        "served_by": _server_address(conn),
        "count": len(items),
    }


def _ping(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


def _status(conn: psycopg.Connection) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT inet_server_addr()::text AS server,
                   pg_is_in_recovery()       AS in_recovery,
                   current_database()        AS database,
                   current_user              AS username,
                   version()                 AS version,
                   pg_postmaster_start_time() AS started_at
            """
        )
        row = cur.fetchone()
    return {
        "service": SERVICE,
        "database": DATABASE_KIND,
        "mode": MODE,
        "server": row[0] or "",
        "in_recovery": row[1],
        "current_database": row[2],
        "current_user": row[3],
        "version": row[4],
        "postmaster_start_time": _jsonable(row[5]),
        "configured_host": DB_HOST,
        "configured_port": DB_PORT,
        "application_name": APP_NAME,
    }


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
@app.get("/")
def root() -> dict:
    return {"service": SERVICE, "database": DATABASE_KIND, "mode": MODE}


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
        document = _with_retry(lambda conn: _insert(conn, value))
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
        result = _with_retry(lambda conn: _find(conn, limit))
    except Exception as exc:
        UP.set(0)
        log("error", "read_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})
    READS.inc()
    UP.set(1)
    return result


@app.get("/loop")
def loop(seconds: float = 20, interval: float = 1):
    """Write once per tick; the timeline shows raw first-attempt failures."""
    seconds = max(1.0, min(60.0, float(seconds)))
    interval = max(0.2, min(5.0, float(interval)))
    deadline = time.monotonic() + seconds

    attempts = ok = errors = 0
    events: list[dict] = []
    while True:
        attempts += 1
        try:
            document = _with_retry(
                lambda conn: _insert(conn, _value_from(None)), retry=False
            )
            ok += 1
            WRITES.inc()
            UP.set(1)
            events.append(
                {"t": datetime.utcnow().isoformat() + "Z", "ok": True, "id": document["id"]}
            )
        except Exception as exc:
            errors += 1
            WRITE_ERRORS.inc()
            UP.set(0)
            events.append(
                {"t": datetime.utcnow().isoformat() + "Z", "ok": False, "error": _error(exc)}
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
