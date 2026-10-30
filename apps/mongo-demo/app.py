"""Lab 6 - MongoDB demo API.

A deliberately small service that talks to a MongoDB replica set through the
official driver.  It is built for *observing* topology changes:

* no connection is opened at import time or on startup, so the Pod starts even
  when the database does not exist yet;
* every database error is turned into an HTTP 503, never a crash;
* a failed connection closes the cached client, so the next request resolves
  the replica-set topology again (and a new primary can be discovered);
* /write and /read retry once after 0.5 s, which hides a normal election;
* /loop records the *raw* first attempt of every tick, which makes the
  election visible as a timeline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

from fastapi import Body, FastAPI
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
)
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from pymongo import MongoClient, ReadPreference
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    NotPrimaryError,
    PyMongoError,
    ServerSelectionTimeoutError,
)
from pymongo.write_concern import WriteConcern

# ---------------------------------------------------------------------------
# Configuration - read once, never log the password.
# ---------------------------------------------------------------------------
SERVICE = "mongo-demo"
DATABASE_KIND = "mongodb"

PORT = int(os.getenv("PORT", "8080"))
MONGO_HOSTS = os.getenv(
    "MONGO_HOSTS",
    "mongodb-0.mongodb-svc:27017,mongodb-1.mongodb-svc:27017,mongodb-2.mongodb-svc:27017",
)
MONGO_DB = os.getenv("MONGO_DB", "lab6")
MONGO_USER = os.getenv("MONGO_USER", "app")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "admin")
MONGO_READ_PREFERENCE = os.getenv("MONGO_READ_PREFERENCE", "secondaryPreferred")
MODE = os.getenv("MONGO_MODE", "replica-set")
APP_NAME = "lab06-mongo-demo"
COLLECTION = "notes"


def _read_preference_name() -> str:
    """Validate the env value against the driver's canonical mode names."""
    if MONGO_READ_PREFERENCE in {
        "primary",
        "primaryPreferred",
        "secondary",
        "secondaryPreferred",
        "nearest",
    }:
        return MONGO_READ_PREFERENCE
    log("warning", "read_preference_fallback", MONGO_READ_PREFERENCE)
    return "secondaryPreferred"


def _read_preference_mode() -> ReadPreference:
    """MongoClient wants the name, get_database() wants the mode object."""
    return {
        "primary": ReadPreference.PRIMARY,
        "primaryPreferred": ReadPreference.PRIMARY_PREFERRED,
        "secondary": ReadPreference.SECONDARY,
        "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
        "nearest": ReadPreference.NEAREST,
    }[_read_preference_name()]


def _build_uri() -> str:
    user = quote_plus(MONGO_USER)
    password = quote_plus(MONGO_PASSWORD)
    hosts = ",".join(h.strip() for h in MONGO_HOSTS.split(",") if h.strip())
    return (
        f"mongodb://{user}:{password}@{hosts}/"
        f"?authSource={quote_plus(MONGO_AUTH_SOURCE)}"
        "&retryWrites=true&retryReads=true"
        "&serverSelectionTimeoutMS=3000&connectTimeoutMS=3000"
    )


MONGO_URI = _build_uri()

# ---------------------------------------------------------------------------
# Prometheus metrics.  Names are part of the lab: compare all three apps.
# ---------------------------------------------------------------------------
WRITES = Counter("mongo_demo_writes_total", "Successful MongoDB writes")
WRITE_ERRORS = Counter("mongo_demo_write_errors_total", "Failed MongoDB writes")
READS = Counter("mongo_demo_reads_total", "Successful MongoDB reads")
RECONNECTS = Counter("mongo_demo_reconnects_total", "Cached MongoDB clients reset")
UP = Gauge("mongo_demo_up", "1 when the last database operation succeeded")

# Errors that mean "the topology may have changed / the connection is gone".
# Only these reset the client; a validation error from the server does not.
CONNECTION_ERRORS = (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    NotPrimaryError,
    ServerSelectionTimeoutError,
)

app = FastAPI(title=SERVICE, version="1.0.0")

_lock = threading.Lock()
_client: MongoClient | None = None


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def log(level: str, event: str, detail: Any = "") -> None:
    """One JSON object per line, so `kubectl logs` is easy to grep."""
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


def _address_str(address: Any) -> str:
    """client.address is a (host, port) tuple; render it as 'host:port'."""
    if isinstance(address, (tuple, list)) and len(address) >= 2:
        return f"{address[0]}:{address[1]}"
    return str(address) if address else ""


def _served_by(client: MongoClient) -> str:
    return _address_str(getattr(client, "address", None))


def _value_from(payload: dict | None) -> str:
    if isinstance(payload, dict):
        value = payload.get("value")
        if value is not None:
            return str(value)
    return f"hello-{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"


# ---------------------------------------------------------------------------
# Cached client.  MongoClient is lazy: constructing it does not connect.
# ---------------------------------------------------------------------------
def _get_client() -> MongoClient:
    global _client
    with _lock:
        if _client is None:
            _client = MongoClient(
                MONGO_URI,
                appname=APP_NAME,
                readPreference=_read_preference_name(),
                serverSelectionTimeoutMS=3000,
                connectTimeoutMS=3000,
                retryWrites=True,
                retryReads=True,
            )
            log("info", "client_created", {"hosts": MONGO_HOSTS, "read_preference": MONGO_READ_PREFERENCE})
        return _client


def _reset_client(reason: str) -> None:
    """Drop the cached client so the next call re-resolves the topology."""
    global _client
    with _lock:
        client, _client = _client, None
    if client is not None:
        try:
            client.close()
        except Exception:  # closing must never surface as a request error
            pass
    RECONNECTS.inc()
    log("warning", "client_reset", reason)


def _with_retry(operation, retry: bool = True):
    """Run *operation(client)*, resetting the client on connection errors.

    /write and /read get one retry (0.5 s backoff) so a primary election is
    mostly invisible.  /loop asks for no retry because it wants the raw event.
    """
    try:
        return operation(_get_client())
    except CONNECTION_ERRORS as exc:
        _reset_client(_error(exc))
        if not retry:
            raise
        time.sleep(0.5)
        return operation(_get_client())


# ---------------------------------------------------------------------------
# Database operations (each raises on failure)
# ---------------------------------------------------------------------------
def _insert(client: MongoClient, value: str) -> dict:
    created_at = datetime.utcnow()
    collection = client[MONGO_DB].get_collection(
        COLLECTION, write_concern=WriteConcern(w="majority")
    )
    result = collection.insert_one({"value": value, "created_at": created_at})
    return {
        "id": str(result.inserted_id),
        "value": value,
        "created_at": created_at.isoformat(),
        "served_by": _served_by(client),
    }


def _find(client: MongoClient, limit: int) -> dict:
    collection = client.get_database(
        MONGO_DB, read_preference=_read_preference_mode()
    ).get_collection(COLLECTION)
    cursor = (
        collection.find({}, {"value": 1, "created_at": 1})
        .sort("created_at", -1)
        .limit(limit)
    )
    items = [
        {
            "id": str(doc.get("_id", "")),
            "value": doc.get("value"),
            "created_at": _jsonable(doc.get("created_at")),
        }
        for doc in cursor
    ]
    return {"items": items, "served_by": _served_by(client), "count": len(items)}


def _ping(client: MongoClient) -> None:
    client.admin.command("ping")


def _topology_servers(client: MongoClient) -> list[dict]:
    """Driver view of the replica set, across pymongo minor API changes."""
    description = client.topology_description
    descriptions: list = []
    getter = getattr(description, "server_descriptions", None)
    if callable(getter):
        descriptions = list(getter().values())
    elif isinstance(getter, dict):
        descriptions = list(getter.values())
    elif hasattr(description, "servers"):
        servers = description.servers
        descriptions = list(servers.values() if isinstance(servers, dict) else servers)
    return [
        {
            "address": _address_str(server.address),
            "server_type": _SERVER_TYPES.get(
                int(server.server_type), str(server.server_type)
            ),
        }
        for server in descriptions
    ]


# pymongo.server_type.SERVER_TYPE is not callable; keep a small stable mapping.
_SERVER_TYPES = {
    0: "Unknown",
    1: "Mongos",
    2: "RSPrimary",
    3: "RSSecondary",
    4: "RSArbiter",
    5: "RSOther",
    6: "RSGhost",
    7: "Standalone",
    8: "LoadBalancer",
}


def _status() -> dict:
    client = _get_client()
    hello = client.admin.command("hello")
    servers = _topology_servers(client)
    result = {
        "service": SERVICE,
        "database": DATABASE_KIND,
        "mode": MODE,
        "primary": hello.get("primary"),
        "me": hello.get("me"),
        "isWritablePrimary": hello.get("isWritablePrimary", hello.get("ismaster")),
        "hosts": hello.get("hosts", []),
        "setName": hello.get("setName"),
        "read_preference": MONGO_READ_PREFERENCE,
        "servers": servers,
    }
    try:
        repl_status = client.admin.command("replSetGetStatus")
        result["members"] = [
            {
                "name": member.get("name"),
                "stateStr": member.get("stateStr"),
                "health": member.get("health"),
            }
            for member in repl_status.get("members", [])
        ]
    except PyMongoError as exc:
        # A user without clusterMonitor rights cannot read this; not an error
        # for the demo, so the key is simply omitted.
        log("info", "replset_status_skipped", _error(exc))
    return result


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
        payload = _status()
        UP.set(1)
        return payload
    except Exception as exc:
        UP.set(0)
        if isinstance(exc, CONNECTION_ERRORS):
            _reset_client(_error(exc))
        log("error", "status_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})


@app.post("/write")
def write(payload: dict | None = Body(default=None)):
    value = _value_from(payload)
    try:
        document = _with_retry(lambda client: _insert(client, value))
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
        result = _with_retry(lambda client: _find(client, limit))
    except Exception as exc:
        UP.set(0)
        log("error", "read_failed", _error(exc))
        return JSONResponse(status_code=503, content={"error": _error(exc)})
    READS.inc()
    UP.set(1)
    return result


@app.get("/loop")
def loop(seconds: float = 20, interval: float = 1):
    """Write once per tick and return the raw success/error timeline.

    The first attempt of every tick is *not* retried: during a failover the
    timeline should show the failures, not hide them.
    """
    seconds = max(1.0, min(60.0, float(seconds)))
    interval = max(0.2, min(5.0, float(interval)))
    deadline = time.monotonic() + seconds

    attempts = ok = errors = 0
    events: list[dict] = []
    while True:
        attempts += 1
        try:
            document = _with_retry(
                lambda client: _insert(client, _value_from(None)), retry=False
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
            # _with_retry(..., retry=False) already reset the client on
            # connection errors; resetting twice would double-count.
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


if __name__ == "__main__":  # local convenience; the container uses uvicorn
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
