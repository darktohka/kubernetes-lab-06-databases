# postgres-demo

A small FastAPI service that writes notes to the **PostgreSQL** cluster from
`../postgres/cluster` and reports which server handled each request.

It is deployed by `postgres/demo`: `DB_HOST=postgres-rw`, the CloudNativePG
Service that always points at the current primary. A failed operation is
retried once on a new connection, which is enough to survive a normal failover
without restarting the Pod.

## Endpoints

| method | path | purpose |
|---|---|---|
| GET | `/` | service identity, no database call |
| GET | `/ui` | browser page that writes and lists notes |
| GET | `/healthz` | liveness, text `ok` |
| GET | `/readyz` | `SELECT 1`; `503` when the database is unreachable |
| GET | `/status` | server address, `pg_is_in_recovery()`, version, database |
| POST | `/write` | insert one note; body `{"value": "..."}` is optional |
| GET | `/read?limit=10` | newest notes first (`1..100`) |
| GET | `/loop?seconds=20&interval=1` | one write per tick; success/error timeline |
| GET | `/metrics` | Prometheus counters and `postgres_demo_up` |

## Environment

| variable | default | meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `DB_HOST` | `postgres-rw` | CloudNativePG read-write Service |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `lab6` | database |
| `DB_USER` | `app` | username |
| `DB_PASSWORD` | *(empty)* | password, from `postgres-credentials` |
| `DB_SSLMODE` | `disable` | libpq `sslmode` |

The table `notes` is created lazily on the first request.

## Run locally

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

DB_HOST=localhost DB_NAME=lab6 DB_USER=app DB_PASSWORD=lab6-postgres-password \
  uvicorn app:app --host 0.0.0.0 --port 8080
```

## Image

`lighthouse.tohka.us/example-projects/lab6-postgres-demo:latest`, built for
`linux/amd64` and `linux/arm64` by `../scripts/build-and-push.sh`.
