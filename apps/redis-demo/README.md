# redis-demo

A small FastAPI service that writes notes to the **Redis** replication set from
`../redis/cluster` and reports which node served each request.

It asks Sentinel (`redis-s-hl:26379`) for the current master on every
(re)connect. When a command fails because the master changed, the cached client
is dropped and the next request resolves the new master: no restart and no
configuration change. Reads and writes both go to the master by default.

## Endpoints

| method | path | purpose |
|---|---|---|
| GET | `/` | service identity, no database call |
| GET | `/ui` | browser page that writes and lists notes |
| GET | `/healthz` | liveness, text `ok` |
| GET | `/readyz` | `PING` through Sentinel; `503` when unreachable |
| GET | `/status` | role, replicas, Sentinel's view of the master |
| POST | `/write` | write one note; body `{"value": "..."}` is optional |
| GET | `/read?limit=10` | newest notes first (`1..100`) |
| GET | `/cache/{key}` | read a plain TTL-cached value |
| POST | `/cache/{key}` | body `{"value": "..."}`, stored with a TTL |
| GET | `/loop?seconds=20&interval=1` | one write per tick; success/error timeline |
| GET | `/metrics` | Prometheus counters and `redis_demo_up` |

## Environment

| variable | default | meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `REDIS_MODE` | `sentinel` | `sentinel` discovery, anything else = direct node |
| `REDIS_SENTINELS` | `redis-s-hl:26379` | comma-separated Sentinel `host:port` list |
| `REDIS_MASTER_NAME` | `mymaster` | Sentinel master set name |
| `REDIS_PASSWORD` | *(empty)* | password, from `redis-credentials` |
| `REDIS_DB` | `0` | database index |
| `REDIS_KEY_PREFIX` | `lab06:` | prefix for every key |
| `REDIS_CACHE_TTL` | `300` | TTL in seconds |

## Run locally

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

REDIS_SENTINELS=localhost:26379 REDIS_PASSWORD=lab6-redis-password \
  uvicorn app:app --host 0.0.0.0 --port 8080
```

## Image

`lighthouse.tohka.us/example-projects/lab6-redis-demo:latest`, built for
`linux/amd64` and `linux/arm64` by `../scripts/build-and-push.sh`.
