# mongo-demo

A small FastAPI service that writes notes to the **MongoDB** replica set from
`../mongodb/cluster` and reports which member served each operation.

It connects to the three members of the headless `mongodb-svc` Service and lets
the driver discover the primary. Reads use `secondaryPreferred`; writes always
go to the primary. A failed operation is retried once on a fresh client, which
is enough to survive a normal election without restarting the Pod.

## Endpoints

| method | path | purpose |
|---|---|---|
| GET | `/` | service identity, no database call |
| GET | `/ui` | browser page that writes and lists notes |
| GET | `/healthz` | liveness, text `ok` |
| GET | `/readyz` | `ping`; `503` when the deployment is unreachable |
| GET | `/status` | primary, member list, driver's server types |
| POST | `/write` | insert one note; body `{"value": "..."}` is optional |
| GET | `/read?limit=10` | newest notes first (`1..100`) |
| GET | `/loop?seconds=20&interval=1` | one write per tick; success/error timeline |
| GET | `/metrics` | Prometheus counters and `mongo_demo_up` |

## Environment

| variable | default | meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `MONGO_HOSTS` | `mongodb-0.mongodb-svc:27017,...` | comma-separated seed list |
| `MONGO_DB` | `lab6` | database |
| `MONGO_USER` | `app` | username |
| `MONGO_PASSWORD` | *(empty)* | password, from `mongodb-credentials` |
| `MONGO_AUTH_SOURCE` | `admin` | authentication database |
| `MONGO_READ_PREFERENCE` | `secondaryPreferred` | driver read preference |
| `MONGO_MODE` | `replica-set` | label shown by `/` and `/status` |

Writes use write concern `majority`.

## Run locally

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

MONGO_HOSTS=localhost:27017 MONGO_PASSWORD=lab6-mongodb-password \
  uvicorn app:app --host 0.0.0.0 --port 8080
```

## Image

`lighthouse.tohka.us/example-projects/lab6-mongo-demo:latest`, built for
`linux/amd64` and `linux/arm64` by `../scripts/build-and-push.sh`.
