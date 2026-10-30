# mongodb-manual

A hand-built three-member MongoDB replica set. This is the "you write the
orchestration" half of use case 1.

## What it creates

| object | name | purpose |
|---|---|---|
| Secret | `mongodb-credentials` | app, admin and exporter passwords plus the replica set keyfile |
| ConfigMap | `mongodb-bootstrap` | the bootstrap script run by the sidecar |
| Service | `mongodb-svc` | headless, `publishNotReadyAddresses: true`, per-Pod DNS |
| StatefulSet | `mongodb` | 3 x `mongod`, a keyfile init container, bootstrap and exporter sidecars |
| PVCs | `data-mongodb-0..2` | 1Gi each, from the `data` volumeClaimTemplate |

The release name must be `mongodb`. All names are derived from
`.Release.Name`; no template hard-codes a namespace.

## Install

```sh
helm install mongodb use-cases/1-mongodb/mongodb-manual -n "$NS"
kubectl get pods -n "$NS" -l app.kubernetes.io/instance=mongodb -w
```

The first start takes 30-90 seconds: all three `mongod` processes must be
running before `mongodb-0` can initiate the replica set and create users.

## Values

| key | default | meaning |
|---|---|---|
| `replicaCount` | `3` | replica set members; use an odd number |
| `image.repository` / `image.tag` | `mongo` / `8.0.34` | MongoDB image (multi-arch) |
| `exporter.image.*` | `percona/mongodb_exporter:0.49.0` | metrics sidecar |
| `auth.appPassword` | `lab05-app-password` | app user password, also used by `mongo-demo` |
| `auth.adminPassword` | `lab05-admin-password` | `root` password |
| `auth.exporterPassword` | `lab05-exporter-password` | exporter user password |
| `auth.keyfile` | 64-character lab string | internal replica set authentication |
| `persistence.size` | `1Gi` | per-member volume |
| `persistence.storageClass` | `""` | empty = cluster default, e.g. `local-path` |
| `resources.mongod` | `150m/256Mi` -> `600m/768Mi` | fits three members in the quota |
| `resources.bootstrap` | `20m/64Mi` -> `100m/256Mi` | sidecar (`mongosh` is Node.js and needs the memory) |
| `antiAffinity.enabled` | `true` | soft hostname spread |

## How it works

The StatefulSet gives stable names and volumes. It does **not** run
`rs.initiate()`, create users or seed a keyfile. Those are done by:

* `keyfile-permissions` (initContainer): copies the Secret keyfile to an
  emptyDir and sets mode `0400`, owner `999:999`. `mongod` refuses to start
  with a group/world-readable keyfile.
* `bootstrap` (sidecar on `mongodb-0` only, idles elsewhere): waits for
  localhost, initiates the replica set, waits for primary, creates `root` via
  the localhost exception, then `app` and `exporter` authenticated as root.
  It is idempotent and safe across restarts.
* `exporter` (sidecar on every member): Prometheus metrics on port 9216.

The bootstrap cannot be a Job because the localhost exception only applies to
connections from the Pod's own loopback interface.

## Verify

```sh
kubectl get pods,pvc,svc -n "$NS"
kubectl logs -n "$NS" mongodb-0 -c bootstrap
kubectl exec -n "$NS" mongodb-0 -c mongod -- \
  mongosh "mongodb://root:$ADMIN_PW@127.0.0.1:27017/admin?directConnection=true" \
  --eval 'rs.status().members.map(m => m.name + " " + m.stateStr)'
```

See the use-case README for the election and persistence exercises.

## Uninstall

```sh
helm uninstall mongodb -n "$NS"
kubectl delete pvc -n "$NS" -l app.kubernetes.io/part-of=lab05
```

The PVCs are intentionally not removed by `helm uninstall`.
