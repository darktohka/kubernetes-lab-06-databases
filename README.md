# Microservices and Kubernetes: Lab 6 - Databases with Operators

Lab 5 scaled a stateless service. Every Pod was disposable: kill one and its
replacement was identical.

Databases are different. A database member has an identity, a disk that must
follow that identity, a startup order, and a replication topology in which
exactly one member accepts writes. Kubernetes gives you the building blocks
for that (`StatefulSet`, headless `Service`, `PersistentVolumeClaim`) but not
the database logic: who is primary, when to promote a replica, how to rejoin a
node that was down.

An **operator** supplies that logic. This lab creates three databases the
modern way - by declaring a custom resource and letting the operator do the
rest:

| folder | database | operator | custom resource |
|---|---|---|---|
| [`postgres/`](postgres) | PostgreSQL | CloudNativePG | `Cluster` |
| [`mongodb/`](mongodb) | MongoDB | MongoDB Community Operator | `MongoDBCommunity` |
| [`redis/`](redis) | Redis | OT Redis Operator | `RedisReplication` |

Each folder has the same two parts:

```text
<database>/
├── cluster/    the database cluster (custom resource + credentials)
└── demo/       Deployment + Service + Ingress for the demo application
```

> **Environment.** Everything targets the shared `sapi.click` Kubernetes
> cluster. The three operators are already installed cluster-wide - you only
> create the custom resources.

---

## 1. Why databases need an operator

### 1.1 What Kubernetes gives you

| object | what it provides |
|---|---|
| `StatefulSet` | stable Pod names (`postgres-0`, `mongodb-1`, ...), stable DNS, ordered rollout |
| `PersistentVolumeClaim` | durable storage per member |
| `Service` | a stable address for clients |

### 1.2 What it does not give you

- **Replication** - members must actually copy data from one another.
- **Leader election** - the cluster must agree on who is primary.
- **Failover** - when the primary dies, a replica must be promoted, and the
  old primary must rejoin as a replica when it comes back.
- **Role-aware Services** - clients need an address that always points at the
  current primary for writes, and at replicas for reads.
- **Upgrades and backups** - changing the database version without losing data.

You could write all of that yourself with sidecars and scripts. It is a lot of
work, and the edge cases are where data disappears. An operator is the same
idea, implemented once, tested by many people.

### 1.3 The operator pattern

An operator is a controller plus one or more **Custom Resource Definitions**
(CRDs). You describe the desired state:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: postgres
spec:
  instances: 3
```

The controller watches this object and continuously reconciles the real world
toward it: it creates Pods and PVCs, configures replication, watches health,
and reacts to failures. The same YAML works on your laptop cluster and in
production; only the storage class and the number of instances usually change.

```text
     you                 API server               operator
      │  kubectl apply        │                       │
      ├──────────────────────▶│  stores the CR        │
      │                       │◀──────────────────────┤ watches
      │                       │                       │
      │                       │  creates Pods, PVCs,  │
      │                       │  Services, Secrets    │
      │                       │◀──────────────────────┤
      │                       │                       │
      │  kubectl get pods     │  status is written    │
      ├──────────────────────▶│  back to the CR       │
```

Check that the operators are installed:

```sh
kubectl get pods -n cnpg-system
kubectl get pods -n mongodb-operator
kubectl get pods -n redis-operator

kubectl get crd | grep -E 'cnpg|mongodbcommunity|redis'
```

Expected versions on the shared cluster:

```text
CloudNativePG            1.30.1
MongoDB Community        0.13.0
OT Redis Operator        v0.27.0
```

### 1.4 The demo application

`apps/{postgres,mongo,redis}-demo` contain one small FastAPI service per
database. All three expose the **same API**, so the same `curl` commands and
the same browser page work everywhere:

| method | path | purpose |
|---|---|---|
| `GET` | `/` | service identity (no database call) |
| `GET` | `/ui` | a small HTML/JavaScript page to write and list objects |
| `GET` | `/healthz` | liveness, no database call |
| `GET` | `/readyz` | checks the database; `503` while it is unreachable |
| `POST` | `/write` | body `{"value": "..."}` (empty body also works) |
| `GET` | `/read?limit=10` | newest objects first (`1..100`) |
| `GET` | `/loop?seconds=20&interval=1` | one write per second, returns the success/error timeline |
| `GET` | `/metrics` | Prometheus counters |

The images are prebuilt for `linux/amd64` and `linux/arm64`:

```text
lighthouse.tohka.us/example-projects/lab6-postgres-demo:latest
lighthouse.tohka.us/example-projects/lab6-mongo-demo:latest
lighthouse.tohka.us/example-projects/lab6-redis-demo:latest
```

They are already published. If you change the apps and want to publish your
own copy:

```sh
./scripts/build-and-push.sh
# or: ./scripts/build-and-push.sh lighthouse.tohka.us/<your-namespace> latest
```

The cluster has amd64 and arm64 workers, so the build must be multi-arch - a
single-architecture image fails with `exec format error` on the other nodes.

---

## 2. PostgreSQL with CloudNativePG

### 2.1 The cluster

[`postgres/cluster/10-postgres-cluster.yaml`](postgres/cluster/10-postgres-cluster.yaml)
contains a Secret and one `Cluster`:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: postgres
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:18.6
  storage:
    size: 512Mi
  bootstrap:
    initdb:
      database: lab6
      owner: app
      secret:
        name: postgres-credentials
```

What the operator does with it:

- creates `postgres-1`, `postgres-2`, `postgres-3`, one PVC each;
- runs `initdb` once, creates the `app` role and the `lab6` database from the
  Secret;
- configures streaming replication from the primary to the other two;
- creates three Services and keeps their selectors up to date.

Apply it:

```sh
export NS=<your-username>
kubectl config set-context --current --namespace="$NS"

kubectl apply -f postgres/cluster/
kubectl get cluster postgres --watch
```

Wait until `READY` is `3`:

```text
NAME       AGE   INSTANCES   READY   STATUS                     PRIMARY
postgres   90s   3           3       Cluster in healthy state   postgres-1
```

Now look at what the operator created:

```sh
kubectl get pods -l cnpg.io/cluster=postgres
kubectl get pvc
kubectl get svc | grep postgres
```

```text
postgres-1   1/1   Running   # primary
postgres-2   1/1   Running   # replica
postgres-3   1/1   Running   # replica

postgres-rw   ClusterIP   ...   5432/TCP   # always the current primary
postgres-ro   ClusterIP   ...   5432/TCP   # replicas only
postgres-r    ClusterIP   ...   5432/TCP   # any instance
```

Use `postgres-rw` for writes. A Service that round-robins over all three Pods
would send every third `INSERT` to a read-only replica:

```text
ERROR: cannot execute INSERT in a read-only transaction
SQLSTATE: 25006
```

That is exactly the mistake the role-aware Services prevent.

Inspect the primary directly:

```sh
kubectl exec -it postgres-1 -- psql -U postgres -c \
  'select pg_is_in_recovery(), inet_server_addr();'
```

### 2.2 The demo

[`postgres/demo/`](postgres/demo) contains:

- `10-deployment.yaml` - the demo app, configured with `DB_HOST=postgres-rw`;
- `20-service.yaml` - a ClusterIP Service on port 80;
- `30-ingress.yaml` - publishes it at `danipostgres.sapi.click`.

The password is read from the same Secret the Cluster used:

```yaml
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: postgres-credentials
      key: password
```

Change the Ingress host to your own namespace before applying:

```sh
sed -i 's/danipostgres/<your-username>postgres/' postgres/demo/30-ingress.yaml
kubectl apply -f postgres/demo/
kubectl rollout status deployment/postgres-demo
```

Open the UI through the Service:

```sh
kubectl port-forward svc/postgres-demo 8080:80
```

<http://localhost:8080/ui> writes and lists objects. The same operations from
the command line:

```sh
curl -s -X POST localhost:8080/write \
  -H 'Content-Type: application/json' -d '{"value":"hello postgres"}'

curl -s 'localhost:8080/read?limit=5'
curl -s localhost:8080/status
```

`served_by` is the address of the PostgreSQL instance that handled the
request. With `DB_HOST=postgres-rw` it is always the primary.

The Ingress should answer at `https://<your-username>postgres.sapi.click`.
DNS and the certificate are automatic; a new host can take a minute.

### 2.3 Experiment: kill the primary

Start a write loop and watch what the app sees:

```sh
kubectl port-forward svc/postgres-demo 8080:80 &
curl -s 'localhost:8080/loop?seconds=40&interval=1' | jq
```

In a second terminal, delete the primary Pod:

```sh
kubectl get cluster postgres          # PRIMARY column
kubectl delete pod postgres-1         # use the Pod in the PRIMARY column
```

CloudNativePG notices that the primary is gone, promotes a replica, and
repoints `postgres-rw` at it. The old Pod comes back as a replica. The
`/loop` output shows one or two `ok:false` events while the promotion happens,
then `ok:true` again. The app is never restarted: only its database connection
is re-established.

```sh
kubectl get cluster postgres --watch
kubectl get pods -l cnpg.io/cluster=postgres
curl -s localhost:8080/status | jq
```

The `server` field changes, `in_recovery` stays `false`, and
`postgres_demo_reconnects_total` on `/metrics` increases.

---

## 3. MongoDB with the MongoDB Community Operator

### 3.1 The cluster

The MongoDB operator needs a little identity in your namespace
(`mongodb-database` ServiceAccount + Role + RoleBinding), because its own
installation only creates that identity in its own namespace. The manifest in
[`mongodb/cluster/`](mongodb/cluster/10-mongodb-cluster.yaml) includes it.

The custom resource is the whole database:

```yaml
apiVersion: mongodbcommunity.mongodb.com/v1
kind: MongoDBCommunity
metadata:
  name: mongodb
spec:
  members: 3
  type: ReplicaSet
  version: "8.0.34"
  security:
    authentication:
      modes: ["SCRAM"]
  users:
    - name: app
      db: admin
      passwordSecretRef:
        name: mongodb-credentials
        key: app-password
      roles:
        - name: readWrite
          db: lab6
        - name: clusterMonitor
          db: admin
```

The operator creates:

```text
mongodb-0, mongodb-1, mongodb-2     the replica set members
mongodb-svc                         a headless Service (clusterIP: None)
mongodb-<name>-scram-credentials    the generated SCRAM Secrets
```

Apply it:

```sh
kubectl apply -f mongodb/cluster/
kubectl get mdbc mongodb --watch
```

Phase `Running` means the replica set has a primary:

```text
NAME      PHASE     VERSION
mongodb   Running   8.0.34
```

```sh
kubectl get pods -l app=mongodb
kubectl get svc mongodb-svc
kubectl get mdbc mongodb -o jsonpath='{.status.mongoURI}{"\n"}'
```

`mongodb-svc` is headless, so it has no load-balanced ClusterIP. Clients use
the individual member names instead:

```text
mongodb-0.mongodb-svc:27017
mongodb-1.mongodb-svc:27017
mongodb-2.mongodb-svc:27017
```

That is how the driver discovers the primary. Applications never configure
"the primary's address": they configure a seed list and the driver resolves
the current topology.

### 3.2 The demo

[`mongodb/demo/`](mongodb/demo) is the same Deployment/Service/Ingress shape
as PostgreSQL, with the three members as `MONGO_HOSTS` and

```yaml
- name: MONGO_READ_PREFERENCE
  value: secondaryPreferred
```

Reads may go to a secondary; writes always go to the primary. Reads from a
secondary can be a little stale, which is normal for a replica set.

```sh
sed -i 's/danimongo/<your-username>mongo/' mongodb/demo/30-ingress.yaml
kubectl apply -f mongodb/demo/
kubectl rollout status deployment/mongodb-demo

kubectl port-forward svc/mongodb-demo 8081:80
```

<http://localhost:8081/ui> - same page, same `/write` and `/read` API.

```sh
curl -s -X POST localhost:8081/write \
  -H 'Content-Type: application/json' -d '{"value":"hello mongo"}'

curl -s 'localhost:8081/read?limit=5'
curl -s localhost:8081/status | jq
```

`/status` shows the primary, the member list, and each member's state
(`RSPrimary` / `RSSecondary`). That output is the fastest way to see that the
replica set is healthy.

### 3.3 Experiment: step down the primary

Run a loop and delete the Pod that `/status` reports as primary:

```sh
kubectl port-forward svc/mongodb-demo 8081:80 &
curl -s 'localhost:8081/loop?seconds=40&interval=1' | jq

kubectl get pods -l app=mongodb -o wide
kubectl delete pod mongodb-<n>       # the primary
```

The driver notices that its primary is gone, the remaining members elect a new
one, and the client reconnects. Depending on how fast the election is, the
loop may show a few `NotPrimaryError` or `ServerSelectionTimeoutError` events,
or the single retry in `/write` may hide them completely. Either way `/status`
ends up reporting a different `RSPrimary`. MongoDB writes are acknowledged by
the primary only, so no write that returned success is lost in a normal
election.

---

## 4. Redis with the OT Redis Operator

### 4.1 The cluster

[`redis/cluster/`](redis/cluster/10-redis-cluster.yaml) creates a Secret and a
`RedisReplication`:

```yaml
apiVersion: redis.redis.opstreelabs.in/v1beta2
kind: RedisReplication
metadata:
  name: redis
spec:
  clusterSize: 3
  kubernetesConfig:
    image: quay.io/opstree/redis:v8.10.1
    redisSecret:
      name: redis-credentials
      key: password
  sentinel:
    size: 3
    image: quay.io/opstree/redis-sentinel:v8.10.1
    redisSecret:
      name: redis-credentials
      key: password
  storage:
    volumeClaimTemplate:
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 512Mi
```

The operator creates:

```text
redis-0, redis-1, redis-2           data nodes (one master, two replicas)
redis-s-0, redis-s-1, redis-s-2     Sentinel nodes
redis-master                        Service for the current master
redis-replica                       Service for read traffic
redis-headless                      per-Pod DNS for the data nodes
redis-s-hl                          headless Service for Sentinel
```

Apply it:

```sh
kubectl apply -f redis/cluster/
kubectl get redisreplication redis --watch
kubectl get pods | grep redis
```

```text
redis-0     1/1   Running
redis-1     1/1   Running
redis-2     1/1   Running
redis-s-0   1/1   Running
redis-s-1   1/1   Running
redis-s-2   1/1   Running
```

Check the topology from inside the cluster:

```sh
kubectl exec -it redis-0 -- sh -c \
  'redis-cli -a "$REDIS_PASSWORD" info replication' 2>/dev/null | head
```

Sentinel knows which node is master:

```sh
kubectl exec -it redis-s-0 -- sh -c \
  'redis-cli -p 26379 -a "$REDIS_PASSWORD" sentinel get-master-addr-by-name mymaster'
```

### 4.2 The demo

The demo connects to Sentinel, not to a fixed Redis host:

```yaml
- name: REDIS_SENTINELS
  value: redis-s-hl:26379
- name: REDIS_MASTER_NAME
  value: mymaster
```

When a command fails because the master disappeared, the app drops its cached
client and asks Sentinel for the new master. No configuration change and no
restart.

```sh
sed -i 's/daniredis/<your-username>redis/' redis/demo/30-ingress.yaml
kubectl apply -f redis/demo/
kubectl rollout status deployment/redis-demo

kubectl port-forward svc/redis-demo 8082:80
```

<http://localhost:8082/ui> - the same page again.

```sh
curl -s -X POST localhost:8082/write \
  -H 'Content-Type: application/json' -d '{"value":"hello redis"}'

curl -s 'localhost:8082/read?limit=5'
curl -s localhost:8082/status | jq
```

`/status` reports `role`, `connected_slaves`, and Sentinel's current view of
the master (`sentinel_master`). The demo also has a cache example:

```sh
curl -s -X POST localhost:8082/cache/session \
  -H 'Content-Type: application/json' -d '{"value":"abc123"}'
curl -s localhost:8082/cache/session
```

### 4.3 Experiment: kill the master

```sh
kubectl port-forward svc/redis-demo 8082:80 &
curl -s 'localhost:8082/loop?seconds=40&interval=1' | jq

kubectl get pods -L redis-role | grep master
kubectl delete pod redis-<n>          # the Pod with redis-role=master
```

Sentinel elects one of the replicas. The loop shows a `ConnectionError` (for
example "The previous master is now a slave"), then recovers, and
`sentinel_master` in `/status` points at a different IP. Redis replication is
asynchronous: a write accepted just before the master died may be lost, which
is the trade-off this cache-style use case accepts.

---

## 5. Troubleshooting

```sh
# what did the operator say?
kubectl describe cluster postgres
kubectl describe mdbc mongodb
kubectl describe redisreplication redis

# what are the database Pods doing?
kubectl get pods
kubectl logs <pod> --all-containers --tail=50
kubectl get events --sort-by=.lastTimestamp | tail -30

# is the app's dependency reachable?
kubectl get endpoints postgres-rw
kubectl get endpoints mongodb-demo
kubectl get endpoints redis-demo
kubectl logs deploy/postgres-demo --tail=30

# images and storage
kubectl describe pod <pod> | grep -A5 Events
kubectl get pvc
```

---

## 6. Submission

Fork this repository. That's it.

---

## 8. Further reading

- Kubernetes Operators - <https://kubernetes.io/docs/concepts/extend-kubernetes/operator/>
- Custom Resources - <https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/>
- StatefulSets - <https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/>
- CloudNativePG - <https://cloudnative-pg.io/documentation/>
- MongoDB Community Operator - <https://github.com/mongodb/mongodb-kubernetes-operator>
- OT Redis Operator - <https://github.com/OT-CONTAINER-KIT/redis-operator>
- PostgreSQL streaming replication - <https://www.postgresql.org/docs/current/warm-standby.html>
- MongoDB replica sets - <https://www.mongodb.com/docs/manual/replication/>
- Redis Sentinel - <https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/>
