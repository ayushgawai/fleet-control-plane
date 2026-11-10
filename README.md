# fleet-control-plane

Backend control plane that registers, monitors, and remotely configures a
large fleet of networked security cameras and sensors: versioned desired /
reported config shadows, heartbeat presence, config fan-out with retries and
a dead-letter queue, and a frame-detection worker.

```
                        +----------------------+
   operators / CI ----> |  Go REST API (cmd/api)|
                        |  registry, shadows,   |
                        |  /metrics (Prometheus)|
                        +---+-------+-------+---+
                            |       |       |
              desired write |       |       | heartbeat
              (conditional, |       |       v
               version=N)   |       |   +---------+      +----------+
                            v       |   |  Redis  |      | Redpanda |
                        +---------+ |   | presence|      | (Kafka)  |
                        |DynamoDB | |   | TTL +   |      | heart-   |
                        | shadows | |   | rate    |      | beats,   |
                        |(desired/| |   | limits  |      | detections|
                        |reported)| |   +---------+      +----^-----+
                        +---------+ |                         |
                            |       |                         | detections
                  config cmd|       v                         |
                            v   +---------+   frames   +------+-------+
                        +---------+  SQS    | -------> | Python worker|
                        | devices | (DLQ +  | <------- | YOLO / stub  |
                        | (sim)   | retries)|  acks    | person/vehicle|
                        +---------+         +----------+ +--------------+
   Postgres: device registry + append-only config history (migrations/)
```

## Design

| Concern | Choice | Where |
|---|---|---|
| Device registry, config history | PostgreSQL (`migrations/001_init.sql`) | prod; in-memory fake in tests |
| Desired/reported shadow | DynamoDB conditional write on `version` (optimistic concurrency, `409` on conflict) | `internal/fleet/fleet.go: SetDesired` |
| Presence | Redis TTL keys from heartbeats (90s) | `Store.Heartbeat/Online` |
| Rate limits | Per-device and per-org sliding window (Redis in prod) | `Store.Allow`, API returns `429` |
| Events | Kafka/Redpanda topics `heartbeats`, `config-changes`, `frames`, `detections` | `Bus` fake + `docker-compose.yml` |
| Config fan-out | SQS + retries (3) + DLQ, idempotent ack; duplicate/stale reports never regress a device | `Queue`, `Store.Report` |
| Detection | Python worker, YOLO via ultralytics if installed, deterministic stub fallback | `worker/worker.py` |
| Metrics | Prometheus text on `GET /metrics` | `internal/fleet/http.go` |
| Infra | Terraform (DynamoDB, SQS+DLQ, ECS skeleton — validate only, never applied), Kubernetes Deployments/HPA/probes | `terraform/`, `k8s/` |

The Go service has **zero external dependencies** (stdlib `net/http` only).
Production adapters for Postgres/DynamoDB/Redis/Kafka/SQS sit behind the same
semantics implemented and tested in-memory in `internal/fleet`; wiring a real
driver is the main unfinished piece (see Limitations).

## Run

```sh
go run ./cmd/api                          # :8080
docker compose up                         # full stack: Postgres, Redis, Redpanda, DynamoDB Local, LocalStack SQS, api

curl -XPOST localhost:8080/devices -d '{"id":"cam1","org":"o1","model":"x"}'
curl -XPOST localhost:8080/devices/cam1/heartbeat -d '{}'
curl -XPUT  localhost:8080/devices/cam1/desired -d '{"config":{"fps":30},"expected_version":0}'
curl -XPOST localhost:8080/devices/cam1/ack -d '{"command_id":"cam1:v1","version":1,"config":{"fps":30}}'
curl localhost:8080/devices/cam1/shadow
curl localhost:8080/metrics

cd worker && python worker.py --demo      # stub detector, no model needed
```

## Test

```sh
go test -race ./...
python3 -m venv .venv && .venv/bin/python -m pip install pytest && .venv/bin/python -m pytest -q worker   # tests pin the stub; no Kafka/YOLO needed
terraform -chdir=terraform init -backend=false && terraform -chdir=terraform validate  # never apply
```

Verification status of this commit:

| Check | Status |
|---|---|
| `go build ./...`, `go vet ./...` | Pass (go1.27.3 darwin/arm64) |
| `go test -race ./...` | Pass, 6 tests in `internal/fleet` |
| `go run ./cmd/fleetload` (100,000 devices, default flags) | Pass, 0 stale devices. Output below. The first run exposed two bugs, both fixed in this commit (see below) |
| `go run -race ./cmd/fleetload -devices 10000` | Pass, no races reported |
| `pytest worker` | Pass, 4 tests (pytest 9.2.0 in a local `.venv`, Python 3.15.0) |
| `terraform validate` | **Not run.** Terraform is not installed on the verification machine |
| `docker compose up` | **Not run.** Docker is not installed on the verification machine |

Bugs found by the first fleetload run and fixed:
- `Queue.Ack` scanned the whole pending slice under the lock on every ack,
  duplicates included, so draining was O(n²). At 100k devices the drain took
  3m22s and p50 ack latency was 2m29s. Acked entries are now skipped lazily in
  `Receive`, so `Ack` is O(1).
- The simulator called `Acked` and then `Ack` as two separate locked calls.
  Two workers holding duplicate deliveries could both count as "first ack",
  which gave 378,476 acks for 200,000 commands. `Ack` now returns whether it
  was the first ack, atomically.

## Bench / simulation

`cmd/fleetload` simulates the fleet in-process against the real store/queue.
Every device is a goroutine that sends heartbeats. Then two desired-config
rounds (v1, then v2) are pushed to every device. Each command is enqueued
twice (duplicate delivery), the first delivery for ~10% of devices is dropped
and retried, and every ack is sent twice. A pool of delivery goroutines drains
the queue. At the end the run checks that every device's `ReportedVersion`
equals its latest desired `Version`, and exits non-zero if any device is
stale.

```sh
go run ./cmd/fleetload                                  # 100,000 devices (default)
go run ./cmd/fleetload -devices 10000 -beats 3 -fail 10 # smaller laptop run
```

Output reports heartbeat ingest rate, config-push ack latency p50/p99/max
(enqueue to first ack, including queue wait), injected failures, DLQ depth
and the stale-device count.

Laptop sanity run, Apple Silicon Mac (darwin/arm64, go1.27.3), default
flags, full 100,000 devices (not scaled down). Real output, copied verbatim:

```
$ go run ./cmd/fleetload
devices=100000 online=100000 heartbeats=300000 ingest=1669366 hb/s (180ms)
config pushes=200000 acks=200000 drained in 257ms, injected failures=10000, duplicate deliveries=200000, dlq=0
config-push ack latency p50=168.356ms p99=253.492ms max=256.582ms
stale devices after failures+duplicates: 0
OK: every device converged to the latest desired version
```

A second run gave the same picture: 1,590,974 hb/s, p50 167.6ms, p99 250.4ms,
0 stale. Read the latency carefully. All 400k messages (2 rounds plus
duplicates) are enqueued before the delivery pool starts, so p50/p99 mostly
measure queue wait inside a 250ms drain. They are not per-message round-trip
time. These are in-process numbers behind one global mutex, with no network,
Kafka or SQS involved.

## Limitations (honest)

- **Adapters are in-memory.** The Go API runs against the tested in-memory
  implementations. Postgres/DynamoDB/Redis/Kafka/SQS are provisioned by
  docker-compose/Terraform and the schemas/topics/queues are defined, but
  real client wiring (pgx, AWS SDK, kafka-go, go-redis) is not done — adding
  those dependencies is deliberately left for when the stack is deployed.
- **Simulator is in-process.** `fleetload` measures store/queue logic behind
  one global mutex. It does not measure network, Kafka or SQS throughput, so
  its numbers are an upper bound on logic cost, not a capacity claim.
- **SQS fake has no visibility timeout.** Retries happen only on an explicit
  `Fail`. A consumer that crashes mid-message loses that message in the fake.
  Real SQS redelivers it, and Terraform sets `maxReceiveCount = 3` for the DLQ.
- **Infra unverified.** Go and Python are built and tested (see the
  verification table), but `terraform validate` and `docker compose up`
  have not been run.
- **Worker stub is not a detector.** Without ultralytics + weights it emits
  deterministic hash-derived boxes for tests only. Real YOLO path needs
  `worker/requirements.txt` and a GPU for production frame rates.
- **No auth/TLS.** Device identity, mTLS, and API auth are out of scope for
  this skeleton and mandatory before any real deployment.
- Terraform is a skeleton that has never been applied, and has so far been
  checked by inspection only (see above). Set `subnet_ids` per VPC. The ECS
  task also needs an execution role before it could ever run.
