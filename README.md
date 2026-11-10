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
export PATH=$PWD/../gotool/go/bin:$PATH   # if go is not installed system-wide
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
cd worker && pip install pytest && python -m pytest -q   # tests pin the stub; no Kafka/YOLO needed
terraform -chdir=terraform init -backend=false && terraform -chdir=terraform validate  # never apply
```

Verification status of this commit:

| Check | Status |
|---|---|
| Worker logic (determinism, both labels, normalized bboxes, empty frame) | Same assertions as `test_worker.py`, run with plain `python3` (pytest could not be installed in the build session) |
| `go test ./...` | **Not run.** The build session could not execute the Go toolchain. The code was reviewed by hand only |
| `go run ./cmd/fleetload` | **Not run**, same reason |
| `terraform validate` | **Not run.** Terraform is not installed in the build environment |

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

**Laptop sanity run: not recorded yet.** The session that built this repo
was not allowed to run the Go toolchain, so no `go test` or `fleetload`
output exists to paste here, and none has been made up. Run the commands
above and paste the output in this block:

```
(not yet run; see note above)
```

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
- **Unverified build.** See the verification table above. Run `go test` and
  `fleetload` before relying on this code.
- **Worker stub is not a detector.** Without ultralytics + weights it emits
  deterministic hash-derived boxes for tests only. Real YOLO path needs
  `worker/requirements.txt` and a GPU for production frame rates.
- **No auth/TLS.** Device identity, mTLS, and API auth are out of scope for
  this skeleton and mandatory before any real deployment.
- Terraform is a skeleton that has never been applied, and has so far been
  checked by inspection only (see above). Set `subnet_ids` per VPC. The ECS
  task also needs an execution role before it could ever run.
