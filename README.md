# StreamPipe

A production-grade Change Data Capture (CDC) pipeline built on PostgreSQL logical replication. StreamPipe tails the PostgreSQL Write-Ahead Log (WAL) and streams row-level changes — inserts, updates, and deletes — to pluggable sinks in real time.

No Kafka. No Debezium. No external message broker. Just PostgreSQL doing what it was always capable of.

```
source-db (WAL) ──► StreamPipe consumer ──► postgres sink (replica)
                         │                ──► file sink   (JSONL/CSV)
                         │                ──► stdout sink (jq-friendly)
                         │
                    HTTP API :8080
                    Prometheus metrics
                    Replication lag monitor
```

## Why this exists

Most CDC tooling (Debezium, AWS DMS, Fivetran) is heavy infrastructure. PostgreSQL has supported logical replication since version 10 — the `pgoutput` plugin ships with every installation. StreamPipe is a minimal, self-contained implementation that shows exactly how the protocol works: replication slots, WAL decoding, LSN tracking, and standby status feedback.

## Features

- **WAL-based CDC** via PostgreSQL logical replication (`pgoutput` plugin)
- **Pluggable sinks** — PostgreSQL replica, JSONL/CSV file, or stdout
- **Resumable** — tracks the confirmed LSN and resumes from the right position after restart
- **Column redaction** — mask sensitive fields (e.g. `email`, `phone`) before they reach the sink
- **Replication lag monitoring** — polls `pg_replication_slots` and exposes `streampipe_replication_lag_bytes` to Prometheus
- **HTTP API** — `GET /status`, `GET /metrics`, `POST /pause`, `POST /resume`
- **Fully containerised** — one `docker compose up` starts source DB, replica DB, consumer, and Prometheus

## Architecture

```
┌─────────────────────────────────┐
│  source-db (PostgreSQL 16)      │
│  wal_level = logical            │
│                                 │
│  ┌─────────────────────────┐    │
│  │ WAL (write-ahead log)   │    │
│  └────────────┬────────────┘    │
│               │                 │
│  ┌────────────▼────────────┐    │
│  │ replication slot        │    │
│  │ plugin: pgoutput        │    │
│  └────────────┬────────────┘    │
└───────────────┼─────────────────┘
                │ logical replication protocol
┌───────────────▼─────────────────┐
│  StreamPipe (Go)                │
│                                 │
│  WAL decoder                    │
│    ├─ RelationMessage → schema  │
│    ├─ InsertMessage  → INSERT   │
│    ├─ UpdateMessage  → UPDATE   │
│    └─ DeleteMessage  → DELETE   │
│                                 │
│  Transform layer                │
│    └─ column redaction          │
│                                 │
│  LSN tracker (atomic uint64)    │
│  Standby status feedback loop   │
│                                 │
│  Sink router                    │
│    ├─ PostgreSQL (upsert/delete)│
│    ├─ File (JSONL or CSV)       │
│    └─ Stdout                    │
│                                 │
│  HTTP API  :8080                │
│  Prometheus metrics             │
└─────────────────────────────────┘
                │
┌───────────────▼─────────────────┐
│  replica-db (PostgreSQL 16)     │
│  receives upserts + deletes     │
└─────────────────────────────────┘
```

### Key PostgreSQL concepts used

| Concept | What StreamPipe does with it |
|---|---|
| `wal_level = logical` | Required server setting to enable logical decoding |
| Replication slot | Created once, persists WAL until StreamPipe confirms it |
| `pgoutput` plugin | Built-in logical decoding plugin, no extensions needed |
| `pg_publication` | Declares which tables to replicate |
| LSN (Log Sequence Number) | Monotonically increasing WAL position; used for resume |
| Standby status update | StreamPipe sends heartbeats so PostgreSQL can reclaim WAL |
| `pg_stat_replication` | Queried by the lag monitor every N seconds |
| `REPLICA IDENTITY FULL` | Required on source tables for DELETE before-images |

## Quick start

### Prerequisites

- Docker and Docker Compose v2
- Go 1.23+ (only needed for local development — not required to run via Docker)

### Run the full stack

```bash
git clone https://github.com/Shaonlib/streampipe
cd streampipe
docker compose up --build
```

No need to run `go mod tidy` or install Go locally — the Docker build handles dependency resolution inside the container.

This starts:
- `source-db` on `localhost:5433` — PostgreSQL with `wal_level=logical` and seed data
- `replica-db` on `localhost:5434` — empty replica, schema pre-created
- `streampipe` on `localhost:8080` — the consumer
- `prometheus` on `localhost:9090` — scrapes `/metrics`

StreamPipe will create the replication slot and find the existing publication on first run, then begin streaming.

### Watch it work

In a second terminal, generate some changes on the source. If you have `psql` installed:

```bash
psql postgres://postgres:postgres@localhost:5433/sourcedb \
  -c "INSERT INTO users (name, email) VALUES ('Dave Test', 'dave@example.com');"
```

Or use Docker directly (no `psql` install needed):

```bash
# Insert on source
docker exec -it streampipe-source psql -U postgres -d sourcedb \
  -c "INSERT INTO users (name, email) VALUES ('Dave Test', 'dave@example.com');"

# Update a product price
docker exec -it streampipe-source psql -U postgres -d sourcedb \
  -c "UPDATE products SET price_cents = 9999 WHERE name = 'USB-C Hub';"
```

Verify the replica received the changes (note: `email` and `phone` are redacted by the transform rules):

```bash
docker exec -it streampipe-replica psql -U postgres -d replicadb \
  -c "SELECT * FROM users;"
```

You should see the row appear with `email = ***REDACTED***` — StreamPipe applied the column redaction transform in-flight before writing to the sink.

### Check the API

```bash
# Pipeline status + current LSN
curl http://localhost:8080/status | jq

# Pause the stream
curl -X POST http://localhost:8080/pause

# Resume
curl -X POST http://localhost:8080/resume

# Raw Prometheus metrics
curl http://localhost:8080/metrics | grep streampipe
```

## Configuration

All configuration lives in `config.yaml`. The Docker image mounts it at `/app/config.yaml`.

```yaml
source:
  host: source-db
  port: 5432
  user: replicator
  password: replicator_pass
  database: sourcedb
  slot_name: streampipe_slot       # name of the replication slot
  publication_name: streampipe_pub  # name of the pg_publication

sink:
  type: postgres  # postgres | file | stdout

  postgres:
    host: replica-db
    port: 5432
    user: postgres
    password: postgres
    database: replicadb

  file:
    path: /data/changes.jsonl
    format: jsonl  # jsonl | csv

tables:
  - public.users
  - public.orders
  - public.products

# Redact columns before they reach the sink
transforms:
  - table: public.users
    redact:
      - email
      - phone

api:
  port: 8080

monitor:
  lag_warn_bytes: 1048576   # log a warning when lag exceeds 1MB
  poll_interval_seconds: 5
```

### Switching sinks

To write JSONL instead of replicating to Postgres, change `sink.type`:

```yaml
sink:
  type: file
  file:
    path: /data/changes.jsonl
    format: jsonl
```

Then restart:

```bash
docker compose restart streampipe
```

## Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `streampipe_replication_lag_bytes` | Gauge | Bytes between `pg_current_wal_lsn()` and confirmed flush LSN |
| `streampipe_confirmed_lsn` | Gauge | Last confirmed LSN as a numeric XLogRecPtr |
| `streampipe_events_total` | Counter | Events processed, labelled by `table` and `op` |
| `streampipe_errors_total` | Counter | Sink write failures |

Open `http://localhost:9090` and query `streampipe_replication_lag_bytes` to see lag in real time.

## Local development (without Docker)

```bash
# Start just the databases
docker compose up source-db replica-db -d

# Run StreamPipe locally
go run . -config config.yaml
```

Run tests:

```bash
go test ./...
```

## Project structure

```
streampipe/
├── main.go                        # entry point, wires everything together
├── config.yaml                    # pipeline configuration
├── Dockerfile                     # multi-stage Go build → alpine runtime
├── docker-compose.yml             # source-db, replica-db, streampipe, prometheus
├── prometheus.yml                 # Prometheus scrape config
├── migrations/
│   ├── 001_seed_source.sql        # source schema + seed data + replication user
│   └── 002_replica_schema.sql     # replica schema (no data)
└── internal/
    ├── config/config.go           # config loading + validation
    ├── replication/consumer.go    # WAL streaming loop, slot + publication management
    ├── decoder/decoder.go         # pgoutput message → ChangeEvent
    ├── sink/
    │   ├── sink.go                # Sink interface + transform helper
    │   ├── postgres.go            # upsert/delete to replica PostgreSQL
    │   ├── file.go                # JSONL and CSV file output
    │   └── stdout.go              # JSON to stdout
    ├── monitor/lag.go             # pg_stat_replication poller + Prometheus metrics
    └── api/server.go              # HTTP API
```

## Design decisions

**Why Go?** The `pglogrepl` and `pgx` libraries give low-level access to the PostgreSQL replication protocol with no abstraction overhead. The result is a single static binary that starts in milliseconds.

**Why not use Debezium?** Debezium is the right answer for production at scale. StreamPipe exists to demonstrate the underlying mechanism — what Debezium is doing under the hood.

**Why `pgoutput` instead of `wal2json`?** `pgoutput` is built into PostgreSQL and requires no extension installation. `wal2json` produces friendlier output but needs a separate `CREATE EXTENSION`. For a portable CDC tool, `pgoutput` is the right default.

**Why does DELETE need `REPLICA IDENTITY FULL`?** By default, PostgreSQL only includes the primary key in the WAL record for a DELETE. Setting `REPLICA IDENTITY FULL` writes the entire before-image, which StreamPipe needs to reconstruct a `WHERE` clause for the replica `DELETE`.

**Replication slot durability** — the slot persists WAL on the source until StreamPipe confirms it via standby status updates. If StreamPipe is down for a long time, WAL accumulates and can fill disk. For production use, monitor `pg_replication_slots.pg_wal_lsn_diff` and set a `max_slot_wal_keep_size` safety limit.

## Extending StreamPipe

Adding a new sink takes about 30 lines. Implement the `Sink` interface:

```go
type Sink interface {
    Write(ctx context.Context, event *decoder.ChangeEvent) error
    Close() error
}
```

Then add a case to `buildSink()` in `main.go` and a new entry in `config.yaml`. Ideas: HTTP webhook sink, S3 JSONL sink, Redis pub/sub sink.

## Roadmap

- [ ] Kafka sink
- [ ] Schema registry support (Avro serialisation)
- [ ] `TRUNCATE` event handling
- [ ] Per-table LSN checkpointing (survive partial failures)
- [ ] Web UI dashboard (replace raw `/status` JSON)
- [ ] Integration test suite using `testcontainers-go`

## License

MIT — free to use, copy, modify, and distribute for any purpose, including commercially. See [LICENSE](LICENSE) for the full text.
