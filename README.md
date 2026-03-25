# Transactional Outbox Pattern — Go + PostgreSQL + Kafka

A production-ready implementation of the **Transactional Outbox Pattern** in Go, using PostgreSQL as the datastore and Redpanda (Kafka-compatible) as the message broker.

Based on the article: [How to Implement the Outbox Pattern in Go and PostgreSQL](https://www.freecodecamp.org/news/how-to-implement-the-outbox-pattern-in-go-and-postgresql/)

---

## The Problem

In microservice architectures, a common requirement is to **update a database and publish an event** as part of a single business operation. For example, when creating an order:

1. Insert the order row into PostgreSQL
2. Publish an `OrderCreated` event to Kafka

The naive approach — doing both independently — leads to **dual-write failures**:

```mermaid
flowchart LR
    A[API Service] -->|1. INSERT order| B[(PostgreSQL)]
    A -->|2. Publish event| C[Kafka]

    style A fill:#f9f,stroke:#333
    style B fill:#4db8ff,stroke:#333
    style C fill:#ff9933,stroke:#333
```

**What can go wrong:**

| Scenario | DB Write | Kafka Publish | Result |
|---|---|---|---|
| Happy path | ✅ | ✅ | Consistent |
| Kafka down | ✅ | ❌ | Order exists, no event — **downstream never knows** |
| App crashes after DB write | ✅ | ❌ | Same as above |
| DB down | ❌ | ✅ | Event published for non-existent order — **ghost event** |

There is **no way to atomically commit to both** a database and a message broker in a single transaction.

---

## The Solution: Transactional Outbox

Instead of publishing directly to Kafka, we write the event into an **outbox table** in the **same database transaction** as the business data. A background **publisher** polls the outbox table and relays events to Kafka.

```mermaid
flowchart TB
    subgraph Single DB Transaction
        A[API Handler] -->|1. INSERT order| B[(orders table)]
        A -->|2. INSERT event| C[(outbox_events table)]
    end

    D[Outbox Publisher] -->|3. Poll & claim batch| C
    D -->|4. Publish to Kafka| E[Kafka / Redpanda]
    D -->|5. Mark as SENT| C

    style A fill:#f9f,stroke:#333
    style B fill:#4db8ff,stroke:#333
    style C fill:#4db8ff,stroke:#333
    style D fill:#66cc66,stroke:#333
    style E fill:#ff9933,stroke:#333
```

**Guarantees:**
- **Atomicity** — The order and its event are written in the same transaction. Either both succeed or both roll back.
- **At-least-once delivery** — The publisher retries failed events with exponential backoff. Events are never lost.
- **Ordering** — Events are processed in `created_at` order within each polling batch.
- **Concurrency safety** — `FOR UPDATE SKIP LOCKED` ensures multiple publisher instances don't process the same event.

---

## Architecture

```mermaid
flowchart TB
    Client([HTTP Client]) -->|POST /orders| GIN[Gin HTTP Server<br/>:8080]

    subgraph Go Application
        GIN --> Handler[OrdersHandler]
        Handler -->|BEGIN TX| PG[(PostgreSQL<br/>:5433)]
        Handler -->|1. CreateOrder| PG
        Handler -->|2. InsertOutboxEvent| PG
        Handler -->|COMMIT TX| PG

        Publisher[Outbox Publisher<br/>Background Goroutine] -->|Poll every 500ms| PG
        Publisher -->|ClaimOutboxBatch<br/>FOR UPDATE SKIP LOCKED| PG
        Publisher -->|Publish| KAFKA[Redpanda / Kafka<br/>:9092]
        Publisher -->|MarkOutboxSent| PG
    end

    CONFIG[config.yaml] -.->|Loaded at startup| GIN
    ENV[Environment Variables] -.->|Override config| GIN
    CONSOLE([Redpanda Console<br/>:8888]) -.->|Browse topics & messages| KAFKA

    style Client fill:#e6e6e6,stroke:#333
    style GIN fill:#f9f,stroke:#333
    style Handler fill:#f9f,stroke:#333
    style PG fill:#4db8ff,stroke:#333
    style Publisher fill:#66cc66,stroke:#333
    style KAFKA fill:#ff9933,stroke:#333
    style CONFIG fill:#ffffcc,stroke:#333
    style ENV fill:#ffffcc,stroke:#333
    style CONSOLE fill:#ccffcc,stroke:#333
```

---

## Sequence Diagrams

### Order Creation (Write Path)

```mermaid
sequenceDiagram
    participant C as HTTP Client
    participant H as OrdersHandler
    participant DB as PostgreSQL

    C->>H: POST /orders {customerId, amount}
    H->>DB: BEGIN
    H->>DB: INSERT INTO orders (id, customer_id, amount, status)
    H->>DB: INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload)
    H->>DB: COMMIT
    H->>C: 201 Created {orderId, status}

    Note over H,DB: Both writes are in the SAME transaction.<br/>If either fails, both roll back.
```

### Outbox Publishing (Read Path)

```mermaid
sequenceDiagram
    participant P as Outbox Publisher
    participant DB as PostgreSQL
    participant K as Kafka / Redpanda

    loop Every 500ms (configurable)
        P->>DB: BEGIN
        P->>DB: SELECT ... FROM outbox_events<br/>WHERE status='NEW' AND next_attempt_at <= now()<br/>FOR UPDATE SKIP LOCKED
        P->>DB: UPDATE locked_by = worker_id
        P->>DB: COMMIT

        loop For each claimed event
            P->>K: Produce message (key=aggregate_id, value=payload)
            alt Success
                P->>DB: UPDATE status='SENT', sent_at=now()
            else Failure
                P->>DB: UPDATE attempts+1, next_attempt_at=backoff
                Note over P,DB: Exponential backoff: 2^attempts seconds (max 60s).<br/>After max attempts → status='DEAD'.
            end
        end
    end
```

### Failure & Retry Flow

```mermaid
stateDiagram-v2
    [*] --> NEW: Event inserted
    NEW --> SENT: Published successfully
    NEW --> NEW: Publish failed (retry with backoff)
    NEW --> DEAD: Max attempts exceeded
    SENT --> [*]
    DEAD --> [*]: Requires manual intervention
```

### End-to-End Validation Flow

```mermaid
sequenceDiagram
    participant U as You (curl)
    participant API as Go API (:8080)
    participant DB as PostgreSQL (:5433)
    participant K as Redpanda (:9092)
    participant UI as Redpanda Console (:8888)

    U->>API: POST /orders
    API->>DB: INSERT order + outbox event (single TX)
    API->>U: 201 {orderId, status}

    Note over U: Validate Step 1:<br/>Query orders table

    U->>DB: SELECT * FROM orders
    DB->>U: Order row exists ✅

    Note over U: Validate Step 2:<br/>Query outbox_events table

    U->>DB: SELECT * FROM outbox_events
    DB->>U: Event status = 'SENT' ✅

    Note over API: Background publisher picks up event

    API->>DB: ClaimOutboxBatch (FOR UPDATE SKIP LOCKED)
    API->>K: Produce message
    API->>DB: MarkOutboxSent

    Note over U: Validate Step 3:<br/>Check Kafka messages

    U->>K: rpk topic consume orders.events
    K->>U: Message with payload ✅

    Note over U: Validate Step 4:<br/>Browse in Redpanda Console

    U->>UI: Open http://localhost:8888
    UI->>U: Topics → orders.events → Messages ✅
```

---

## Project Structure

```
Transactional-Outbox/
├── cmd/api/
│   └── main.go                  # Application entrypoint, wiring
├── internal/
│   ├── config/
│   │   └── config.go            # YAML + env var config loading
│   ├── db/
│   │   ├── postgres.go          # Connection pool setup
│   │   └── sqlc/                # Generated code (sqlc)
│   │       ├── db.go
│   │       ├── models.go
│   │       └── queries.sql.go
│   ├── domain/
│   │   └── events.go            # Domain event types
│   ├── http/
│   │   └── orders_handler.go    # HTTP handler with transactional write
│   └── outbox/
│       ├── kafka_producer.go    # Kafka producer wrapper
│       └── publisher.go         # Background outbox poller/publisher
├── sql/
│   ├── schema.sql               # Database schema
│   └── queries.sql              # SQL queries (used by sqlc)
├── config.yaml                  # Externalized configuration
├── docker-compose.yml           # PostgreSQL + Redpanda + Console
├── go.mod
├── sqlc.yaml                    # sqlc code generation config
└── README.md
```

---

## Database Schema

### orders

| Column | Type | Description |
|---|---|---|
| id | UUID | Primary key |
| customer_id | TEXT | Customer identifier |
| amount | NUMERIC(12,2) | Order amount |
| status | TEXT | Order status (e.g. CREATED) |
| created_at | TIMESTAMPTZ | Auto-set on insert |

### outbox_events

| Column | Type | Description |
|---|---|---|
| id | UUID | Primary key (event ID) |
| aggregate_type | TEXT | e.g. "order" |
| aggregate_id | TEXT | e.g. the order UUID |
| event_type | TEXT | e.g. "OrderCreated" |
| payload | JSONB | Event payload |
| status | TEXT | NEW → SENT or DEAD |
| attempts | INT | Delivery attempt count |
| next_attempt_at | TIMESTAMPTZ | When to retry next |
| locked_at | TIMESTAMPTZ | When a worker claimed it |
| locked_by | TEXT | Worker ID that claimed it |
| created_at | TIMESTAMPTZ | Auto-set on insert |
| sent_at | TIMESTAMPTZ | When successfully published |
| last_error | TEXT | Last failure message |

**Indexes:**
- `idx_outbox_pick` on `(status, next_attempt_at, created_at)` — optimizes the polling query
- `idx_outbox_agg` on `(aggregate_type, aggregate_id)` — optimizes lookups by aggregate

---

## Configuration

All configuration is externalized in `config.yaml` and can be overridden by environment variables at runtime — **no rebuild required**.

**Priority:** Environment variable > config.yaml

### config.yaml

```yaml
server:
  port: ":8080"
  readHeaderTimeout: "5s"
  shutdownTimeout: "10s"

db:
  url: "postgres://postgres:admin@localhost:5433/postgres?sslmode=disable"
  maxConns: 20
  minConns: 2
  maxConnLifetime: "30m"
  maxConnIdleTime: "5m"

kafka:
  brokers: "localhost:9092"
  topic: "orders.events"
  retries: 10
  lingerMs: 5
  batchNumMessages: 10000
  compressionType: "snappy"
  maxSendRetries: 10
  flushTimeoutMs: 10000

outbox:
  batchSize: 50
  interval: "500ms"
  maxAttempts: 10
```

### Environment Variable Overrides

| Variable | Config Path | Example |
|---|---|---|
| `CONFIG_PATH` | — | `/etc/app/config.yaml` |
| `SERVER_PORT` | server.port | `:9090` |
| `SERVER_READ_HEADER_TIMEOUT` | server.readHeaderTimeout | `10s` |
| `SERVER_SHUTDOWN_TIMEOUT` | server.shutdownTimeout | `30s` |
| `DATABASE_URL` | db.url | `postgres://user:pass@host:5432/db` |
| `DB_MAX_CONNS` | db.maxConns | `50` |
| `DB_MIN_CONNS` | db.minConns | `5` |
| `DB_MAX_CONN_LIFETIME` | db.maxConnLifetime | `1h` |
| `DB_MAX_CONN_IDLE_TIME` | db.maxConnIdleTime | `10m` |
| `KAFKA_BROKERS` | kafka.brokers | `broker1:9092,broker2:9092` |
| `KAFKA_TOPIC` | kafka.topic | `orders.events.prod` |
| `KAFKA_RETRIES` | kafka.retries | `5` |
| `KAFKA_LINGER_MS` | kafka.lingerMs | `10` |
| `KAFKA_BATCH_NUM_MESSAGES` | kafka.batchNumMessages | `5000` |
| `KAFKA_COMPRESSION_TYPE` | kafka.compressionType | `lz4` |
| `KAFKA_MAX_SEND_RETRIES` | kafka.maxSendRetries | `5` |
| `KAFKA_FLUSH_TIMEOUT_MS` | kafka.flushTimeoutMs | `15000` |
| `OUTBOX_BATCH_SIZE` | outbox.batchSize | `100` |
| `OUTBOX_INTERVAL` | outbox.interval | `1s` |
| `OUTBOX_MAX_ATTEMPTS` | outbox.maxAttempts | `5` |

---

## Getting Started

### Prerequisites

**Software:**

| Tool | Version | Purpose | Install |
|---|---|---|---|
| Go | 1.26+ | Application runtime & build | [go.dev/dl](https://go.dev/dl/) or `brew install go` |
| Docker | 20.10+ | Run PostgreSQL, Redpanda, Console | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Docker Compose | v2+ | Orchestrate multi-container setup | Included with Docker Desktop |
| sqlc | 1.30+ | Generate type-safe Go code from SQL | [docs.sqlc.dev](https://docs.sqlc.dev/en/latest/overview/install.html) or `brew install sqlc` |
| curl | any | Test API endpoints | Pre-installed on macOS/Linux |
| jq *(optional)* | any | Pretty-print JSON responses | `brew install jq` |

**Knowledge (helpful but not required):**

- Basic Go (structs, interfaces, goroutines, channels)
- SQL fundamentals (INSERT, SELECT, UPDATE, transactions)
- What Kafka topics, partitions, and producers are
- REST API concepts (HTTP methods, JSON)
- Docker basics (containers, ports, volumes)

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 17** on port `5433`
- **Redpanda** (Kafka-compatible) on port `9092`
- **Redpanda Console** (Web UI) on port `8888`

Verify all containers are running:
```bash
docker compose ps
```

Expected output:
```
NAME                                      STATUS          PORTS
transactional-outbox-postgres-1           Up              0.0.0.0:5433->5432/tcp
transactional-outbox-redpanda-1           Up              0.0.0.0:9092->9092/tcp
transactional-outbox-redpanda-console-1   Up              0.0.0.0:8888->8080/tcp
```

### 2. Create Database Tables

```bash
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres < sql/schema.sql
```

Expected output:
```
CREATE TABLE
CREATE TABLE
CREATE INDEX
CREATE INDEX
```

### 3. Generate Type-Safe Query Code (sqlc)

The project uses [sqlc](https://sqlc.dev) to generate type-safe Go code from the SQL schema and queries. The generated code is already committed in `internal/db/sqlc/`, but if you modify `sql/schema.sql` or `sql/queries.sql`, regenerate it:

```bash
sqlc generate
```

This reads `sqlc.yaml` and generates:
- `internal/db/sqlc/db.go` — DBTX interface and Queries struct
- `internal/db/sqlc/models.go` — Go structs matching the DB tables
- `internal/db/sqlc/queries.sql.go` — Type-safe functions for each SQL query

> **Note:** You can skip this step if you haven't changed any SQL files — the generated code is already up to date.

### 4. Install Go Dependencies

```bash
go mod tidy
```

This downloads all required Go modules (Gin, pgx, confluent-kafka-go, etc.) and updates `go.sum`.

### 5. Run the Application

```bash
go run ./cmd/api
```

You should see:
```
[GIN-debug] POST   /orders   --> ...
[GIN-debug] GET    /health   --> ...
[outbox] started worker=<your-hostname>
HTTP listening on :8080
```

### 6. Create an Order

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-123", "amount": 99.99}'
```

Expected response:
```json
{
  "orderId": "a1b2c3d4-...",
  "status": "CREATED"
}
```

---

## Validating the End-to-End Flow

After creating an order, validate each step of the outbox pattern:

### Step 1 — Verify the Order in PostgreSQL

```bash
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres -c "SELECT id, customer_id, amount, status, created_at FROM orders;"
```

Expected:
```
                  id                  | customer_id | amount | status  |          created_at
--------------------------------------+-------------+--------+---------+-------------------------------
 a1b2c3d4-...                         | cust-123    |  99.99 | CREATED | 2026-03-24 14:06:07.123456+00
```

### Step 2 — Verify the Outbox Event Was Processed

```bash
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres -c "SELECT id, event_type, status, attempts, sent_at FROM outbox_events;"
```

Expected (event should be `SENT` within ~500ms):
```
                  id                  |  event_type   | status | attempts |           sent_at
--------------------------------------+---------------+--------+----------+-------------------------------
 e5f6a7b8-...                         | OrderCreated  | SENT   |        0 | 2026-03-24 14:06:07.654321+00
```

If `status` is still `NEW`, the publisher hasn't picked it up yet — wait a moment and re-run.

If `status` is `DEAD`, check the `last_error` column:
```bash
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres -c "SELECT id, status, attempts, last_error FROM outbox_events WHERE status='DEAD';"
```

### Step 3 — Verify the Kafka Message via CLI

```bash
docker exec -it $(docker ps -q -f name=redpanda-1) \
  rpk topic consume orders.events --num 1
```

Expected:
```json
{
  "topic": "orders.events",
  "key": "a1b2c3d4-...",
  "value": "{\"orderId\":\"a1b2c3d4-...\",\"customerId\":\"cust-123\",\"amount\":99.99}",
  "headers": [
    {"key": "event_id", "value": "e5f6a7b8-..."},
    {"key": "event_type", "value": "OrderCreated"},
    {"key": "aggregate_type", "value": "order"},
    {"key": "aggregate_id", "value": "a1b2c3d4-..."}
  ]
}
```

### Step 4 — Verify via Redpanda Console (Web UI)

1. Open **http://localhost:8888** in your browser
2. Click **Topics** in the left sidebar
3. Click on **orders.events**
4. Click the **Messages** tab
5. You should see each published event with its key, value, headers, and timestamp

### Step 5 — Health Check

```bash
curl http://localhost:8080/health
```

Expected:
```
ok
```

---

## Validation Cheat Sheet

Quick one-liner to validate the full pipeline after creating an order:

```bash
# Create order
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-456", "amount": 250.00}' | jq .

# Check DB (orders)
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres -c "SELECT id, customer_id, amount, status FROM orders ORDER BY created_at DESC LIMIT 1;"

# Check DB (outbox)
docker exec -i $(docker ps -q -f name=postgres) \
  psql -U postgres -d postgres -c "SELECT id, event_type, status, attempts, sent_at FROM outbox_events ORDER BY created_at DESC LIMIT 1;"

# Check Kafka
docker exec -it $(docker ps -q -f name=redpanda-1) \
  rpk topic consume orders.events --num 1 --offset -1
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `relation "outbox_events" does not exist` | Tables not created | Run Step 2 (Create Database Tables) |
| `failed to create order` | Check app logs for the actual error | Look at the terminal running `go run ./cmd/api` |
| Outbox event stuck in `NEW` | Kafka/Redpanda not reachable | Verify `docker compose ps` shows redpanda is running |
| Outbox event is `DEAD` | Max retry attempts exceeded | Check `last_error` column: `SELECT last_error FROM outbox_events WHERE status='DEAD';` |
| Redpanda Console blank page | Container still starting | Wait 10s and refresh; check `docker compose logs redpanda-console` |
| `version "go1.23.5" does not match go tool version "go1.26.1"` | Stale Go installation at `/usr/local/go` | Set `export GOROOT=/opt/homebrew/opt/go/libexec` in your `~/.zshrc` and run `source ~/.zshrc` |
| Connection refused on port `5433` | PostgreSQL container not running | Run `docker compose up -d` |
| No messages in Kafka topic | Publisher hasn't run yet | Events are polled every 500ms; wait and re-check |

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Polling-based publisher** | Simpler than CDC/WAL-tailing. No extra infrastructure (e.g. Debezium). Good enough for most workloads. |
| **FOR UPDATE SKIP LOCKED** | Allows multiple publisher instances to run concurrently without processing the same event twice. |
| **Exponential backoff** | Failed events are retried with increasing delay (2^attempts seconds, capped at 60s) to avoid hammering a failing broker. |
| **DEAD status** | After max attempts, events are marked DEAD for manual investigation rather than retrying forever. |
| **Idempotent Kafka producer** | `enable.idempotence=true` + `acks=all` ensures exactly-once delivery to Kafka at the producer level. |
| **YAML + env var config** | Config file for base settings, env vars for per-environment overrides. No rebuild needed to change any value. |
| **sqlc for queries** | Type-safe, compile-time checked SQL. No ORM overhead. |

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go |
| HTTP Framework | [Gin](https://github.com/gin-gonic/gin) |
| Database | PostgreSQL 17 |
| Database Driver | [pgx/v5](https://github.com/jackc/pgx) |
| SQL Code Gen | [sqlc](https://sqlc.dev) |
| Message Broker | Redpanda (Kafka-compatible) |
| Kafka Client | [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) |
| Monitoring UI | [Redpanda Console](https://github.com/redpanda-data/console) |
| Config | YAML + environment variables |

---

## References

- [Transactional Outbox Pattern — Microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
- [How to Implement the Outbox Pattern in Go and PostgreSQL — freeCodeCamp](https://www.freecodecamp.org/news/how-to-implement-the-outbox-pattern-in-go-and-postgresql/)
- [PostgreSQL Advisory Locks & SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [Redpanda Console Documentation](https://docs.redpanda.com/current/reference/console/)
