# TCP Message Processing System

A TCP-based message processing system with long-lived connections, implementing a job distribution and result submission protocol. The server handles concurrent client sessions, validates submissions, enforces rate limits, and publishes events to RabbitMQ. A separate consumer process records statistics in PostgreSQL.

## Prerequisites

- Go 1.25.4 or later
- Docker and Docker Compose (for PostgreSQL and RabbitMQ)
- macOS or Linux (required by challenge)

## Building and Running

### 1. Start Dependencies

Start PostgreSQL and RabbitMQ using Docker Compose:

```bash
make docker-up
# or
docker compose up -d
```

This will start:
- PostgreSQL on port `5432` (user: `luxor`, password: `luxor`, database: `luxor`)
- RabbitMQ on ports `5672` (AMQP) and `15672` (management UI)

### 2. Build the Project

Build the server, client, and consumer:

```bash
make build
```

Or build individually:

```bash
make build-server    # Builds server to bin/server
make build-client    # Builds client to bin/client
make build-consumer  # Builds consumer to bin/consumer
```

### 3. Run the Consumer

The consumer will:
- Connect to PostgreSQL and run database migrations
- Connect to RabbitMQ and subscribe to the submission queue
- Process submission events and record statistics

```bash
make run-consumer
# or
./bin/consumer
```

### 4. Run the Server

The server will:
- Connect to RabbitMQ
- Listen on port `1337` for TCP connections (configurable via `-addr` flag)
- Start generating and broadcasting jobs

```bash
make run-server
# or
./bin/server
./bin/server -addr :8080  # custom port
```

### 5. Run the Client

In a separate terminal, run one or more clients:

```bash
make run-client
# or
./bin/client
./bin/client -addr localhost:8080  # custom server address
```

Each client will:
- Connect to the server
- Authenticate with a randomly generated username
- Receive job assignments
- Submit results every 2 seconds (within rate limits)

### 6. Testing with Multiple Clients

To test concurrency, run multiple client instances in separate terminals:

```bash
# Terminal 1
./bin/client

# Terminal 2
./bin/client

# Terminal 3
./bin/client
```

## Overview

This system implements a challenge-response protocol where:
- Clients connect via TCP and authenticate with a username
- The server distributes jobs every 30 seconds with a server nonce
- Clients compute SHA256 hashes and submit results
- The server validates submissions and publishes events to RabbitMQ
- A consumer process reads events from RabbitMQ and records statistics per user per minute in PostgreSQL

## Architecture

### Components

1. **TCP Server** (`cmd/server/`)
   - Listens on a configurable TCP address (default `:1337`)
   - Manages concurrent client sessions with per-connection mutexes
   - Generates and broadcasts jobs every 30 seconds
   - Validates submissions and enforces rate limits
   - Publishes accepted submissions to RabbitMQ
   - Graceful shutdown on SIGINT/SIGTERM: stops accepting connections, waits for in-flight requests

2. **Consumer** (`cmd/consumer/`)
   - Subscribes to the RabbitMQ submission queue
   - Unmarshals events and records them in PostgreSQL
   - Always acknowledges messages (discards poison messages to avoid re-queue loops)
   - Runs database migrations on startup
   - Graceful shutdown with a 10-second drain timeout

3. **TCP Client** (`cmd/client/`)
   - Connects to the server at a configurable address (default `:1337`)
   - Authenticates with a randomly generated username
   - Receives job assignments and computes SHA256 hashes
   - Submits results every 2 seconds
   - Graceful shutdown on SIGINT/SIGTERM or server disconnect

4. **Queue** (`internal/queue/`)
   - RabbitMQ integration with publisher confirms and manual acknowledgement
   - `SubmissionPublisher` and `SubmissionConsumer` interfaces for testability

5. **Database Store** (`internal/store/`)
   - PostgreSQL integration for statistics tracking
   - Aggregates submissions by username and minute via upsert

### Key Features Implemented

**Authentication Flow**
- Client sends authorization request with username
- Server validates and tracks username per session
- Supports concurrent authenticated sessions

**Task Distribution**
- Server generates new jobs every 30 seconds
- Each job includes a unique `job_id` and `server_nonce`
- Jobs broadcast only to authenticated clients
- Job history maintained per session for detailed error responses

**Result Submission**
- Client generates random `client_nonce` per submission
- Computes `SHA256(server_nonce + client_nonce)`
- Server validates:
  - Authentication status
  - Rate limits (1/second max)
  - Job ID matches current job (with expired vs unknown distinction)
  - SHA256 hash correctness
  - No duplicate client nonces
- Rate limit timestamp updated even on failed validations to prevent spam

**Message Queue Integration**
- Server publishes submission events to RabbitMQ (durable queue, persistent messages, publisher confirms)
- Consumer processes events independently, decoupling ingestion from storage
- Poison messages are discarded and acknowledged to avoid infinite re-queue

**Concurrency Safety**
- `Session.Authenticated` uses `atomic.Bool` for lock-free reads
- `Session.ConnMu` protects connection writes (JSON encoder)
- `Session.DataMu` protects `JobHistory` map access
- `sessionManager.mu` (RWMutex) protects the session map
- `jobManager.mu` (RWMutex) protects job state

**Graceful Shutdown**
- Server: stops listener, cancels context, waits for connections with a 10-second timeout
- Consumer: cancels context, drains in-flight messages with a 10-second timeout
- Client: cancels context, closes connection, waits for goroutines with a 10-second timeout

## Testing

Run the test suite:

```bash
make test
```

Run tests with race detection:

```bash
make test-race
```

Run tests with verbose output:

```bash
make test-verbose
```

## Protocol Specification

### Message Format

All messages are JSON-encoded and sent over TCP. Messages requiring responses must include an `id` field. Messages not requiring responses use `id: null`.

### 1. Authentication

**Client -> Server:**
```json
{
    "id": 1,
    "method": "authorize",
    "params": {
        "username": "admin"
    }
}
```

**Server -> Client:**
```json
{
    "id": 1,
    "result": true
}
```

### 2. Task Assignment

**Server -> Client:**
```json
{
    "id": null,
    "method": "job",
    "params": {
        "job_id": 1,
        "server_nonce": "123"
    }
}
```

### 3. Result Submission

**Client -> Server:**
```json
{
    "id": 2,
    "method": "submit",
    "params": {
        "job_id": 1,
        "client_nonce": "456",
        "result": "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
    }
}
```

**Server -> Client (Success):**
```json
{
    "id": 2,
    "result": true
}
```

**Server -> Client (Error):**
```json
{
    "id": 2,
    "result": false,
    "error": "error_message"
}
```

### SHA256 Calculation

The result hash is computed as:
```
SHA256(server_nonce + client_nonce)
```

Example:
- `server_nonce = "123"`
- `client_nonce = "456"`
- `result = SHA256("123456") = "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"`

**Important:** The order matters! `SHA256("123456") ≠ SHA256("654321")`

## Error Conditions

The server returns specific error messages for various conditions:

| Condition | Error Message |
|-----------|---------------|
| Bad request (e.g. empty username) | `"Bad request"` |
| Already authenticated | `"Already authenticated"` |
| Not authenticated | `"Not authenticated"` |
| Invalid job_id | `"Task does not exist"` |
| Expired job_id | `"Task expired"` |
| Invalid SHA256 result | `"Invalid result"` |
| Rate limit exceeded | `"Submission too frequent"` |
| Duplicate client_nonce | `"Duplicate submission"` |
| Internal failure (unmarshal/publish) | `"Internal server error"` |

## Database Schema

The `submissions` table tracks statistics:

```sql
CREATE TABLE submissions (
    username VARCHAR(255),
    timestamp TIMESTAMP,
    submission_count INT DEFAULT 1,
    UNIQUE(username, timestamp)
);
```

Submissions are aggregated by minute (timestamp truncated to minute precision). The table uses an upsert pattern: if a submission exists for a username and minute, the count is incremented.

## Project Structure

```
tcphashsubmit/
├── cmd/
│   ├── client/             # TCP client
│   │   ├── main.go
│   │   ├── client.go
│   │   └── client_state.go
│   ├── consumer/           # RabbitMQ consumer → PostgreSQL
│   │   └── main.go
│   └── server/             # TCP server
│       ├── main.go
│       ├── app.go
│       ├── handlers.go
│       ├── job_manager.go
│       └── session_manager.go
├── internal/
│   ├── models/             # Message models and session type
│   │   ├── messages.go
│   │   └── session.go
│   ├── queue/              # RabbitMQ abstraction
│   │   ├── queue.go
│   │   └── submission_queue.go
│   ├── store/              # PostgreSQL layer
│   │   ├── database.go
│   │   └── submission_store.go
│   └── util/               # Utility functions (hashing, nonce)
│       └── util.go
├── migrations/             # Goose database migrations
│   ├── 00001_submissions.sql
│   └── fs.go
├── compose.yaml            # Docker Compose (PostgreSQL + RabbitMQ)
├── Makefile
├── go.mod
└── go.sum
```

## Configuration

### Server

| Setting | Default | Flag |
|---------|---------|------|
| Listen address | `:1337` | `-addr` |
| RabbitMQ URL | `amqp://guest:guest@localhost:5672/` | — |
| Job interval | 30 seconds | — |

### Consumer

| Setting | Default |
|---------|---------|
| PostgreSQL DSN | `host=localhost user=luxor password=luxor dbname=luxor port=5432 sslmode=disable` |
| RabbitMQ URL | `amqp://guest:guest@localhost:5672/` |

### Client

| Setting | Default | Flag |
|---------|---------|------|
| Server address | `:1337` | `-addr` |
| Submission interval | 2 seconds | — |
| Username | Random (`user-{nonce}`) | — |

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make build` | Build server, client, and consumer |
| `make build-server` | Build server only |
| `make build-client` | Build client only |
| `make build-consumer` | Build consumer only |
| `make run-server` | Build and run server |
| `make run-client` | Build and run client |
| `make run-consumer` | Build and run consumer |
| `make test` | Run tests |
| `make test-race` | Run tests with race detection |
| `make test-verbose` | Run tests with verbose output |
| `make docker-up` | Start Docker services |
| `make docker-down` | Stop Docker services |
| `make docker-reset` | Reset Docker volumes and restart |
| `make migrate` | Run database migrations |
| `make clean` | Remove build artifacts |

## Limitations and Future Work

1. **Configuration**
   - Connection strings and intervals are hardcoded (only listen/dial addresses are flag-configurable)
   - No configuration file or environment variable support

2. **Monitoring**
   - No metrics or health check endpoints
   - Limited observability beyond structured JSON logs

3. **Unbounded nonce tracking**
   - `SeenClientNonces` grows without bound for long-lived sessions; production use should add LRU eviction or periodic pruning

## License

This project was created as part of a technical challenge.
