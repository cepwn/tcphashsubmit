# TCP Message Processing System

A TCP-based message processing system with long-lived connections, implementing a job distribution and result submission protocol. This system handles concurrent client sessions, validates submissions, enforces rate limits, and tracks statistics in PostgreSQL.

## Prerequisites

- Go 1.25.4 or later
- Docker and Docker Compose (for PostgreSQL and RabbitMQ)
- macOS or Linux (required by challenge)

## Building and Running

### 1. Start Dependencies

Start PostgreSQL (and RabbitMQ, though not used) using Docker Compose:

```bash
make docker-up
# or
docker compose up -d
```

This will start:
- PostgreSQL on port `5432` (user: `luxor`, password: `luxor`, database: `luxor`)
- RabbitMQ on ports `5672` and `15672` (not used in current implementation)

### 2. Build the Project

Build both server and client:

```bash
make build
```

Or build individually:

```bash
make build-server  # Builds server to bin/server
make build-client  # Builds client to bin/client
```

### 3. Run the Server

The server will:
- Connect to PostgreSQL
- Run database migrations automatically
- Listen on port `1337` for TCP connections
- Start generating and broadcasting jobs

```bash
make run-server
# or
./bin/server
```

### 4. Run the Client

In a separate terminal, run one or more clients:

```bash
make run-client
# or
./bin/client
```

Each client will:
- Connect to the server
- Authenticate with a randomly generated username
- Receive job assignments
- Submit results every 2 seconds (within rate limits)

### 5. Testing with Multiple Clients

To test concurrency, run multiple client instances in separate terminals:

```bash
# Terminal 1
./bin/client

# Terminal 2
./bin/client

# Terminal 3
./bin/client
```

## Limitations and Future Work

1. **Message processing (RabbitMQ)**
   - Statistics are already written off the request path via a goroutine and channel; a natural next step is to integrate RabbitMQ so submission events are durable and the server can handle restarts and scale better.

2. **Graceful Shutdown**
   - Server doesn't handle shutdown signals gracefully
   - Connections are terminated immediately on server exit

3. **Configuration**
   - Hardcoded values (port, database connection, intervals)
   - No configuration file or environment variable support

4. **Monitoring**
   - No metrics or health check endpoints
   - Limited observability beyond logs

5. **Job history per session**
   - Maintain a job_id ↔ server_nonce history per session so the server can return better-detailed error information (e.g. distinguish expired vs unknown job_id, or surface the expected nonce when a result is invalid).

## Overview

This system implements a challenge-response protocol where:
- Clients connect via TCP and authenticate with a username
- The server distributes jobs every 30 seconds with a server nonce
- Clients compute SHA256 hashes and submit results
- The server validates submissions and tracks statistics per user per minute

## Architecture

### Components

1. **TCP Server** (`cmd/server/`)
   - Listens on port `1337` for TCP connections
   - Manages concurrent client sessions
   - Generates and broadcasts jobs every 30 seconds
   - Validates submissions and enforces rate limits
   - Records statistics to PostgreSQL (off the request path via a goroutine and channel)

2. **TCP Client** (`cmd/client/`)
   - Connects to the server
   - Authenticates with a username
   - Receives job assignments
   - Computes SHA256 hashes and submits results
   - Maintains persistent connection

3. **Database Store** (`internal/store/`)
   - PostgreSQL integration for statistics tracking
   - Aggregates submissions by username and minute

### Key Features Implemented

✅ **Authentication Flow**
- Client sends authorization request with username
- Server validates and tracks username per session
- Supports concurrent authenticated sessions

✅ **Task Distribution**
- Server generates new jobs every 30 seconds
- Each job includes a unique `job_id` and `server_nonce`
- Jobs broadcasted to all authenticated clients
- Job history maintained per session (bonus feature)

✅ **Result Submission**
- Client generates random `client_nonce` per submission
- Computes `SHA256(server_nonce + client_nonce)`
- Submits at rate: 1/second maximum, 1/minute minimum
- Server validates:
  - Job ID matches current job
  - SHA256 calculation is correct
  - Rate limits are enforced (1/second max)
  - No duplicate client nonces

✅ **Error Handling**
- Invalid job_id: `"Task does not exist"`
- Expired job_id: `"Task expired"`
- Invalid result: `"Invalid result"`
- Rate limit exceeded: `"Submission too frequent"`
- Duplicate nonce: `"Duplicate submission"`

✅ **Statistics Collection**
- Tracks submissions per username
- Aggregates by minute
- Writes to PostgreSQL off the request path (goroutine + channel); upsert per (username, minute)

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
│   ├── client/          # TCP client implementation
│   │   ├── main.go
│   │   ├── client.go
│   │   └── client_state.go
│   └── server/          # TCP server implementation
│       ├── main.go
│       ├── handlers.go
│       ├── job_manager.go
│       └── session_manager.go
├── internal/
│   ├── models/          # Message models and types
│   │   ├── messages.go
│   │   └── session.go
│   ├── store/           # Database layer
│   │   ├── database.go
│   │   └── submission_store.go
│   └── util/            # Utility functions
│       └── util.go
├── migrations/          # Database migrations
│   ├── 00001_submissions.sql
│   └── fs.go
├── compose.yaml         # Docker Compose configuration
├── Makefile            # Build and run commands
├── go.mod
└── go.sum
```

## Configuration

### Server Configuration

The server uses hardcoded configuration:
- **Port:** `1337`
- **Database:** PostgreSQL at `localhost:5432`
- **Database credentials:** `luxor/luxor` (database: `luxor`)
- **Job generation interval:** 30 seconds

To modify, edit `cmd/server/main.go`.

### Client Configuration

The client uses hardcoded configuration:
- **Server address:** `localhost:1337`
- **Submission interval:** 2 seconds (within rate limits)
- **Username:** Randomly generated (`user-{nonce}`)

To modify, edit `cmd/client/main.go`.

## Makefile Commands

- `make build` - Build both server and client
- `make build-server` - Build server only
- `make build-client` - Build client only
- `make run-server` - Build and run server
- `make run-client` - Build and run client
- `make test` - Run tests
- `make test-race` - Run tests with race detection
- `make docker-up` - Start Docker services
- `make docker-down` - Stop Docker services
- `make docker-reset` - Reset Docker volumes and restart
- `make clean` - Remove build artifacts

## Error Conditions

The server returns specific error messages for various conditions:

| Condition | Error Message |
|-----------|---------------|
| Invalid job_id | `"Task does not exist"` |
| Expired job_id | `"Task expired"` |
| Invalid SHA256 result | `"Invalid result"` |
| Rate limit exceeded | `"Submission too frequent"` |
| Duplicate client_nonce | `"Duplicate submission"` |
| Not authenticated | `"Not authenticated"` |

## License

This project was created as part of a technical challenge.
