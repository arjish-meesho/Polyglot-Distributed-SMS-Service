# Polyglot Distributed SMS Service — Full Documentation

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Complete System Flow](#4-complete-system-flow)
5. [Service Details — Java SMS Sender](#5-service-details--java-sms-sender)
6. [Service Details — Go SMS Store](#6-service-details--go-sms-store)
7. [Error Handling](#7-error-handling)
8. [Test Suite](#8-test-suite)
9. [Infrastructure & Docker Setup](#9-infrastructure--docker-setup)
10. [Configuration & Environment Variables](#10-configuration--environment-variables)
11. [API Reference](#11-api-reference)
12. [Design Decisions](#12-design-decisions)

---

## 1. Project Overview

This is a **polyglot, event-driven microservices system** built to simulate a production-grade SMS processing pipeline. It demonstrates how two services written in different languages — **Java** and **Go** — can work together over **Apache Kafka** to handle SMS ingestion and storage asynchronously.

The system was built and hardened at Meesho as a learning project covering distributed systems patterns including event-driven architecture, resilience engineering, input validation, graceful shutdown, and proper test coverage.

**What it does:**
- Accepts an SMS request via a REST API
- Validates the request (phone number format, message length)
- Checks if the user is blocked (via Redis)
- Simulates sending via a 3rd-party vendor (80% success rate)
- Publishes the event to Kafka with an idempotency key
- A Go worker consumes the event and stores it in MongoDB
- A second API serves the stored SMS history with pagination

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          CLIENT                                      │
│              POST /v1/sms/send                                       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
          ┌────────────────────────────────────┐
          │   SMS SENDER  (Java / Port 8080)   │
          │                                    │
          │  1. Validate input                 │
          │  2. Check Redis blocklist          │──────► Redis
          │  3. Simulate vendor call           │
          │  4. Publish SmsEvent to Kafka      │──────► Kafka
          │  5. Return HTTP response           │
          └────────────────────────────────────┘
                           │
                  Kafka topic: sms_events
                           │
                           ▼
          ┌────────────────────────────────────┐
          │   SMS STORE   (Go / Port 8081)     │
          │                                    │
          │  1. Consume from Kafka             │◄───── Kafka
          │  2. Skip FAILED events             │
          │  3. Save to MongoDB                │──────► MongoDB
          │  4. Commit Kafka offset            │
          │                                    │
          │  Also serves:                      │
          │  GET /v1/user/{phone}/messages     │
          └────────────────────────────────────┘
```

**Why polyglot?**
Java is used for the ingestion service because it has mature frameworks for REST APIs, Redis integration, and Kafka producers (Spring Boot ecosystem). Go is used for the storage worker because it has excellent concurrency primitives for high-throughput Kafka consumption and is lightweight for long-running background processes.

---

## 3. Technology Stack

### Java Service (`sms-sender`)

| Dependency | Version | Purpose |
|---|---|---|
| **Spring Boot** | 3.2.3 | Framework for building the REST API, dependency injection, auto-configuration |
| **spring-boot-starter-web** | (managed) | Embeds Tomcat, enables `@RestController`, `@RequestMapping`, HTTP request/response handling |
| **spring-boot-starter-data-redis** | (managed) | Integrates with Redis via `StringRedisTemplate` for blocklist lookup |
| **spring-kafka** | (managed) | Kafka producer via `KafkaTemplate` for publishing `SmsEvent` objects as JSON |
| **spring-boot-starter-validation** | (managed) | Enables Jakarta Bean Validation — `@NotBlank`, `@Pattern`, `@Size` on DTOs |
| **Lombok** | 1.18.46 | Generates boilerplate code at compile time: `@Data` (getters/setters), `@AllArgsConstructor` |
| **spring-boot-starter-test** | (managed) | Test infrastructure: JUnit 5, Mockito, MockMvc, AssertJ |
| **Mockito** | 5.23.0 | Mocking framework for unit tests — creates fake versions of Redis, Kafka, etc. |
| **byte-buddy** | 1.17.7 | Bytecode manipulation library used internally by Mockito for creating mock classes |
| **Java** | 17 (target) | Language version |
| **Maven** | 3.9.x | Build tool — compiles, runs tests, packages into a JAR |

**Maven Plugins:**
| Plugin | Purpose |
|---|---|
| `spring-boot-maven-plugin` | Packages the app as a self-contained executable JAR |
| `maven-compiler-plugin` | Configured with Lombok in `annotationProcessorPaths` so Lombok generates code during compilation |
| `maven-surefire-plugin` | Runs the unit tests; configured with `--add-opens` JVM flags for Mockito compatibility with JDK 25 |

---

### Go Service (`sms-store`)

| Dependency | Version | Purpose |
|---|---|---|
| **kafka-go** (`github.com/segmentio/kafka-go`) | 0.4.51 | Kafka consumer — reads messages from the `sms_events` topic, supports manual offset commits |
| **mongo-driver** (`go.mongodb.org/mongo-driver`) | 1.17.9 | MongoDB client — connects, pings, inserts records, runs queries with filters and pagination |
| **Go stdlib** | 1.26.3 | `net/http` (HTTP server + routing), `encoding/json` (serialisation), `context` (timeouts, cancellation), `os/signal` (shutdown), `log` (structured logging), `math/rand` (backoff jitter) |

**Indirect dependencies (transitive, not used directly):**
| Dependency | Purpose |
|---|---|
| `klauspost/compress`, `golang/snappy`, `pierrec/lz4` | Compression codecs used internally by kafka-go |
| `xdg-go/pbkdf2`, `xdg-go/scram`, `youmark/pkcs8` | Authentication mechanisms for MongoDB (SCRAM-SHA auth) |
| `golang.org/x/crypto`, `x/sync`, `x/text` | Cryptography, concurrency, and text utilities used by mongo-driver |

---

### Infrastructure

| Tool | Version | Purpose |
|---|---|---|
| **Docker** | — | Containerises each service for consistent environments |
| **Docker Compose** | v3.8 | Orchestrates all 6 containers locally: defines services, networks, environment variables, healthchecks, and startup order |
| **Apache Kafka** (Confluent) | 7.4.0 | Message broker — decouples the Java ingestion service from the Go storage service. Topic: `sms_events` |
| **Apache Zookeeper** (Confluent) | 7.4.0 | Cluster coordination for Kafka (required by this version of Kafka) |
| **Redis** | 7.2.4-alpine | In-memory key-value store — used for the SMS blocklist (`blocked_users` set) |
| **MongoDB** | 7.0.5 | Document database — stores SMS records with phone number, message, status, timestamps |

---

## 4. Complete System Flow

### Sending an SMS (end-to-end)

```
Step 1 — Client sends request
  POST /v1/sms/send
  Body: { "phoneNumber": "9876543210", "message": "Hello!", "requestId": "abc-123" }

Step 2 — Input validation (Java)
  @Valid on the request body runs Bean Validation:
  - phoneNumber must match regex: ^[6-9]\d{9}$
  - message must be 1–160 characters
  If invalid → HTTP 400 returned immediately. No Redis/Kafka calls.

Step 3 — Redis blocklist check (Java)
  redisTemplate.opsForSet().isMember("blocked_users", "9876543210")
  If user is blocked → throw UserBlockedException → HTTP 422
  If Redis is down → RedisConnectionFailureException → HTTP 503

Step 4 — Vendor simulation (Java)
  vendorSimulator.getAsBoolean() → true (SUCCESS) or false (FAILED)
  Uses Math.random() > 0.2 in production (80% success, 20% failure)
  Result determines the "status" field in the event

Step 5 — Build event (Java)
  SmsEvent {
    requestId: "abc-123" (client-provided) or UUID.randomUUID() (server-generated)
    phoneNumber: "9876543210"
    message: "Hello!"
    status: "SUCCESS" or "FAILED"
    timestamp: "2026-06-04T08:11:58.803Z"
  }

Step 6 — Publish to Kafka (Java)
  kafkaTemplate.send("sms_events", event).get()
  .get() BLOCKS until Kafka confirms the message was received
  If Kafka fails → KafkaException → HTTP 503

Step 7 — HTTP response returned (Java)
  SUCCESS: HTTP 200 { "status": "SUCCESS", "message": "SMS processed successfully." }
  FAILED vendor: HTTP 502 { "status": "FAILED", "error": "Vendor Failure", "message": "..." }

Step 8 — Go consumer receives the event (async)
  kafka.Reader.FetchMessage(ctx) → kafka.Message
  Deserialise JSON bytes → models.SMSRecord

Step 9 — FAILED event filter (Go)
  If smsRecord.Status == "FAILED":
    log "Skipping FAILED event"
    return nil  ← treated as success so offset gets committed
  The event stays in Kafka as an audit log but is never written to MongoDB

Step 10 — Save to MongoDB (Go)
  record.CreatedAt = time.Now().UTC()
  collection.InsertOne(ctx, record)  ← 5-second timeout

Step 11 — Commit Kafka offset (Go)
  reader.CommitMessages(ctx, msg)
  Only committed AFTER successful save — guarantees no message loss on restart

Step 12 — Retrieving history
  GET /v1/user/9876543210/messages?page=1&limit=10
  MongoDB query: { phoneNumber: "9876543210" }
  With: skip=(page-1)*limit, limit=limit, sort={createdAt: -1}
  Returns JSON array, newest messages first
```

---

## 5. Service Details — Java SMS Sender

**Location:** `sms-sender/`
**Entry point:** `SmsSenderApplication.java`
**Port:** 8080

### Package Structure

```
src/main/java/com/meesho/sms/
├── SmsSenderApplication.java       ← Spring Boot entry point + BooleanSupplier bean
├── controller/
│   └── SmsController.java          ← POST /v1/sms/send endpoint
├── service/
│   └── SmsService.java             ← Business logic (blocklist, vendor, Kafka)
├── dto/
│   ├── SmsRequest.java             ← Input model with validation annotations
│   ├── SmsResponse.java            ← Response model
│   └── SmsEvent.java               ← Kafka message model
└── exception/
    ├── GlobalExceptionHandler.java  ← @ControllerAdvice — all HTTP error mapping
    ├── UserBlockedException.java    ← Thrown when user is in Redis blocklist
    └── VendorFailureException.java  ← Thrown when vendor simulation fails
```

### Key Classes

**`SmsService.java`** — The core business logic:
1. Checks Redis blocklist using `StringRedisTemplate`
2. Calls `vendorSimulator.getAsBoolean()` (the injectable 80/20 simulator)
3. Builds `SmsEvent` with `requestId`, `phoneNumber`, `message`, `status`, `timestamp`
4. Publishes to Kafka with `.get()` (blocking) to detect failures
5. Throws `UserBlockedException` or `VendorFailureException` instead of returning FAILED responses

**`GlobalExceptionHandler.java`** — Central HTTP error mapping:
- `MethodArgumentNotValidException` → **400** Bad Request
- `UserBlockedException` → **422** Unprocessable Entity
- `VendorFailureException` → **502** Bad Gateway
- `RedisConnectionFailureException` → **503** Service Unavailable
- `KafkaException` → **503** Service Unavailable
- `Exception` (catch-all) → **500** Internal Server Error

**`SmsSenderApplication.java`** — Registers the vendor simulator as a Spring bean:
```java
@Bean
public BooleanSupplier vendorSimulator() {
    return () -> Math.random() > 0.2;  // 80% success, 20% failure
}
```
This makes the simulator injectable and testable without changing production behaviour.

---

## 6. Service Details — Go SMS Store

**Location:** `sms-store/`
**Entry point:** `main.go`
**Port:** 8081

### Package Structure

```
sms-store/
├── main.go                    ← Entry point, wiring, graceful shutdown
├── models/
│   └── sms.go                 ← SMSRecord struct (PhoneNumber, Message, Status, RequestId, Timestamp, CreatedAt)
├── database/
│   └── mongo.go               ← MongoDB connection, index creation, SaveSMS, GetSMSHistory
├── kafka/
│   └── consumer.go            ← MessageReader interface, Consumer struct, Start() loop, backoff logic
└── handlers/
    └── api.go                 ← SMSStore interface, Server struct, GetMessagesHandler
```

### Key Components

**`database/mongo.go`** — MongoDB layer:
- `InitDB()`: connects using `MONGO_URI` env var, pings to verify connectivity, creates compound index `(phoneNumber ASC, createdAt DESC)`, calls `log.Fatal` if either step fails
- `SaveSMS()`: stamps `CreatedAt = time.Now().UTC()`, inserts with 5-second timeout
- `GetSMSHistory()`: queries by phone number with skip/limit pagination, 10-second timeout, sorted newest-first

**`kafka/consumer.go`** — Kafka consumer:
- `MessageReader` interface: abstracts `kafka.Reader` so tests don't need a real Kafka broker
- `backoffDuration(attempt int)`: exponential backoff with jitter (1s → 2s → 4s → cap 60s, ±25% random)
- `ProcessMessage()`: deserialises JSON → skips FAILED events → saves to DB
- `Start(ctx context.Context)`: main loop — fetches messages, handles poison pills, retries DB failures, commits offsets, exits cleanly on context cancellation

**`main.go`** — Wiring and lifecycle:
- `signal.NotifyContext` for SIGTERM/SIGINT handling
- Supervised consumer goroutine with `defer recover()` — restarts on panic
- `http.Server.Shutdown(shutdownCtx)` for graceful HTTP drain on shutdown

---

## 7. Error Handling

### Java Service

| Error Situation | Where Caught | What Happens |
|---|---|---|
| Invalid phone number / empty message | `GlobalExceptionHandler` via `@Valid` | HTTP 400, describes which field failed |
| User is in Redis blocklist | `SmsService` throws `UserBlockedException` | HTTP 422, "User Blocked" |
| Vendor call fails (20% simulation) | `SmsService` throws `VendorFailureException` | HTTP 502, "Vendor Failure" — event is still published to Kafka |
| Redis is unreachable | `GlobalExceptionHandler` catches `RedisConnectionFailureException` | HTTP 503, "Cache Service Unavailable" |
| Kafka publish fails | `SmsService` catches `ExecutionException` on `.get()`, rethrows as `KafkaException` | HTTP 503, "Message Broker Unavailable" |
| Any unexpected error | `GlobalExceptionHandler` catches `Exception` (catch-all) | HTTP 500, "Internal Server Error" |

### Go Service

| Error Situation | Where Handled | What Happens |
|---|---|---|
| Invalid JSON in Kafka message (poison pill) | `ProcessMessage()` returns `ErrInvalidJSON` | Message is logged and skipped, Kafka offset is committed so it's never reprocessed |
| FAILED vendor event | `ProcessMessage()` checks `Status == "FAILED"` | Skipped silently, offset committed. Stays in Kafka as audit log, never written to MongoDB |
| MongoDB unavailable | `Start()` inner retry loop | Blocks on the same message with exponential backoff (1s→2s→4s→…→60s). Resumes when DB recovers. Kafka offset is NOT committed until save succeeds |
| Kafka broker unavailable | `Start()` fetch error handler | Logs the error, sleeps with exponential backoff, retries fetch |
| MongoDB uninitialized (nil collection) | `SaveSMS()` / `GetSMSHistory()` nil guard | Returns `fmt.Errorf("database not initialized")` |
| MongoDB startup ping failure | `InitDB()` | `log.Fatalf` — app crashes immediately with a clear error rather than starting in a broken state |
| Consumer goroutine panic | `main.go` `defer recover()` wrapper | Panic is logged, goroutine restarts after 5 seconds |
| HTTP handler DB failure | `GetMessagesHandler()` | Logs the error with user ID, returns HTTP 500 |
| Server startup failure | `main.go` goroutine | `log.Fatal` — exits with code 1 so Docker/Kubernetes knows to restart |

---

## 8. Test Suite

### Java Tests — 10 tests across 3 files

**`SmsServiceTest.java`** — Unit tests for business logic
Uses `@ExtendWith(MockitoExtension.class)`. Redis, Kafka, and the vendor simulator are all mocked.

| Test | What it verifies |
|---|---|
| `shouldThrowUserBlockedExceptionWhenUserIsBlocked` | Redis returns blocked=true → `UserBlockedException` is thrown, Kafka is never called |
| `shouldReturnSuccessAndSendToKafkaWhenVendorSucceeds` | Vendor returns true → HTTP 200 response, Kafka called exactly once |
| `shouldThrowVendorFailureExceptionAndStillPublishToKafkaWhenVendorFails` | Vendor returns false → `VendorFailureException` thrown, but Kafka is still called (event published for audit) |

**`SmsControllerTest.java`** — Unit tests for the HTTP layer
Uses `@WebMvcTest` (loads only the web layer, mocks the service).

| Test | What it verifies |
|---|---|
| `shouldAcceptRequestAndReturnSuccess` | Valid request → HTTP 200 with correct JSON body |
| `shouldReturn422WhenUserIsBlocked` | Service throws `UserBlockedException` → HTTP 422 with `"error": "User Blocked"` |
| `shouldReturn502WhenVendorFails` | Service throws `VendorFailureException` → HTTP 502 with `"error": "Vendor Failure"` |

**`GlobalExceptionHandlerTest.java`** — Unit tests for all error handlers
Uses `@WebMvcTest` with both controller and handler loaded.

| Test | What it verifies |
|---|---|
| `shouldReturnBadRequestWhenRequestIsInvalid` | Blank phoneNumber → HTTP 400 with `"error": "Validation Failed"` |
| `shouldReturnServiceUnavailableWhenRedisFails` | `RedisConnectionFailureException` → HTTP 503 with `"error": "Cache Service Unavailable"` |
| `shouldReturnServiceUnavailableWhenKafkaFails` | `KafkaException` → HTTP 503 with `"error": "Message Broker Unavailable"` |
| `shouldReturnInternalServerErrorForUnexpectedException` | `RuntimeException` → HTTP 500 with `"error": "Internal Server Error"` |

---

### Go Tests — 15 tests across 3 files

**`kafka/consumer_test.go`** — Unit tests for message processing + Start() loop

*ProcessMessage unit tests:*

| Test | What it verifies |
|---|---|
| `TestProcessMessage_Success` | Valid SUCCESS JSON → record saved, no error returned |
| `TestProcessMessage_InvalidJSON` | Malformed JSON → `ErrInvalidJSON` returned (not a generic error) |
| `TestProcessMessage_DatabaseFailure` | DB error → error returned and it is NOT `ErrInvalidJSON` |
| `TestProcessMessage_FailedStatus` | `status: "FAILED"` → `nil` returned, `SaveSMS` never called |

*Start() loop integration tests (using MockReader — no real Kafka needed):*

| Test | What it verifies |
|---|---|
| `TestStart_SuccessfulMessage` | One valid message → saved to DB, offset committed |
| `TestStart_PoisonPillSkipped` | Invalid JSON → `SaveSMS` not called, offset still committed |
| `TestStart_FailedEventSkipped` | FAILED status event → `SaveSMS` not called, offset still committed |
| `TestStart_DBRetryAndRecovery` | DB fails first 2 attempts, succeeds on 3rd → `SaveSMS` called 3 times, offset committed once after recovery |

**`handlers/api_test.go`** — Unit tests for the HTTP handler

| Test | What it verifies |
|---|---|
| `TestGetMessagesHandler_Success` | Mock DB returns data → HTTP 200 |
| `TestGetMessagesHandler_DBError` | Mock DB returns error → HTTP 500 |
| `TestGetMessagesHandler_PaginationParams` | `?page=2&limit=5` in URL → HTTP 200 (params pass through) |

**`database/mongo_test.go`** — Integration test (requires real MongoDB)

| Test | What it verifies |
|---|---|
| `TestSaveAndGetSMS` | Connects to MongoDB, saves a record, retrieves it by phone number, confirms the message is present |

---

## 9. Infrastructure & Docker Setup

**File:** `docker-compose.yml`

All services run in an isolated Docker network. Only the application ports (8080, 8081, 9092) are exposed to the host. Internal services (MongoDB, Redis) are not accessible from outside Docker.

### Service Startup Order

Healthchecks ensure services start only when their dependencies are actually ready — not just when the container starts:

```
zookeeper  → (no deps)
redis      → (no deps)       healthcheck: redis-cli ping
mongodb    → (no deps)       healthcheck: mongosh ping
kafka      → zookeeper       healthcheck: kafka-broker-api-versions
java-api   → redis (healthy), kafka (healthy)
go-store   → mongodb (healthy), kafka (healthy)
```

### Service Configuration

| Service | Image | Port (host) | Credentials |
|---|---|---|---|
| redis | `redis:7.2.4-alpine` | not exposed | none (internal only) |
| mongodb | `mongo:7.0.5` | not exposed | `smsuser` / `smspassword` |
| zookeeper | `confluentinc/cp-zookeeper:7.4.0` | not exposed | — |
| kafka | `confluentinc/cp-kafka:7.4.0` | 9092 | — |
| java-api | custom build | 8080 | — |
| go-store | custom build | 8081 | — |

### Kafka Topic

The topic `sms_events` is auto-created on Kafka startup via `KAFKA_CREATE_TOPICS: "sms_events:1:1"` (1 partition, 1 replica). This is sufficient for local development; production would use multiple partitions for horizontal scaling.

### Dockerfiles

**Java** (`sms-sender/Dockerfile`) — Multi-stage build:
1. `maven:3-eclipse-temurin-17` image runs `mvn package` to build the JAR
2. `eclipse-temurin:17-jre-alpine` (lightweight) runs `java -jar app.jar`

**Go** (`sms-store/Dockerfile`) — Multi-stage build:
1. `golang:1.26-alpine` downloads dependencies and builds the binary
2. `alpine:latest` (tiny, ~5MB) runs the compiled binary

---

## 10. Configuration & Environment Variables

### Java Service

Configured in `sms-sender/src/main/resources/application.yml`. All values support environment variable overrides with Docker defaults:

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP port |
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |

### Go Service

Read at startup using `os.Getenv()` with in-code defaults:

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://mongodb:27017` | Full MongoDB connection string (include credentials in URI for auth) |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka broker address |

### Docker Compose `environment:` overrides

```yaml
java-api:
  environment:
    REDIS_HOST: redis
    REDIS_PORT: 6379
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092

go-store:
  environment:
    MONGO_URI: "mongodb://smsuser:smspassword@mongodb:27017"
    KAFKA_BROKERS: kafka:9092
```

---

## 11. API Reference

### POST `/v1/sms/send` — Java Service (port 8080)

**Request:**
```json
{
  "phoneNumber": "9876543210",
  "message": "Hello from the polyglot gateway!",
  "requestId": "optional-uuid"
}
```

| Field | Required | Rules |
|---|---|---|
| `phoneNumber` | Yes | 10-digit Indian mobile number (starts 6–9) |
| `message` | Yes | 1 to 160 characters |
| `requestId` | No | Client-provided idempotency key; server generates UUID if absent |

**Responses:**

| HTTP Status | When | Body |
|---|---|---|
| 200 OK | SMS accepted and event published to Kafka | `{"status": "SUCCESS", "message": "SMS processed successfully."}` |
| 400 Bad Request | Validation failed | `{"status": "FAILED", "error": "Validation Failed", "message": "phoneNumber: ..."}` |
| 422 Unprocessable Entity | User is blocklisted | `{"status": "FAILED", "error": "User Blocked", "message": "User ... is blocked"}` |
| 502 Bad Gateway | Vendor simulation failed | `{"status": "FAILED", "error": "Vendor Failure", "message": "SMS failed at vendor."}` |
| 503 Service Unavailable | Redis or Kafka is down | `{"status": "FAILED", "error": "Cache/Broker Unavailable", "message": "..."}` |
| 500 Internal Server Error | Unexpected error | `{"status": "FAILED", "error": "Internal Server Error", "message": "..."}` |

---

### GET `/v1/user/{phoneNumber}/messages` — Go Service (port 8081)

**Query parameters:**

| Parameter | Default | Max | Description |
|---|---|---|---|
| `page` | 1 | — | Page number |
| `limit` | 20 | 100 | Records per page |

**Example:** `GET http://localhost:8081/v1/user/9876543210/messages?page=1&limit=5`

**Response (200 OK):**
```json
[
  {
    "phoneNumber": "9876543210",
    "message": "Hello from the polyglot gateway!",
    "status": "SUCCESS",
    "requestId": "de3dc6a5-b8d0-4933-8b74-71391391e299",
    "timestamp": "2026-06-04T08:11:58.803Z",
    "createdAt": "2026-06-04T08:11:59.669Z"
  }
]
```

Results are sorted newest-first by `createdAt`. Only SUCCESS messages are returned (FAILED vendor events are filtered at the consumer and never stored).

**Error responses:**

| HTTP Status | When |
|---|---|
| 200 with empty array `[]` | No messages found for this phone number |
| 500 Internal Server Error | MongoDB query failed |

---

## 12. Design Decisions

### Why is the Go consumer the one that filters FAILED events, not the Java service?

The Kafka topic `sms_events` is a complete audit log of everything that happened — every SMS attempt, whether it succeeded or failed at the vendor. If Java only published SUCCESS events, that data would be permanently lost. Future consumers (an alerting service, a reporting dashboard) might legitimately need FAILED events. The Go consumer's job is to decide what to persist in the user-facing database — so it's the right place to filter.

### Why does the Kafka publish block (`.get()`) instead of fire-and-forget?

The REST API must report delivery status synchronously. If Kafka publish is async and fails after the HTTP response has been sent, the client believes the SMS is queued when it isn't. The ~5-15ms blocking cost for a local Kafka write is acceptable for this API. The `KafkaException` that surfaces from a failed publish is caught by `GlobalExceptionHandler` and returns HTTP 503 to the client.

### Why throw exceptions for business failures (UserBlocked, VendorFailure) instead of returning a FAILED response body?

HTTP status codes exist precisely for this. Returning HTTP 200 with `"status": "FAILED"` in the body forces every client to parse the body to detect failure — they can't rely on standard HTTP semantics. Exceptions propagate to `GlobalExceptionHandler` which maps them to the correct HTTP status codes (422, 502), keeping the controller completely clean.

### Why is the vendor simulator an injectable `BooleanSupplier` instead of `Math.random()` in the service?

`Math.random()` hardcoded in the service made every test that reached the vendor path non-deterministic — 20% of the time a test expecting SUCCESS would get FAILED. The `BooleanSupplier` approach keeps the exact same 80/20 production behaviour but lets tests pin the outcome to `true` or `false`. It's a one-line change to production behaviour, and a significant improvement to test reliability.

### Why use a `MessageReader` interface for the Kafka consumer?

`kafka.Reader` is a concrete struct with no interface. Without abstraction, testing `Start()` requires a real running Kafka broker — not acceptable for unit tests. Defining a `MessageReader` interface that `kafka.Reader` satisfies means tests can inject a `MockReader` that plays back a deterministic sequence of messages. The `if c.Reader == nil { create real reader }` fallback means `main.go` doesn't need to change — the interface is transparent to production code.

### Why exponential backoff with jitter instead of a fixed sleep?

A fixed 5-second sleep between DB retries means if multiple consumer instances restart simultaneously (e.g., after a deployment), they all hammer the recovering database at exactly the same intervals. Exponential backoff reduces load as the outage continues. The ±25% random jitter spreads out concurrent retries so they don't all hit at the same time. The delay progression (1s → 2s → 4s → … → 60s cap) is the standard pattern from AWS architecture best practices.

### Why add `client.Ping()` at startup?

`mongo.Connect()` does not open a TCP connection — it creates a client pool that connects lazily on first use. Without a ping, the Go service starts successfully even when MongoDB is completely unreachable. The first message from Kafka then calls `SaveSMS`, which either panics (nil pointer) or returns a confusing error. Pinging at startup produces a clear fatal error at boot time: "Failed to connect to MongoDB: connection refused".

### Why is pagination offset-based (`?page=1&limit=20`) rather than cursor-based?

Cursor-based pagination is superior for large datasets with frequent writes (it avoids the "page drift" problem) but requires exposing a cursor token in the API response, which adds complexity. For SMS history per user, the dataset is bounded (thousands of messages at most, not billions), and callers are likely UIs doing "load next page". The added complexity of cursor tokens is not justified at this scale.

### Why are MongoDB and Redis not exposed on host ports?

Internal services should only be accessible within the Docker network. Exposing MongoDB port 27017 to the host machine with no credentials means anyone on the local network (or, if misconfigured, the internet) can read and write all SMS data. Removing the host port mappings means the services are only reachable by other containers in the same Docker Compose network.

### Why use `depends_on: condition: service_healthy` instead of bare `depends_on`?

`depends_on: kafka` only waits for the container process to start. Kafka takes ~15 seconds after the container starts to elect a leader and become ready to accept connections. Without the healthcheck condition, `java-api` and `go-store` start before Kafka is ready, fail to connect, and require a manual restart. The `condition: service_healthy` makes Docker Compose wait until each service passes its health check before starting dependent services.
