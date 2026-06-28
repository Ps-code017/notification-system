# Notification System

A production-grade multi-channel notification system built to handle 
high-throughput delivery across email channels with guaranteed reliability,
idempotent processing, and failure recovery.

## Architecture

```
Client
    ↓
REST API (Express)
    ↓
Notification Service
(validates user, checks rate limit, saves to DB)
    ↓
Kafka Topic: "notifications" (fan-out)
    ↓
Email Worker
(dedup check → send email → update DB)
    ↓
┌─────────────────────────────────┐
│  Redis              Postgres    │
│  - deduplication   - delivery   │
│  - rate limiting     tracking   │
└─────────────────────────────────┘
```

## Key Engineering Decisions

**Why Kafka for fan-out?**
A single notification request triggers delivery across multiple channels
independently. Kafka allows separate workers to consume the same event
without coupling — adding a new channel means deploying a new worker,
not modifying existing code. Each worker fails and retries independently
without affecting other channels.

**Why Redis for deduplication?**
Kafka guarantees at-least-once delivery — the same message can arrive
twice under failure conditions (worker crashes after sending but before
acknowledging). Before processing any message, workers check a Redis key
(`dedup:notificationId:channel`). If it exists, the message is a duplicate
and gets skipped. Keys expire after 24 hours automatically via Redis TTL
so memory never grows unbounded.

**Why Postgres transactions for DB writes?**
Creating a notification involves inserting into `notifications` and
inserting one row per channel into `delivery_attempts`. These must
succeed together or not at all. If the `delivery_attempts` insert fails
halfway, the notification row must also be rolled back — otherwise the
worker finds a notification with missing delivery rows and silently
never delivers it. `BEGIN/COMMIT/ROLLBACK` guarantees this atomicity.

**Why exponential backoff for retries?**
Temporary failures (SMTP overload, network hiccup) are recoverable.
Immediately retrying hammers an already-struggling service. Backoff
doubles the wait between attempts (`5s → 10s → 20s`), giving the
failing service time to recover. After 3 total attempts the delivery
is permanently marked failed — the `attempt_count` column persists
this count across Kafka redeliveries so worker restarts don't reset
the counter.

**Why rate limiting in Redis and not Postgres?**
Rate limit checks happen on every notification request — before any DB
writes or Kafka publishes. At scale this runs millions of times per day.
Postgres requires a disk read per check. Redis atomic `INCR` with
TTL-based window reset handles this in microseconds. The `INCR` operation
is atomic at the Redis server level — two simultaneous requests can't
both read the same count and both think they're under the limit.

**Why 202 Accepted and not 200 OK?**
`200 OK` means the work is complete. When the API responds, the email
hasn't been sent yet — it's been persisted to Postgres and published
to Kafka. `202 Accepted` is semantically honest: "I received your
request and queued it for processing." Using 200 would be lying to
the client about the state of their request.

## System Design Highlights

- **Fan-out pattern** — one Kafka event triggers independent delivery
  per channel, each tracked separately in `delivery_attempts`
- **Idempotent processing** — Redis deduplication prevents duplicate
  sends even when Kafka redelivers the same message
- **Atomic DB writes** — Postgres transactions ensure zero partial
  state across notification and delivery_attempt inserts
- **Exponential backoff** — 3-attempt retry with doubling delays
  (5s→10s→20s) before permanent failure
- **Per-user rate limiting** — Redis atomic INCR with TTL-based window
  reset, max 5 notifications per channel per hour
- **User preference engine** — per-channel opt-in/out checked before
  any DB writes or Kafka publishes — disabled channels are never queued
- **Correct failure ordering** — DB committed before Redis dedup key
  is set, ensuring a failed Redis write triggers retry rather than
  silent data loss
- **Graceful shutdown** — SIGTERM/SIGINT handlers cleanly disconnect
  Kafka consumers before process exit, preventing ghost consumer
  issues in the consumer group
- **Health check endpoint** — `/health` queries `SELECT 1` to verify
  live DB connectivity, suitable for load balancer liveness probes

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| API | Node.js + Express | REST endpoints |
| Queue | Apache Kafka + Zookeeper | Async fan-out, at-least-once delivery |
| Cache | Redis (ioredis) | Deduplication + rate limiting |
| Database | PostgreSQL | Source of truth for all state |
| Email | Nodemailer + Mailtrap | SMTP delivery (sandbox testing) |
| Logging | Winston | Structured timestamped logs |

## Database Schema

```
users
  id UUID PK, email, phone, fcm_token
  email_enabled, sms_enabled, push_enabled
  created_at, updated_at

notifications
  id UUID PK, user_id FK → users
  type, title, message
  status (pending → delivered | failed)
  created_at, updated_at

delivery_attempts
  id UUID PK, notification_id FK → notifications
  channel (email | sms | push)
  status (pending → processing → delivered | failed)
  attempt_count, last_attempted_at, delivered_at
  error_message, created_at
```

**Why three tables?**
One notification fans out to N channels. Each channel tracks delivery
independently — email can succeed while SMS fails and retries without
touching the email record. `attempt_count` and `error_message` per row
give complete visibility into every delivery attempt per channel.

**Why UUID primary keys?**
Auto-increment integers require the database to be the single authority
assigning IDs — a bottleneck in distributed systems. UUIDs are generated
independently by any server without coordination. `gen_random_uuid()` is
built into Postgres 13+ with no extension needed.

## API Reference

### Users
```
POST   /api/users
       Create a new user
       Body: { email, phone?, fcmToken? }
       Returns: 201 + user object

GET    /api/users/:id
       Fetch user profile and preferences
       Returns: 200 + user object

PATCH  /api/users/:id/preferences
       Update channel preferences (partial update)
       Body: { emailEnabled?, smsEnabled?, pushEnabled? }
       Returns: 200 + updated user

GET    /api/users/:id/rate-limit-status
       Check current rate limit usage across all channels
       Returns: 200 + { rateLimits: { email, sms, push } }
```

### Notifications
```
POST   /api/notifications
       Trigger a notification for a user
       Body: { userId, type, title, message }
       Returns: 202 Accepted + notificationId
       Returns: 429 Too Many Requests if rate limit exceeded
                (includes Retry-After header)

GET    /api/notifications/:id
       Check delivery status + all delivery attempts
       Returns: 200 + { notification, deliveryAttempts[] }
```

## Notification Lifecycle

```
POST /api/notifications
         ↓
validate request fields
         ↓
verify user exists + fetch preferences
         ↓
check rate limit (Redis) ──→ 429 if exceeded
         ↓
BEGIN transaction
  INSERT notifications   (status: pending)
  INSERT delivery_attempts per channel (status: pending)
COMMIT
         ↓
increment rate limit counter (Redis)
         ↓
publish to Kafka
         ↓
[worker picks up message]
         ↓
dedup check (Redis) ──→ skip if duplicate
         ↓
check attempt_count ──→ skip if >= MAX_ATTEMPTS
         ↓
fetch user email
         ↓
withRetry (max 3 attempts, exponential backoff)
  │
  ├── attempt:
  │     BEGIN transaction
  │       UPDATE delivery_attempts → processing
  │       sendEmail()
  │       UPDATE delivery_attempts → delivered
  │       check all channels done
  │       UPDATE notifications → delivered (if all done)
  │     COMMIT
  │
  ├── on failure → ROLLBACK → wait → retry
  │
  └── on exhausted → permanentlyFail()
         ↓
markAsProcessed (Redis dedup key set)
```

## Project Structure

```
notification-system/
├── src/
│   ├── api/
│   │   └── routes/
│   │       ├── notification.routes.js
│   │       └── user.routes.js
│   ├── services/
│   │   ├── kafka.service.js
│   │   ├── redis.service.js
│   │   ├── notification.service.js
│   │   └── email.service.js
│   ├── workers/
│   │   └── email.worker.js
│   ├── db/
│   │   ├── postgres.js
│   │   ├── schema.sql
│   │   └── migrate.js
│   ├── config/
│   │   └── index.js
│   └── utils/
│       ├── logger.js
│       └── retry.js
├── .env.example
├── .gitignore
└── server.js
```

## Running Locally

**Prerequisites:** Node.js 18+, Docker (for Postgres, Redis, Kafka)

```bash
git clone https://github.com/yourusername/notification-system
cd notification-system
npm install
cp .env.example .env
# Add your Mailtrap SMTP credentials to .env
```

Start dependencies:
```bash
docker compose up -d postgres redis zookeeper kafka
```

Run migration:
```bash
npm run migrate
```

Start API server and worker in separate terminals:
```bash
# Terminal 1
npm run dev

# Terminal 2
npm run worker:email:dev
```

Verify:
```bash
curl http://localhost:3000/health
# { "status": "ok", "db": "connected" }
```

## What I Would Add With More Time

- **Outbox pattern** — eliminate the gap between Postgres commit and
  Kafka publish by writing the Kafka message inside the DB transaction
  and publishing via a separate reliable relay process
- **Dead letter queue** — route permanently failed messages to a
  separate Kafka topic for inspection and manual replay
- **Prometheus metrics** — counters for delivery rate, failure rate,
  retry rate, and rate limit hits per channel
- **SMS + Push channels** — independent workers following identical
  architecture, Twilio for SMS and Firebase FCM for push
- **Full Docker Compose** — single `docker compose up` orchestration
  with healthchecks and correct startup ordering
- **Admin dashboard** — minimal UI for viewing delivery status,
  failure reasons, and rate limit usage across users