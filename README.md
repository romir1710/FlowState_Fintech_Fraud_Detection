<div align="center">

<h1>⚡ FlowState</h1>
<h3>Real-Time Transaction Fraud Detection Pipeline</h3>

<p>
  <img src="https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge&logo=typescript&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-7.6-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Next.js-16-000000?style=for-the-badge&logo=nextdotjs&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
</p>

<p>
  An event-driven, real-time fraud detection pipeline that ingests a high-frequency simulated payment stream via Apache Kafka, performs velocity-based risk scoring using Redis, persists flagged transactions in PostgreSQL, and broadcasts live results to a Next.js frontend over WebSockets.
</p>

</div>

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Fraud Detection Logic](#fraud-detection-logic)
- [Redis Velocity Design](#redis-velocity-design)
- [API & Data Contracts](#api--data-contracts)
- [Infrastructure Services](#infrastructure-services)
- [License](#license)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Docker Network                            │
│                                                                  │
│  ┌───────────┐    ┌───────────────────────────────────────────┐  │
│  │ Producer  │───▶│  Kafka Topic: transaction-events (3 part) │  │
│  │ (Node.js) │    └──────────────────────┬────────────────────┘  │
│  └───────────┘                           │                        │
│                                          ▼                        │
│                              ┌───────────────────┐               │
│                              │   Consumer         │               │
│                              │   (Node.js)        │               │
│                              │                    │               │
│                              │  1. Parse message  │               │
│                              │  2. Redis INCR     │──▶ Redis      │
│                              │     velocity check │    (fixed     │
│                              │  3. Score (0-100)  │     window)   │
│                              │  4. riskScore>75?  │               │
│                              │     → PostgreSQL   │──▶ Postgres   │
│                              │  5. Broadcast all  │               │
│                              └────────┬──────────┘               │
│                                       │                           │
│                              ┌────────▼──────────┐               │
│                              │  WebSocket Server  │               │
│                              │     (port 8080)    │               │
│                              └────────┬──────────┘               │
└───────────────────────────────────────┼──────────────────────────┘
                                        │
                               ┌────────▼──────────┐
                               │   Next.js Frontend │
                               │    (port 3000)     │
                               │                    │
                               │ ✅ Approved txns   │
                               │ 🚨 Flagged txns    │
                               └────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Message Broker** | Apache Kafka 7.6 (Confluent) | High-throughput event streaming |
| **Coordination** | Apache Zookeeper 7.6 | Kafka cluster management |
| **Cache / Store** | Redis 7 (Alpine) | Fixed-window velocity counters |
| **Database** | PostgreSQL 16 (Alpine) | Persistent fraud record storage |
| **Backend** | Node.js + TypeScript 5 | Producer, Consumer, WS Server |
| **Frontend** | Next.js 16 + React 18 | Real-time monitoring UI |
| **Containerization** | Docker + Docker Compose | Full local environment |

---

## Project Structure

```
FlowState/
├── docker-compose.yml              # All infrastructure services
│
├── postgres/
│   └── init.sql                    # Schema: flagged_transactions + indexes
│
├── backend/
│   ├── package.json
│   ├── tsconfig.json
│   ├── .env.example                # All configurable environment variables
│   └── src/
│       ├── types.ts                # Transaction & ProcessedTransaction interfaces
│       ├── producer.ts             # Kafka mock transaction producer
│       ├── consumer.ts             # Fraud detection engine
│       └── websocket-server.ts     # WebSocket broadcast server
│
└── frontend/
    ├── package.json
    ├── tsconfig.json
    ├── next.config.ts              # Standalone output + WS URL env passthrough
    ├── .env.local.example          # Frontend environment variable template
    ├── types/
    │   └── transaction.ts          # Frontend-side type definitions
    └── app/
        ├── layout.tsx
        └── page.tsx                # WebSocket client + live transaction display
```

---

## Quickstart

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- [Node.js 20+](https://nodejs.org/) and npm

### 1. Clone the repository

```bash
git clone https://github.com/romir1710/FlowState_Fintech_Fraud_Detection.git
cd FlowState_Fintech_Fraud_Detection
```

### 2. Start the infrastructure

```bash
docker-compose up -d
```

Wait approximately **30–45 seconds** for Kafka to initialize. Verify the topic was created:

```bash
docker exec flowstate-kafka \
  kafka-topics --bootstrap-server localhost:9092 --list
# Expected output: transaction-events
```

### 3. Start the backend

```bash
cd backend
npm install

# Terminal A — Consumer (fraud detector + WebSocket server on :8080)
npm run start:consumer

# Terminal B — Producer (5 transactions/sec by default)
npm run start:producer
```

### 4. Start the frontend

```bash
cd frontend
npm install

# Optional: configure WebSocket URL
cp .env.local.example .env.local

npm run dev
```

Open [http://localhost:3000](http://localhost:3000). Transactions will appear in real-time split into **Approved** and **Flagged (FRAUD)** lists.

---

## Configuration

All values are configurable via environment variables. Sensible defaults work out of the box with `docker-compose`.

### Backend (`backend/.env.example`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | `transaction-events` | Kafka topic name |
| `KAFKA_GROUP_ID` | `flowstate-fraud-detector` | Consumer group ID |
| `PRODUCE_INTERVAL_MS` | `200` | Producer frequency (200ms = 5 tx/sec) |
| `FRAUD_THRESHOLD` | `75` | Risk score cutoff — scores above this are flagged |
| `VELOCITY_WINDOW_SECONDS` | `60` | Fixed-window duration for velocity counter |
| `WS_PORT` | `8080` | WebSocket server port |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `PG_HOST` | `localhost` | PostgreSQL hostname |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `flowstate` | Database name |
| `PG_USER` | `flowstate_user` | Database user |
| `PG_PASSWORD` | `flowstate_pass` | Database password |

### Frontend (`frontend/.env.local.example`)

| Variable | Default (fallback in code) | Description |
|---|---|---|
| `NEXT_PUBLIC_WS_URL` | `ws://localhost:8080` | WebSocket server URL — override for Docker/production |

---

## Fraud Detection Logic

Each incoming Kafka message is processed through a two-factor risk scoring model.

### Risk Score Formula

```
amountScore   = (amount / 5_000) × 50     →  0 – 50 points
velocityScore = min(velocity, 10) × 5     →  0 – 50 points
─────────────────────────────────────────────────────────────
riskScore     = amountScore + velocityScore   (range: 0 – 100)
isFraud       = riskScore > FRAUD_THRESHOLD   (default: 75)
```

### Score Examples

| Scenario | Amount | Velocity (60s) | Score | Result |
|---|---|---|---|---|
| Small, rare | $100 | 1 tx | 1 + 5 = **6** | ✅ Approved |
| Large, rare | $4,000 | 1 tx | 40 + 5 = **45** | ✅ Approved |
| Small, rapid-fire | $200 | 12 tx | 2 + 50 = **52** | ✅ Approved |
| Large, frequent | $4,000 | 8 tx | 40 + 40 = **80** | 🚨 **FRAUD** |
| Maximum signal | $5,000 | 10+ tx | 50 + 50 = **100** | 🚨 **FRAUD** |

Flagged transactions are:
1. Logged to the console with `WARN` level
2. Persisted to the PostgreSQL `flagged_transactions` table (idempotent via `ON CONFLICT DO NOTHING`)
3. Broadcast to all connected WebSocket clients (same as approved transactions)

---

## Redis Velocity Design

The velocity counter uses a **fixed-window** approach. This is a deliberate design choice:

```typescript
// The naive (broken) approach — resets the window on every transaction:
await redis.incr(key);
await redis.expire(key, 60); // ❌ TTL resets every call → infinite window

// The correct approach (implemented):
const velocity = await redis.incr(key);
if (velocity === 1) {
  // Key was just created — start the 60s clock once, never reset it.
  await redis.expire(key, VELOCITY_WINDOW_SECONDS); // ✅
}
```

**Why this matters:** If `EXPIRE` were called on every increment, a user transacting every 59 seconds would accumulate velocity indefinitely — the window never closes. By only setting the TTL when `velocity === 1` (key creation), the 60-second clock starts on the first transaction and expires naturally, providing a true fixed-window rate counter.

---

## API & Data Contracts

### Kafka Message Schema (`transaction-events` topic)

```json
{
  "transactionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "userId": "user_007",
  "amount": 3842.50,
  "timestamp": "2026-07-26T10:30:00.000Z",
  "location": "Singapore, SG"
}
```

### WebSocket Broadcast Schema

Sent to all connected clients for every transaction (approved and flagged):

```json
{
  "transactionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "userId": "user_007",
  "amount": 3842.50,
  "timestamp": "2026-07-26T10:30:00.000Z",
  "location": "Singapore, SG",
  "riskScore": 83,
  "isFraud": true,
  "velocity": 9
}
```

### PostgreSQL Schema (`flagged_transactions`)

```sql
CREATE TABLE flagged_transactions (
  id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id  VARCHAR(64)   NOT NULL UNIQUE,
  user_id         VARCHAR(64)   NOT NULL,
  amount          NUMERIC(12,2) NOT NULL,
  timestamp       TIMESTAMPTZ   NOT NULL,
  location        VARCHAR(128),
  risk_score      SMALLINT      NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
  velocity        SMALLINT      NOT NULL CHECK (velocity >= 0),
  created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
```

---

## Infrastructure Services

| Service | Image | Host Port | Purpose |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 | Message broker |
| `kafka-init` | `confluentinc/cp-kafka:7.6.0` | — | One-shot topic creator |
| `redis` | `redis:7-alpine` | 6379 | Velocity cache (LRU, 256MB) |
| `postgres` | `postgres:16-alpine` | 5432 | Flagged transaction store |

### Teardown

```bash
# Stop all containers (preserve data volumes)
docker-compose down

# Full reset — remove containers and all volumes
docker-compose down -v
```

---

## License

MIT License

Copyright (c) 2026 Romir

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
