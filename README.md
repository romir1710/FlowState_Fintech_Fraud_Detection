<div align="center">

<h1>🌐 FlowState</h1>
<h3>Real-Time Fraud Detection Pipeline</h3>

<p>
  <img src="https://img.shields.io/badge/Node.js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white" />
  <img src="https://img.shields.io/badge/TypeScript-3178C6?style=for-the-badge&logo=typescript&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB" />
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" />
</p>

<p>
  A production-deployed, full-stack real-time fraud detection system built on Apache Kafka. Transactions flow from a simulated payment producer → Kafka → a fraud-scoring consumer → WebSocket → a live browser dashboard.
</p>

</div>

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quickstart](#quickstart)
- [Live Frontend Features](#live-frontend-features)
- [Fraud Detection Logic](#fraud-detection-logic)
- [Redis Velocity Design](#redis-velocity-design)
- [API & Data Contracts](#api--data-contracts)
- [Configuration](#configuration)
- [Infrastructure Services (Local Docker)](#infrastructure-services-local-docker)
- [Production Deployment (Render + Vercel)](#production-deployment-render--vercel)
- [License](#license)

---

## Architecture

```
┌─────────────────┐     Kafka      ┌──────────────────────┐     WebSocket    ┌─────────────┐
│  Producer       │ ─────────────► │  Consumer            │ ────────────────► │  Frontend   │
│  (Render)       │  transaction-  │  (Render)            │   enriched tx     │  (Vercel)   │
│                 │  events topic  │                       │   (all + fraud)   │             │
│  Generates tx   │                │  • Fraud scoring      │                   │  Live feed  │
│  at configurable│                │  • Redis velocity     │                   │  Filter UI  │
│  interval       │                │  • PostgreSQL persist │                   │  Spotlight  │
└─────────────────┘                └──────────────────────┘                   └─────────────┘
                                            │
                                            │ SQL (fraud only)
                                            ▼
                                   ┌─────────────────┐     ┌──────────────────┐
                                   │  PostgreSQL      │     │  Redis           │
                                   │  (Render)        │     │  (Upstash)       │
                                   │                  │     │                  │
                                   │  flagged_txns    │     │  velocity        │
                                   │  table           │     │  counters        │
                                   └─────────────────┘     └──────────────────┘
```

**Cloud services used (all free tier):**
- **Vercel** — Frontend hosting (auto-deploys on every push to `main`)
- **Render** — Backend hosting for Consumer + Producer services
- **Aiven** — Managed Apache Kafka with SASL/SCRAM-256 + TLS
- **Upstash** — Serverless Redis for velocity counters
- **Render PostgreSQL** — Persistent storage for flagged transactions

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Message Broker** | Apache Kafka (Aiven cloud) |
| **Velocity Cache** | Redis (Upstash serverless) |
| **Fraud Store** | PostgreSQL (Render managed) |
| **Backend Runtime** | Node.js 20 + TypeScript |
| **WebSocket** | `ws` library (combined HTTP + WS server) |
| **Frontend** | React 18 + Vite + TypeScript + Tailwind CSS |
| **Icons** | lucide-react |
| **Deployment** | Vercel (frontend) + Render (backend) |

---

## Project Structure

```
FlowState-FinTech_Fraud_Detection_Pipeline/
│
├── docker-compose.yml              # Local dev infra (Kafka, Redis, Postgres)
├── render.yaml                     # Render IaC — consumer, producer, postgres
│
├── postgres/
│   └── init.sql                    # Schema: flagged_transactions table
│
└── backend/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── types.ts                # Shared Transaction + ProcessedTransaction types
│       ├── producer.ts             # Kafka transaction producer
│       ├── consumer.ts             # Fraud detection engine + WebSocket broadcaster
│       └── websocket-server.ts     # Combined HTTP (/health) + WebSocket server
│
└── frontend/
    ├── index.html                  # Vite entry point
    ├── vite.config.ts
    ├── tailwind.config.ts
    ├── postcss.config.js
    ├── package.json
    ├── tsconfig.json
    ├── .env.local.example          # Frontend environment variable template
    └── src/
        ├── main.tsx                # React 18 createRoot entry
        ├── index.css               # Tailwind + Google Fonts + hero animations
        ├── App.tsx                 # Main page: WebSocket client + layout + state
        ├── vite-env.d.ts           # import.meta.env type declarations
        ├── types/
        │   └── transaction.ts      # Frontend-side type definitions
        └── components/
            └── RevealLayer.tsx     # Cursor spotlight reveal component
```

---

## Quickstart

> **Live Application**: The direct live public URL is **[https://flowstate-fintech-fraud-detection.vercel.app](https://flowstate-fintech-fraud-detection.vercel.app)**, so the web app can be accessed through here directly. However, assuming you would like to run it locally, follow the steps below.

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

# Terminal B — Producer (5 transactions/sec by default locally)
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

Open [http://localhost:5173](http://localhost:5173). Transactions will appear in real-time split into **Approved** and **Flagged (FRAUD)** lists.

---

## Live Frontend Features

The dashboard is built as a full-screen dark-themed interface with:

- **Cursor spotlight** — A soft circular mask follows the mouse, revealing a second background image beneath the base image in real time, using a hidden canvas and CSS `maskImage`.
- **Inter + Playfair Display** typography — Wordmark in Playfair Display italic; UI in Inter.
- **Staggered hero animations** — Blur-rise for the heading, zoom-out for the background, fade-up for the stream area.
- **Filter nav bar** — Pill-style navigation (All / Approved / Flagged) with glassmorphic backdrop.
- **Smart count display** — In "All" view, shows `50+` when the list cap is reached. In the individual "Approved" or "Flagged" views, shows the exact running total regardless of cap.
- **Total Transactions counter** — Displayed below the heading in "All" view, counts every transaction seen since page load.
- **Glassmorphic transaction cards** — Key-value pair layout with a green or red status dot, dark blurred background for readability.
- **Auto-reconnect** — WebSocket reconnects automatically after 3 seconds if the connection drops.
- **Free-tier wake-up ping** — On page load, silently pings the producer's health endpoint so Render wakes it from sleep before a recruiter sees a blank dashboard.

---

## Fraud Detection Logic

Each incoming Kafka message is processed through a two-factor risk scoring model.

### Risk Score Formula

```
amountScore   = (amount / 5_000) × 65     →  0 – 65 points
velocityScore = min(velocity, 10) × 3.5   →  0 – 35 points
─────────────────────────────────────────────────────────────
riskScore     = amountScore + velocityScore   (range: 0 – 100)
isFraud       = riskScore > FRAUD_THRESHOLD   (default: 75)
```

The formula weights transaction **amount** more heavily than velocity (65 vs 35 pts). This means a single suspiciously large transaction can breach the threshold on its own merits — closer to how real-world fraud models behave — rather than requiring both signals to align simultaneously.

### Score Examples

| Scenario | Amount | Velocity (60s) | Score | Result |
|---|---|---|---|---|
| Small, rare | $100 | 1 tx | 1 + 4 = **5** | ✅ Approved |
| Large, rare | $4,000 | 1 tx | 52 + 4 = **56** | ✅ Approved |
| Large, normal velocity | $4,500 | 5 tx | 58 + 18 = **76** | 🚨 **FRAUD** |
| Small, high velocity | $200 | 10 tx | 3 + 35 = **38** | ✅ Approved |
| Maximum signal | $5,000 | 10+ tx | 65 + 35 = **100** | 🚨 **FRAUD** |

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

**Why this matters:** If `EXPIRE` were called on every increment, a user transacting every 59 seconds would accumulate velocity indefinitely. By only setting the TTL when `velocity === 1`, the 60-second clock starts on the first transaction and expires naturally, providing a true fixed-window rate counter.

---

## API & Data Contracts

### Kafka Message Schema (`transaction-events` topic)

```json
{
  "transactionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "userId": "user_007",
  "amount": 3842.50,
  "timestamp": "2026-07-28T10:30:00.000Z",
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
  "timestamp": "2026-07-28T10:30:00.000Z",
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

## Configuration

### Backend (`backend/.env.example`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses (`host:port`) |
| `KAFKA_SASL_USERNAME` | — | Aiven SASL username (production only) |
| `KAFKA_SASL_PASSWORD` | — | Aiven SASL password (production only) |
| `KAFKA_TOPIC` | `transaction-events` | Kafka topic name |
| `KAFKA_GROUP_ID` | `flowstate-fraud-detector` | Consumer group ID |
| `PRODUCE_INTERVAL_MS` | `200` | Producer frequency (600ms ≈ 1.7 tx/sec recommended in production) |
| `FRAUD_THRESHOLD` | `75` | Risk score cutoff |
| `VELOCITY_WINDOW_SECONDS` | `60` | Fixed-window duration for velocity counter |
| `REDIS_URL` | — | Full Upstash `rediss://` URL (takes priority over HOST/PORT) |
| `REDIS_HOST` | `localhost` | Redis hostname (local dev fallback) |
| `REDIS_PORT` | `6379` | Redis port (local dev fallback) |
| `DATABASE_URL` | — | Full Render Postgres connection string (takes priority over PG_*) |
| `PG_HOST` | `localhost` | PostgreSQL hostname (local dev fallback) |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `flowstate` | Database name |
| `PG_USER` | `flowstate_user` | Database user |
| `PG_PASSWORD` | `flowstate_pass` | Database password |

### Frontend (`frontend/.env.local.example`)

| Variable | Default (fallback in code) | Description |
|---|---|---|
| `VITE_WS_URL` | `ws://localhost:8080` | WebSocket server URL — set to `wss://flowstate-consumer.onrender.com` in production |

---

## Infrastructure Services (Local Docker)

| Service | Image | Port | Purpose |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 | Message broker |
| `kafka-init` | `confluentinc/cp-kafka:7.6.0` | — | One-shot topic creator |
| `redis` | `redis:7-alpine` | 6379 | Velocity cache (256MB) |
| `postgres` | `postgres:16-alpine` | 5432 | Flagged transaction store |

### Teardown

```bash
# Stop all containers (preserve data volumes)
docker-compose down

# Full reset — remove containers and all volumes
docker-compose down -v
```

---

## Production Deployment (Render + Vercel)

The repo ships with `render.yaml` which auto-provisions both backend services and the PostgreSQL database. On every push to `main`, Render and Vercel auto-deploy.

**Environment variables to set manually in Render (not stored in repo):**

| Service | Variable | Source |
|---|---|---|
| consumer + producer | `KAFKA_BROKERS` | Aiven → Service → Overview → Connection info |
| consumer + producer | `KAFKA_SASL_USERNAME` | Aiven credentials |
| consumer + producer | `KAFKA_SASL_PASSWORD` | Aiven credentials |
| consumer | `REDIS_URL` | Upstash → Database → REST URL (use `rediss://` connection string) |

**Environment variable to set in Vercel:**

| Variable | Value |
|---|---|
| `VITE_WS_URL` | `wss://flowstate-consumer.onrender.com` |

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
