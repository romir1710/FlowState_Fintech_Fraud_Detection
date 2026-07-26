/**
 * consumer.ts — Kafka Consumer + Fraud Detection Engine + WebSocket Broadcaster
 *
 * Pipeline for each incoming transaction:
 *   1. Parse raw Kafka message into a Transaction object
 *   2. Increment Redis velocity counter (FIXED WINDOW — EXPIRE only on key creation)
 *   3. Calculate risk score (0–100) from amount + velocity
 *   4. If riskScore > threshold: persist to PostgreSQL
 *   5. Broadcast all processed transactions via WebSocket
 *
 * Configuration (environment variables):
 *   KAFKA_BROKERS              Comma-separated broker list (default: "localhost:9092")
 *   KAFKA_TOPIC                Topic to consume from (default: "transaction-events")
 *   KAFKA_GROUP_ID             Consumer group ID (default: "flowstate-fraud-detector")
 *   REDIS_HOST                 Redis hostname (default: "localhost")
 *   REDIS_PORT                 Redis port (default: 6379)
 *   PG_HOST                    PostgreSQL hostname (default: "localhost")
 *   PG_PORT                    PostgreSQL port (default: 5432)
 *   PG_DATABASE                Database name (default: "flowstate")
 *   PG_USER                    Database user (default: "flowstate_user")
 *   PG_PASSWORD                Database password (default: "flowstate_pass")
 *   FRAUD_THRESHOLD            Risk score cutoff for fraud (default: 75)
 *   VELOCITY_WINDOW_SECONDS    Fixed window duration in seconds (default: 60)
 *   WS_PORT                    WebSocket server port (default: 8080)
 */

import { Kafka, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { Transaction, ProcessedTransaction } from './types';
import { initWebSocketServer, broadcast, closeWebSocketServer } from './websocket-server';

// ─── Config ──────────────────────────────────────────────────────────────────
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC ?? 'transaction-events';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? 'flowstate-fraud-detector';

const REDIS_HOST = process.env.REDIS_HOST ?? 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? '6379', 10);

const PG_HOST = process.env.PG_HOST ?? 'localhost';
const PG_PORT = parseInt(process.env.PG_PORT ?? '5432', 10);
const PG_DATABASE = process.env.PG_DATABASE ?? 'flowstate';
const PG_USER = process.env.PG_USER ?? 'flowstate_user';
const PG_PASSWORD = process.env.PG_PASSWORD ?? 'flowstate_pass';

const FRAUD_THRESHOLD = parseInt(process.env.FRAUD_THRESHOLD ?? '75', 10);
const VELOCITY_WINDOW_SECONDS = parseInt(process.env.VELOCITY_WINDOW_SECONDS ?? '60', 10);

// ─── Clients ──────────────────────────────────────────────────────────────────
const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  lazyConnect: true,
});

const pgPool = new Pool({
  host: PG_HOST,
  port: PG_PORT,
  database: PG_DATABASE,
  user: PG_USER,
  password: PG_PASSWORD,
  max: 10,               // max pool connections
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

// ─── Velocity: Fixed Window Counter ──────────────────────────────────────────
/**
 * Increments the userId's transaction count in Redis and returns the current value.
 *
 * CRITICAL DESIGN NOTE — Why EXPIRE is only set when velocity === 1:
 *
 *   ❌ Broken pattern (what we do NOT do):
 *      INCR key    → e.g., returns 5
 *      EXPIRE key 60   ← resets TTL on EVERY call!
 *      Result: If a user transacts every 59s, the key NEVER expires,
 *              creating an infinite accumulation window — not 60 seconds.
 *
 *   ✅ Correct pattern (what we DO):
 *      velocity = INCR key
 *      if (velocity === 1) EXPIRE key 60   ← set TTL only on key creation
 *      Result: The 60s clock starts on the first transaction and is
 *              never reset. After 60s the key auto-expires, starting fresh.
 *              This implements a true fixed-window rate counter.
 */
async function getAndIncrementVelocity(userId: string): Promise<number> {
  const key = `flowstate:velocity:${userId}`;

  // INCR atomically creates the key (value=1) or increments it.
  const velocity = await redis.incr(key);

  // Only attach the TTL the first time this key is created.
  // Subsequent INCRs leave the existing TTL untouched.
  if (velocity === 1) {
    await redis.expire(key, VELOCITY_WINDOW_SECONDS);
  }

  return velocity;
}

// ─── Risk Scoring ─────────────────────────────────────────────────────────────
/**
 * Calculates a composite risk score in the range [0, 100].
 *
 * Formula:
 *   amountScore   = (amount / 5000) * 50      → 0–50 pts, proportional to max amount
 *   velocityScore = min(velocity, 10) * 5     → 0–50 pts, 5 pts per tx (capped at 10)
 *   riskScore     = amountScore + velocityScore
 *
 * Examples:
 *   $5000 tx, 10+ tx in window → 50 + 50 = 100 (max fraud signal)
 *   $100  tx, 1 tx in window   →  1 +  5 =   6 (safe)
 *   $4500 tx, 8 tx in window   → 45 + 40 =  85 (FRAUD — exceeds threshold 75)
 */
function calculateRiskScore(amount: number, velocity: number): number {
  const amountScore = Math.min((amount / 5000) * 50, 50);
  const velocityScore = Math.min(velocity, 10) * 5;
  return Math.round(amountScore + velocityScore);
}

// ─── Persistence ─────────────────────────────────────────────────────────────
/**
 * Persists a flagged transaction to PostgreSQL.
 * ON CONFLICT DO NOTHING guards against duplicate Kafka deliveries.
 */
async function saveFlaggedTransaction(tx: ProcessedTransaction): Promise<void> {
  const query = `
    INSERT INTO flagged_transactions
      (transaction_id, user_id, amount, timestamp, location, risk_score, velocity)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (transaction_id) DO NOTHING
  `;
  await pgPool.query(query, [
    tx.transactionId,
    tx.userId,
    tx.amount,
    tx.timestamp,
    tx.location,
    tx.riskScore,
    tx.velocity,
  ]);
}

// ─── Main ────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  // Connect external services
  await redis.connect();
  console.log(`[Consumer] ✅ Redis connected (${REDIS_HOST}:${REDIS_PORT})`);

  // Test PostgreSQL connection
  const pgClient = await pgPool.connect();
  pgClient.release();
  console.log(`[Consumer] ✅ PostgreSQL connected (${PG_HOST}:${PG_PORT}/${PG_DATABASE})`);

  // Start WebSocket server
  initWebSocketServer();

  // Set up Kafka consumer
  const kafka = new Kafka({
    clientId: 'flowstate-consumer',
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN,
  });

  const consumer = kafka.consumer({
    groupId: KAFKA_GROUP_ID,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
  });

  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

  console.log(`[Consumer] ✅ Kafka subscribed to: ${KAFKA_TOPIC} (group: ${KAFKA_GROUP_ID})`);
  console.log(`[Consumer] Fraud threshold: riskScore > ${FRAUD_THRESHOLD} | Velocity window: ${VELOCITY_WINDOW_SECONDS}s`);

  // ─── Stats tracking ───────────────────────────────────────────────────────
  let totalProcessed = 0;
  let totalFlagged = 0;

  // ─── Message loop ─────────────────────────────────────────────────────────
  await consumer.run({
    // Process one message at a time within a partition for velocity accuracy
    eachMessage: async ({ topic: _topic, partition: _partition, message }) => {
      if (!message.value) return;

      // 1. Parse
      let tx: Transaction;
      try {
        tx = JSON.parse(message.value.toString()) as Transaction;
      } catch {
        console.warn('[Consumer] ⚠️  Could not parse message — skipping.');
        return;
      }

      // 2. Velocity (fixed-window Redis counter — EXPIRE only on key creation)
      const velocity = await getAndIncrementVelocity(tx.userId);

      // 3. Risk Score
      const riskScore = calculateRiskScore(tx.amount, velocity);
      const isFraud = riskScore > FRAUD_THRESHOLD;

      // 4. Assemble enriched payload
      const processed: ProcessedTransaction = {
        ...tx,
        velocity,
        riskScore,
        isFraud,
      };

      // 5. Persist if fraud
      if (isFraud) {
        totalFlagged++;
        console.warn(
          `[Consumer] 🚨 FRAUD  | ${tx.userId} | $${tx.amount.toFixed(2)} | risk=${riskScore} | velocity=${velocity} | ${tx.location}`,
        );
        try {
          await saveFlaggedTransaction(processed);
        } catch (err) {
          console.error('[Consumer] ❌ Failed to persist flagged tx:', (err as Error).message);
        }
      }

      // 6. Always broadcast (approved + flagged)
      broadcast(processed);

      // 7. Periodic stats log
      totalProcessed++;
      if (totalProcessed % 50 === 0) {
        const fraudRate = ((totalFlagged / totalProcessed) * 100).toFixed(1);
        console.log(
          `[Consumer] 📊 Stats | processed=${totalProcessed} | flagged=${totalFlagged} (${fraudRate}%) | approved=${totalProcessed - totalFlagged}`,
        );
      }
    },
  });

  // ─── Graceful Shutdown ───────────────────────────────────────────────────
  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n[Consumer] Received ${signal}. Shutting down gracefully...`);
    await consumer.disconnect();
    await redis.quit();
    await pgPool.end();
    await closeWebSocketServer();
    console.log('[Consumer] All connections closed. Goodbye.');
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err: unknown) => {
  console.error('[Consumer] Fatal startup error:', err);
  process.exit(1);
});
