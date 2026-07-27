/**
 * producer.ts — Kafka Transaction Producer
 *
 * Simulates a high-frequency payment stream by generating randomized
 * transaction payloads and publishing them to the `transaction-events` topic.
 *
 * Configuration (environment variables):
 *   KAFKA_BROKERS          Comma-separated list (default: "localhost:9092")
 *   KAFKA_SASL_USERNAME    Upstash SASL username (omit for local dev)
 *   KAFKA_SASL_PASSWORD    Upstash SASL password (omit for local dev)
 *   KAFKA_TOPIC            Topic name (default: "transaction-events")
 *   PRODUCE_INTERVAL_MS    Publish interval in ms (default: 200ms locally; set 10000 on Render)
 *   PORT                   HTTP health endpoint port (injected by Render)
 */

import { Kafka, Partitioners, logLevel } from 'kafkajs';
import * as http from 'http';
import { v4 as uuidv4 } from 'uuid';
import { Transaction } from './types';

// ─── Config ──────────────────────────────────────────────────────────────────
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const KAFKA_SASL_USERNAME = process.env.KAFKA_SASL_USERNAME;
const KAFKA_SASL_PASSWORD = process.env.KAFKA_SASL_PASSWORD;
const TOPIC = process.env.KAFKA_TOPIC ?? 'transaction-events';
const PRODUCE_INTERVAL_MS = parseInt(process.env.PRODUCE_INTERVAL_MS ?? '200', 10);
const HEALTH_PORT = parseInt(process.env.PORT ?? '3001', 10);

// ─── Mock Data Pools ─────────────────────────────────────────────────────────
/** Fixed pool of 20 user IDs — velocity checks become interesting with repetition */
const USER_IDS: string[] = Array.from(
  { length: 20 },
  (_, i) => `user_${String(i + 1).padStart(3, '0')}`,
);

const LOCATIONS: string[] = [
  'New York, US',
  'London, UK',
  'Tokyo, JP',
  'Berlin, DE',
  'Mumbai, IN',
  'Sydney, AU',
  'Toronto, CA',
  'Paris, FR',
  'Singapore, SG',
  'Dubai, AE',
  'São Paulo, BR',
  'Seoul, KR',
];

// ─── Payload Generator ────────────────────────────────────────────────────────
function generateTransaction(): Transaction {
  return {
    transactionId: uuidv4(),
    userId: USER_IDS[Math.floor(Math.random() * USER_IDS.length)],
    // Skew: 70% of transactions are low-value (<$500), 30% are high-value
    amount: Math.random() < 0.7
      ? parseFloat((Math.random() * 500).toFixed(2))
      : parseFloat((500 + Math.random() * 4500).toFixed(2)),
    timestamp: new Date().toISOString(),
    location: LOCATIONS[Math.floor(Math.random() * LOCATIONS.length)],
  };
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  // SASL/TLS conditionally added when Upstash env vars are present;
  // plain connection used for local dev (no SASL vars set).
  const kafka = new Kafka({
    clientId: 'flowstate-producer',
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN,
    ...(KAFKA_SASL_USERNAME && KAFKA_SASL_PASSWORD
      ? {
          sasl: {
            mechanism: 'scram-sha-256' as const,
            username: KAFKA_SASL_USERNAME,
            password: KAFKA_SASL_PASSWORD,
          },
          ssl: { rejectUnauthorized: false },
        }
      : {}),
  });

  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
    allowAutoTopicCreation: false,
  });

  await producer.connect();
  console.log(`[Producer] ✅ Connected to Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);
  console.log(`[Producer] Topic: ${TOPIC} | Interval: ${PRODUCE_INTERVAL_MS}ms (~${(1000 / PRODUCE_INTERVAL_MS).toFixed(1)} tx/sec)`);

  // HTTP health endpoint — keeps Render's free-tier web service alive
  let totalSent = 0;
  const healthServer = http.createServer((_req: http.IncomingMessage, res: http.ServerResponse) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'flowstate-producer', sent: totalSent }));
  });
  healthServer.listen(HEALTH_PORT, () => {
    console.log(`[Producer] ✅ Health endpoint listening on port ${HEALTH_PORT}`);
  });

  let errorCount = 0;

  const interval = setInterval(async () => {
    const tx = generateTransaction();
    try {
      await producer.send({
        topic: TOPIC,
        messages: [
          {
            // Partition by userId so all tx from same user land on same partition (ordering)
            key: tx.userId,
            value: JSON.stringify(tx),
          },
        ],
      });

      totalSent++;
      if (totalSent % 10 === 0) {
        console.log(`[Producer] Sent: ${totalSent} | Errors: ${errorCount} | Last → ${tx.userId} $${tx.amount} @ ${tx.location}`);
      }
    } catch (err) {
      errorCount++;
      console.error(`[Producer] ❌ Failed to send message (total errors: ${errorCount}):`, (err as Error).message);
    }
  }, PRODUCE_INTERVAL_MS);

  // ─── Graceful Shutdown ───────────────────────────────────────────────────
  const shutdown = async (signal: string): Promise<void> => {
    console.log(`\n[Producer] Received ${signal}. Shutting down gracefully...`);
    clearInterval(interval);
    await producer.disconnect();
    healthServer.close();
    console.log(`[Producer] Disconnected. Total sent: ${totalSent}. Goodbye.`);
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err: unknown) => {
  console.error('[Producer] Fatal startup error:', err);
  process.exit(1);
});
