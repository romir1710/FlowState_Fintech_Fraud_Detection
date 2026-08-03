/**
 * kafka-ping.js — Aiven Keepalive Script
 *
 * Connects to the Aiven Kafka cluster and sends a single lightweight
 * keepalive message to the transaction-events topic, then disconnects.
 *
 * This prevents Aiven from pausing the free-tier service due to inactivity.
 * Run via GitHub Actions on a schedule every 3 days.
 *
 * Required env vars:
 *   KAFKA_BROKERS         e.g. "kafka-xxxx.aivencloud.com:12345"
 *   KAFKA_SASL_USERNAME   Aiven service username
 *   KAFKA_SASL_PASSWORD   Aiven service password
 *   KAFKA_TOPIC           (optional, defaults to "transaction-events")
 */

const { Kafka, Partitioners, logLevel } = require('kafkajs');

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const KAFKA_SASL_USERNAME = process.env.KAFKA_SASL_USERNAME;
const KAFKA_SASL_PASSWORD = process.env.KAFKA_SASL_PASSWORD;
const TOPIC = process.env.KAFKA_TOPIC ?? 'transaction-events';

async function ping() {
  if (!KAFKA_SASL_USERNAME || !KAFKA_SASL_PASSWORD) {
    console.error('❌  KAFKA_SASL_USERNAME or KAFKA_SASL_PASSWORD is not set.');
    process.exit(1);
  }

  const kafka = new Kafka({
    clientId: 'flowstate-keepalive',
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN,
    sasl: {
      mechanism: 'scram-sha-256',
      username: KAFKA_SASL_USERNAME,
      password: KAFKA_SASL_PASSWORD,
    },
    ssl: { rejectUnauthorized: false },
  });

  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
    allowAutoTopicCreation: false,
  });

  try {
    await producer.connect();
    console.log('✅  Connected to Aiven Kafka.');

    const keepalivePayload = {
      transactionId: `keepalive-${Date.now()}`,
      userId: 'keepalive',
      amount: 0,
      timestamp: new Date().toISOString(),
      location: 'keepalive',
    };

    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(keepalivePayload) }],
    });

    console.log(`✅  Keepalive message sent to topic "${TOPIC}".`);
  } finally {
    await producer.disconnect();
    console.log('🔌  Disconnected cleanly.');
  }
}

ping().catch((err) => {
  console.error('❌  Kafka ping failed:', err.message);
  process.exit(1);
});
