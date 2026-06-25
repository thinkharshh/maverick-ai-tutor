// src/kafka.js — Confluent Cloud Kafka wrapper (ES module).
//
// Owned by Track B. Read by server.js (producer) and recommender.js (consumer).
//
// IMPORTANT: This module must NOT throw at import time if env vars are
// missing. Callers may run setup.js / parts of server.js without Confluent
// creds available. We only fail when connect() is actually invoked.
//
// SCHEMA REGISTRY:
//   produce()         — if SR creds are set, value is encoded with the
//                       Confluent wire format (magic byte + schema id +
//                       JSON payload). Otherwise falls back to raw JSON
//                       (back-compat with the pre-SR demo path).
//   createConsumer()  — uses safeDecode(): wire-format if it sees the
//                       magic byte, plain JSON.parse otherwise. So a
//                       single consumer reads both old and new messages.

import { Kafka, logLevel } from 'kafkajs';
import { encode as srEncode, safeDecode, getRegistry } from './schemaRegistry.js';

const CLIENT_ID = 'maverick-ai-tutor';

/**
 * Build a Kafka client lazily from env. Returns null-shaped error info if
 * creds are missing instead of throwing — callers turn that into a friendly
 * "BLOCKED: Confluent creds" message.
 */
function buildKafka() {
  const bootstrap = process.env.CONFLUENT_BOOTSTRAP;
  const username  = process.env.CONFLUENT_API_KEY;
  const password  = process.env.CONFLUENT_API_SECRET;

  const missing = [];
  if (!bootstrap) missing.push('CONFLUENT_BOOTSTRAP');
  if (!username)  missing.push('CONFLUENT_API_KEY');
  if (!password)  missing.push('CONFLUENT_API_SECRET');
  if (missing.length) {
    const err = new Error(
      `Missing Confluent env vars: ${missing.join(', ')}. ` +
      `Fill them in .env (see docs/manager/track-b-notes.md §B2).`
    );
    err.code = 'CONFLUENT_CREDS_MISSING';
    throw err;
  }

  return new Kafka({
    clientId: CLIENT_ID,
    brokers: [bootstrap],
    ssl: true,
    sasl: { mechanism: 'plain', username, password },
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
    logLevel: logLevel.ERROR,
  });
}

/**
 * createProducer() — returns a connected KafkaJS producer.
 * Caller is responsible for disconnecting on shutdown.
 */
export async function createProducer() {
  const kafka = buildKafka();
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: false,
  });
  await producer.connect();
  return producer;
}

/**
 * encodeValue(topic, value) — Schema Registry if creds present, else raw JSON.
 * Exported so long-running producers (server.js, recommender.js) can reuse it.
 */
export async function encodeValue(topic, value) {
  if (getRegistry()) {
    return srEncode(topic, value);
  }
  return Buffer.from(JSON.stringify(value));
}

/**
 * produce(topic, key, value) — one-shot send.
 *
 * For long-running services (server.js), prefer createProducer() once at
 * boot and keep the connection. produce() is a convenience for tests and
 * lazy producers. value is Schema-Registry-encoded if SR creds are set,
 * otherwise JSON.stringified (back-compat).
 */
export async function produce(topic, key, value) {
  const producer = await createProducer();
  try {
    await producer.send({
      topic,
      messages: [{
        key: key == null ? null : String(key),
        value: await encodeValue(topic, value),
      }],
    });
    return true;
  } finally {
    await producer.disconnect().catch(() => {});
  }
}

/**
 * createConsumer({ groupId, topic, onMessage })
 *
 * Subscribes to `topic`, then runs `onMessage({ key, value, raw })` for each
 * record. `value` is JSON.parsed (or the raw string if parse fails). The
 * function does NOT auto-start unless `autoRun` is true — returns the
 * underlying consumer so the caller can attach shutdown handlers.
 *
 * Resolves once the consumer is running.
 */
export async function createConsumer({ groupId, topic, onMessage, autoRun = true, fromBeginning = false }) {
  if (!groupId)  throw new Error('createConsumer: groupId is required');
  if (!topic)    throw new Error('createConsumer: topic is required');
  if (typeof onMessage !== 'function') throw new Error('createConsumer: onMessage must be a function');

  const kafka = buildKafka();
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });

  if (autoRun) {
    await consumer.run({
      eachMessage: async ({ topic: t, partition, message }) => {
        const keyStr = message.key ? message.key.toString('utf8') : null;
        let parsed;
        try {
          // safeDecode handles both Confluent wire format (SR) and raw JSON.
          parsed = await safeDecode(message.value);
        } catch (err) {
          // last-ditch fallback so a single bad message never wedges the loop
          parsed = message.value ? message.value.toString('utf8') : '';
        }
        try {
          await onMessage({
            key: keyStr,
            value: parsed,
            raw: message.value,
            topic: t,
            partition,
            offset: message.offset,
          });
        } catch (err) {
          // Log + continue — never crash the consumer loop on a single bad msg.
          console.error('[kafka.consumer] onMessage threw:', err?.message || err);
        }
      },
    });
  }

  return consumer;
}

/**
 * healthcheck() — connect briefly to validate creds + reachability.
 * Resolves true on success, throws on failure. Used by:
 *   node -e "import('./src/kafka.js').then(k => k.healthcheck()).then(console.log)"
 */
export async function healthcheck() {
  const kafka = buildKafka();
  const admin = kafka.admin();
  await admin.connect();
  try {
    // listTopics() is a light call that requires real broker auth — perfect HC.
    await admin.listTopics();
  } finally {
    await admin.disconnect().catch(() => {});
  }
  return true;
}
