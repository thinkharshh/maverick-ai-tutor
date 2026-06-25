// src/schemaRegistry.js — Confluent Cloud Schema Registry wrapper (ES module).
//
// Wraps @kafkajs/confluent-schema-registry to give the rest of the app a tiny
// surface: encode(topic, value) → Buffer, decode(buffer) → value, plus a one-
// shot registerAll() called by scripts/register-schemas.js.
//
// Subject naming: TopicNameStrategy — `<topic>-value`. This matches the
// filenames in /schemas (learning.events-value.json, etc.) and is what Flink's
// `json-registry` reader expects out of the box.
//
// Like src/kafka.js, this module does NOT throw at import time if SR creds
// are missing. Callers that don't need SR (e.g. unit tests) can keep working.

import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname  = path.dirname(fileURLToPath(import.meta.url));
const SCHEMAS_DIR = path.resolve(__dirname, '..', 'schemas');

// Public subject convention: <topic>-value
export function subjectFor(topic) {
  return `${topic}-value`;
}

let _registry = null;
let _registryReason = '';

/**
 * getRegistry() — lazy singleton. Returns null if creds are missing
 * (and stashes the reason for diagnostic logs).
 */
export function getRegistry() {
  if (_registry) return _registry;

  const host   = process.env.CONFLUENT_SCHEMA_REGISTRY_URL;
  const key    = process.env.CONFLUENT_SR_API_KEY;
  const secret = process.env.CONFLUENT_SR_API_SECRET;

  const missing = [];
  if (!host)   missing.push('CONFLUENT_SCHEMA_REGISTRY_URL');
  if (!key)    missing.push('CONFLUENT_SR_API_KEY');
  if (!secret) missing.push('CONFLUENT_SR_API_SECRET');
  if (missing.length) {
    _registryReason = `Schema Registry creds missing: ${missing.join(', ')}`;
    return null;
  }

  _registry = new SchemaRegistry({
    host,
    auth: { username: key, password: secret },
  });
  _registryReason = 'ok';
  return _registry;
}

export function registryReason() {
  return _registryReason || 'not initialized';
}

/**
 * Topic → schema file mapping. New topics get added here.
 */
const TOPIC_SCHEMAS = {
  [process.env.TOPIC_LEARNING_EVENTS || 'learning.events']:
    'learning.events-value.json',
  [process.env.TOPIC_RECOMMENDATIONS || 'learner.recommendations']:
    'learner.recommendations-value.json',
};

async function loadSchemaFile(filename) {
  const full = path.join(SCHEMAS_DIR, filename);
  const text = await fs.readFile(full, 'utf8');
  return text;
}

/**
 * registerAll() — registers/updates all topic schemas in Schema Registry.
 * Idempotent: registering the same schema twice returns the existing id.
 * Returns { topic, subject, id }[] for human-readable logging.
 */
export async function registerAll() {
  const reg = getRegistry();
  if (!reg) throw new Error(registryReason());

  const out = [];
  for (const [topic, filename] of Object.entries(TOPIC_SCHEMAS)) {
    const schema = await loadSchemaFile(filename);
    const subject = subjectFor(topic);
    const { id } = await reg.register(
      { type: SchemaType.JSON, schema },
      { subject }
    );
    out.push({ topic, subject, id, schemaFile: filename });
  }
  return out;
}

// Per-topic cached schema id (so encode() doesn't hit the registry every call).
const _idCache = new Map();

async function getSchemaIdForTopic(topic) {
  if (_idCache.has(topic)) return _idCache.get(topic);

  const reg = getRegistry();
  if (!reg) throw new Error(registryReason());

  const subject = subjectFor(topic);
  const id = await reg.getLatestSchemaId(subject);
  _idCache.set(topic, id);
  return id;
}

/**
 * encode(topic, value) — returns a Buffer with the Confluent wire format
 * (magic byte + schema id + JSON payload). Pass straight to KafkaJS's
 * `messages[].value`.
 */
export async function encode(topic, value) {
  const reg = getRegistry();
  if (!reg) throw new Error(registryReason());
  const id = await getSchemaIdForTopic(topic);
  return reg.encode(id, value);
}

/**
 * decode(buffer) — returns the parsed value. Throws if the buffer is not
 * Confluent wire-formatted (e.g. legacy raw-JSON messages on the topic).
 */
export async function decode(buffer) {
  const reg = getRegistry();
  if (!reg) throw new Error(registryReason());
  return reg.decode(buffer);
}

/**
 * safeDecode(buffer) — tolerant of legacy raw-JSON. If the wire format
 * isn't recognized, falls back to JSON.parse(utf8). Used by consumers
 * during the transition (kafka.js).
 */
export async function safeDecode(buffer) {
  if (!buffer) return null;
  // Confluent wire format: first byte is 0x00, then 4-byte schema id (BE).
  if (buffer.length >= 5 && buffer[0] === 0x00) {
    try {
      return await decode(buffer);
    } catch (err) {
      // fall through to plain-JSON path
    }
  }
  const str = buffer.toString('utf8');
  try { return JSON.parse(str); }
  catch { return str; }
}
