// src/recommender.js — Node-side delivery loop.
//
// Owned by Track B. Default architecture is DELIVERER-ONLY:
//
//   Consumer B (groupId: maverick-deliverer)
//     subscribes:  learner.recommendations
//     delivers  :  POST /scenarios/create-with-gpt → POST /meetings
//                  → POST /internal/push  (SSE → page announces it)
//                  with graceful fallbacks for Waterr's known billing_id 422.
//
// The transform (LearningEvent → LessonRecommendation, including the Claude-
// generated next_scenario_prompt) is done by the Flink SQL job in
// `flink/job.sql` running on Confluent Cloud. Both topics now have JSON
// Schemas registered in Schema Registry; src/kafka.js + the Flink job both
// speak the Confluent wire format, so they interoperate.
//
// FALLBACK MODE (set USE_NODE_TRANSFORMER=true in .env):
//   Spins up a second consumer (groupId: maverick-transformer) that reads
//   learning.events and produces to learner.recommendations from Node. Use
//   this if the Flink job is not running (dev, demo recovery, no AI Model
//   Inference on your CC tier). The two modes are mutually compatible —
//   Flink is the source of truth in production.
//
// Error policy: NEVER crash the consumer loop on a single bad message. Log
// the failure with key/offset and continue.
//
// Refs: docs/TECHNICAL_PLAN.md §2.1, §2.2, §5; docs/status/CURRENT.md
//       "Known issues" (Waterr POST /meetings 422 billing_id bug).

import 'dotenv/config';
import { createConsumer, createProducer } from './kafka.js';

const API           = process.env.WATERR_API_BASE || 'https://api.waterr.ai/v1';
const API_KEY       = process.env.WATERR_API_KEY;
const SCENARIO_ID   = process.env.SCENARIO_ID || '';
const TOPIC_EVENTS  = process.env.TOPIC_LEARNING_EVENTS || 'learning.events';
const TOPIC_RECS    = process.env.TOPIC_RECOMMENDATIONS  || 'learner.recommendations';
const GROUP_XFORM   = process.env.RECOMMENDER_GROUP_ID   || 'maverick-transformer';
const GROUP_DELIVER = process.env.DELIVERER_GROUP_ID     || 'maverick-deliverer';
const PUSH_URL      = process.env.INTERNAL_PUSH_URL      || 'http://localhost:3000/internal/push';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
// NOTE: there is no claude-3-7. Use the current Sonnet id ('claude-sonnet-4-6'
// per project orchestrator brief). Override via env if needed.
const CLAUDE_MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';

// USE_NODE_TRANSFORMER=true → also spin Consumer A (Flink-fallback mode).
// Default off: Flink is the producer of learner.recommendations.
const USE_NODE_TRANSFORMER =
  String(process.env.USE_NODE_TRANSFORMER || '').toLowerCase() === 'true';

/* ============================================================
 * TRANSFORM — LearningEvent → LessonRecommendation
 * ============================================================ */

function pickDifficulty(avgScore) {
  const n = Number(avgScore);
  if (!Number.isFinite(n)) return 'same';
  if (n < 60) return 'easier';
  if (n > 85) return 'harder';
  return 'same';
}

function planBPrompt({ subject, avg, growthAreas }) {
  return (
    `You are Aarya, a patient blind-friendly tutor. ` +
    `Subject: ${subject}. ` +
    `Previous score: ${avg}/100. ` +
    `Re-teach with strict audio-only language and sound-based analogies. ` +
    `Specific growth areas: ${growthAreas}. ` +
    `End by asking the learner to explain it back in their own words.`
  );
}

async function claudePrompt({ subject, avg, growthAreas }) {
  // Anthropic Messages API. Single user turn, instruct it to OUTPUT ONLY the
  // tutor system prompt — no preamble, no markdown fences.
  const userMsg =
    `You are designing the next 12-minute lesson for a blind learner. ` +
    `Subject: ${subject}. Previous score: ${avg}/100. ` +
    `Growth areas: ${growthAreas}. ` +
    `Output ONLY the new tutor system prompt for a tutor named Aarya — ` +
    `strict audio-only language, sound-based analogies, ask the learner to ` +
    `explain it back. No preamble, no markdown fences.`;

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 600,
      messages: [{ role: 'user', content: userMsg }],
    }),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Anthropic ${res.status}: ${text.slice(0, 300)}`);
  }
  const json = JSON.parse(text);
  const out = json?.content?.[0]?.text?.trim();
  if (!out) throw new Error(`Anthropic returned no text: ${text.slice(0, 200)}`);
  return out;
}

async function buildNextScenarioPrompt(args) {
  if (ANTHROPIC_API_KEY) {
    try {
      return await claudePrompt(args);
    } catch (err) {
      console.warn(`[transformer] Claude call failed, falling back to template: ${err.message}`);
      return planBPrompt(args);
    }
  }
  return planBPrompt(args);
}

/**
 * eventToRecommendation(evt) — pure transform.
 * Input matches LearningEvent (docs/TECHNICAL_PLAN.md §2.1).
 * Output matches LessonRecommendation (docs/TECHNICAL_PLAN.md §2.2).
 */
async function eventToRecommendation(evt) {
  const learner       = evt?.learner || {};
  const meeting       = evt?.meeting || {};
  const performance   = evt?.performance || {};
  const avg           = Number(performance.average_score ?? 0);
  const subject       = meeting.subject || 'general';
  const growthAreas   = performance.growth_areas || 'general review';
  const difficulty    = pickDifficulty(avg);

  const nextPrompt    = await buildNextScenarioPrompt({
    subject,
    avg,
    growthAreas,
  });

  const recEventId =
    evt.event_id ? `rec_${evt.event_id}` : `rec_${Date.now()}`;

  return {
    schema_version: 1,
    event_id: recEventId,
    occurred_at: new Date().toISOString(),
    learner_id: learner.id || 'unknown',
    contact_channel: learner.contact_channel || '',
    recommendation: {
      subject,
      difficulty,
      next_scenario_prompt: nextPrompt,
      estimated_duration_min: 12,
    },
    // include the originating event for debugging — not required by sink
    _source_event_id: evt.event_id || null,
  };
}

/* ============================================================
 * DELIVERY — recommendation → Waterr → /internal/push
 * ============================================================ */

async function waterrJson(method, path, body) {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; }
  catch { json = { raw: text }; }
  if (!res.ok) {
    const err = new Error(`Waterr ${method} ${path} → ${res.status}: ${text.slice(0, 300)}`);
    err.status = res.status;
    err.body = text;
    throw err;
  }
  return json;
}

async function createScenarioFromPrompt(prompt, durationMin = 12) {
  const form = new FormData();
  form.append('userInput', prompt);
  form.append('duration', String(durationMin));

  const res = await fetch(`${API}/scenarios/create-with-gpt`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${API_KEY}` }, // let fetch set multipart boundary
    body: form,
  });
  const text = await res.text();
  if (!res.ok) {
    const err = new Error(`Waterr scenarios/create-with-gpt → ${res.status}: ${text.slice(0, 300)}`);
    err.status = res.status;
    err.body = text;
    throw err;
  }
  return JSON.parse(text);
}

async function createMeeting({ scenarioId, learnerId, subject, difficulty, noteContext }) {
  const ctx = noteContext ||
    `Adaptive follow-up. Subject: ${subject}. Difficulty: ${difficulty}.`;
  return waterrJson('POST', '/meetings', {
    person_name: learnerId,
    scenario_id: scenarioId,
    note_context: ctx,
  });
}

async function pushJoinLink(learnerId, joinUrl, extras = {}) {
  const res = await fetch(PUSH_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ learner_id: learnerId, join_url: joinUrl, ...extras }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`internal/push → ${res.status}: ${text.slice(0, 200)}`);
  }
}

/**
 * deliver(rec) — three fallback tiers:
 *   Tier 1: POST /scenarios/create-with-gpt + POST /meetings  (best path)
 *   Tier 2: reuse SCENARIO_ID, POST /meetings with note_context = the prompt
 *   Tier 3: share URL  https://waterr.ai/scenario/<SCENARIO_ID>
 *
 * Returns { join_url, mode } on success.
 * If WATERR_API_KEY is missing, returns null after logging.
 */
async function deliver(rec) {
  const learnerId  = rec?.learner_id;
  const r          = rec?.recommendation || {};
  const subject    = r.subject || 'general';
  const difficulty = r.difficulty || 'same';
  const prompt     = r.next_scenario_prompt;
  const durationMin = Number(r.estimated_duration_min || 12);

  if (!learnerId) throw new Error('recommendation missing learner_id');
  if (!prompt)    throw new Error('recommendation missing next_scenario_prompt');

  if (!API_KEY) {
    console.log(`[deliverer] WATERR_API_KEY missing — skipping delivery for ${learnerId}. ` +
                `Prompt was: ${prompt.slice(0, 120)}…`);
    return null;
  }

  // Tier 1 — fresh tailored scenario + meeting
  try {
    const scenarioResp = await createScenarioFromPrompt(prompt, durationMin);
    const scenarioId   = scenarioResp?.data?.id || scenarioResp?.id;
    if (!scenarioId) {
      throw new Error(`create-with-gpt no id: ${JSON.stringify(scenarioResp).slice(0, 200)}`);
    }
    const meetingResp = await createMeeting({
      scenarioId,
      learnerId,
      subject,
      difficulty,
    });
    const meeting = meetingResp?.data || meetingResp;
    const joinUrl = meeting?.meeting_url || meeting?.daily_meeting_url;
    if (joinUrl) {
      await pushJoinLink(learnerId, joinUrl, { mode: 'fresh-scenario', subject, difficulty });
      console.log(`[deliverer] tier1 OK learner=${learnerId} url=${joinUrl}`);
      return { join_url: joinUrl, mode: 'fresh-scenario' };
    }
    throw new Error(`meeting returned no join URL: ${JSON.stringify(meetingResp).slice(0, 200)}`);
  } catch (tier1Err) {
    console.warn(`[deliverer] tier1 failed: ${tier1Err.message.slice(0, 220)}`);
  }

  // Tier 2 — reuse the SAME scenario, embed the prompt as note_context.
  // (Waterr 422 billing_id bug usually hits here too — see Known issues.)
  if (SCENARIO_ID) {
    try {
      const meetingResp = await createMeeting({
        scenarioId: SCENARIO_ID,
        learnerId,
        subject,
        difficulty,
        noteContext: prompt,  // the recommendation prompt rides in note_context
      });
      const meeting = meetingResp?.data || meetingResp;
      const joinUrl = meeting?.meeting_url || meeting?.daily_meeting_url;
      if (joinUrl) {
        await pushJoinLink(learnerId, joinUrl, { mode: 'reuse-scenario', subject, difficulty });
        console.log(`[deliverer] tier2 OK learner=${learnerId} url=${joinUrl}`);
        return { join_url: joinUrl, mode: 'reuse-scenario' };
      }
      throw new Error('meeting returned no join URL');
    } catch (tier2Err) {
      console.warn(`[deliverer] tier2 failed: ${tier2Err.message.slice(0, 220)}`);
    }
  }

  // Tier 3 — public share URL (always works as long as SCENARIO_ID is set)
  if (!SCENARIO_ID) {
    throw new Error('No SCENARIO_ID set; cannot use share-URL fallback');
  }
  const shareUrl = `https://waterr.ai/scenario/${SCENARIO_ID}`;
  await pushJoinLink(learnerId, shareUrl, {
    mode: 'share-url',
    subject,
    difficulty,
    note: 'Used public scenario share URL; note_context not delivered.',
  });
  console.log(`[deliverer] tier3 (share URL) OK learner=${learnerId} url=${shareUrl}`);
  return { join_url: shareUrl, mode: 'share-url' };
}

/* ============================================================
 * boot — wire two consumers + one producer
 * ============================================================ */

function isTransientKafkaErr(err) {
  // First-run race when topics were just created on Confluent Cloud:
  // KafkaJS surfaces "UNKNOWN_TOPIC_OR_PARTITION" until metadata refreshes.
  const msg = String(err?.message || err);
  return /UNKNOWN_TOPIC_OR_PARTITION/i.test(msg)
      || /This server does not host this topic/i.test(msg);
}

async function main() {
  if (!process.env.CONFLUENT_BOOTSTRAP) {
    console.error('[recommender] FATAL: CONFLUENT_BOOTSTRAP missing — Kafka not configured.');
    process.exit(1);
  }
  if (!API_KEY) {
    console.warn('[recommender] WARN: WATERR_API_KEY missing — delivery step will be a no-op.');
  }

  if (USE_NODE_TRANSFORMER) {
    console.log('[recommender] booting in FALLBACK mode (USE_NODE_TRANSFORMER=true)');
    console.log(`[recommender]   transformer:  ${TOPIC_EVENTS}  → ${TOPIC_RECS}   (group=${GROUP_XFORM})`);
  } else {
    console.log('[recommender] booting in DELIVERER-ONLY mode (Flink writes ' +
                TOPIC_RECS + ')');
  }
  console.log(`[recommender]   deliverer:    ${TOPIC_RECS}                       (group=${GROUP_DELIVER})`);
  console.log(`[recommender]   anthropic:    ${ANTHROPIC_API_KEY ? 'ENABLED (' + CLAUDE_MODEL + ')' : 'disabled — using Plan-B template'}`);

  // Shared producer is only needed when the transformer runs.
  let producer = null;
  let xform = null;
  if (USE_NODE_TRANSFORMER) {
    producer = await createProducer();

    // ---- Consumer A: transformer (fallback mode only) -------------------
    xform = await createConsumer({
      groupId: GROUP_XFORM,
      topic: TOPIC_EVENTS,
      autoRun: true,
      fromBeginning: false,
      onMessage: async ({ value, key, offset }) => {
        try {
          // Defensive: kafka.js parses for us; if value is a string, it
          // means parse failed and we should skip.
          if (typeof value !== 'object' || value === null) {
            throw new Error('non-object value (parse failed upstream)');
          }
          const rec = await eventToRecommendation(value);
          const { encodeValue } = await import('./kafka.js');
          await producer.send({
            topic: TOPIC_RECS,
            messages: [{
              key: rec.learner_id ? String(rec.learner_id) : null,
              value: await encodeValue(TOPIC_RECS, rec),
            }],
          });
          console.log(`[transformer] event_id=${value.event_id || '?'} → rec ${rec.event_id} ` +
                      `(learner=${rec.learner_id}, difficulty=${rec.recommendation.difficulty})`);
        } catch (err) {
          if (isTransientKafkaErr(err)) {
            console.warn(`[transformer] transient kafka error, will retry on next msg: ${err.message}`);
            return;
          }
          console.error(`[transformer] SKIP key=${key} offset=${offset}: ${err.message || err}`);
        }
      },
    });
  }

  // ---- Consumer B: deliverer --------------------------------------------
  const deliverer = await createConsumer({
    groupId: GROUP_DELIVER,
    topic: TOPIC_RECS,
    autoRun: true,
    fromBeginning: false,
    onMessage: async ({ value, key, offset }) => {
      try {
        if (typeof value !== 'object' || value === null) {
          throw new Error('non-object value (parse failed upstream)');
        }
        await deliver(value);
      } catch (err) {
        if (isTransientKafkaErr(err)) {
          console.warn(`[deliverer] transient kafka error, will retry on next msg: ${err.message}`);
          return;
        }
        console.error(`[deliverer] SKIP key=${key} offset=${offset}: ${err.message || err}`);
      }
    },
  });

  const shutdown = async (signal) => {
    console.log(`[recommender] ${signal} received — shutting down`);
    if (xform)    { try { await xform.disconnect();    } catch {} }
    try { await deliverer.disconnect(); } catch {}
    if (producer) { try { await producer.disconnect(); } catch {} }
    process.exit(0);
  };
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  console.log('[recommender] running — waiting for learning events…');
}

// Run only when executed directly (not when imported in tests).
// Note: import.meta.url URL-encodes spaces (e.g. "AI%20tutor%20"), while
// process.argv[1] keeps them as literal spaces. Compare via decoded URL.
import { fileURLToPath as _fileURLToPath } from 'node:url';
const _selfPath = _fileURLToPath(import.meta.url);
const isDirectRun = process.argv[1] && (process.argv[1] === _selfPath);
if (isDirectRun) {
  main().catch(err => {
    console.error('[recommender] fatal:', err?.message || err);
    process.exit(1);
  });
}

export {
  eventToRecommendation,
  pickDifficulty,
  planBPrompt,
  buildNextScenarioPrompt,
  deliver,
};
