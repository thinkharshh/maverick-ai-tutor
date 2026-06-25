// src/setup.js
//
// Idempotent bootstrap for the Maverick AI Tutor demo.
//
//   1. GET  /voices/all   → pick a calm female ElevenLabs voice
//   2. POST /personas     → create "Aarya" (skip if a persona with that name already exists)
//   3. POST /scenarios    → create the tutor scenario with TUTOR_PROMPT
//                           (skip if a scenario with that name already exists)
//
// Prints:
//   PERSONA_ID=<uuid>
//   SCENARIO_ID=<uuid>
//
// These two lines are appended to .env by the caller (or by hand).
//
// Refs: docs/TECHNICAL_PLAN.md §3.1 + §3.2

import 'dotenv/config';

const API     = process.env.WATERR_API_BASE || 'https://api.waterr.ai/v1';
const API_KEY = process.env.WATERR_API_KEY;

if (!API_KEY) {
  console.error('FATAL: WATERR_API_KEY is missing from .env');
  process.exit(1);
}

const PERSONA_NAME  = 'Aarya';
const SCENARIO_NAME = 'Maverick: Audio-First Tutor';

// ---------------------------------------------------------------------------
// TUTOR_PROMPT — verbatim from docs/TECHNICAL_PLAN.md §3.2
// ---------------------------------------------------------------------------
const TUTOR_PROMPT = `You are Aarya, a warm and infinitely patient AI tutor designed for blind and
low-vision learners. Teach the subject the learner names at the start.

## Hard rules
- Audio-only language. NEVER say "see", "look at", "watch", "as you can see",
  "in the diagram", "on the screen". Substitute with "imagine", "picture in
  your mind", "feel like", "sounds like".
- Use sound-based analogies: clapping rhythms, splitting groups, slicing a
  chapati, ringing bells, footsteps.
- Repeat any explanation that follows a "I don't get it" / "again" / "wait" /
  silence > 6 seconds. Never sigh, never rush.
- Ask the learner to explain it back ("Can you tell me in your own words?")
  before moving on. This is your truth test.
- When confidence is low, slow the pace and shrink the scope.

## Flow
1. Greet by name (provided in note_context). Ask what they'd like to learn.
2. Teach one concept at a time. Use 2-3 audible analogies per concept.
3. After each concept, ask them to explain it back.
4. End with one small celebration ("That was excellent. You explained X back
   to me without help.").

## Goals (for post-call scoring)
- concept_understood: Did the learner explain the concept back accurately?
- learner_confident: Tone, hesitations, willingness to attempt.
- asked_clarifying_question: A sign of engagement.`;

// ---------------------------------------------------------------------------
// thin Waterr fetch helper
// ---------------------------------------------------------------------------
async function wf(method, path, body) {
  const url = `${API}${path}`;
  const opts = {
    method,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      Accept: 'application/json',
    },
  };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(url, opts);
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  if (!res.ok) {
    throw new Error(`Waterr ${method} ${path} → ${res.status} ${res.statusText}: ${text.slice(0, 500)}`);
  }
  return json;
}

// Waterr wraps payloads as { status, data }. Unwrap to the array/object.
function unwrap(resp) {
  if (resp && Object.prototype.hasOwnProperty.call(resp, 'data')) return resp.data;
  return resp;
}

// ---------------------------------------------------------------------------
// pickVoice — calm female ElevenLabs voice, prefer Bella / Janvi / Ayesha
// ---------------------------------------------------------------------------
function pickVoice(voices) {
  const isElevenFemale = (v) =>
    v.provider === 'elevenlabs' &&
    /female|bella|rachel|sarah|ayesha|janvi|shakuntala|elli|domi/i.test(v.name || '');

  const preferred =
    voices.find(v => v.provider === 'elevenlabs' && /bella/i.test(v.name)) ||
    voices.find(v => v.provider === 'elevenlabs' && /(janvi|ayesha)/i.test(v.name)) ||
    voices.find(isElevenFemale) ||
    voices.find(v => v.provider === 'elevenlabs') ||
    voices[0];

  return preferred;
}

// ---------------------------------------------------------------------------
// idempotent finders
// ---------------------------------------------------------------------------
async function findPersonaByName(name) {
  const list = unwrap(await wf('GET', '/personas'));
  if (!Array.isArray(list)) return null;
  return list.find(p => (p.name || '').trim().toLowerCase() === name.toLowerCase()) || null;
}

async function findScenarioByName(name) {
  const list = unwrap(await wf('GET', '/scenarios'));
  if (!Array.isArray(list)) return null;
  return list.find(s => (s.name || '').trim().toLowerCase() === name.toLowerCase()) || null;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
async function main() {
  // 1. pick voice
  const voices = unwrap(await wf('GET', '/voices/all'));
  if (!Array.isArray(voices) || voices.length === 0) {
    throw new Error('No voices returned from /voices/all');
  }
  const voice = pickVoice(voices);
  console.error(`[setup] using voice: ${voice.name} (${voice.provider}) id=${voice.id}`);

  // 2. persona — idempotent
  let persona = await findPersonaByName(PERSONA_NAME);
  if (persona) {
    console.error(`[setup] persona "${PERSONA_NAME}" already exists, reusing id=${persona.id}`);
  } else {
    console.error(`[setup] creating persona "${PERSONA_NAME}"`);
    const created = await wf('POST', '/personas', {
      name: PERSONA_NAME,
      job_title: 'Patient Audio-First Tutor',
      demeanor: 'empathetic',
      background:
        'A patient tutor specialized in teaching visually impaired learners. ' +
        'Never uses visual language. Repeats concepts as many times as needed. ' +
        'Asks the learner to explain back in their own words.',
      gender: 'female',
      voice_id: voice.id,
    });
    persona = unwrap(created);
    if (!persona || !persona.id) {
      throw new Error(`Persona creation returned no id: ${JSON.stringify(created).slice(0, 400)}`);
    }
  }

  // 3. scenario — idempotent
  let scenario = await findScenarioByName(SCENARIO_NAME);
  if (scenario) {
    console.error(`[setup] scenario "${SCENARIO_NAME}" already exists, reusing id=${scenario.id}`);
  } else {
    console.error(`[setup] creating scenario "${SCENARIO_NAME}"`);
    const created = await wf('POST', '/scenarios', {
      name: SCENARIO_NAME,
      type: 'upskill',
      persona_id: persona.id,
      call_duration: 15,
      visibility: 'public',
      welcome_message:
        "Hello, I'm Aarya. Tell me what you'd like to learn today — I'm all ears.",
      prompt: TUTOR_PROMPT,
    });
    scenario = unwrap(created);
    if (!scenario || !scenario.id) {
      throw new Error(`Scenario creation returned no id: ${JSON.stringify(created).slice(0, 400)}`);
    }
  }

  // 4. emit ids — stdout only, machine-parseable
  console.log(`PERSONA_ID=${persona.id}`);
  console.log(`SCENARIO_ID=${scenario.id}`);
}

main().catch((err) => {
  console.error('[setup] FAILED:', err && err.message ? err.message : err);
  process.exit(1);
});
