// scripts/datagen-cohort.js
//
// Synthetic learner cohort generator — produces realistic LearningEvent records
// into `learning.events` at a configurable rate. Gives the Flink job real
// volume to process during the demo (instead of one event per real Waterr
// session), so judges see the stream pipeline working at scale.
//
// USAGE
//   node --env-file=.env scripts/datagen-cohort.js [--rps=5] [--duration=300]
//
//   --rps      records per second  (default 5)
//   --duration seconds to run      (default 300; 0 = forever)
//   --topic    target topic        (default $TOPIC_LEARNING_EVENTS or 'learning.events')
//
// Each event matches /schemas/learning.events-value.json exactly and is
// encoded through Schema Registry if creds are set. Partition key = learner.id.

import 'dotenv/config';
import crypto from 'node:crypto';
import { createProducer, encodeValue } from '../src/kafka.js';

// ---- args ------------------------------------------------------------------
function arg(name, fallback) {
  const m = process.argv.find(a => a.startsWith(`--${name}=`));
  return m ? m.slice(name.length + 3) : fallback;
}
const RPS      = Number(arg('rps', 5));
const DURATION = Number(arg('duration', 300));
const TOPIC    = arg('topic', process.env.TOPIC_LEARNING_EVENTS || 'learning.events');

// ---- synthetic universe ----------------------------------------------------
const NAMES = [
  'Asha', 'Rohan', 'Meera', 'Arjun', 'Priya', 'Karan', 'Diya', 'Vivaan',
  'Sara', 'Ishaan', 'Anaya', 'Aarav', 'Tara', 'Kabir', 'Zara', 'Reyansh',
  'Aisha', 'Dhruv', 'Myra', 'Ayaan', 'Saanvi', 'Veer', 'Riya', 'Krishna',
];
const SUBJECTS = [
  'math.fractions', 'math.algebra', 'math.geometry',
  'reading.phonics', 'reading.comprehension',
  'science.matter', 'science.life_cycles',
  'social.geography', 'social.civics',
];
const GROWTH_AREAS = {
  'math.fractions':       'denominators confused; needs concrete sound-based examples',
  'math.algebra':         'variable substitution feels arbitrary; ground in story problems',
  'math.geometry':        'angle vocabulary unsure; use physical hand-positioning analogies',
  'reading.phonics':      'consonant blends are inconsistent; slow drilling needed',
  'reading.comprehension':'follows plot but misses inference; teach signposting words',
  'science.matter':       'states-of-matter examples beyond water needed',
  'science.life_cycles':  'butterfly stages reversed; reteach with audio sequencing',
  'social.geography':     'directions confused; need cardinal-direction sound game',
  'social.civics':        'roles of officials mixed up; use named-real-people anchors',
};
const STRENGTHS = [
  'patient and willing to retry',
  'asks clarifying questions',
  'explains concepts back in their own words',
  'energetic and engaged',
  'remembers prior lessons unprompted',
];

const COHORT_SIZE = 60;
const COHORT = Array.from({ length: COHORT_SIZE }, (_, i) => {
  const name = NAMES[i % NAMES.length];
  return {
    id: `learner_${String(i + 1).padStart(3, '0')}`,
    display_name: `${name}-${i + 1}`,
    contact_channel: i % 3 === 0
      ? `sms:+9198${String(20000000 + i).padStart(8, '0')}`
      : `sse:web`,
  };
});

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randInt(lo, hi) { return lo + Math.floor(Math.random() * (hi - lo + 1)); }
function randFloat(lo, hi, dp = 2) {
  return Number((lo + Math.random() * (hi - lo)).toFixed(dp));
}

// Score is bimodal — most learners do OK (60-80), some struggle (30-50), few excel (88-96).
function sampleScore() {
  const r = Math.random();
  if (r < 0.15) return randInt(28, 54);   // struggling
  if (r > 0.85) return randInt(86, 96);   // excelling
  return randInt(58, 82);                  // on-track
}

function makeEvent() {
  const learner = pick(COHORT);
  const subject = pick(SUBJECTS);
  const score   = sampleScore();
  const meetingId = crypto.randomUUID();
  const eventId   = `evt_${meetingId}`;

  return {
    schema_version: 1,
    event_id: eventId,
    occurred_at: new Date().toISOString(),
    learner: {
      id: learner.id,
      display_name: learner.display_name,
      contact_channel: learner.contact_channel,
    },
    meeting: {
      id: meetingId,
      scenario_id: process.env.SCENARIO_ID || 'syn-cohort',
      subject,
      duration_seconds: randInt(420, 900),
    },
    performance: {
      average_score: score,
      filler_word_rate: randFloat(0.02, 0.18),
      growth_areas: GROWTH_AREAS[subject] || 'general review',
      strengths: pick(STRENGTHS),
      goal_results: [
        { goal: 'concept_understood',        score: randInt(1, 5), feedback: '' },
        { goal: 'learner_confident',         score: randInt(1, 5), feedback: '' },
        { goal: 'asked_clarifying_question', score: randInt(1, 5), feedback: '' },
      ],
    },
    raw_transcript_ref: `waterr://meetings/${meetingId}`,
  };
}

// ---- main loop -------------------------------------------------------------
async function main() {
  if (!process.env.CONFLUENT_BOOTSTRAP) {
    console.error('[datagen] FATAL: CONFLUENT_BOOTSTRAP missing');
    process.exit(1);
  }

  console.log(`[datagen] topic=${TOPIC} rps=${RPS} duration=${DURATION || 'forever'}s cohort=${COHORT_SIZE}`);
  const producer = await createProducer();

  const startedAt = Date.now();
  let produced = 0;
  const intervalMs = Math.max(1, Math.floor(1000 / RPS));

  let stopping = false;
  const stop = () => { stopping = true; };
  process.on('SIGINT',  stop);
  process.on('SIGTERM', stop);

  // Steady cadence with simple sleep loop (RPS is low enough).
  while (!stopping) {
    const evt = makeEvent();
    try {
      await producer.send({
        topic: TOPIC,
        messages: [{
          key: evt.learner.id,
          value: await encodeValue(TOPIC, evt),
        }],
      });
      produced++;
      if (produced % Math.max(10, RPS * 5) === 0) {
        console.log(`[datagen] produced=${produced} learners_seen≈${Math.min(produced, COHORT_SIZE)} elapsed=${Math.round((Date.now() - startedAt) / 1000)}s`);
      }
    } catch (err) {
      console.error('[datagen] send failed:', err?.message || err);
    }

    if (DURATION > 0 && (Date.now() - startedAt) / 1000 >= DURATION) break;
    await new Promise(r => setTimeout(r, intervalMs));
  }

  console.log(`[datagen] done. total=${produced}`);
  await producer.disconnect().catch(() => {});
  process.exit(0);
}

main().catch(err => {
  console.error('[datagen] fatal:', err?.message || err);
  process.exit(1);
});
