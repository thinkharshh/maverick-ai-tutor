# Technical Plan — Maverick · Preventive Maintenance Coach

This is the code-level companion to [claude.md](claude.md). It defines the data model, services, topics, SQL, and the exact sequence to ship in 60 minutes for the shop-floor preventive-maintenance training use case at a car manufacturing plant.

> **Field-name note:** topics and JSON envelopes keep generic names (`learning.events`, `learner.recommendations`, `learner.id`). Read "learner" as "the technician training on this station". Kept generic so the same Kafka contract serves multiple plant types without a rename.

---

## 1. System components (5 small pieces)

| # | Component | Runtime | LOC est. | Responsibility |
|---|-----------|---------|---------:|----------------|
| 1 | `setup.js` | Node, one-shot | ~80 | Idempotently create persona + scenario + register webhook. |
| 2 | `server.js` | Node/Express, long-running | ~150 | Accessible landing page, `/start` (creates meeting), `/webhook/waterr` (HMAC verify → Kafka producer). |
| 3 | Confluent Cloud | managed | n/a | 2 Kafka topics + 1 Flink SQL job + Tableflow. |
| 4 | `recommender.js` | Node consumer, long-running | ~100 | Consumes `learner.recommendations`, calls Waterr to create the *next* adaptive meeting, pushes URL out. |
| 5 | `public/index.html` | static | ~60 | Screen-reader-first landing page (one button + `aria-live` region). |

Five files. Three processes. One Flink job. Done.

---

## 2. Data model (the single source of truth)

### 2.1 Kafka envelope — `LearningEvent`

Produced by the webhook bridge after every completed session.

```json
{
  "schema_version": 1,
  "event_id": "evt_01HXY...",
  "occurred_at": "2026-06-25T10:42:00Z",
  "learner": {
    "id": "tech_anon_abc",
    "display_name": "Ravi (Press Shop A)",
    "contact_channel": "sms:+91XXXXXXXXXX"
  },
  "meeting": {
    "id": "meeting-uuid-from-waterr",
    "scenario_id": "scenario-uuid",
    "subject": "press.hydraulic.daily-pm",
    "duration_seconds": 612
  },
  "performance": {
    "average_score": 64,
    "goal_results": [
      { "goal": "procedure_understood",      "score": 2, "feedback": "Missed two steps in the LOTO sequence on the hydraulic isolation valve." },
      { "goal": "technician_confident",      "score": 3, "feedback": "" },
      { "goal": "asked_clarifying_question", "score": 4, "feedback": "" }
    ],
    "filler_word_rate": 0.12,
    "growth_areas": "Re-teach LOTO sequence on hydraulic isolation valve with stepwise call-back; technician asked 'why' 4 times without resolution.",
    "strengths": "Methodical, double-checks readings against the OEM service interval card."
  },
  "raw_transcript_ref": "waterr://meetings/{meeting_id}"
}
```

**Partition key:** `learner.id` (technician id) — guarantees per-technician ordering in Flink state.

### 2.2 Kafka envelope — `LessonRecommendation`

Emitted by the Flink job into `learner.recommendations`. (Logically a `DrillRecommendation` for this use case; the type name is kept for wire compatibility.)

```json
{
  "schema_version": 1,
  "event_id": "rec_01HXY...",
  "occurred_at": "2026-06-25T10:43:10Z",
  "learner_id": "tech_anon_abc",
  "contact_channel": "sms:+91XXXXXXXXXX",
  "recommendation": {
    "subject": "press.hydraulic.daily-pm",
    "difficulty": "easier",
    "next_scenario_prompt": "You are Aarya, a senior preventive-maintenance coach. The technician Ravi missed two steps in the LOTO sequence on the hydraulic isolation valve. Re-teach the daily-PM walk-around for an 800-ton hydraulic press starting from energy isolation. Cite OEM service interval (daily, pre-shift) and IATF 16949 clause 7.1.5.2 on monitoring equipment. End by asking him to walk back his first three moves in his own words.",
    "estimated_duration_min": 12
  }
}
```

---

## 3. WaterrAI integration — exact calls

### 3.1 `setup.js` — idempotent bootstrap

```js
// 1. find a calm female ElevenLabs voice
const voices = await wf('GET', '/voices/all');
const voice  = voices.find(v => v.provider === 'elevenlabs' && /sarah|rachel|bella/i.test(v.name)) || voices[0];

// 2. create the persona
const persona = await wf('POST', '/personas', {
  name: 'Aarya',
  job_title: 'Senior Preventive Maintenance Coach',
  demeanor: 'methodical, safety-first, patient',
  background: 'A senior preventive-maintenance coach who has trained shop-floor technicians at car manufacturing plants for 15 years. Speaks SOP-grade plant language. Cites OEM service intervals (daily / shift / weekly / monthly / yearly) and IATF 16949 clauses where applicable. Uses station-grounded analogies (a press die\'s heartbeat, the hum of a healthy servo). Asks the technician to walk procedures back in their own words. Never bypasses LOTO, never advises running a station with a known fault code.',
  gender: 'female',
  voice_id: voice.id
});

// 3. create the coach scenario (goals can be inlined; see prompting guide)
const scenario = await wf('POST', '/scenarios', {
  name: 'Maverick: Preventive Maintenance Coach',
  type: 'upskill',
  persona_id: persona.data.id,
  call_duration: 15,
  visibility: 'public',
  welcome_message: "Hello, I'm Aarya. Tell me which station you're drilling today and the procedure you want to walk — I'll meet you on the line.",
  prompt: COACH_PROMPT,   // see §3.2
  goals: [/* optional precreated goal UUIDs */]
});

// 4. register webhook on the scenario
await wf('PUT', `/scenarios/${scenario.data.id}/session-options`, {
  webhook_url: `${process.env.WEBHOOK_PUBLIC_URL}/webhook/waterr`
});

console.log('SCENARIO_ID=' + scenario.data.id);  // copy to .env
```

### 3.2 The coach prompt (`COACH_PROMPT`)

```text
You are Aarya, a methodical and infinitely patient senior preventive-maintenance
coach for shop-floor technicians at a car manufacturing plant. Walk the
technician through the PM procedure on the station they name at the start
(e.g. robotic spot-welder, hydraulic press, paint booth, conveyor lubrication,
EV battery line, powertrain test bench, QC gauge calibration).

## Hard rules
- SOP-grade plant language. Use "LOTO" (lockout-tagout), "torque-to-spec",
  "OEM service interval", "vibration signature", "thermal imaging", "MTBF",
  "MTTR", "fault code", "shift handover".
- Cite the relevant PM interval (daily / shift / weekly / monthly / yearly)
  and the reference document (OEM manual section / IATF 16949 clause / ISO 9001
  clause) whenever a step has one.
- Use station-grounded analogies: a press die's heartbeat, the hum of a healthy
  servo, the rhythm of a balanced robot arm, the smell of a properly cured
  paint booth.
- Repeat any explanation that follows "I don't get it" / "again" / "wait" /
  silence > 6 seconds. Never sigh, never rush — technicians are often on the
  line under shift-time pressure.
- Ask the technician to walk the procedure back ("Can you walk me through your
  first move in your own words?") before moving on. This is your truth test.
- When confidence is low, slow the pace and shrink the scope to one sub-system.
- Safety first: never bypass LOTO, never advise running a station with a known
  fault code, never skip the try-step. If a question implies an unsafe
  shortcut, redirect to the safe SOP.

## Flow
1. Greet by name (provided in note_context). Ask which station and which
   failure mode / procedure they want to drill today.
2. Walk one PM step at a time. Use 2-3 station-grounded analogies per step.
3. After each step, ask them to walk it back.
4. End with one small celebration ("Solid. You walked back the bearing
   inspection without a prompt.").

## Goals (for post-shift scoring)
- procedure_understood: Did the technician walk back the PM procedure accurately?
- technician_confident: Tone, hesitations, willingness to attempt.
- asked_clarifying_question: A sign of engagement — flags rising competence.
```

### 3.3 `/start` route (create-meeting)

```js
app.post('/start', express.json(), async (req, res) => {
  const { learner_name = 'Friend', subject = 'press.hydraulic.daily-pm', contact = '' } = req.body;
  const meeting = await wf('POST', '/meetings', {
    person_name: learner_name,
    scenario_id: process.env.SCENARIO_ID,
    note_context: `Technician name: ${learner_name}. Station / procedure requested: ${subject}. Contact for follow-up: ${contact}.`
  });
  res.json({
    meeting_id: meeting.data.id,
    join_url:  meeting.data.meeting_url,   // hosted page — easiest
    daily_url: meeting.data.daily_meeting_url
  });
});
```

### 3.4 `/webhook/waterr` — HMAC + Kafka producer

```js
import crypto from 'node:crypto';
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  brokers: [process.env.CONFLUENT_BOOTSTRAP],
  ssl: true,
  sasl: { mechanism: 'plain', username: process.env.CONFLUENT_API_KEY, password: process.env.CONFLUENT_API_SECRET }
});
const producer = kafka.producer();
await producer.connect();

app.post('/webhook/waterr',
  express.raw({ type: 'application/json' }),
  async (req, res) => {
    const sig = req.headers['x-waterr-signature'] || '';
    const expected = 'sha256=' + crypto
      .createHmac('sha256', process.env.WATERR_WEBHOOK_SECRET)
      .update(req.body).digest('hex');
    if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) {
      return res.status(401).end();
    }

    const payload = JSON.parse(req.body.toString('utf8'));
    if (payload.event !== 'session.analysis_complete') return res.json({ ok: true });

    const evt = toLearningEvent(payload);  // shape from §2.1
    await producer.send({
      topic: process.env.TOPIC_LEARNING_EVENTS,
      messages: [{ key: evt.learner.id, value: JSON.stringify(evt) }]
    });
    res.json({ ok: true });
  });
```

---

## 4. Confluent Cloud setup

### 4.1 Topics (create in UI, 1 partition is fine for demo)

| Topic | Cleanup | Key | Notes |
|---|---|---|---|
| `learning.events` | delete, 7d | `learner_id` (technician id) | All PM session / drill outcomes. |
| `learner.recommendations` | delete, 7d | `learner_id` (technician id) | Output of Flink. Consumed by `recommender.js`. |

Both topics: **Tableflow → ON** (Iceberg). Zero extra code; we get the IATF / ISO audit trail for free.

### 4.2 Flink SQL job

Paste this in Confluent Cloud → Flink → SQL Workspace. Assumes the topics above and a registered AI model named `next_lesson_model` pointing at Claude (Confluent Cloud → AI Model Inference → Anthropic).

```sql
-- Source: learning events
CREATE TABLE learning_events (
  schema_version INT,
  event_id       STRING,
  occurred_at    TIMESTAMP_LTZ(3),
  learner        ROW<id STRING, display_name STRING, contact_channel STRING>,
  meeting        ROW<id STRING, scenario_id STRING, subject STRING, duration_seconds INT>,
  performance    ROW<
    average_score INT,
    goal_results  ARRAY<ROW<goal STRING, score INT, feedback STRING>>,
    filler_word_rate DOUBLE,
    growth_areas  STRING,
    strengths     STRING
  >,
  raw_transcript_ref STRING,
  WATERMARK FOR occurred_at AS occurred_at - INTERVAL '5' SECOND
) WITH (
  'connector' = 'confluent',
  'topic' = 'learning.events',
  'value.format' = 'json'
);

-- Sink: lesson recommendations
CREATE TABLE learner_recommendations (
  schema_version INT,
  event_id       STRING,
  occurred_at    TIMESTAMP_LTZ(3),
  learner_id     STRING,
  contact_channel STRING,
  recommendation ROW<
    subject STRING,
    difficulty STRING,
    next_scenario_prompt STRING,
    estimated_duration_min INT
  >
) WITH (
  'connector' = 'confluent',
  'topic' = 'learner.recommendations',
  'value.format' = 'json'
);

-- The job: enrich each event with an AI-generated next-lesson prompt
INSERT INTO learner_recommendations
SELECT
  1                                              AS schema_version,
  CONCAT('rec_', event_id)                       AS event_id,
  CURRENT_TIMESTAMP                              AS occurred_at,
  learner.id                                     AS learner_id,
  learner.contact_channel                        AS contact_channel,
  ROW(
    meeting.subject,
    CASE
      WHEN performance.average_score < 60 THEN 'easier'
      WHEN performance.average_score > 85 THEN 'harder'
      ELSE 'same'
    END,
    -- Single ML_PREDICT call generates the full next-lesson prompt
    ML_PREDICT(
      'next_lesson_model',
      CONCAT(
        'You are designing the next 12-minute preventive-maintenance drill for a ',
        'shop-floor technician at a car manufacturing plant. ',
        'Station / subject: ', meeting.subject, '. ',
        'Previous score: ', CAST(performance.average_score AS STRING), '/100. ',
        'Skill gaps observed last session: ', performance.growth_areas, '. ',
        'Output ONLY the new coach system prompt — SOP-grade language, station-grounded ',
        'analogies, cite the relevant PM interval and OEM / IATF reference where applicable, ',
        'ask the technician to walk the procedure back. No preamble.'
      )
    ),
    12
  )                                              AS recommendation
FROM learning_events
WHERE performance.average_score IS NOT NULL;
```

### 4.3 Tableflow

In each topic's settings → **Enable Tableflow**. Iceberg tables appear automatically. Mention this in the pitch as the "audit trail for plant supervisors, quality, and IATF 16949 / ISO 9001 auditors" — zero extra code.

---

## 5. `recommender.js` — closes the loop

```js
import { Kafka } from 'kafkajs';

const consumer = kafka.consumer({ groupId: 'maverick-recommender' });
await consumer.connect();
await consumer.subscribe({ topic: process.env.TOPIC_RECOMMENDATIONS });

await consumer.run({
  eachMessage: async ({ message }) => {
    const rec = JSON.parse(message.value.toString());

    // 1. ask Waterr to generate a new scenario from the AI-crafted prompt
    const form = new FormData();
    form.append('userInput', rec.recommendation.next_scenario_prompt);
    form.append('duration', String(rec.recommendation.estimated_duration_min));
    const scenario = await fetch(`${API}/scenarios/create-with-gpt`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${process.env.WATERR_API_KEY}` },
      body: form
    }).then(r => r.json());

    // 2. spin up the actual meeting
    const meeting = await wf('POST', '/meetings', {
      person_name: rec.learner_id,
      scenario_id: scenario.data.id,
      note_context: `Adaptive PM drill follow-up. Station / procedure: ${rec.recommendation.subject}. Difficulty: ${rec.recommendation.difficulty}.`
    });

    // 3. push the link back (SMS via Twilio, or our SSE channel)
    await deliverLink(rec.contact_channel, meeting.data.meeting_url);
  }
});
```

---

## 6. Workstation-kiosk landing page (`public/index.html`)

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Maverick — Preventive Maintenance Coach</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
  <main>
    <h1>Maverick — Preventive Maintenance Coach</h1>
    <p>Press the button below to start a preventive-maintenance drill. A senior coach named Aarya will greet you on the line.</p>
    <form id="f" aria-describedby="hint">
      <label for="name">Your name</label>
      <input id="name" name="learner_name" autocomplete="given-name" required>
      <label for="subj">Which station / procedure are you drilling today?</label>
      <input id="subj" name="subject" value="press.hydraulic.daily-pm" required>
      <button type="submit" id="go">Start drill</button>
    </form>
    <div id="status" role="status" aria-live="polite" aria-atomic="true"></div>
  </main>
  <script>
    const f = document.getElementById('f'), s = document.getElementById('status');
    f.addEventListener('submit', async e => {
      e.preventDefault();
      s.textContent = 'Connecting Aarya — one moment.';
      const body = Object.fromEntries(new FormData(f));
      const r = await fetch('/start', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) }).then(r=>r.json());
      s.textContent = 'Connected. Opening the drill room now.';
      window.location = r.join_url;
    });
  </script>
</body>
</html>
```

Key shop-floor UX details:
- One `<main>` landmark, one `<h1>`.
- `<label for>` on every input — every kiosk / tablet announces them through the headset.
- `aria-live="polite"` status region: every state change is read aloud automatically.
- No ARIA-required JS focus traps; native form submit handles it (good for single-button line-side actuators).
- Big native button — usable with gloved hands.

---

## 7. Run order (10 commands)

```bash
# Terminal 1 — tunnel for Waterr webhook
ngrok http 3000
# copy https URL into .env WEBHOOK_PUBLIC_URL

# Terminal 2 — bootstrap (run once)
cd "AI tutor"
npm install express kafkajs node-fetch dotenv
node setup.js                    # creates persona + scenario; prints SCENARIO_ID

# update .env with SCENARIO_ID and the webhook secret printed by setup

# Terminal 2 — web + webhook bridge
node server.js                   # listens on :3000

# Terminal 3 — recommender consumer
node recommender.js

# Confluent Cloud — paste flink/job.sql, click Run
```

---

## 8. Demo script (3 minutes — what we say + what the judges see)

| 00:00 | Open `https://<ngrok>` on the workstation kiosk — headset on, hands behind your back (gloves on). Page narrates "Maverick — Preventive Maintenance Coach. Your name, edit text." |
| 00:20 | Enter "Ravi", station "press.hydraulic.daily-pm", press Enter. Status region announces "Connected. Opening the drill room now." |
| 00:30 | Aarya greets in a clear coaching voice. Ravi (you) intentionally says "I always trip on the LOTO sequence on the isolation valve". Aarya re-teaches the LOTO sequence step by step, citing OEM daily PM interval. End the call after ~90s. |
| 02:00 | Switch to Confluent Cloud → Flink → show the `learning.events` topic receiving the event live → show `learner.recommendations` getting a row 5-10s later (the `next_scenario_prompt` is the AI-generated next drill). |
| 02:30 | Phone buzzes (Twilio SMS) with a new meeting link tailored to the LOTO sequence. Click it — Aarya immediately starts re-teaching with a *new* approach (step-by-step call-and-response). |
| 02:55 | Show the Iceberg table via Tableflow — "this is the audit trail for plant supervisors, quality, and IATF auditors." |

End: "The technician's hands stayed on the tools. The Confluent stream made the next drill adapt in seconds, not after the next monthly training cycle."

---

## 9. Risks & cuts (in order, if we slip)

1. **Drop SMS** — replace with SSE on the landing page; status region announces the new link. (Saves Twilio setup.)
2. **Drop Tableflow** — claim it as roadmap. (Saves 2 minutes of UI clicking.)
3. **Skip Flink `ML_PREDICT`** — let `recommender.js` call Claude directly when it consumes the message. (Loses the "AI inside the stream" wow factor — but still works.)
4. **Hardcode one scenario** — skip `create-with-gpt`, just `note_context` the next lesson hints into the same scenario. (Lower polish, still demos.)

Cut order is left → right. Right-to-left = better demo.

---

## 10. What I still need from you

- **Confluent Cloud:** bootstrap server, API key + secret, Schema Registry creds. (Schema Registry can be skipped for the demo — JSON works.)
- **Webhook secret:** the value Waterr will set when we register the webhook (printed by setup script — we'll capture it then).
- Confirm: laptop with `node 20+`, `ngrok` installed?
- Confirm Twilio creds or "skip SMS, use SSE" (default I'll assume **SSE**).

Reply with the Confluent creds (or paste a Confluent Cloud cluster name and I'll walk you through where to grab them) and I'll start writing `setup.js`.
