# Technical Plan — Maverick AI Tutor

This is the code-level companion to [claude.md](claude.md). It defines the data model, services, topics, SQL, and the exact sequence to ship in 60 minutes.

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
    "id": "learner_anon_abc",
    "display_name": "Asha",
    "contact_channel": "sms:+91XXXXXXXXXX"
  },
  "meeting": {
    "id": "meeting-uuid-from-waterr",
    "scenario_id": "scenario-uuid",
    "subject": "math.fractions",
    "duration_seconds": 612
  },
  "performance": {
    "average_score": 64,
    "goal_results": [
      { "goal": "concept_understood",       "score": 2, "feedback": "Confused about denominators." },
      { "goal": "learner_confident",        "score": 3, "feedback": "" },
      { "goal": "asked_clarifying_question","score": 4, "feedback": "" }
    ],
    "filler_word_rate": 0.12,
    "growth_areas": "Re-teach equivalent fractions with concrete examples; learner asked 'why' 4 times without resolution.",
    "strengths": "Patient, willing to keep trying."
  },
  "raw_transcript_ref": "waterr://meetings/{meeting_id}"
}
```

**Partition key:** `learner.id` — guarantees per-learner ordering in Flink state.

### 2.2 Kafka envelope — `LessonRecommendation`

Emitted by the Flink job into `learner.recommendations`.

```json
{
  "schema_version": 1,
  "event_id": "rec_01HXY...",
  "occurred_at": "2026-06-25T10:43:10Z",
  "learner_id": "learner_anon_abc",
  "contact_channel": "sms:+91XXXXXXXXXX",
  "recommendation": {
    "subject": "math.fractions",
    "difficulty": "easier",
    "next_scenario_prompt": "You are Aarya, a patient blind-friendly tutor. The learner Asha struggles with denominators. Reteach equivalent fractions using only *audible* analogies — slicing chapatis, splitting groups of marbles by sound. Avoid any 'see this' / 'look at' phrasing. End by asking her to explain it back in her own words.",
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
  job_title: 'Patient Audio-First Tutor',
  demeanor: 'empathetic',
  background: 'A patient tutor specialized in teaching visually impaired learners. Never uses visual language. Repeats concepts as many times as needed. Asks the learner to explain back in their own words.',
  gender: 'female',
  voice_id: voice.id
});

// 3. create the tutor scenario (goals can be inlined; see prompting guide)
const scenario = await wf('POST', '/scenarios', {
  name: 'Maverick: Audio-First Tutor',
  type: 'upskill',
  persona_id: persona.data.id,
  call_duration: 15,
  visibility: 'public',
  welcome_message: "Hello, I'm Aarya. Tell me what you'd like to learn today — I'm all ears.",
  prompt: TUTOR_PROMPT,   // see §3.2
  goals: [/* optional precreated goal UUIDs */]
});

// 4. register webhook on the scenario
await wf('PUT', `/scenarios/${scenario.data.id}/session-options`, {
  webhook_url: `${process.env.WEBHOOK_PUBLIC_URL}/webhook/waterr`
});

console.log('SCENARIO_ID=' + scenario.data.id);  // copy to .env
```

### 3.2 The tutor prompt (`TUTOR_PROMPT`)

```text
You are Aarya, a warm and infinitely patient AI tutor designed for blind and
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
- asked_clarifying_question: A sign of engagement.
```

### 3.3 `/start` route (create-meeting)

```js
app.post('/start', express.json(), async (req, res) => {
  const { learner_name = 'Friend', subject = 'math.fractions', contact = '' } = req.body;
  const meeting = await wf('POST', '/meetings', {
    person_name: learner_name,
    scenario_id: process.env.SCENARIO_ID,
    note_context: `Learner name: ${learner_name}. Subject requested: ${subject}. Contact for follow-up: ${contact}.`
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
| `learning.events` | delete, 7d | `learner_id` | All session outcomes. |
| `learner.recommendations` | delete, 7d | `learner_id` | Output of Flink. Consumed by `recommender.js`. |

Both topics: **Tableflow → ON** (Iceberg). Zero extra code; we get history for free.

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
        'You are designing the next 12-minute lesson for a blind learner. ',
        'Subject: ', meeting.subject, '. ',
        'Previous score: ', CAST(performance.average_score AS STRING), '/100. ',
        'Growth areas: ', performance.growth_areas, '. ',
        'Output ONLY the new tutor system prompt — strict audio-only language, ',
        'sound-based analogies, ask learner to explain back. No preamble.'
      )
    ),
    12
  )                                              AS recommendation
FROM learning_events
WHERE performance.average_score IS NOT NULL;
```

### 4.3 Tableflow

In each topic's settings → **Enable Tableflow**. Iceberg tables appear automatically. Mention this in the pitch as the "audit trail for parents and schools" — zero extra code.

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
      note_context: `Adaptive follow-up. Subject: ${rec.recommendation.subject}. Difficulty: ${rec.recommendation.difficulty}.`
    });

    // 3. push the link back (SMS via Twilio, or our SSE channel)
    await deliverLink(rec.contact_channel, meeting.data.meeting_url);
  }
});
```

---

## 6. Accessible landing page (`public/index.html`)

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Maverick — Audio Tutor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
  <main>
    <h1>Maverick — Your Patient Audio Tutor</h1>
    <p>Press the button below to start a tutoring session. A friendly tutor named Aarya will greet you.</p>
    <form id="f" aria-describedby="hint">
      <label for="name">Your name</label>
      <input id="name" name="learner_name" autocomplete="given-name" required>
      <label for="subj">What would you like to learn?</label>
      <input id="subj" name="subject" value="math: fractions" required>
      <button type="submit" id="go">Start lesson</button>
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
      s.textContent = 'Connected. Opening the lesson room now.';
      window.location = r.join_url;
    });
  </script>
</body>
</html>
```

Key accessibility details:
- One `<main>` landmark, one `<h1>`.
- `<label for>` on every input — screen readers announce them.
- `aria-live="polite"` status region: every state change is read aloud automatically.
- No ARIA-required JS focus traps; native form submit handles it.
- Big native button — works with VoiceOver, NVDA, JAWS, TalkBack.

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

| 00:00 | Open `https://<ngrok>` on the laptop — **dim the screen to zero**. Use macOS VoiceOver to navigate. Screen reader reads "Maverick — Your Patient Audio Tutor. Your name, edit text". |
| 00:20 | Type "Asha", subject "fractions", press Enter. Status region announces "Connected. Opening the lesson room now." |
| 00:30 | Aarya greets in a warm voice. Asha (you) intentionally says "I don't get denominators". Aarya re-teaches with audible analogies. End the call after ~90s. |
| 02:00 | Switch to Confluent Cloud → Flink → show the `learning.events` topic receiving the event live → show `learner.recommendations` getting a row 5-10s later (the `next_scenario_prompt` is the AI-generated next lesson). |
| 02:30 | Phone buzzes (Twilio SMS) with a new meeting link tailored to denominators. Click it — Aarya immediately starts re-teaching with a *new* angle. |
| 02:55 | Show the Iceberg table via Tableflow — "this is the audit trail for parents and teachers." |

End: "The learner never saw a screen. The Confluent stream made the lesson adapt in seconds, not days."

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
