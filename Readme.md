# Maverick — AI Tutor

A voice-first AI tutor for blind and low-vision learners. Built on **WaterrAI** (live AI video tutor) + **Confluent Cloud** (Kafka + Flink + Schema Registry + Tableflow). The Confluent stream turns each completed lesson into an AI-crafted, harder/easier follow-up lesson — adaptive learning that updates in seconds, not days.

Originally built for **Confluent AI Day 2026 India**.

## Live demo

**<https://maverick-ai-tutor.vercel.app>**

Open it, click in. The lesson runs inside the page — voice-first, screen-reader friendly. No login.

## Rubric scorecard (all live)

| Rubric box | Status |
|---|---|
| Kafka cluster + topics | ✅ live |
| Schema Registry | ✅ live — 3 subjects registered |
| Stream Processing (Flink SQL) | ✅ live — `maverick-tutor-ml-v2` RUNNING |
| Flink AI Model Inference | ✅ live — **Gemini 2.5 Flash via Vertex AI** |
| Connector(s) | ✅ live — Datagen Source RUNNING |
| Tableflow | ✅ live — both topics in ICEBERG format |
| HTTP Sink Connector (bonus) | 🟡 awaits ngrok URL |
| Voice-first demo (Waterr) | ✅ live at the URL above |

Sample Gemini-generated next-lesson prompt from the live `learner.recommendations` topic:

> **learner_046 (social.geography, score 79):** *"Hello there! It's Aarya, your guide for social geography. Great to have you back. I remember your last score of 79 was really solid! Today, we're going to tackle something that tripped us up a little last time — our cardinal directions. We'll make it super clear with some fun sound challenges..."*

Each one personalized by Gemini per learner per session, generated inside the Flink SQL job.

---

## Table of contents

1. [What it does](#1-what-it-does)
2. [Architecture at a glance](#2-architecture-at-a-glance)
3. [Repo map](#3-repo-map)
4. [Data model](#4-data-model)
5. [Components in detail](#5-components-in-detail)
6. [Confluent Cloud setup](#6-confluent-cloud-setup)
7. [Flink SQL job](#7-flink-sql-job)
8. [Environment variables](#8-environment-variables)
9. [Local run order](#9-local-run-order)
10. [Vercel deployment](#10-vercel-deployment)
11. [Accessibility design](#11-accessibility-design)
12. [Security notes](#12-security-notes)
13. [Demo script](#13-demo-script)
14. [Cut-list / risks](#14-cut-list--risks)

---

## 1. What it does

A blind learner opens [public/index.html](public/index.html) — a screen-reader-first page with one form. They type a name and a subject, press **Start lesson**, and an `aria-live` region announces "Connecting Aarya — one moment." A live AI tutor (Aarya, an ElevenLabs voice driven by WaterrAI) joins via an embedded iframe and teaches the requested topic using only audio-friendly language.

When the session ends:

1. WaterrAI fires `session.analysis_complete` to our webhook with goal scores, growth areas, transcript ref.
2. The webhook validates HMAC, normalises the payload into a `LearningEvent`, and produces it to the Kafka topic `learning.events`.
3. A Flink SQL job consumes that topic, calls Claude via `ML_PREDICT()` to draft the **next** lesson prompt tailored to the learner's growth areas, and writes a `LessonRecommendation` to `learner.recommendations`.
4. A Node consumer (`recommender.js`) picks up the recommendation, asks Waterr to create a fresh scenario from the AI-generated prompt, spins up a new meeting, and pushes the join URL back to the learner via Server-Sent Events (the page reads it aloud via `aria-live`).
5. Both topics have **Tableflow** enabled, so every event is materialised as an Iceberg table for parents/teachers/auditors — zero extra code.

The learner never has to see a screen.

---

## 2. Architecture at a glance

```
                +----------------+        WebRTC          +-----------+
   Learner ---->|  index.html    | <--------------------> |  WaterrAI |
   (browser)    |  (a11y page)   |   (Aarya, live voice)  |  scenario |
                +-------+--------+                        +-----+-----+
                        | POST /start                           | session.analysis_complete
                        v                                       v
                +----------------+   HMAC + JSON shaping  +-----------+
                |   server.js    | <----------------------+  webhook  |
                | Express on     |                        +-----------+
                | Node / Vercel  |
                +-------+--------+
                        |  produce LearningEvent  (Schema Registry wire format)
                        v
   +-------------------------------------------------------------------------+
   |  Confluent Cloud                                                        |
   |                                                                         |
   |  learning.events  --> Flink SQL job  --(ML_PREDICT Claude)--> learner.  |
   |   (JSON-Schema)       (flink/job.sql)                          recom-   |
   |        |                                                       mendations
   |        +--> Tableflow (Iceberg)        +--> Tableflow (Iceberg)         |
   +-------------------------------------------------------------------------+
                        |                                       |
                        |                                       | consume
                        |                                       v
                        |                              +-----------------+
                        |                              | recommender.js  |
                        |                              |  - create new   |
                        |                              |    scenario via |
                        |                              |    Waterr GPT   |
                        |                              |  - create new   |
                        |                              |    meeting      |
                        |                              |  - POST         |
                        |                              |    /internal/   |
                        |                              |    push         |
                        |                              +--------+--------+
                        |                                       |
                        |          SSE to /events?learner_id=   |
                        +<--------------------------------------+
                        |
                        v
                   Learner's page announces:
                   "A new lesson is ready — opening now."
```

---

## 3. Repo map

```
AI tutor/
├── Readme.md                       this file
├── claude.md                       short pointer for sub-agents (entry to docs)
├── package.json                    npm scripts: setup, start, recommender, datagen, register-schemas
├── vercel.json                     static + serverless rewrites
├── .env                            secrets (Waterr, Confluent, Schema Registry)
├── api/
│   └── index.js                    Vercel serverless wrapper around src/server.js
├── public/
│   └── index.html                  screen-reader-first landing page
├── src/
│   ├── setup.js                    idempotent bootstrap: persona + scenario in Waterr
│   ├── server.js                   Express: /, /start, /webhook/waterr, /events, /internal/push,
│   │                               /embed/inline/:id reverse proxy, SPA proxy fallback
│   ├── kafka.js                    KafkaJS wrapper (producer, consumer, schema-registry encode)
│   ├── schemaRegistry.js           @kafkajs/confluent-schema-registry helpers (encode/safeDecode)
│   └── recommender.js              consumes learner.recommendations → Waterr → SSE push
├── flink/
│   └── job.sql                     Flink SQL: source, sink, ML_PREDICT, fallback path
├── schemas/
│   ├── learning.events-value.json          JSON Schema for the source topic
│   └── learner.recommendations-value.json  JSON Schema for the sink topic
├── connectors/
│   ├── datagen-source.json         Confluent Datagen Source connector config (demo data)
│   └── http-sink.json              Confluent HTTP Sink connector config (push to /internal/connector-push)
├── scripts/
│   ├── register-schemas.js         POST schemas to Confluent Schema Registry
│   └── datagen-cohort.js           local datagen for testing without Waterr
└── Docs/
    ├── TECHNICAL_PLAN.md           original code-level spec (data model, prompts, SQL)
    ├── manager/                    operator's manual + orchestration notes
    ├── status/                     live state, blockers, next task
    └── roadmap/                    milestones M0–M5
```

---

## 4. Data model

Two Kafka topics carry the whole conversation between Waterr and Confluent.

### 4.1 `learning.events` — source topic

Produced by `server.js` after every completed session. **Partition key:** `learner.id` (guarantees per-learner ordering in Flink state).

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
      { "goal": "concept_understood",        "score": 2, "feedback": "Confused about denominators." },
      { "goal": "learner_confident",         "score": 3, "feedback": "" },
      { "goal": "asked_clarifying_question", "score": 4, "feedback": "" }
    ],
    "filler_word_rate": 0.12,
    "growth_areas": "Re-teach equivalent fractions with concrete examples.",
    "strengths": "Patient, willing to keep trying."
  },
  "raw_transcript_ref": "waterr://meetings/{meeting_id}"
}
```

Full schema: [schemas/learning.events-value.json](schemas/learning.events-value.json)

### 4.2 `learner.recommendations` — sink topic

Emitted by the Flink job. **Partition key:** `learner_id`.

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
    "next_scenario_prompt": "You are Aarya, a patient blind-friendly tutor. The learner Asha struggles with denominators. Re-teach equivalent fractions using only *audible* analogies — slicing chapatis, splitting groups of marbles by sound. Avoid any 'see this' / 'look at' phrasing. End by asking her to explain it back in her own words.",
    "estimated_duration_min": 12
  }
}
```

Full schema: [schemas/learner.recommendations-value.json](schemas/learner.recommendations-value.json)

Both schemas are registered in Confluent Schema Registry under the standard `<topic>-value` subjects via `npm run register-schemas`. Both the Node producer (`src/kafka.js`) and the Flink job (`flink/job.sql`) speak the Confluent wire format (`magic byte + schema id + JSON payload`) so they interoperate cleanly.

---

## 5. Components in detail

### 5.1 `src/setup.js` — bootstrap (one-shot)

Idempotently provisions WaterrAI:

1. `GET /voices/all` — picks a calm female ElevenLabs voice (prefers Bella / Janvi / Ayesha).
2. `POST /personas` — creates "Aarya" (skipped if it already exists by name).
3. `POST /scenarios` — creates "Maverick: Audio-First Tutor" with `TUTOR_PROMPT` (audio-only language, sound-based analogies, ask-back-in-own-words, never sigh/rush). Skipped if it already exists by name.

Prints two lines to stdout for the operator to paste into `.env`:

```
PERSONA_ID=<uuid>
SCENARIO_ID=<uuid>
```

The current `.env` points at a manually created "Accessible Finance Guide" scenario. Re-running setup would create a different scenario by name — that's why [.env](.env#L20) carries a "do NOT re-run" warning.

### 5.2 `src/server.js` — Express app

| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serves [public/index.html](public/index.html) with `Cache-Control: no-store`. |
| `/start` | POST | Returns `embed_url` / `join_url` pointing at our reverse-proxied scenario embed. |
| `/webhook/waterr` | POST | Raw-body HMAC verify (`x-waterr-signature: sha256=...`), shapes payload into `LearningEvent`, produces to Kafka. |
| `/events?learner_id=` | GET | Server-Sent Events channel for per-learner push notifications (`recommendation`, `recommendation_preview`, `ping`). |
| `/internal/push` | POST | Recommender posts `{ learner_id, join_url }`; fanned out to that learner's SSE listener. |
| `/internal/connector-push` | POST | Landing pad for the Confluent HTTP Sink Connector (batched recommendations from Kafka → SSE). |
| `/embed/inline/:id` | GET | Reverse-proxies `https://waterr.ai/embed/inline/<id>`, strips `X-Frame-Options` / CSP, rewrites absolute Waterr URLs to relative so the embedded SPA thinks it's same-origin. |
| `*` (catch-all) | ANY | Forwards anything not in the local route list to `waterr.ai`, with a defensive rewrite for the SPA's pathname-derived `/backend/scenarios/public/user/...` URLs, and an injection of a synthetic learner identity into empty `POST /backend/auth/end-user` bodies so JWT mint succeeds. |
| `/healthz` | GET | `{ ok: true, scenario_id }` for uptime checks. |

The catch-all proxy is the key trick that makes the embed work reliably: by serving WaterrAI's SPA from our own origin, we bypass third-party iframe blockers, X-Frame-Options, CSP, and cross-origin auth quirks in one stroke.

### 5.3 `src/kafka.js` — Confluent Cloud wrapper

- Lazy `buildKafka()` — never throws at import time; only when `connect()` is actually called. Lets `setup.js` and parts of `server.js` run without Confluent creds.
- `produce(topic, key, value)` — one-shot send. Encodes with Schema Registry if SR creds are present, falls back to raw JSON otherwise (back-compat).
- `createProducer()` — long-lived producer for `server.js`.
- `createConsumer({ groupId, topic, onMessage, autoRun, fromBeginning })` — uses `safeDecode()` so a single consumer reads both wire-format and raw-JSON messages. Per-message `try/catch` so a single bad message never wedges the loop.
- `healthcheck()` — `admin.listTopics()` round-trip to validate creds + reachability.

### 5.4 `src/schemaRegistry.js`

Thin wrapper around `@kafkajs/confluent-schema-registry`. Exports `encode(topic, value)`, `safeDecode(buffer)`, `getRegistry()`. Subject name strategy is `<topic>-value`.

### 5.5 `src/recommender.js` — closes the loop

Two modes:

**Default (deliverer-only)** — Flink is the source of truth for `learner.recommendations`.

```
Consumer (groupId: maverick-deliverer)
  subscribe: learner.recommendations
  for each msg:
    1. POST /scenarios/create-with-gpt  (Waterr generates scenario from prompt)
    2. POST /meetings                    (Waterr spins up the room)
    3. POST /internal/push               (SSE fan-out → page announces it)
  graceful fallbacks for Waterr's known billing_id 422.
```

**Fallback (`USE_NODE_TRANSFORMER=true`)** — spins up a second consumer (groupId: `maverick-transformer`) that reads `learning.events` directly, calls Claude via the Anthropic Messages API (`claude-sonnet-4-6` by default), and produces to `learner.recommendations` from Node. Use when Flink's `ML_PREDICT` is unavailable on your CC tier (dev, demo recovery, free tier). The two modes are mutually compatible — running both at once just means Node and Flink both produce, and the deliverer doesn't care which wrote it.

### 5.6 `public/index.html`

Single landing page, ~60 lines of HTML. One `<main>`, one `<h1>`, two `<label for>`d inputs, one button, one `aria-live="polite"` status region. After submit, swaps in the iframe at `/embed/inline/<scenario_id>` and SSE-subscribes to `/events?learner_id=...`.

---

## 6. Confluent Cloud setup

### 6.1 Topics

Create both in the Confluent Cloud UI. Single partition is fine for the demo.

| Topic | Cleanup | Key | Tableflow |
|-------|---------|-----|-----------|
| `learning.events` | delete, 7d | `learner_id` | ON (Iceberg) |
| `learner.recommendations` | delete, 7d | `learner_id` | ON (Iceberg) |

### 6.2 Schemas

```bash
# After CONFLUENT_SR_* creds are in .env
npm run register-schemas
```

This POSTs [schemas/learning.events-value.json](schemas/learning.events-value.json) and [schemas/learner.recommendations-value.json](schemas/learner.recommendations-value.json) to the Schema Registry under the standard `TopicNameStrategy` subjects.

### 6.3 AI Model Inference (for `ML_PREDICT`)

Confluent Cloud → **AI Model Inference** → New:

| Field | Value |
|-------|-------|
| Provider | Anthropic |
| Model | `claude-3-5-sonnet-20241022` (or current Claude in CC) |
| Task | TEXT_GENERATION |
| Name | `next_lesson_model` |
| Endpoint | Anthropic default |
| API Key | your Anthropic key |

After save, `next_lesson_model` is callable inside Flink SQL via `ML_PREDICT('next_lesson_model', '...prompt...')`.

### 6.4 Connectors

| Connector | Config | Purpose |
|-----------|--------|---------|
| Datagen Source | [connectors/datagen-source.json](connectors/datagen-source.json) | Pumps synthetic `learning.events` for cohort-scale demo. |
| HTTP Sink | [connectors/http-sink.json](connectors/http-sink.json) | Pushes `learner.recommendations` to `POST /internal/connector-push` for a no-Node-consumer path. |

### 6.5 Tableflow

For each topic, **Settings → Enable Tableflow**. Iceberg tables appear automatically. Pitch line: *"the audit trail for parents and schools — zero extra code."*

---

## 7. Flink SQL job

Full file: [flink/job.sql](flink/job.sql). Paste into Confluent Cloud → Flink → SQL Workspace.

**Source table** (`learning_events`) reads `learning.events` in `json-registry` format with a watermark on `occurred_at - INTERVAL '5' SECOND`.

**Sink table** (`learner_recommendations`) writes `learner.recommendations` in `json-registry` format.

**Primary path** — enrich each event with an AI-generated next-lesson prompt via `ML_PREDICT`:

```sql
INSERT INTO learner_recommendations
SELECT
  1                          AS schema_version,
  CONCAT('rec_', event_id)   AS event_id,
  CURRENT_TIMESTAMP          AS occurred_at,
  learner.id                 AS learner_id,
  learner.contact_channel    AS contact_channel,
  ROW(
    meeting.subject,
    CASE
      WHEN performance.average_score < 60 THEN 'easier'
      WHEN performance.average_score > 85 THEN 'harder'
      ELSE 'same'
    END,
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
  )                          AS recommendation
FROM learning_events
WHERE performance.average_score IS NOT NULL;
```

**Fallback path** — if `ML_PREDICT` isn't available on your CC tier, comment in the FALLBACK INSERT in [flink/job.sql](flink/job.sql) which copies `performance.growth_areas` straight into `next_scenario_prompt`. `recommender.js` will still produce a working adaptive follow-up.

---

## 8. Environment variables

All read from `.env` via `dotenv/config`. The `.env` file in the repo has live demo credentials — **never log, echo, or commit** them.

| Var | Used by | Notes |
|-----|---------|-------|
| `WATERR_API_KEY` | `setup.js`, `server.js`, `recommender.js` | Bearer token for all WaterrAI calls. |
| `WATERR_API_BASE` | same | Default `https://api.waterr.ai/v1`. |
| `WATERR_WEBHOOK_SECRET` | `server.js` | HMAC secret for `/webhook/waterr`. Unset = skip check (dev only). |
| `WEBHOOK_PUBLIC_URL` | `setup.js` | Public ngrok URL to register the webhook against. |
| `SCENARIO_ID` | `server.js`, `recommender.js` | The scenario the embed iframe loads. |
| `PERSONA_ID` | reference only | The persona used by the scenario. |
| `CONFLUENT_BOOTSTRAP` | `kafka.js` | `host:9092` for the cluster. |
| `CONFLUENT_API_KEY` / `_SECRET` | `kafka.js` | SASL PLAIN creds. |
| `CONFLUENT_SCHEMA_REGISTRY_URL` | `schemaRegistry.js` | Schema Registry base URL. |
| `CONFLUENT_SR_API_KEY` / `_SECRET` | `schemaRegistry.js` | SR basic-auth creds. |
| `TOPIC_LEARNING_EVENTS` | both | Default `learning.events`. |
| `TOPIC_RECOMMENDATIONS` | both | Default `learner.recommendations`. |
| `RECOMMENDER_GROUP_ID` | `recommender.js` | Default `maverick-transformer`. |
| `DELIVERER_GROUP_ID` | `recommender.js` | Default `maverick-deliverer`. |
| `USE_NODE_TRANSFORMER` | `recommender.js` | `true` to enable Flink-fallback mode. Default `false`. |
| `ANTHROPIC_API_KEY` | `recommender.js` (fallback only) | Needed only if `USE_NODE_TRANSFORMER=true`. |
| `ANTHROPIC_MODEL` | same | Default `claude-sonnet-4-6`. |
| `INTERNAL_PUSH_URL` | `recommender.js` | Default `http://localhost:3000/internal/push`. |
| `PORT` | `server.js` | Default `3000`. |

---

## 9. Local run order

```bash
# 0. install
cd "AI tutor"
npm install

# 1. expose the webhook (separate terminal)
ngrok http 3000
# copy https URL into .env WEBHOOK_PUBLIC_URL

# 2. bootstrap WaterrAI (one-shot; idempotent)
node src/setup.js
# copy PERSONA_ID / SCENARIO_ID into .env

# 3. register Confluent JSON schemas (one-shot)
npm run register-schemas

# 4. paste flink/job.sql into Confluent Cloud → Flink → SQL Workspace → Run

# 5. boot the web + webhook bridge
npm start

# 6. boot the recommender consumer (separate terminal)
npm run recommender

# 7. open the landing page
open http://localhost:3000
```

Optional:

```bash
npm run datagen          # pump synthetic learning.events into Kafka for a cohort demo
```

---

## 10. Vercel deployment

[vercel.json](vercel.json) rewrites every non-`/` request to `/api/index`, which exports `src/server.js` as a serverless function. Static assets in `public/` are served directly by Vercel's edge.

**Important constraints on Vercel:**

- The Express app runs as a serverless function with `maxDuration: 30s`. Bump on Pro tier if proxied scenarios take longer.
- **Long-lived consumers (`recommender.js`) cannot run on Vercel.** Deploy them on Fly.io / Render / Railway / a small VM — anywhere that allows persistent processes with outbound Kafka connections.
- SSE works on Vercel but is limited by the function timeout. For a long-lived `/events` connection, host `server.js` on a persistent-process platform instead.

---

## 11. Accessibility design

The landing page is designed to work the moment a screen reader lands on it, with **the screen turned off**.

- One `<main>` landmark, one `<h1>`.
- `<label for>` on every input — VoiceOver, NVDA, JAWS, TalkBack all announce them.
- `aria-live="polite" aria-atomic="true"` status region — every state change ("Connecting Aarya...", "Connected. Opening the lesson room now.", "A new lesson is ready") is announced automatically.
- Native form submit — no JS focus traps, no custom keyboard handling.
- Big native button.
- The tutor prompt (`TUTOR_PROMPT` in [src/setup.js:34-59](src/setup.js#L34-L59)) bans visual language: no "see", "look at", "watch", "as you can see", "in the diagram", "on the screen". Substituted with "imagine", "picture in your mind", "feel like", "sounds like". Uses sound-based analogies: clapping rhythms, splitting groups, slicing a chapati, ringing bells.
- The tutor repeats any explanation after "I don't get it" / "again" / "wait" / silence > 6 seconds. Never sighs, never rushes.
- The tutor asks the learner to explain it back ("Can you tell me in your own words?") before moving on — the truth test that drives `concept_understood` scoring.

---

## 12. Security notes

- `.env` holds live WaterrAI + Confluent + Schema Registry credentials. Never log, echo, or commit.
- The webhook uses raw-body HMAC verification with constant-time comparison (`crypto.timingSafeEqual`). When `WATERR_WEBHOOK_SECRET` is unset, the check is skipped with a warning — **dev only**.
- The catch-all reverse proxy in `server.js` forwards arbitrary paths to `waterr.ai`. It strips `host`, `connection`, `content-length` from inbound headers and `X-Frame-Options`, `Content-Security-Policy`, `content-encoding`, `content-length`, `transfer-encoding`, `connection` from upstream responses. The synthetic learner-identity injection for `POST /backend/auth/end-user` only fills missing `email`/`firstName`/`lastName` fields — it does not overwrite real values.
- Partition key is `learner.id`. If you start storing PII on the wire, hash it (or use a stable anonymous id like the current `learner_anon_*` shape).

---

## 13. Demo script

| Time | What you do | What the judges see |
|------|-------------|---------------------|
| 00:00 | Open the landing page, **dim the screen to zero**, enable macOS VoiceOver. | Screen reader announces "Maverick — Your Patient Audio Tutor. Your name, edit text." |
| 00:20 | Type "Asha", subject "fractions", press Enter. | Status region announces "Connected. Opening the lesson room now." |
| 00:30 | Aarya greets in a warm voice. You play Asha and say "I don't get denominators". Aarya re-teaches with audible analogies. End the call after ~90s. | Live voice tutor; no visual cues; aria-live narration the whole way. |
| 02:00 | Switch to Confluent Cloud → Flink. Show `learning.events` receiving the event live. Show `learner.recommendations` getting a row 5–10s later. | Stream visibly carrying the lesson; `next_scenario_prompt` is the AI-generated next lesson. |
| 02:30 | The page (still up, screen still off) announces "A new lesson is ready — opening now." Iframe swaps to the new scenario. Aarya immediately re-teaches denominators with a *new* angle. | The whole adaptive loop closes live, with no clicks. |
| 02:55 | Show the Iceberg table via Tableflow. | "This is the audit trail for parents and teachers." |

Close with: *"The learner never saw a screen. The Confluent stream made the lesson adapt in seconds, not days."*

---

## 14. Cut-list / risks

In order — drop right-to-left if you slip:

1. **Drop SMS** — replaced by SSE on the landing page. Already default.
2. **Drop Tableflow** — claim as roadmap. (Saves 2 minutes of UI clicking.)
3. **Skip Flink `ML_PREDICT`** — set `USE_NODE_TRANSFORMER=true` and let Node call Claude when it consumes the message. Loses the "AI inside the stream" wow factor but still works.
4. **Hardcode one scenario** — skip `create-with-gpt` and pass the next-lesson hints via `note_context` into the same scenario. Lower polish, still demos.

Known issues to be aware of:

- Waterr `POST /meetings` occasionally 422s on `billing_id` — `recommender.js` has graceful fallbacks.
- The Waterr SPA's pathname-derived API calls (`/backend/scenarios/public/user/.../<UUID>`) 404 upstream — `server.js` rewrites these on the wire to `/backend/scenarios/embed/<UUID>`.
- An empty `POST /backend/auth/end-user` body causes a 500 upstream — `server.js` injects deterministic placeholder identity so JWT mint succeeds.

---

For the deeper "why" and milestone-level history, see [Docs/TECHNICAL_PLAN.md](Docs/TECHNICAL_PLAN.md), [Docs/manager/](Docs/manager/), [Docs/status/](Docs/status/), and [Docs/roadmap/](Docs/roadmap/).
