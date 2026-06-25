# Track B notes — Confluent integration

Free-form scratchpad for whichever agent picks up Track B next.

---

## B1 — Confluent CLI install

**Result:** SUCCESS via Homebrew.

```bash
brew install confluent-cli
# Installed Cask confluent-cli 4.67.0
# Linked binary to /opt/homebrew/bin/confluent
```

Verify:
```bash
confluent version
# Version: v4.67.0  (Build 2026-06-22)
```

Fallback (not needed — kept here in case macOS user later wipes brew):
```bash
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- -b $HOME/.local/bin
# then add to PATH:  export PATH="$HOME/.local/bin:$PATH"
```

---

## B2 — Login

This is the **one** interactive command the human (Harshit) runs once per laptop. We cannot run it inside an agent loop because it pops a browser SSO.

```bash
confluent login --save
# --save writes the session to ~/.confluent/config.json so subsequent
# `confluent kafka cluster list`, `confluent flink ...` calls don't re-prompt.
```

After that:
```bash
confluent environment list
confluent environment use env-XXXXXX
confluent kafka cluster list
confluent kafka cluster use lkc-XXXXXX
```

---

## Where in the Confluent Cloud UI to get the three secrets we need

Login at https://confluent.cloud.

1. **Bootstrap server (CONFLUENT_BOOTSTRAP)**
   - Left nav → *Environments* → pick env → click your Kafka **cluster**.
   - Sidebar → **Cluster settings** (or *Cluster overview* on newer UI).
   - Scroll to **Endpoints** → copy the *Bootstrap server* value
     (looks like `pkc-xxxx.us-east-2.aws.confluent.cloud:9092`).
   - Paste into `.env` as `CONFLUENT_BOOTSTRAP=<host:port>` — **no `SASL_SSL://` prefix**, just `host:port`.

2. **API key + secret (CONFLUENT_API_KEY, CONFLUENT_API_SECRET)**
   - From the same cluster page → sidebar → **API Keys** → **Create key**.
   - Pick *Global access* (fine for demo) or scope to topics `learning.events`,
     `learner.recommendations` for a tighter cred.
   - Copy the key + secret **immediately** — the secret is shown once.
   - Paste into `.env` as `CONFLUENT_API_KEY=...` and `CONFLUENT_API_SECRET=...`.

3. **Schema Registry creds (optional for demo — we use plain JSON)**
   - Env page → *Stream Governance API* → *API Keys* → *Create key*.
   - Only fill `CONFLUENT_SCHEMA_REGISTRY_URL`, `CONFLUENT_SR_API_KEY`,
     `CONFLUENT_SR_API_SECRET` if we decide to wire Schemas later.

---

## .env vars `src/kafka.js` reads

| Var | Required? | Example |
|---|---|---|
| `CONFLUENT_BOOTSTRAP` | yes | `pkc-xxxx.us-east-2.aws.confluent.cloud:9092` |
| `CONFLUENT_API_KEY` | yes | `KAFKAKEYABC123` |
| `CONFLUENT_API_SECRET` | yes | `xxxxxxxx...` |
| `TOPIC_LEARNING_EVENTS` | already set | `learning.events` |
| `TOPIC_RECOMMENDATIONS` | already set | `learner.recommendations` |

`src/kafka.js` does **not** throw at import time if these are missing — it
only fails at `connect()`. This lets the rest of the app (e.g. setup.js,
server.js without webhook wiring) run with an empty `.env`.

---

## Topics to create in Confluent Cloud UI

Same cluster:
- `learning.events`   (1 partition, delete cleanup, 7d retention)
- `learner.recommendations` (1 partition, delete cleanup, 7d retention)

Both: **Tableflow → Enable** (Iceberg). Free audit trail; we pitch it in the demo.

---

## Flink AI Model registration (for `flink/job.sql`)

Confluent Cloud → **AI Model Inference** → **New model** →
- Provider: **Anthropic**
- Model: `claude-3-5-sonnet-20241022` (or whichever Claude is current in CC)
- Task: TEXT_GENERATION
- Name: `next_lesson_model`
- Endpoint + API key: from Anthropic console.

Then `ML_PREDICT('next_lesson_model', <prompt>)` works inside Flink SQL.

If AI Model Inference is **not** enabled in your CC tier, see the
`Plan B` block at the top of `flink/job.sql` — switch to the fallback
SELECT and `recommender.js` handles the Claude call instead.

---

## Healthcheck command (run once `.env` is filled)

```bash
node -e "import('./src/kafka.js').then(k => k.healthcheck()).then(r => console.log('hc:', r)).catch(e => { console.error(e.message); process.exit(1); })"
```

Expected output: `hc: true`. If you see `SASL Authentication failed`, the
API key/secret pair is wrong (or scoped to a different cluster than
`CONFLUENT_BOOTSTRAP`).

---

## Gotchas

- KafkaJS expects `brokers: [host:port]` — just the host:port, **never**
  the full `SASL_SSL://host:port` URL. Confluent Cloud UI shows it without
  the scheme; do not paste in a scheme.
- `mechanism: 'plain'` (lowercase) — KafkaJS is case-sensitive.
- For Confluent Cloud, `ssl: true` is required.
- Consumer groupId must be unique per process **per topic role**. We use
  `maverick-transformer` (events → recommendations) and `maverick-deliverer`
  (recommendations → Waterr) in the same Node process — two separate
  groupIds let both consume independently and let us scale out later.
- If you see `Topic 'learning.events' does not exist`, create it in the UI
  before running — KafkaJS won't auto-create on Confluent Cloud.
- `import.meta.url` URL-encodes spaces. If a folder name contains a space
  (it does — `AI tutor `), comparing `import.meta.url === file://${argv[1]}`
  silently fails and your direct-run guard never fires. Use
  `fileURLToPath(import.meta.url) === process.argv[1]` instead.

---

## M3 pivot — Flink moved to Node (current architecture)

We **do not run** `flink/job.sql` for the demo. Reason: running Flink SQL
against our raw-JSON topics on Confluent Cloud requires Schema Registry +
JSON Schema registration on both topics. That's too much setup for the
hackathon window. The SQL file stays in-repo verbatim as the
"production architecture" slide of the pitch — same shape, same contracts,
just moved to a different runtime.

### What runs instead

`src/recommender.js` is one Node process with **two consumers + one producer**:

```
                                 ┌── producer (TOPIC_RECOMMENDATIONS) ──┐
                                 │                                      ▼
  learning.events  ──► consumer A (group maverick-transformer)   learner.recommendations
                         │ transforms LearningEvent → LessonRecommendation:
                         │   difficulty = score < 60 → easier
                         │              > 85 → harder
                         │              else  → same
                         │   next_scenario_prompt = Claude (claude-sonnet-4-6)
                         │                          if ANTHROPIC_API_KEY set,
                         │                          else deterministic Plan-B template
                         │
  learner.recommendations ──► consumer B (group maverick-deliverer)
                                  │ tier 1: POST /scenarios/create-with-gpt
                                  │         + POST /meetings
                                  │ tier 2 (on 422 billing_id bug):
                                  │         POST /meetings with SCENARIO_ID
                                  │         and note_context = next_scenario_prompt
                                  │ tier 3 (last resort):
                                  │         https://waterr.ai/scenario/<SCENARIO_ID>
                                  │
                                  └► POST http://localhost:3000/internal/push
                                     { learner_id, join_url } → SSE → page
                                     announces via aria-live.
```

### Why route through `learner.recommendations` at all

The transformer could call the deliverer in-process. We deliberately do not.
Going through Kafka means:
- Judges see records flowing through both topics during the demo.
- The "production architecture" slide is one config flip away from real —
  swap Consumer A for the Flink job and Consumer B keeps working unchanged.
- Decouples the slow Waterr API path from the fast event-ingest path.

### Graceful behaviors

- If `ANTHROPIC_API_KEY` missing: skip Claude, use Plan-B template.
- If Claude call fails: log a warning, fall through to Plan-B template.
- If `WATERR_API_KEY` missing: skip delivery, log the would-be prompt.
- If Waterr `POST /meetings` returns 422 (billing_id bug): cascade through
  tiers 2 and 3 above.
- If topics are missing on first run: KafkaJS surfaces
  `UNKNOWN_TOPIC_OR_PARTITION`; we catch + log as "transient" and let the
  next message retry. (Topics already exist on cluster `lkc-k88xzzp`, so
  this is a safety net, not a normal-case path.)
- Never crash the consumer loop on a single bad message — every handler
  body is wrapped and errors are logged with key + offset.

### Env vars added/used

| Var | Required? | Default | Purpose |
|---|---|---|---|
| `ANTHROPIC_API_KEY` | optional | — | Enables Claude-generated next_scenario_prompt. |
| `ANTHROPIC_MODEL` | optional | `claude-sonnet-4-6` | Override the model id. |
| `RECOMMENDER_GROUP_ID` | optional | `maverick-transformer` | Consumer A groupId. |
| `DELIVERER_GROUP_ID` | optional | `maverick-deliverer` | Consumer B groupId. |
| `INTERNAL_PUSH_URL` | optional | `http://localhost:3000/internal/push` | server.js push endpoint. |

### Smoke-test command

```bash
cd "AI tutor "
node --env-file=.env src/recommender.js
# expect:
#   [recommender] booting two-consumer + one-producer node-side stream processor
#   [recommender]   transformer:  learning.events  → learner.recommendations   (group=maverick-transformer)
#   [recommender]   deliverer:    learner.recommendations                       (group=maverick-deliverer)
#   [recommender]   anthropic:    disabled — using Plan-B template       (or ENABLED if key set)
#   [recommender] running — waiting for learning events…
# then end a Waterr session → see [transformer] log a rec produced → [deliverer] log a join_url pushed.
```

### Flink stays documented but not executed

`flink/job.sql` is unchanged. If we ever want to flip back:
1. Enable Schema Registry on the env, register JSON Schemas for both topics.
2. Stop consumer A in `recommender.js` (env-flag it off).
3. Paste `flink/job.sql` into Confluent Cloud Flink SQL Workspace, register
   `next_lesson_model`, click Run.
4. Consumer B keeps working unchanged — same envelope on the wire.
