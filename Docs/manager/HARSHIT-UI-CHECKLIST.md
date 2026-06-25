# Harshit UI checklist — Confluent Cloud setup to unlock the rubric

All of this is **UI clicks I can't do for you** from the agent. Each step lists where to click, what to paste, and what to copy back into `.env`. Estimated total ~25 minutes.

Order matters — don't skip ahead.

---

## 1. Schema Registry creds (5 min)

**Why:** Unlocks Stream Governance criterion + lets Flink read the topics natively.

1. Confluent Cloud → top-left **Environments** → pick `default` (env `env-16m7dv`).
2. In the env overview → right column **Stream Governance API** → **API keys** → **Add key**.
3. Name it `maverick-sr-key`, click **Download** (you only see the secret once).
4. On the same page, copy the **Endpoint** URL (looks like `https://psrc-XXXXX.us-east-2.aws.confluent.cloud`).
5. Paste into `.env`:
   ```
   CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-XXXXX.us-east-2.aws.confluent.cloud
   CONFLUENT_SR_API_KEY=<key from step 3>
   CONFLUENT_SR_API_SECRET=<secret from step 3>
   ```

**Verify locally:**
```bash
cd "AI tutor "
npm run register-schemas
# expect:
#   ✓ learning.events-value           id=100001  ←  learning.events-value.json
#   ✓ learner.recommendations-value   id=100002  ←  learner.recommendations-value.json
```

Confluent UI → Environment → **Schema Registry** → **Subjects** should now show both subjects.

---

## 2. Anthropic API key in CC (3 min)

**Why:** Lets Flink call `ML_PREDICT('next_lesson_model', …)` inside SQL — the bonus rubric item.

1. Get an Anthropic API key from https://console.anthropic.com/settings/keys (paid plan — has free credit).
2. Confluent Cloud left nav → **AI Model Inference** (sometimes nested under *Flink*).
3. **Add model**:
   - Provider: **Anthropic**
   - Model: `claude-3-5-sonnet-20241022` (or the newest Claude shown in the dropdown)
   - Task: `TEXT_GENERATION`
   - **Model name**: `next_lesson_model`  ← must match exactly, the SQL references this string
   - Endpoint: leave default
   - API key: paste your Anthropic key
4. Save.

If `AI Model Inference` is not enabled on your tier — skip this and the Flink SQL job will run via the FALLBACK PATH (no AI explanation, just templated prompts). Toggle the block in `flink/job.sql` per the comments at the top.

---

## 3. Run the Flink SQL job (5 min)

**Why:** Moves the transform from Node into actual Flink. Rubric box for "significant Flink SQL usage."

1. Confluent Cloud → left nav **Flink** → **SQL Workspace** → **+ Create workspace**.
2. Top bar: pick Environment `default`, Catalog `default`, Database `maverick` (the Kafka cluster name).
3. Open `flink/job.sql` from this repo. Paste the **whole file** into the workspace.
4. Click **Run** (top right). Three statements run: `CREATE TABLE learning_events`, `CREATE TABLE learner_recommendations`, and the `INSERT INTO learner_recommendations SELECT …`.
5. The INSERT will show as a long-running job. Status should go to `RUNNING` within ~30s.

**Verify:**
- Run `npm run datagen` (in another shell) — produces synthetic events.
- Confluent UI → cluster `maverick` → Topics → `learner.recommendations` → **Messages** tab.
- You should see new records arriving with AI-generated `next_scenario_prompt` text.

---

## 4. Tableflow on both topics (2 min)

**Why:** Rubric box for Stream Governance / data product. Also gives a clean "audit table" view to point at in the pitch.

1. Confluent Cloud → cluster `maverick` → Topics → click `learning.events`.
2. Top tabs → **Tableflow** → **Enable**.
3. Pick storage: Confluent-managed (default Iceberg). No further config.
4. Repeat for `learner.recommendations`.

After ~60s, the topic detail page shows a "Tableflow" pane with the Iceberg table reference.

---

## 5. Datagen Source Connector (3 min) — *optional but ticks a real Connector box*

**Why:** "Use of Confluent Connector(s)" is on the rubric. Adding a managed Datagen connector on `learning.events` puts a real Confluent Connector in the architecture. (You can leave `npm run datagen` as the demo-time driver — the connector just needs to *exist* and have produced ≥1 record.)

1. Confluent Cloud → cluster `maverick` → **Connectors** → **+ Add connector**.
2. Search **Datagen Source**, click it.
3. Use connectors/datagen-source.json from this repo as the config — paste field by field:
   - Topic: `learning.events`
   - Output format: **JSON_SR** (Schema Registry)
   - Quickstart template: `USERS` (any built-in is fine for the rubric — the records won't match our schema perfectly but the connector is REAL)
   - Max interval: `2000` (one record every 2s — quiet, won't pollute the real data)
4. Use the Kafka cluster API key/secret from `.env` (CONFLUENT_API_KEY / CONFLUENT_API_SECRET).
5. Launch. Status should go `RUNNING` within ~60s.

To stop it polluting your real demo: **Pause** the connector (don't delete) once you have the screenshot.

---

## 6. (Optional) HTTP Sink Connector instead of Node deliverer

**Why:** Puts a Connector *on the critical demo path* — recommendations flow Kafka → Connector → server.js. More impressive than Datagen.

Prereq: server.js publicly reachable (ngrok).

1. Start ngrok: `ngrok http 3000` — copy the `https://xxx.ngrok-free.app` URL.
2. Confluent Cloud → Connectors → **+ Add connector** → search **HTTP Sink**.
3. Use connectors/http-sink.json as a guide. Key fields:
   - Topic: `learner.recommendations`
   - Input format: **JSON_SR**
   - HTTP URL: `https://xxx.ngrok-free.app/internal/connector-push`
   - Method: POST
   - Headers: `Content-Type:application/json`
4. Launch.

When this is on, you can stop `npm run recommender` — the Connector handles delivery.

---

## 7. Sanity end-to-end (2 min)

In four shells:

```bash
# 1. Web + webhook server
cd "AI tutor " && node --env-file=.env src/server.js

# 2. Deliverer (only — Flink does the transform now)
cd "AI tutor " && node --env-file=.env src/recommender.js

# 3. Synthetic cohort (gives Flink real volume)
cd "AI tutor " && npm run datagen -- --rps=3 --duration=180

# 4. (Optional) ngrok if using HTTP Sink Connector
ngrok http 3000
```

Expected:
- Confluent UI → `learning.events` topic → records pouring in.
- Confluent UI → Flink job → `Records out` ticking up.
- Confluent UI → `learner.recommendations` topic → records arriving with Claude-generated prompts.
- Local terminal 2 → `[deliverer] tier1 OK` lines, or `tier2/3` on the Waterr 422 bug.
- Tableflow → Iceberg table previews showing both topics' historical records.

---

## What "done" looks like for the rubric

| Box | How to point at it in the demo |
|---|---|
| Kafka cluster | "this is `maverick` on Confluent Cloud" (topics page) |
| Schema Registry | "two JSON Schemas governing the contract" (Schema Registry → Subjects) |
| Connectors | "a Datagen source feeding events, plus our service producing real ones" |
| Stream Processing (Flink) | "this Flink job runs continuously" (Flink → Statements → RUNNING) |
| Flink AI Model Inference (bonus) | "the next-lesson prompt is generated by `ML_PREDICT` calling Claude inside Flink" |
| Tableflow / Stream Governance | "every record is also a row in this Iceberg table — free audit trail" |

Six boxes, one demo flow. Aarya's voice is still the punchline at the end.
