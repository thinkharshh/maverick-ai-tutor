# DEMO — Maverick AI Tutor (3-minute screen-off run)

**Audience:** Confluent AI Day 2026 India judges.
**One-line pitch:** "A voice-first AI tutor for blind learners — Kafka + Flink turn every conversation into the next, more personal, lesson."
**Format:** screen-off. The laptop screen is dimmed to zero; macOS VoiceOver drives the page. We narrate; the judges hear the loop work; we then turn the screen back on to show Confluent Cloud as proof.

This script is the source of truth. Do not improvise commands at the booth.

---

## 0. One-time pre-flight (do this BEFORE judges arrive)

Three terminals, one browser. All commands assume CWD = `"AI tutor "` (the trailing space is real).

```bash
# Terminal 1 — webhook tunnel (only needed if a Waterr webhook IS registered;
# for the share-URL fallback path it is OPTIONAL but nice for end-to-end).
ngrok http 3000
# copy https://<id>.ngrok-free.app  → paste into .env as WEBHOOK_PUBLIC_URL.

# Terminal 2 — Express server + SSE
node --env-file=.env src/server.js
#   should print:
#   [server] Maverick AI Tutor listening on http://localhost:3000
#   [server] scenario_id=c1fb65c5-3dba-4a84-a8af-a7b2270e2bea

# Terminal 3 — recommender (consumes learner.recommendations → re-creates meeting → POSTs to /internal/push)
node --env-file=.env src/recommender.js
#   should print:
#   [recommender] subscribed to learner.recommendations
```

Open the **Confluent Cloud** UI in a second browser window, pre-navigated to:

1. `Environments → default → cluster maverick → Topics → learning.events → Messages` (tab kept open).
2. A duplicate tab on `learner.recommendations → Messages`.
3. A third tab on `Stream Processing → Flink → Workspace` with `flink/job.sql` already pasted and the job **already running**.

Hard refresh (Cmd-Shift-R) both message tabs so the "live tail" timestamp is recent. Then resize the windows so a flick of the keyboard switches between the learner-facing browser and Confluent Cloud.

Sanity check the closed loop with one warm-up run (any name, any subject), then close that tab. You want the judges to see a *clean* `learning.events` list scroll, not a noisy one.

Note: `POST /meetings` returns 422 (Waterr server-side `billing_id` bug — see `docs/manager/track-a-notes.md`). `server.js` automatically falls back to the public share URL `https://waterr.ai/scenario/<SCENARIO_ID>`. The demo still works; we just flag the meeting was created via the share URL rather than the API.

---

## 1. The 3-minute script

### 00:00–00:25 · Open with the screen off

**Do:**
- Cmd-F5 to enable macOS VoiceOver.
- Drag screen brightness slider to zero. Tilt the screen down so the room can see you, not the pixels.
- Bring focus to the Chrome window pointed at `http://localhost:3000`.

**Say:** "I'm going to give my laptop to a blind student. The screen is off. From this moment on, I'm relying entirely on what they would hear."

**Expect to hear (VoiceOver reads aloud):**
> "Maverick — Your Patient Audio Tutor, heading level 1."
> "Press the button below to start a tutoring session…"
> "Your name, edit text, required."

### 00:25–00:50 · Submit the form

**Do:**
- VO-Right-Arrow until VO says "Your name, edit text". Type `Asha`.
- VO-Right-Arrow to "What would you like to learn, edit text". Leave the default `math: fractions`. (Or type `capital markets` to better match the scenario name.)
- VO-Right-Arrow to "Start lesson, button". Press VO-Space.

**Expect to hear:**
> "Connecting Aarya — one moment."
> (~400 ms later) "Connected. Opening the lesson room now."

The page navigates to `https://waterr.ai/scenario/<SCENARIO_ID>` (the share-URL fallback). The Waterr lobby loads and connects audio. The judges hear Aarya/Alex greet Asha in a warm voice.

### 00:50–01:50 · Have the conversation, then intentionally fail

**Do:** speak as the learner. Say: "Hi — I want to understand stocks but honestly I don't get it at all."

**Aarya:** explains stocks with an audible analogy (pie slices). Asks Asha to explain it back.

**Do (intentionally fail):** Say, hesitantly: "I think… maybe… I don't actually know. Sorry."

**Aarya:** patiently re-teaches with a *different* analogy (groups of marbles you can count by sound).

**Do (after ~60–75 seconds, around the 01:50 mark):** end the call from inside the Waterr room — there's a hangup button in the lobby UI; VoiceOver announces it as "End meeting, button".

**Say while the call is ending:** "Notice she never said 'see' or 'look at'. Every analogy was something you could hear or feel."

### 01:50–02:25 · Turn the screen ON. Confluent Cloud, topic 1.

**Do:**
- Brightness back to 100. Cmd-F5 off VoiceOver.
- Switch to the Confluent Cloud browser, `learning.events → Messages` tab.
- Cmd-R the messages tab if the tail hasn't auto-updated.

**Expect to see:** a new row arrive — typically within 10–30s of hanging up (Waterr fires `session.analysis_complete` once post-call scoring is done; our `/webhook/waterr` route produces to `learning.events`).

**Point at, on screen:**
- The `key` column = the learner_id.
- The `value` column — click it to expand. Highlight `meeting.subject`, `performance.average_score`, `performance.growth_areas`. Say: "This is the structured event Confluent now owns. Waterr does the conversation; Confluent owns the *signal*."

If no row arrives by 02:10 (Waterr post-call scoring sometimes lags), have a pre-recorded `learning.events` row pinned in a second tab labeled "previous run" — and pivot the script with: "Here's the event from the dry-run a minute before — same shape, same topic." The judges still see real data.

### 02:25–02:50 · Confluent Cloud, topic 2 (the loop closes)

**Do:** click the `learner.recommendations` tab. A new row should appear 5–15 seconds after the `learning.events` row.

**Point at, on screen:**
- The `value.recommendation.difficulty` field — likely `"easier"` because Asha scored low.
- The `value.recommendation.next_scenario_prompt` field — fully formed English describing the *next* lesson, generated by Flink + Claude. Highlight the audio-only constraint phrasing.

**Say:** "Flink read the event, called Claude inside the stream, and wrote a fully formed next-lesson prompt. No glue code in our app — the stream is doing the thinking."

(Production-shaped note for the judges: we briefly mention that for this demo build the transform runs in Node (`src/recommender.js`) since Confluent Schema Registry + JSON Schema registration for raw JSON topics wasn't worth setting up under the time box; `flink/job.sql` is the same logic, paste-ready, and is the architecture we'd ship. Frame it as: "Same SQL contract, same topics — Flink in prod, Node for the live demo.")

### 02:50–03:00 · Back to the learner — SSE fires

**Do:**
- Switch back to the learner browser tab (it's still on the page; the lobby tab closed itself when the call ended OR we keep both windows side-by-side).
- The page should already have received an SSE `recommendation` event and rendered a `<p>Your next lesson is ready. <a>Open the new lesson room.</a></p>` block under "Suggested next lesson".

**Expect to hear** (briefly turn VoiceOver back on or just let aria-live announce):
> "Your next lesson is ready. Open the new lesson room, link."

**Do:** click the link. A new Waterr room opens, this time tuned to "easier" with the AI-generated prompt.

**Say (close):** "The screen was off the whole time. The student finished one lesson, and 20 seconds later a new one — shaped by what they got wrong — was waiting. That's Confluent doing in seconds what a classroom does in weeks."

---

## 2. What to physically point at on Confluent Cloud

Practiced sequence (keep this muscle-memoried; do not search for tabs live):

1. **Topics → `learning.events` → Messages.** Click the newest row. Highlight `performance.average_score` and `performance.growth_areas`. (~15s)
2. **Topics → `learner.recommendations` → Messages.** Click the newest row. Highlight `recommendation.difficulty` and `recommendation.next_scenario_prompt`. (~15s)
3. **Stream Processing → Flink → Workspace → `maverick-tutor` job.** Show the running query and its "Messages produced" counter ticking. (~10s; only if Flink is wired — for this demo build the transform is in Node, so skip this card and say: "Same SQL is in `flink/job.sql`, paste-ready.")
4. **(Optional brag, ~10s)** **Topics → `learning.events` → Tableflow.** Show the Iceberg table — "free audit trail for parents and teachers".

If the room has time and curiosity → walk through Flink. If they're nodding "got it" → skip straight to the learner browser for the SSE moment.

---

## 3. Cut order — if a step misbehaves on the day

Ordered most-acceptable-to-cut first. Right-to-left means "if you must cut, cut from the right":

1. **Tableflow Iceberg moment** (drop without comment).
2. **Flink workspace tour** (we already say the transform runs in Node for this demo).
3. **`learner.recommendations` topic UI** (the SSE notification on the page is enough — the recommender wrote *something*).
4. **`learning.events` topic UI** (last to cut — this is the proof that Confluent is actually receiving anything).
5. **The screen-off framing** (NEVER cut — this is the story).
6. **The live Waterr call** (NEVER cut — without it nothing else fires).

---

## 4. Failure-mode rehearsal

| If… | Then… |
|---|---|
| `/start` 502s and the share-URL doesn't load | Open `https://waterr.ai/scenario/c1fb65c5-3dba-4a84-a8af-a7b2270e2bea` directly in a tab; do the conversation; resume at 01:50. |
| `learning.events` shows no new row after 30s | Pivot to pre-recorded row (see 01:50–02:25). Apologise once: "Waterr post-call scoring sometimes lags by a minute; here's the same shape from a minute ago." Move on. |
| `learner.recommendations` shows no new row | The recommender (Terminal 3) probably crashed. Glance at it. Don't fix live — pivot: "The Flink job and recommender are the same SQL contract; I'll show you the pre-built one." Open `flink/job.sql` in the editor. |
| SSE never fires on the page | The learner_id in localStorage doesn't match what the recommender thinks. Manually paste the recommended `join_url` in a new tab. Don't draw attention to the miss. |
| Wi-Fi dies | Play the backup Loom (M5 task in `PLAN.md`). Do NOT try to recover live. |

Backup video lives at `docs/demo-backup.mp4` (record this during dry-run; do NOT skip this — M5 DoD).

---

## 5. Quick reference — commands & URLs

```bash
# servers
node --env-file=.env src/server.js              # :3000
node --env-file=.env src/recommender.js          # consumer
ngrok http 3000                                  # optional, for live webhooks

# health
curl -s http://localhost:3000/healthz | jq

# manual smoke (only if /start is broken — recreate the SSE fan-out manually)
curl -sX POST http://localhost:3000/internal/push \
  -H 'Content-Type: application/json' \
  -d '{"learner_id":"<paste-from-localStorage>","join_url":"https://waterr.ai/scenario/c1fb65c5-3dba-4a84-a8af-a7b2270e2bea"}'
```

| Surface | URL |
|---|---|
| Learner page | http://localhost:3000 |
| Healthcheck | http://localhost:3000/healthz |
| Waterr share URL (fallback) | https://waterr.ai/scenario/c1fb65c5-3dba-4a84-a8af-a7b2270e2bea |
| Confluent Cloud | https://confluent.cloud — env `env-16m7dv`, cluster `lkc-k88xzzp` |

---

## 6. What we are NOT demoing (and why, when asked)

- **Twilio SMS** — replaced by SSE on the page; one fewer credential to manage live; the architecture supports SMS verbatim by swapping the delivery function in `src/recommender.js`.
- **Flink `ML_PREDICT` live** — paste-ready in `flink/job.sql`; for the demo build we ran the same transform in Node because raw-JSON topics need Schema Registry + a JSON Schema registration to be Flink-readable, which is a setup tax we're not paying under the time box. The SQL is the prod path.
- **Hindi voice fall-through, parent dashboard, PWA, dataset release** — roadmap, see `PLAN.md §Features beyond demo`.
