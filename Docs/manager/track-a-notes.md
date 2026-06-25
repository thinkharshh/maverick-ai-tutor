# Track A — WaterrAI · Scratchpad

Last update: 2026-06-25 13:10 IST · iteration 1

## What's done

- **A2** `src/setup.js` — idempotent. ES modules, native `fetch`, `import 'dotenv/config'`.
  - Picks Bella (US Female) ElevenLabs voice in practice. Falls back through Janvi/Ayesha/any female ElevenLabs → any ElevenLabs → voices[0].
  - Skips create if a persona or scenario with the target name already exists (`findPersonaByName`, `findScenarioByName`). Names: `Aarya`, `Maverick: Audio-First Tutor`.
  - TUTOR_PROMPT inlined verbatim from `docs/TECHNICAL_PLAN.md §3.2`.
  - Prints exactly two stdout lines: `PERSONA_ID=<uuid>` and `SCENARIO_ID=<uuid>`. All status/diagnostics go to stderr.
- **A3** Ran live against `https://api.waterr.ai/v1` once. The script created:
  - PERSONA_ID `00d36bd3-4f7d-402e-b530-c3b2ba46ad84` (Aarya)
  - SCENARIO_ID `149642d0-7c7d-4958-b61a-e4ac471261a0` (Maverick: Audio-First Tutor)

  **However**, the user then manually edited `.env` to point at a different, hand-curated scenario:
  - PERSONA_ID `fe1d2b6e-7a2d-4d25-86e8-a46d0840a2a9`
  - SCENARIO_ID `c1fb65c5-3dba-4a84-a8af-a7b2270e2bea` (named "Accessible Finance Guide")

  **DO NOT re-run `node src/setup.js`** — it would create a *different* scenario by name ("Maverick: Audio-First Tutor") and confuse the .env state. If setup must run again, first reconcile names with what's in `.env`.
- **A4** `src/server.js` — Express, ES module. Routes:
  - `GET /` → serves `public/index.html` via `express.static` + explicit fallback.
  - `POST /start` → calls `POST /meetings` with `scenario_id=process.env.SCENARIO_ID`, returns `{ meeting_id, join_url, daily_url }`.
  - `POST /webhook/waterr` → `express.raw` body. HMAC verifies only when `WATERR_WEBHOOK_SECRET` is set (dev-mode skip with a warning log). On `session.analysis_complete` shapes the payload into the §2.1 envelope and calls `produceLearningEvent()` — currently a **stub that logs only** with a `TODO(track-b)` to dynamically import `./kafka.js` once it exists.
  - `GET /events` → SSE keyed by `?learner_id=<id>`. Sends `event: ready` on connect, then `event: recommendation` when pushed. Has 25s ping comments to keep proxies alive. Closes the previous listener if the same learner_id reconnects.
  - `POST /internal/push` → `{ learner_id, join_url, ... }` → broadcasts SSE `recommendation` event to that listener; returns `delivered:false, reason:no_listener` (HTTP 202) when nobody is connected.
  - `GET /healthz` → `{ ok:true, scenario_id }`.
- **A5** `public/index.html` — `<main>`, single `<h1>`, `<label for>` on every input, two `aria-live="polite"` regions (status + recommendation). Generates a stable `learner_id` in `localStorage` and uses it both for the `/start` POST body and the `/events` SSE subscription. Submit → POST `/start` → `window.location = join_url` (400ms delay so the screen reader gets to announce the status).
- **A6** `node --check src/setup.js && node --check src/server.js` → BOTH OK. Server NOT started.

## Open items / hand-offs

- **Rendezvous with Track B (kafka.js):** when `src/kafka.js` exports an async `produce(topic, key, value)`, replace the body of `produceLearningEvent` in `src/server.js` with a dynamic import:

  ```js
  const { produce } = await import('./kafka.js');
  await produce(process.env.TOPIC_LEARNING_EVENTS, evt.learner?.id, evt);
  ```

  Manager (M3) owns this rendezvous; Track A intentionally did not import the file to avoid coupling before Track B publishes.
- **Webhook registration:** the `setup.js` here does NOT call `PUT /scenarios/{id}/session-options` yet — `WEBHOOK_PUBLIC_URL` is empty in `.env` (no ngrok yet). The spec mentions this but this iteration's task list omitted it. Add in a later pass once ngrok is up; the route in `server.js` is ready.
- **WATERR_WEBHOOK_SECRET:** not yet known. We log a dev-mode warning and accept unsigned POSTs until it's set. Add to `.env` once Waterr returns it from the webhook registration.
- **learner_id mapping:** the SSE/recommender channel uses a browser-generated UUID from `localStorage`, not the Waterr `person_name`. The current `/start` body includes `learner_id`, but it isn't yet forwarded into the Waterr `note_context`. For end-to-end recommender → SSE delivery in the same tab this is fine; for cross-device delivery (SMS/email) we'll need to thread it through.

## Gotchas

- Waterr responses are wrapped: `{status:"success", data: ... }`. Both files use a small `unwrap()` helper.
- Voice list contains entries literally named just "Female"/"Male" — those are skipped in favor of named voices like Bella/Janvi/Ayesha.
- The webhook handler uses `express.raw({ type: '*/*' })` so HMAC verification sees the exact bytes. Don't move `express.json()` ahead of it.
- The user manually overrode the persona/scenario in `.env` to a curated "Accessible Finance Guide" scenario — `src/server.js` correctly reads `process.env.SCENARIO_ID` so it picks this up; no code change needed.

## Files I own

- `src/setup.js`
- `src/server.js`
- `public/index.html`
- `docs/manager/track-a-notes.md`

## Files I must NOT touch

- `src/kafka.js`, `src/recommender.js`, `flink/*`
- `docs/status/CURRENT.md` other than appending one block under `## Recent changes`
- `docs/roadmap/PLAN.md`, `package.json`, `.env` (except appending PERSONA_ID/SCENARIO_ID which was explicit in the task)

---

## billing_id probe

Run by Agent D on 2026-06-25 against live `https://api.waterr.ai/v1/meetings` with the
"Accessible Finance Guide" scenario `c1fb65c5-…`.

Reference IDs pulled from `GET /scenarios/${SCENARIO_ID}`:
- `membership_id` = `70a0b021-5655-4678-878b-fb03a60ad9b9`
- `org_id`        = `1446c915-8727-416e-a89f-69dc0829955c`
- org passed by manager (from `GET /organizations`): `e8100005-b5bf-41ce-be42-e137ac681bcf`

All four probes returned **HTTP 422** with the same `"Python server error"` body:
`{"type":"missing","loc":["body","billing_id"],"msg":"Field required",...}`.
The Python service's echoed `input` object contains `person_name`, `user_id`,
`org_id`, `scenario_id`, `prompt`, … but **no `billing_id` and no `language`** —
proving the CoreBackend Node controller is dropping both fields *before* forwarding,
regardless of what the public API receives. The bug is upstream of us.

| # | Variation                                                             | HTTP | First 200 chars of body                                                                                                                                              |
|---|-----------------------------------------------------------------------|-----:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| a | `billing_id = scenario.membership_id` (`70a0b021…`)                   |  422 | `{"success":false,"message":"Python server error","error":{"detail":[{"type":"missing","loc":["body","billing_id"],"msg":"Field required","input":{"person_name":"DemoProbe","user_id":"70a0b021-5655-467` |
| b | `billing_id = org_id` from manager (`e8100005…`)                      |  422 | `{"success":false,"message":"Python server error","error":{"detail":[{"type":"missing","loc":["body","billing_id"],"msg":"Field required","input":{"person_name":"DemoProbe","user_id":"70a0b021-5655-467` |
| b2| `billing_id = scenario.org_id` (`1446c915…`) — bonus run               |  422 | (identical body to (a)/(b))                                                                                                                                          |
| c | header `x-membership-id: <membership_id>` + body `billing_id`         |  422 | `{"success":false,"message":"Python server error","error":{"detail":[{"type":"missing","loc":["body","billing_id"],"msg":"Field required","input":{"person_name":"DemoProbe","user_id":"70a0b021-5655-467` |
| d | + `"language":"en"` + `"meeting_type":"regular"` in body              |  422 | `{"success":false,"message":"Python server error","error":{"detail":[{"type":"missing","loc":["body","billing_id"],"msg":"Field required",...` AND a second `loc:["body","language"]` missing entry      |

Notable: in probe (d) the Python service echoes `meeting_type:"regular"` back in
its `input` — meaning the Node controller forwards `meeting_type` but still
strips `billing_id`/`language`. That isolates the patch site precisely to
`pythonServerData` in `controllers/meetingController.js` (~line 590) where the
two fields need to be added to the outgoing payload.

**Conclusion:** none of (a)–(d) unblock 422 from the *public* API surface.
`src/server.js` is **NOT** updated — the share-URL fallback stays in place,
and the closed-loop M4 path remains gated on a server-side patch by Harshit.
The patch, when shipped, just needs to land `billing_id` (from auth/membership)
and `language` (default `"en"`) into `pythonServerData`.
