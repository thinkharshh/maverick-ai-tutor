# ROADMAP — Maverick AI Tutor

## North star
Voice-first AI tutor for blind learners. Live Waterr video session + Confluent Kafka/Flink learning loop. Demo must be runnable with the screen off — judges hear it work.

## Milestones (in order)

### M0 — Scaffolding · DOD: every file in `docs/manager/CLAUDE.md`'s repo map exists, even if some are stubs
- Folder structure ✅
- `.env` + `.gitignore` ✅
- All docs files written ✅
- `package.json` with deps installed
- Empty stubs for `src/setup.js`, `src/server.js`, `src/kafka.js`, `src/recommender.js`, `flink/job.sql`, `public/index.html`

### M1 — Waterr-only demo · DOD: opening localhost:3000 → submit form → Aarya talks to you
- `src/setup.js` runs idempotently; creates Aarya persona + tutor scenario
- `src/server.js` serves landing page + `/start` route creates a live meeting
- `public/index.html` is screen-reader-friendly
- Smoke test passes (you can join a real Waterr room)

### M2 — Kafka bridge · DOD: every completed Waterr session lands in Confluent `learning.events`
- Confluent CLI installed; cluster reachable
- Topics created: `learning.events`, `learner.recommendations`
- `src/kafka.js` produces from `server.js`'s `/webhook/waterr`
- Webhook URL registered with the Waterr scenario (`PUT /scenarios/{id}/session-options`)
- Manual smoke: end a Waterr meeting → 60s later see the message in Confluent UI

### M3 — Flink adaptive recommendation · DOD: `learner.recommendations` gets a new row within ~10s of an event landing
- Flink SQL job pasted into Confluent Cloud and running
- AI model `next_lesson_model` registered (Anthropic Claude)
- `recommendation.next_scenario_prompt` is sensible English on a real test event

### M4 — Closed loop · DOD: SMS or SSE delivers a new join link to the learner ≤30s after the previous call ends
- `src/recommender.js` consumes recommendations
- Calls `/scenarios/create-with-gpt` then `/meetings`
- Pushes link to `server.js` internal endpoint → SSE → page announces it via `aria-live`
- Manual smoke: end the first call, count the seconds, click the new link

### M5 — Demo polish · DOD: 3-minute screen-off demo, no fumbling
- Tableflow ON (Iceberg tables visible in Confluent UI)
- macOS VoiceOver tested on `index.html`
- Demo script in `docs/roadmap/DEMO.md`
- Backup video recorded (Loom or QuickTime) in case live demo Wi-Fi fails

## Features beyond demo (post-hackathon)
- Multi-modal vision assist (camera describes objects to learner)
- Curriculum-driven scenarios (one persona, many subjects, parent-set learning plan)
- Hindi voice + multi-language fall-through
- Parent/teacher dashboard reading from the Iceberg tables
- Offline-friendly mobile shortcut (PWA add-to-home)
- Open dataset of anonymized learning events (consent-gated)

## Cut order — if we are short on time, drop right-to-left
**Tableflow → SMS delivery → Flink `ML_PREDICT` (move to Node) → Adaptive next-lesson (hardcode follow-up)**

The leftmost cut survives → still a working voice tutor. Rightmost is gloss.

## Definition of "winning the hackathon"
- Live working demo (no slides)
- Confluent Cloud UI shown end-to-end (topics + Flink + Tableflow)
- Screen-off accessibility moment ("the entire flow works without sight")
- Clear social impact framing
- Production-shaped architecture (Kafka contracts, Flink SQL, not just LLM glue)
