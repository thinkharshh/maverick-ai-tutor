# ROADMAP — Maverick · Preventive Maintenance Coach

## North star
Voice-first AI coach for shop-floor preventive-maintenance training at a car manufacturing plant. Live Waterr video session + Confluent Kafka/Flink adaptive-drill loop. Demo must be runnable hands-busy (gloves on, headset on) — judges hear and see the loop work end to end on a workstation kiosk.

## Milestones (in order)

### M0 — Scaffolding · DOD: every file in `docs/manager/CLAUDE.md`'s repo map exists, even if some are stubs
- Folder structure ✅
- `.env` + `.gitignore` ✅
- All docs files written ✅
- `package.json` with deps installed
- Empty stubs for `src/setup.js`, `src/server.js`, `src/kafka.js`, `src/recommender.js`, `flink/job.sql`, `public/index.html`

### M1 — Waterr-only demo · DOD: opening localhost:3000 → submit form → Aarya (PM coach) talks to you
- `src/setup.js` runs idempotently; creates Aarya persona + preventive-maintenance coach scenario
- `src/server.js` serves landing page + `/start` route creates a live meeting
- `public/index.html` is workstation-kiosk-friendly (one main, one h1, labelled inputs, aria-live status)
- Smoke test passes (you can join a real Waterr room)

### M2 — Kafka bridge · DOD: every completed Waterr session lands in Confluent `learning.events`
- Confluent CLI installed; cluster reachable
- Topics created: `learning.events`, `learner.recommendations`
- `src/kafka.js` produces from `server.js`'s `/webhook/waterr`
- Webhook URL registered with the Waterr scenario (`PUT /scenarios/{id}/session-options`)
- Manual smoke: end a Waterr meeting → 60s later see the message in Confluent UI

### M3 — Flink adaptive recommendation · DOD: `learner.recommendations` gets a new row within ~10s of an event landing
- Flink SQL job pasted into Confluent Cloud and running
- AI model `next_lesson_model` registered (Anthropic Claude or Gemini via Vertex)
- `recommendation.next_scenario_prompt` is sensible PM coaching English on a real test event (cites OEM interval / IATF clause where applicable)

### M4 — Closed loop · DOD: SMS or SSE delivers a new drill link to the technician ≤30s after the previous call ends
- `src/recommender.js` consumes recommendations
- Calls `/scenarios/create-with-gpt` then `/meetings`
- Pushes link to `server.js` internal endpoint → SSE → page announces it via `aria-live`
- Manual smoke: end the first drill, count the seconds, click the new drill link

### M5 — Demo polish · DOD: 3-minute hands-busy kiosk demo, no fumbling
- Tableflow ON (Iceberg tables visible in Confluent UI)
- Workstation-kiosk run-through tested on `index.html` (one-button submit, headset audio)
- Demo script in `docs/roadmap/DEMO.md`
- Backup video recorded (Loom or QuickTime) in case live demo Wi-Fi fails

## Features beyond demo (post-hackathon)
- Multi-modal vision assist (technician's helmet-cam describes the part Aarya is referring to)
- Curriculum-driven scenarios (one persona, many stations, plant-trainer-set certification plan)
- Hindi / Tamil / Marathi voice + multi-language fall-through for line operators
- Plant-supervisor + quality dashboard reading from the Iceberg tables (IATF 16949 audit-ready)
- Offline-friendly mobile shortcut (PWA add-to-home for line tablets)
- Anonymized PM-event dataset for predictive-maintenance R&D (consent-gated)

## Cut order — if we are short on time, drop right-to-left
**Tableflow → SMS delivery → Flink `ML_PREDICT` (move to Node) → Adaptive next-drill (hardcode follow-up)**

The leftmost cut survives → still a working PM voice coach. Rightmost is gloss.

## Definition of "winning the hackathon"
- Live working demo (no slides)
- Confluent Cloud UI shown end-to-end (topics + Flink + Tableflow)
- Hands-busy industrial moment ("the entire flow works while the technician's hands stay on the tools")
- Clear industrial-impact framing (skilling cycles measured in seconds, not monthly training quarters)
- Production-shaped architecture (Kafka contracts, Flink SQL, not just LLM glue)
