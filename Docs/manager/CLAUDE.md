# Maverick AI Tutor — Operator's Manual

This file is the Claude Code entry point. Read this *first* in every new session.

## What this project is
A voice-first AI tutor for blind learners, built for Confluent AI Day 2026 India. WaterrAI handles the live AI video session. Confluent Cloud (Kafka + Flink + Tableflow) closes a real-time adaptive learning loop: every session emits events → Flink generates the next personalized lesson → a new Waterr scenario is created → the link is delivered back to the learner.

## Always do this on startup (any agent, any session)
1. `cat docs/status/CURRENT.md` — what state is the project in, what's blocked, what's next.
2. `cat docs/roadmap/PLAN.md` — milestone order, cut order, definition of done per milestone.
3. `cat docs/manager/ORCHESTRATION.md` — multi-agent rules, who owns which files.
4. `cat docs/TECHNICAL_PLAN.md` — code-level spec (data model, SQL, prompts).
5. Then pick the **next pending task in `docs/status/CURRENT.md`** and execute it.

## Speed rules (we have minutes, not hours)
- **No redesign.** Follow `docs/TECHNICAL_PLAN.md` verbatim. Disagreement → log in `docs/status/CURRENT.md` and continue.
- **Parallel by default.** Independent files → spawn independent agents. Don't serialize work that can branch.
- **Cut features per `docs/roadmap/PLAN.md` cut order.** Right-side cuts go first. Demo > completeness.
- **Never log secrets.** `.env` is gitignored. If a tool prints a key, redact in chat.
- **Append to status after every task.** New block at the top of `docs/status/CURRENT.md` under `## Recent changes`. Five lines max.

## Multi-agent rules
- **Track A** = WaterrAI integration. Owns: `src/setup.js`, `src/server.js`, `public/index.html`, `docs/manager/track-a-notes.md`.
- **Track B** = Confluent integration. Owns: `src/kafka.js`, `src/recommender.js`, `flink/job.sql`, `docs/manager/track-b-notes.md`, Confluent CLI install + cluster bootstrap.
- **Cross-track** files: only the lead/manager agent touches `docs/status/CURRENT.md`, `docs/roadmap/PLAN.md`, `package.json`, `.env`.
- On blockers: write `BLOCKED:` in `docs/status/CURRENT.md` with **exactly what's needed**, then move to the next independent task. Never sit on a blocker.

## Repo map
```
AI tutor/
├── claude.md                   ← root pointer (this file's twin) — points to docs/
├── docs/
│   ├── manager/
│   │   ├── CLAUDE.md           ← this file
│   │   ├── ORCHESTRATION.md    ← how parallel agents coordinate
│   │   ├── track-a-notes.md    ← Track A's scratchpad
│   │   └── track-b-notes.md    ← Track B's scratchpad
│   ├── status/
│   │   └── CURRENT.md          ← live state (the single source of truth)
│   ├── roadmap/
│   │   └── PLAN.md             ← milestones, features, cut order
│   └── TECHNICAL_PLAN.md       ← code-level spec
├── src/
│   ├── setup.js                ← creates Waterr persona + scenario (one-shot)
│   ├── server.js               ← Express: /start + /webhook/waterr + SSE
│   ├── kafka.js                ← Confluent producer/consumer wrappers
│   └── recommender.js          ← consumes recommendations → next meeting
├── flink/
│   └── job.sql                 ← Flink SQL pasted into Confluent Cloud UI
├── public/
│   └── index.html              ← accessible single-button page
├── package.json
├── .env                        ← secrets (gitignored)
└── .gitignore
```

## Definition of done for the demo
Open `localhost:3000` → press "Start lesson" → AI tutor "Aarya" greets the learner over video → after the call, Confluent Cloud shows the event in `learning.events` and a row in `learner.recommendations` within ~30s → the page (via SSE) announces a new tailored lesson link.
