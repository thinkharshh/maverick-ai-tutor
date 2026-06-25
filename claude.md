# Maverick — Preventive Maintenance Coach

A voice-first AI coach for **shop-floor preventive-maintenance training** at a car manufacturing plant. Built on **WaterrAI** (live AI video coach) + **Confluent Cloud** (Kafka + Flink + Tableflow) for Confluent AI Day 2026 India.  
  
  
  


**Before doing anything in this repo, read these — in order:**

1. [docs/manager/CLAUDE.md](docs/manager/CLAUDE.md) — operator's manual, multi-agent rules, repo map
2. [docs/status/CURRENT.md](docs/status/CURRENT.md) — live state, blockers, next task
3. [docs/roadmap/PLAN.md](docs/roadmap/PLAN.md) — milestones M0–M5, cut order, definition of done
4. [docs/manager/ORCHESTRATION.md](docs/manager/ORCHESTRATION.md) — how parallel tracks coordinate
5. [docs/TECHNICAL_PLAN.md](docs/TECHNICAL_PLAN.md) — code-level spec (data model, Flink SQL, prompts)

Then pick the highest-priority pending task in your lane from `docs/status/CURRENT.md` and execute.

`.env` holds the Waterr API key — never log or echo it.