# Orchestration — How the cloud agents coordinate

We are racing the clock. The plan is to run two long-lived tracks in parallel, with a thin manager loop checking in every few minutes.

## Tracks

| Track | Owner | Files | Goal |
|---|---|---|---|
| **A — WaterrAI** | `track-a-waterr` agent | `src/setup.js`, `src/server.js`, `public/index.html` | Working video-tutor flow end-to-end with the live Waterr API. |
| **B — Confluent** | `track-b-confluent` agent | `src/kafka.js`, `src/recommender.js`, `flink/job.sql`, Confluent CLI | Kafka topics live, Flink job running, closed loop demonstrated. |
| **M — Manager** | the main Claude loop | `docs/status/CURRENT.md`, `docs/roadmap/PLAN.md`, `.env`, `package.json` | Decide priorities, unblock, coordinate, demo polish. |

## How a track agent runs (every invocation)
1. Read `docs/manager/CLAUDE.md` and `docs/status/CURRENT.md`.
2. Pick the highest-priority pending task **in your lane** from `docs/roadmap/PLAN.md`.
3. Execute. Test locally where possible.
4. Append a status block (max 5 lines) to `docs/status/CURRENT.md` under `## Recent changes` — newest first.
5. Update `docs/manager/track-<x>-notes.md` with anything the next-iteration agent will need (assumptions, URLs, IDs, gotchas).
6. Exit.

## Manager loop (this is what the long-running wakeup runs)
1. Read `docs/status/CURRENT.md`.
2. If both tracks have completed all their tasks → run a smoke test, then write `DEMO READY` to STATUS.
3. If either track has an unsticking blocker (something the *other* track can fix) → dispatch the other track on it.
4. Otherwise, re-dispatch the track that's furthest behind with the next task from `docs/roadmap/PLAN.md`.
5. Schedule the next wakeup.

## Boundaries (do not cross)
- **Track A** must NOT edit `src/kafka.js`, `src/recommender.js`, `flink/*`, or Confluent-cluster settings.
- **Track B** must NOT edit `src/setup.js`, `src/server.js` route handlers, or `public/*`.
- Both agents may **read** the whole repo and `docs/`. Neither may rewrite `docs/roadmap/PLAN.md`.

## Communication protocol
- Status changes → `docs/status/CURRENT.md` (append, newest-first).
- Track-specific scratchpad → `docs/manager/track-{a,b}-notes.md` (free-form).
- Cross-track requests → write `REQUEST: <to track> <ask>` in `docs/status/CURRENT.md`, manager will route it.

## Failure modes & responses
| Symptom | Response |
|---|---|
| Waterr API key invalid / 401 | Track A writes BLOCKED, manager pings user. |
| Confluent CLI install needs sudo | Track B falls back to brew --user install or documents manual step. |
| Confluent creds missing | Track B writes BLOCKED, drafts everything offline, manager pings user. |
| `ML_PREDICT` model not registered in Confluent | Track B uses Cut #3: Node-side Claude call inside `recommender.js`. |
| ngrok offline | Track A skips webhook registration, mocks the event for demo. |
| Out of session minutes (Waterr) | Manager pings user; demo uses cached transcript. |

## Cadence
- Workflow launches both tracks in parallel.
- Manager wakeup: every ~20 minutes (long enough to amortize cache miss, short enough to keep momentum).
- A track agent is expected to take 3–8 minutes per iteration.
