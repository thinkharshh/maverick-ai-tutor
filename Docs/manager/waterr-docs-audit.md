# WaterrAI Product Docs — Gap Audit

**Date:** 2026-06-25
**Scope:** `monorepo/docs-site/` vs `monorepo/CoreBackend/` + `monorepo/meetingMLservice/`
**Coverage today:** ~31 of ~120 endpoints documented (≈26%).

---

## Top 5 priorities (do these first)

1. **Agentic flows guide is missing entirely.** Custom functions, browsing, task automation are fully built (`CoreBackend/routes/customFunctionRoutes.js`, `agenticRoutes.js`) but invisible in docs. High-value for enterprise. Write end-to-end: defining functions, attaching to scenarios, tool-call wire format, timeout/error handling.
2. **BYOK is undocumented.** Customers can pass `use_own_keys: true` + `openai_api_key` / `anthropic_api_key` / `google_api_key` / `elevenlabs_api_key` / `cartesia_api_key` on a per-meeting basis (`meetingMLservice/app.py:210-227`). Zero mention in `meetings.mdx`. Major selling point for data-sensitive customers.
3. **Required-fields lie.** `meetings.mdx:L14-23` says minimum is `scenario_id` + `person_name`, but `meetingMLservice/app.py:106-177` `MeetingRequest` requires `user_id`, `org_id`, `prompt`, `billing_id`, `language`, `meeting_duration_minutes`, etc. The BFF fills most, but the gap is what's making partners hit the 422 we just fought.
4. **Missing endpoint pages** for: meeting controls, participants, custom functions, public embeds, scenario invite-codes (Plus One short links), `default_kind` starter scenarios. Add 5-10 reference pages.
5. **Webhook + polling guide is thin.** Only `session.analysis_complete` documented; no retries, no signature pitfalls (raw-body + Express), no fallback polling story for the still-unimplemented events.

---

## Missing pages (route exists in code, no docs)

| Route | Code | Impact | Why it matters |
|---|---|---|---|
| `*/custom-functions` (CRUD + attach) | `customFunctionRoutes.js:30-130` | high | Agentic tool-call APIs are invisible. |
| `*/agentic/apps|websites|windows|tasks|outputs` | `agenticRoutes.js:15-100` | high | Browsing/automation feature undiscovered. |
| `GET /scenarios/embed/:id` (public, no-auth) | `scenarioRoutes.js:531` | high | The exact embed endpoint we needed; not in docs. |
| `GET /scenarios/link/:scenarioLink` | `scenarioRoutes.js:576` | medium | Public share links flow undocumented. |
| `GET /scenarios/invite-codes/:code` | `scenarioRoutes.js:580` | medium | Plus-One 3-3-3 invite codes. |
| `POST /meetings/{id}/participants` (+CRUD) | `meetingRoutes.js:417-476` | medium | Participant mgmt + RSVP tracking. |
| `*/meetingcontrols` | `meetingControlRoutes.js:1-258` | medium | Recording/chat/vision controls. |
| `POST /meetings/end-user` (mentioned, no dedicated page) | `meetingRoutes.js:161` | medium | The unauthenticated embed-billing flow. |
| `POST /meetings/{id}/notify-end` | `meetingRoutes.js:265` | low | Notification trigger. |
| `PATCH /meetings/{id}/soft-delete` | `meetingRoutes.js:317` | low | Soft vs hard delete distinction. |

---

## Stale / wrong

| Doc | Issue | Evidence |
|---|---|---|
| `meetings.mdx:L14-23` | Says minimum is `scenario_id`+`person_name`. Python service requires 6 more. | `meetingMLservice/app.py:106-177` |
| `meetings.mdx:L42-44` | Mentions `note_context` but not BYOK fields. | `meetingMLservice/app.py:210-227` |
| `meetings.mdx:L50-62` | Response envelope `{success:true,data}` while other docs show `{status:"success",data}`. Inconsistent. | Cross-check `authentication.mdx`, `webhooks.mdx` |
| `scenarios.mdx:L77-86` | Docs use `call_duration`; PUT route schema uses `maxDuration`. Field name mismatch. | `scenarioRoutes.js:770` |
| `webhooks.mdx:L106-114` | Says "no retries in MVP" but doesn't surface the policy plan; lists planned events without a tracking link. | `webhookRoutes.js` |
| `post-meeting.mdx:L43` | `analysis_status` enum documented; nothing on what triggers analysis or whether webhook is required. | `meetingController.js` |
| `authentication.mdx:L74-156` | Covers REST API keys but never explains end-user JWTs (the `allowEmbed` middleware path) — exactly what embed customers need. | `meetingRoutes.js:161` |
| `scenarios.mdx` | No mention of `default_kind`, Plus One, Requirement Gathering starter scenarios beyond a brief callout. | `scenarioRoutes.js` |
| `voices.mdx` | Treats voice as a required setup step. In practice, Waterr defaults to **Gemini Live** which speaks natively from the model — no third-party voice (ElevenLabs/Cartesia/Azure) needs to be configured. The page should say "voices are optional; only configure when overriding Gemini Live's default voice", then explain the override path. | Product default; provider matrix is for overrides only. |

---

## Concept / workflow gaps

1. **End-user billing model** — who pays when (authenticated vs end-user endpoint); how `billing_id` flows.
2. **BYOK** — full guide on `use_own_keys` + the per-vendor key fields.
3. **Public/embedded scenario flow** — generating link, iframe permissions, **X-Frame-Options + CORS** for cross-origin embeds (the exact gap I just hit when wiring `/embed/inline/`).
4. **Agentic workflows** — custom functions definition + attach + invoke.
5. **Webhook lifecycle** — registration via `PUT /scenarios/{id}/session-options`, HMAC verify, retry policy, raw-body gotcha, polling fallback.
6. **Meeting controls vs scenario settings** — separation of concerns.
7. **Recording/transcript/analysis lifecycle + timing** — when each artifact is ready, polling strategy.
8. **Participant management** — RSVP states, calendar source mapping.
9. **Voice + language matrix.** *(Frame as "overrides to Gemini Live's default voice", not as a required-setup step.)*
10. **Error code catalogue** — central reference page.

---

## Errors that aren't documented

- `POST /meetings`: 422 `billing_id missing`, 403 `session limit reached`, 403 `subscription inactive`, 404 `scenario not found`, 400 `invalid voice_id`.
- `POST /scenarios`: 400 `name > 100`, 404 `persona not found`, 404 `goal not found`.
- `GET /analyses/meeting/{id}`: 404, 403 (not owner), 202 (still processing).
- `POST /custom-functions`: 400 `invalid parameters_schema`, 400 `webhook_url required`, 400 `timeout_ms out of range`.

---

## Quality issues

- `endpoint/*.mdx` files are 1-line openapi stubs while details duplicate inside `meetings.mdx` etc. Pick one home; the duplication misleads.
- `webhooks.mdx` example silently breaks if `express.json()` is mounted upstream of the raw-body parser — add a warning.
- No central HTTP error catalogue.
- Transcript field naming (`transcript` vs `sentenced_transcripts`) used interchangeably.
- `session-lifecycle.mdx` doesn't cover session-minutes billing / `maxDuration` defaults.

---

## What I actually hit while integrating (lived experience)

These are the things that broke, in order, while wiring a hackathon app
(`localhost:3000` Node/Express) against the live `api.waterr.ai`. Every line
below cost real time. Each one belongs as either a doc page or a server fix.

### Build-stop blockers

1. **`POST /meetings` → 422 `billing_id missing`.** Docs say body is just
   `person_name` + `scenario_id`. Server (`meetingController.js:~L590`) builds
   `pythonServerData` and forwards to the Python service, which (per
   `meetingMLservice/app.py:106-177` `MeetingRequest`) requires `billing_id`
   and `language` as `Field(...)`. The BFF doesn't include either → instant
   422 on every meeting create.
   - **Doc fix:** state the *true* minimum body server-side, and say what the
     gateway fills in vs what the caller must.
   - **Server fix:** patch `pythonServerData` to include
     `billing_id: billingMembershipId, language: "en"` (or pull from
     `MeetingControls.language`, which is already `"English"` per the
     scenario detail response).

2. **`POST /meetings` → 500 `Failed to create meeting`.** After the billing
   patch lands, the call still 500s for *every* scenario on the account
   (verified against both `Accessible Finance Guide` and the auto-seeded
   `plus_one`). The Python service's catch-all at
   `meetingMLservice/app.py:524` swallows the underlying exception — caller
   sees `{"detail":"Failed to create meeting"}` and no actionable signal.
   - **Server fix:** include the underlying exception class + message (or at
     least a correlation_id) in the 500 body. Today there is literally no way
     to know whether it's Daily.co, Pipecat agent spawn, billing limits, voice
     config, or anything else.
   - **Doc fix:** an "If POST /meetings 500s, check these in order" runbook.

3. **No documented way to discover `billing_id` from the API.** Tried
   `/memberships`, `/billings`, `/billings/active`, `/users/me`, `/me`,
   `/auth/me` — all 404. The membership_id is embedded in the JWT/API key
   context but never returned.
   - **Add endpoint:** `GET /auth/me` or `GET /memberships/me` returning the
     caller's `user_id`, `membership_id`, `org_id`, `billing_id`,
     `remaining_minutes`.
   - **Or doc it explicitly:** "Your API key resolves the org/membership
     server-side; you never need to pass these."

### Embed-side blockers (the iframe story)

4. **`x-frame-options: SAMEORIGIN` on every customer-facing scenario URL.**
   Tested `/scenario/<id>`, `/scenarios/<id>`, `/m/<id>`, `/embed/<id>`,
   `/embed/inline/<id>` — all return SAMEORIGIN. Any third-party iframe
   embed shows the broken-file icon in Chrome and a blank page in Safari.
   - **Server fix:** on `/embed/*` routes specifically, drop
     `X-Frame-Options` and serve CSP `frame-ancestors *` (or an allowlist).
     Embed routes only — keep SAMEORIGIN on the dashboard.

5. **CORS not configured for cross-origin SPA bootstrap.** Even after our
   reverse-proxy bypassed X-Frame, the embed page's React bundle tried to
   fetch from `https://waterr.ai/backend/*` and `https://waterr.ai/ml/*`.
   `OPTIONS` preflights to both (from `Origin: http://localhost:3000`)
   return **HTTP 500** — no `Access-Control-Allow-*` headers at all.
   The SPA can't bootstrap.
   - **Server fix:** an OPTIONS handler with `Access-Control-Allow-Origin: *`
     (or per-tenant allowlist), `-Methods: GET,POST,PUT,DELETE,OPTIONS`,
     `-Headers: Content-Type,Authorization,X-Waterr-Signature`.
   - **Doc fix:** dedicated "Embed Waterr in your own product" page covering
     iframe `allow=` perms, CSP, accepted origins, end-user JWT flow.

6. **`/embed/inline/<id>` is not in the docs at all.** Customers I'd point at
   this would have to guess the URL pattern. The other variants
   (`/scenario/`, `/m/`, `/embed/`) all 200 — unclear which is canonical.
   - **Doc fix:** add `embed.mdx` listing all embed entry points and what
     each does (auto-start vs lobby vs button-launch).

### Auth / lifecycle papercuts

7. **`POST /meetings/end-user` rejects API keys.** Docs say "for public or
   embedded scenarios, you can create a meeting without an API key", but the
   live endpoint returns `"jwt malformed"` when called with a `Bearer wai_...`
   key, and `"No token provided"` without one. So neither the documented "no
   auth" nor "with API key" path actually works.
   - **Doc fix:** clarify the *real* auth — end-user JWT minted by the embed
     SDK, not the workspace API key. Or, change the server to also accept
     the workspace key and bill the workspace.

8. **`GET /users/me` returns `{"message":"Invalid userId: must be a valid
   UUID"}`.** The route is `/users/:userId` and `me` is being parsed as the
   userId. Standard `me` shortcut is missing entirely.
   - **Server fix:** add `/users/me` alias that returns `req.user`.

9. **No webhook secret is ever returned to me.** The webhook verification
   doc shows HMAC-SHA256 with "your webhook secret" — but registering a
   webhook via `PUT /scenarios/{id}/session-options` doesn't surface a
   secret anywhere. Where does the caller get it?
   - **Server fix:** generate-on-register and return in the PUT response.
     Allow rotation via a follow-up endpoint.
   - **Doc fix:** show the full round-trip in `webhooks.mdx`.

10. **`scenario_link` field on scenarios is a slug (e.g.
    `"accessible-finance-guide"`) but no docs say what to do with it.** Is
    it `https://waterr.ai/<slug>`? `https://waterr.ai/scenario/<slug>`?
    Both return 200; neither is the canonical share URL according to docs.
    - **Doc fix:** explain how slugs differ from UUIDs and which URL is the
      shareable one.

11. **`X-Powered-By` header still set** on api.waterr.ai responses (and
    proxy responses). Cosmetic but a security review note.

### What an integrator would have needed up front

If `api-reference/introduction.mdx` started with a 10-line "Embedding Waterr
in your product" callout that said:

> Authenticate with `Authorization: Bearer wai_<key>`. Workspace and billing
> context resolves server-side. Required meeting body is `person_name` +
> `scenario_id`. Embed scenarios with
> `<iframe src="https://waterr.ai/embed/inline/<scenario_id>"
> allow="camera; microphone; autoplay; display-capture; fullscreen">`.
> If you embed from a non-waterr.ai origin, you must allowlist your origin
> via Settings → API Keys → Allowed Origins (we'll send CORS + drop
> `X-Frame-Options` for your origin only).

…the hackathon team would have shipped 90 minutes faster. Worth doing.

---

## Specific embed-side gap I hit just now (worth a dedicated callout)

While wiring our hackathon UI, I tried `https://waterr.ai/embed/inline/<scenario_id>` from `localhost:3000`. Two hidden blockers:

1. `x-frame-options: SAMEORIGIN` on all `/embed/*`, `/scenario/*`, `/scenarios/*`, `/m/*` paths → iframe blocked. Should be `frame-ancestors *` (CSP) or removed for `/embed/*` specifically.
2. OPTIONS preflight against `https://waterr.ai/backend/*` and `https://api.waterr.ai/v1/*` returns 500 (no CORS headers) → cross-origin SPA bootstrap fails even when the iframe gets past X-Frame.

**One-paragraph doc fix needed:** "How to embed a Waterr scenario in your own product — required CORS headers, recommended CSP, accepted origins, iframe `allow=` permissions."
