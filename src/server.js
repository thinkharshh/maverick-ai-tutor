// src/server.js
//
// Express server for the Maverick AI Tutor demo.
//
// Routes:
//   GET  /                  → serve public/index.html
//   POST /start             → calls Waterr POST /meetings; returns { join_url, ... }
//   POST /webhook/waterr    → HMAC-verified webhook from Waterr; for now logs the payload.
//                             TODO: once Track B's src/kafka.js exists, import { produce }
//                             and forward the LearningEvent to TOPIC_LEARNING_EVENTS.
//   GET  /events            → Server-Sent Events channel for next-lesson links.
//                             Subscribers identify themselves via ?learner_id=<id>.
//   POST /internal/push     → recommender posts { learner_id, join_url } here;
//                             we fan-out to the matching SSE listener (if any).
//
// Refs: docs/TECHNICAL_PLAN.md §3.3, §3.4, §6
// Lane:  Track A. Do NOT import or hard-couple to src/kafka.js — leave the
//        webhook bridge as a TODO until Track B publishes its module.

import 'dotenv/config';
import express from 'express';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');

const API     = process.env.WATERR_API_BASE || 'https://api.waterr.ai/v1';
const API_KEY = process.env.WATERR_API_KEY;
const PORT    = Number(process.env.PORT || 3000);

const SCENARIO_ID            = process.env.SCENARIO_ID || '';
const WATERR_WEBHOOK_SECRET  = process.env.WATERR_WEBHOOK_SECRET || '';

if (!API_KEY) {
  console.error('FATAL: WATERR_API_KEY missing from .env');
  process.exit(1);
}
if (!SCENARIO_ID) {
  console.warn('[server] WARN: SCENARIO_ID is empty — run `node src/setup.js` first.');
}

// ---------------------------------------------------------------------------
// thin Waterr fetch helper
// ---------------------------------------------------------------------------
async function wf(method, urlPath, body) {
  const opts = {
    method,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      Accept: 'application/json',
    },
  };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${API}${urlPath}`, opts);
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  if (!res.ok) {
    const msg = `Waterr ${method} ${urlPath} → ${res.status}: ${text.slice(0, 300)}`;
    const err = new Error(msg);
    err.status = res.status;
    throw err;
  }
  return json;
}

const unwrap = (resp) =>
  (resp && Object.prototype.hasOwnProperty.call(resp, 'data')) ? resp.data : resp;

// ---------------------------------------------------------------------------
// SSE listener registry — Map<learner_id, res>
// ---------------------------------------------------------------------------
const listeners = new Map();

function sseSend(res, event, data) {
  if (event) res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

// ---------------------------------------------------------------------------
// produce() stub — Track B will replace this once src/kafka.js exists.
// We intentionally do NOT import kafka.js here so Track A stays decoupled.
// ---------------------------------------------------------------------------
async function produceLearningEvent(evt) {
  if (!process.env.CONFLUENT_BOOTSTRAP) {
    console.log('[webhook] (kafka unconfigured) skipping produce for', evt.event_id);
    return;
  }
  try {
    const { produce } = await import('./kafka.js');
    await produce(process.env.TOPIC_LEARNING_EVENTS || 'learning.events', evt.learner?.id, evt);
    console.log('[webhook] produced', evt.event_id, 'for', evt.learner?.id);
  } catch (err) {
    console.error('[webhook] kafka produce failed:', err.message);
  }
}

// ---------------------------------------------------------------------------
// build app
// ---------------------------------------------------------------------------
const app = express();

// static landing page
app.use(express.static(PUBLIC_DIR, { extensions: ['html'] }));

app.get('/', (_req, res) => {
  // Make sure browsers don't serve a stale shell that points the iframe at
  // a URL with old (cached) framing headers. Saves us from a re-deploy cycle
  // teaching the cache the new CSP.
  res.set('Cache-Control', 'no-store, must-revalidate');
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

// healthcheck
app.get('/healthz', (_req, res) => res.json({ ok: true, scenario_id: SCENARIO_ID || null }));

// -- GET /embed-proxy/:id ------------------------------------------------------
// Reverse-proxy the Waterr scenario embed page so the iframe is same-origin
// with us. We rewrite all absolute https://waterr.ai/ refs to relative,
// so every subsequent request (assets, backend, ml, sockets) goes back
// through this same Express, which forwards to waterr.ai. The SPA then
// thinks it's running on its own origin — zero CORS, zero X-Frame.
// Mount at /embed/inline/:id so the embedded SPA's pathname-derived API
// calls (e.g. /backend/scenarios/public/user/embed/inline/<id>) hit the
// right upstream route. Earlier path (/embed-proxy/<id>) made the SPA build
// /backend/scenarios/public/user/embed-proxy/<id> which 404'd.
app.get('/embed/inline/:id', async (req, res) => {
  const id = String(req.params.id || '').replace(/[^a-zA-Z0-9-]/g, '');
  if (!id) return res.status(400).send('bad scenario id');
  try {
    const upstream = `https://waterr.ai/embed/inline/${id}`;
    const r = await fetch(upstream, {
      headers: {
        'User-Agent': req.headers['user-agent'] || 'Maverick/1.0',
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
      },
      redirect: 'follow',
    });
    let html = await r.text();
    // Strip any existing <base> and rewrite absolute waterr.ai refs to
    // root-relative so they re-enter our proxy.
    html = html.replace(/<base[^>]*>/gi, '');
    html = html.replace(/https:\/\/waterr\.ai(\/[a-zA-Z0-9/_\-.?=&%]*)/g, '$1');
    res.status(r.status);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.removeHeader('X-Frame-Options');
    res.removeHeader('Content-Security-Policy');
    res.send(html);
  } catch (err) {
    console.error('[embed-proxy] error:', err.message);
    res.status(502).send('upstream fetch failed: ' + err.message);
  }
});

// -- Catch-all reverse proxy for SPA assets + API ------------------------------
// Any path that ISN'T one of our local Express routes gets forwarded to
// waterr.ai. This makes the iframe truly same-origin (no CORS, no X-Frame,
// nothing). The SPA can fetch anything it likes from "its own origin" and we
// just forward upstream.
const LOCAL_PATH_PREFIXES = [
  '/start', '/events', '/healthz',
  '/webhook/', '/internal/',
  '/embed/inline/',   // our proxy mount — overrides the catch-all for this exact prefix
];
const isLocal = (p) => p === '/' || p === '/index.html'
  || LOCAL_PATH_PREFIXES.some(pre => p.startsWith(pre));
app.use(async (req, res, next) => {
  if (isLocal(req.path)) return next();
  try {
    // Defensive rewrite: SPA builds /backend/scenarios/public/user/<ctx>/<id>
    // from the page's pathname, but the real upstream endpoint for embed
    // scenario data is /backend/scenarios/embed/<id>. Rewrite on the wire so
    // the SPA renders even if its pathname-derived URL is wrong.
    let upstreamPath = req.originalUrl;
    // Match any /backend/scenarios/public/user/<anything>/<UUID> shape
    // (context may be one or more path segments — we only need the trailing UUID).
    const broken = upstreamPath.match(/^\/backend\/scenarios\/public\/user\/.+?\/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})(\?.*)?$/i);
    if (broken) {
      const fixed = `/backend/scenarios/embed/${broken[1]}${broken[2] || ''}`;
      console.log('[proxy-rewrite]', upstreamPath, '→', fixed);
      upstreamPath = fixed;
    }
    const upstream = `https://waterr.ai${upstreamPath}`;
    const fwd = {
      method: req.method,
      headers: {
        ...Object.fromEntries(
          Object.entries(req.headers).filter(([k]) =>
            !['host','connection','content-length'].includes(k.toLowerCase())
          )
        ),
        host: 'waterr.ai',
      },
      redirect: 'manual',
    };
    if (!['GET','HEAD'].includes(req.method)) {
      // Re-stream body — express hasn't parsed it for these paths
      const chunks = [];
      for await (const c of req) chunks.push(c);
      let bodyBuf = chunks.length ? Buffer.concat(chunks) : null;

      // Inject a synthetic learner identity if the embed SPA calls
      // /backend/auth/end-user with an empty/incomplete body. Upstream
      // createEndUser controller throws 500 when email is undefined; we
      // fill in deterministic values so the JWT mint succeeds and the
      // embed flow can proceed.
      if (req.method === 'POST' && /^\/backend\/auth\/end-user(\?|$)/.test(upstreamPath)) {
        let parsed = {};
        try { parsed = bodyBuf ? JSON.parse(bodyBuf.toString('utf8')) : {}; } catch {}
        if (!parsed.email) parsed.email = `anon-${Date.now()}@maverick.demo`;
        if (!parsed.firstName) parsed.firstName = 'Learner';
        if (!parsed.lastName) parsed.lastName = 'Anon';
        bodyBuf = Buffer.from(JSON.stringify(parsed));
        fwd.headers['content-type'] = 'application/json';
        console.log('[proxy-inject] /backend/auth/end-user body →', parsed.email);
      }
      if (bodyBuf) fwd.body = bodyBuf;
    }
    const r = await fetch(upstream, fwd);
    console.log('[proxy]', req.method, upstreamPath, '→', r.status);
    res.status(r.status);
    r.headers.forEach((v, k) => {
      const kl = k.toLowerCase();
      if (['content-encoding','content-length','transfer-encoding','connection','x-frame-options','content-security-policy'].includes(kl)) return;
      res.setHeader(k, v);
    });
    const buf = Buffer.from(await r.arrayBuffer());
    res.end(buf);
  } catch (err) {
    console.error('[proxy] ', req.method, req.originalUrl, '→', err.message);
    res.status(502).end();
  }
});

// -- POST /start --------------------------------------------------------------
// The demo path uses Waterr's iframe-embed endpoint: /embed/inline/<scenario_id>.
// We still attempt POST /meetings first (for the proper Daily.co room + token),
// but since the meeting-ML server currently returns 500, the embed URL is the
// dependable path and is what the UI iframes.
app.post('/start', express.json(), async (req, res) => {
  try {
    if (!SCENARIO_ID) {
      return res.status(503).json({ error: 'SCENARIO_ID not set — run `node src/setup.js` first.' });
    }
    const {
      learner_name = 'Friend',
      subject      = 'math.fractions',
      contact      = '',
    } = req.body || {};
    // Route the iframe through our same-origin /embed-proxy/. Reasons:
    //   1. Some browser extensions / local firewalls reject third-party
    //      iframe loads to waterr.ai (we saw "refused to connect" in Chrome
    //      even though upstream headers and curl reachability are healthy).
    //   2. With CORS now fixed on /backend and api.waterr.ai, the SPA's
    //      cross-origin API calls work fine from this base.
    // The proxy injects <base href="https://waterr.ai/"> so all relative
    // URLs (assets + API) still resolve to Waterr upstream.
    const proxyUrl  = `/embed/inline/${SCENARIO_ID}`;   // served by our handler above
    const directUrl = `https://waterr.ai/embed/inline/${SCENARIO_ID}`;
    return res.json({
      meeting_id: null,
      embed_url:  proxyUrl,
      join_url:   proxyUrl,
      direct_url: directUrl,
      mode:       'embed-proxy',
      note_context: { learner_name, subject, contact },
    });
  } catch (err) {
    console.error('[start] error:', err.message);
    res.status(502).json({ error: err.message });
  }
});

// -- POST /webhook/waterr -----------------------------------------------------
// Must read the RAW body so we can verify HMAC. If WATERR_WEBHOOK_SECRET is
// not set yet (we haven't registered the webhook), we skip verification — the
// demo still works in dev when the webhook hits via ngrok.
app.post(
  '/webhook/waterr',
  express.raw({ type: '*/*' }),
  async (req, res) => {
    try {
      const raw = req.body instanceof Buffer ? req.body : Buffer.from(req.body || '');

      if (WATERR_WEBHOOK_SECRET) {
        const sig = String(req.headers['x-waterr-signature'] || '');
        const expected = 'sha256=' + crypto
          .createHmac('sha256', WATERR_WEBHOOK_SECRET)
          .update(raw)
          .digest('hex');
        const a = Buffer.from(sig);
        const b = Buffer.from(expected);
        if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
          console.warn('[webhook] HMAC mismatch');
          return res.status(401).end();
        }
      } else {
        console.warn('[webhook] WATERR_WEBHOOK_SECRET unset — skipping HMAC check (dev mode).');
      }

      let payload;
      try { payload = JSON.parse(raw.toString('utf8') || '{}'); }
      catch { return res.status(400).json({ error: 'invalid json' }); }

      // For now, log the payload (per orchestration: leave a TODO for kafka).
      console.log('[webhook] received event:', payload.event || '(unknown)',
        'meeting_id=', payload.meeting?.id || payload.meeting_id || '?');

      // Only forward analysis_complete to Kafka downstream.
      if (payload.event === 'session.analysis_complete') {
        const evt = toLearningEvent(payload);
        await produceLearningEvent(evt);   // stubbed; Track B will wire kafka.js
      }

      res.json({ ok: true });
    } catch (err) {
      console.error('[webhook] error:', err.message);
      res.status(500).json({ error: err.message });
    }
  }
);

// Best-effort shaping into the LearningEvent envelope from §2.1.
// The exact Waterr payload may differ; we map defensively.
function toLearningEvent(payload) {
  const meeting     = payload.meeting     || {};
  const performance = payload.performance || payload.analysis || {};
  const learner     = payload.learner     || {};

  return {
    schema_version: 1,
    event_id: `evt_${meeting.id || crypto.randomUUID()}`,
    occurred_at: payload.occurred_at || new Date().toISOString(),
    learner: {
      id: learner.id || payload.learner_id || payload.person_name || 'unknown',
      display_name: learner.display_name || payload.person_name || '',
      contact_channel: learner.contact_channel || '',
    },
    meeting: {
      id: meeting.id || payload.meeting_id || '',
      scenario_id: meeting.scenario_id || payload.scenario_id || SCENARIO_ID,
      subject: meeting.subject || payload.subject || '',
      duration_seconds: Number(meeting.duration_seconds || meeting.duration || 0),
    },
    performance: {
      average_score: Number(performance.average_score ?? performance.score ?? 0),
      goal_results: Array.isArray(performance.goal_results) ? performance.goal_results : [],
      filler_word_rate: Number(performance.filler_word_rate || 0),
      growth_areas: performance.growth_areas || '',
      strengths: performance.strengths || '',
    },
    raw_transcript_ref: `waterr://meetings/${meeting.id || ''}`,
  };
}

// -- GET /events --------------------------------------------------------------
// SSE channel. Each subscriber identifies itself with ?learner_id=<id>.
app.get('/events', (req, res) => {
  const learnerId = String(req.query.learner_id || '').trim();
  if (!learnerId) {
    return res.status(400).json({ error: 'learner_id query param required' });
  }

  res.set({
    'Content-Type':  'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection:      'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders?.();

  // greet the subscriber so the page can announce "connected" via aria-live
  sseSend(res, 'ready', { learner_id: learnerId });

  // if someone was already subscribed under this id, close the old one.
  const prev = listeners.get(learnerId);
  if (prev && prev !== res) {
    try { prev.end(); } catch { /* noop */ }
  }
  listeners.set(learnerId, res);

  // periodic comment line keeps proxies from idling the connection out
  const ping = setInterval(() => {
    try { res.write(': ping\n\n'); } catch { /* noop */ }
  }, 25000);

  req.on('close', () => {
    clearInterval(ping);
    if (listeners.get(learnerId) === res) listeners.delete(learnerId);
  });
});

// -- POST /internal/push ------------------------------------------------------
// Recommender (Track B) posts { learner_id, join_url, ... } here.
// We forward as an SSE "recommendation" event to the matching listener.
app.post('/internal/push', express.json(), (req, res) => {
  const { learner_id, join_url } = req.body || {};
  if (!learner_id || !join_url) {
    return res.status(400).json({ error: 'learner_id and join_url required' });
  }
  const target = listeners.get(String(learner_id));
  if (!target) {
    console.warn(`[push] no listener for learner_id=${learner_id}`);
    return res.status(202).json({ ok: true, delivered: false, reason: 'no_listener' });
  }
  sseSend(target, 'recommendation', { learner_id, join_url, ...req.body });
  res.json({ ok: true, delivered: true });
});

// -- POST /internal/connector-push --------------------------------------------
// Landing pad for the Confluent HTTP Sink Connector pulling
// `learner.recommendations` straight off Kafka. Same SSE fan-out as
// /internal/push but the payload is the raw recommendation envelope (no
// join_url yet — the connector forwards the prompt and the page reads it
// via aria-live while the Node deliverer races to fill in join_url on its
// own SSE event).
app.post('/internal/connector-push', express.json(), async (req, res) => {
  // HTTP Sink sends batch arrays by default; accept either shape.
  const records = Array.isArray(req.body) ? req.body : [req.body];
  let delivered = 0, skipped = 0;

  for (const rec of records) {
    try {
      const learnerId = rec?.learner_id;
      const r         = rec?.recommendation || {};
      if (!learnerId || !r.next_scenario_prompt) {
        skipped++;
        continue;
      }
      const target = listeners.get(String(learnerId));
      if (!target) {
        console.warn(`[connector-push] no listener for learner_id=${learnerId}`);
        skipped++;
        continue;
      }
      sseSend(target, 'recommendation_preview', {
        learner_id: learnerId,
        subject:    r.subject,
        difficulty: r.difficulty,
        next_scenario_prompt: r.next_scenario_prompt,
        source: 'http-sink-connector',
      });
      delivered++;
    } catch (err) {
      console.error('[connector-push] error:', err?.message || err);
      skipped++;
    }
  }
  res.json({ ok: true, delivered, skipped });
});

// ---------------------------------------------------------------------------
// boot — only listen when run directly (not when imported by Vercel)
// ---------------------------------------------------------------------------
const isMainModule = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMainModule) {
  app.listen(PORT, () => {
    console.log(`[server] Maverick AI Tutor listening on http://localhost:${PORT}`);
    console.log(`[server] scenario_id=${SCENARIO_ID || '(unset)'}`);
  });
}

export default app;
