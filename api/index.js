// Vercel serverless entry. Wraps the existing Express app so every request
// (proxy, /start, /webhook, static) is handled by one function. Long-lived
// Kafka consumers (recommender.js) are NOT deployed here — keep those on a
// host that allows persistent processes.
import app from '../src/server.js';

export default app;
export const config = {
  // Static assets in /public are served by Vercel directly (see vercel.json).
  // This function only handles dynamic routes.
  maxDuration: 30, // sec; bump on Pro tier if proxied scenarios take longer.
};
