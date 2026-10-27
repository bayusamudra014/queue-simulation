// SCENARIO 1 — Service A (API), SYNCHRONOUS version.
// A calls B over HTTP and WAITS for the report to finish before answering.
// A has no limit of its own. B limits itself with a socket limit: when all its
// sockets are busy it drops A's connection, and A answers the client 503.

const express = require('express');
const { parseRows, MAX_ROWS } = require('../shared/report');

const PORT = Number(process.env.PORT || 3000);
const B_URL = process.env.B_URL || 'http://localhost:4000';

// How fetch reports a connection that B's socket limit dropped (reset / closed before a response).
const DROPPED_CODES = new Set(['ECONNRESET', 'EPIPE', 'UND_ERR_SOCKET']);

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ service: 'A-sync' }));

app.post('/reports', async (req, res) => {
  const { email = 'student@example.com', reportType = 'sales' } = req.body || {};
  const rows = parseRows(req.body?.rows);
  if (rows === null) {
    return res.status(400).json({ error: `rows must be an integer from 1 to ${MAX_ROWS}` });
  }
  const started = Date.now();

  try {
    // Blocking call: the client waits here until B is done (or B drops the connection).
    const response = await fetch(`${B_URL}/generate`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email, reportType, rows }),
      signal: AbortSignal.timeout(60_000),
    });
    const data = await response.json();
    const waitedMs = Date.now() - started;

    if (response.ok) {
      console.log(`[A-sync] ${email}: email sent ${(waitedMs / 1000).toFixed(1)} s after the request (${rows} rows)`);
    }
    res.status(response.status).json({ mode: 'sync', waitedMs, ...data });
  } catch (err) {
    const code = err.cause?.code;
    if (DROPPED_CODES.has(code)) {
      // B is at its socket limit and dropped the connection: report it as "busy".
      console.log(`[A-sync] ${email}: B dropped the connection (${code}) -> 503`);
      res.set('Retry-After', '10');
      return res.status(503).json({
        mode: 'sync',
        error: 'Service Unavailable',
        message: 'All report workers are busy, try again later',
        detail: `B dropped the connection (${code})`,
      });
    }
    if (err.name === 'TimeoutError') {
      return res.status(504).json({ error: 'Gateway Timeout', detail: 'B took longer than 60 s' });
    }
    res.status(502).json({ error: 'Bad Gateway', detail: err.message, code });
  }
});

app.listen(PORT, () => console.log(`[A-sync] listening on :${PORT} -> B at ${B_URL}`));
