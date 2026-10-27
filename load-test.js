// Sends requests at a steady rate (like real users arriving) and summarizes status codes.
// Usage: node load-test.js <baseUrl> <totalRequests> <requestsPerSecond> [rows]
//   node load-test.js localhost:3000 30 10
// baseUrl is service A without a path; requests go to <baseUrl>/reports. "http://" is optional.
// Sync A -> expect 503s from busy B; queue A -> expect all 202.
// Queue mode: after the 202, each request listens to its job's SSE stream (eventsUrl)
// and logs when the email was sent, so the test only ends once every job finished.
// rows: report size per request. A number ("500") or a range ("10-1000", random per request).
// Omit it to use the server default (DEFAULT_REPORT_ROWS). Each row adds ROW_MS on top of the DB query.
//
// Only B has a concurrency limit (WORKER_REPLICAS x WORKER_CONCURRENCY). In sync mode,
// requests beyond that get 503 and the report is lost; in queue mode they wait their turn.

const baseArg = process.argv[2] || 'localhost:3000';
const base = /^https?:\/\//.test(baseArg) ? baseArg : `http://${baseArg}`;
const url = new URL('/reports', base).href;
const total = Number(process.argv[3] || 30);
const rate = Number(process.argv[4] || 10);
const rowsArg = process.argv[5];

// Returns the row count for one request, or undefined to let the server pick.
function pickRows() {
  if (!rowsArg) return undefined;
  const [min, max = min] = rowsArg.split('-').map(Number);
  return min + Math.floor(Math.random() * (max - min + 1));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const secs = (ms) => `${(ms / 1000).toFixed(1)} s`;

// Listens to a job's Server-Sent Events until it completes or fails.
// Resolves with the final event: { event: 'completed' | 'failed', data }.
async function watchJob(eventsUrl) {
  const res = await fetch(eventsUrl, { headers: { accept: 'text/event-stream' } });
  if (!res.ok) throw new Error(`SSE ${res.status}`);

  const decoder = new TextDecoder();
  let buffer = '';
  for await (const chunk of res.body) {
    buffer += decoder.decode(chunk, { stream: true });
    // Events are separated by a blank line; lines starting with ':' are heartbeats.
    let end;
    while ((end = buffer.indexOf('\n\n')) !== -1) {
      const block = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      const event = block.match(/^event: (.*)$/m)?.[1];
      const data = block.match(/^data: (.*)$/m)?.[1];
      if (event === 'completed' || event === 'failed') {
        res.body.cancel().catch(() => {});
        return { event, data: JSON.parse(data) };
      }
    }
  }
  throw new Error('SSE stream closed before the job finished');
}
const t0 = Date.now();

async function one(i) {
  const started = Date.now();
  const at = ((started - t0) / 1000).toFixed(1).padStart(5);
  const rows = pickRows();
  const rowsLabel = `rows=${rows ?? 'default'}`.padEnd(13);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email: `user${i}@example.com`, reportType: 'sales', rows }),
    });
    const body = await res.json().catch(() => ({}));
    const ms = Date.now() - started;
    const extra = body.jobId ? `jobId=${body.jobId}` : body.error || body.status || '';
    console.log(`#${String(i).padStart(3)}  sent at ${at}s  ${rowsLabel}->  ${res.status}  in ${String(ms).padStart(6)} ms  ${extra}`);
    if (!body.eventsUrl) return { status: res.status, ms };

    // Queue mode: the job was only scheduled. Wait on SSE until the email is sent.
    const eventsUrl = new URL(body.eventsUrl, url).href;
    let event, data;
    try {
      ({ event, data } = await watchJob(eventsUrl));
    } catch (err) {
      console.log(`#${String(i).padStart(3)}  job ${body.jobId} lost track: ${err.message}`);
      return { status: res.status, ms, job: 'sse-error', doneMs: Date.now() - started };
    }
    const doneMs = Date.now() - started;
    const detail = event === 'completed'
      ? `waited ${secs(data.queuedMs)} in queue, attempts ${data.attemptsMade}`
      : data.failedReason;
    console.log(`#${String(i).padStart(3)}  job ${data.jobId} ${event}: email ${event === 'completed' ? 'sent' : 'NOT sent'} ${secs(doneMs)} after the request (${detail})`);
    return { status: res.status, ms, job: event, doneMs };
  } catch (err) {
    const ms = Date.now() - started;
    console.log(`#${String(i).padStart(3)}  sent at ${at}s  ${rowsLabel}->  ERR  in ${String(ms).padStart(6)} ms  ${err.message}`);
    return { status: 'ERR', ms };
  }
}

(async () => {
  console.log(`Sending ${total} POST requests to ${url} at ${rate} req/s, rows=${rowsArg || 'default'}\n`);

  const pending = [];
  for (let i = 1; i <= total; i++) {
    pending.push(one(i));
    await sleep(1000 / rate);
  }
  const results = await Promise.all(pending);

  const byStatus = {};
  for (const r of results) {
    byStatus[r.status] ??= { count: 0, totalMs: 0, maxMs: 0 };
    byStatus[r.status].count++;
    byStatus[r.status].totalMs += r.ms;
    byStatus[r.status].maxMs = Math.max(byStatus[r.status].maxMs, r.ms);
  }

  console.log('\n=== Summary ===');
  console.table(
    Object.entries(byStatus).map(([status, s]) => ({
      status,
      count: s.count,
      avgMs: Math.round(s.totalMs / s.count),
      maxMs: s.maxMs,
    }))
  );

  const jobs = results.filter((r) => r.job);
  if (jobs.length) {
    const byJob = {};
    for (const r of jobs) {
      byJob[r.job] ??= { count: 0, totalMs: 0, maxMs: 0 };
      byJob[r.job].count++;
      byJob[r.job].totalMs += r.doneMs;
      byJob[r.job].maxMs = Math.max(byJob[r.job].maxMs, r.doneMs);
    }
    console.log('\n=== Jobs (request -> email sent, via SSE) ===');
    console.table(
      Object.entries(byJob).map(([job, s]) => ({
        job,
        count: s.count,
        avgSec: +(s.totalMs / s.count / 1000).toFixed(1),
        maxSec: +(s.maxMs / 1000).toFixed(1),
      }))
    );
  }
  console.log(`Wall time: ${Date.now() - t0} ms`);
})();
