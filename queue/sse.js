// Server-Sent Events: lets a client watch one job live instead of polling GET /reports/:id.
//   GET /reports/:id/events  ->  event: state | active | progress | completed | failed
// Each watcher keeps its connection open until the job finishes (or fails for good).

const { QueueEvents } = require('bullmq');
const { QUEUE_NAME, connection } = require('./connection');

const HEARTBEAT_MS = 15_000; // keeps proxies and clients from closing an idle stream

function addSseRoute(app, reportQueue) {
  // One Redis subscription for the whole server, fanned out to watchers by job id.
  const queueEvents = new QueueEvents(QUEUE_NAME, { connection });
  const watchers = new Map(); // jobId -> Set of (eventName, payload) => void

  const notify = (jobId, event, payload) => {
    for (const send of watchers.get(String(jobId)) ?? []) send(event, payload);
  };
  queueEvents.on('active', ({ jobId }) => notify(jobId, 'active', { jobId }));
  queueEvents.on('progress', ({ jobId, data }) => notify(jobId, 'progress', { jobId, progress: data }));
  queueEvents.on('completed', ({ jobId }) => notify(jobId, 'finished', null));
  queueEvents.on('failed', ({ jobId }) => notify(jobId, 'finished', null));

  // Final event built from the stored job, so it has the same timings as GET /reports/:id.
  async function finalEvent(jobId) {
    const job = await reportQueue.getJob(jobId);
    const state = await job.getState();
    if (state !== 'completed' && state !== 'failed') return null; // e.g. failed but will retry
    return {
      event: state,
      payload: {
        jobId: job.id,
        state,
        attemptsMade: job.attemptsMade,
        queuedMs: job.processedOn - job.timestamp,
        totalMs: job.finishedOn - job.timestamp, // request -> email sent (or failed)
        result: job.returnvalue ?? null,
        failedReason: job.failedReason || null,
      },
    };
  }

  app.get('/reports/:id/events', async (req, res) => {
    const jobId = req.params.id;
    if (!(await reportQueue.getJob(jobId))) return res.status(404).json({ error: 'Job not found' });

    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });
    const write = (event, payload) => res.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);

    let closed = false;
    const close = () => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      watchers.get(jobId)?.delete(send);
      if (watchers.get(jobId)?.size === 0) watchers.delete(jobId);
      res.end();
    };

    const finishIfDone = async () => {
      const final = await finalEvent(jobId);
      if (!final || closed) return;
      write(final.event, final.payload);
      console.log(`[A-queue] SSE: job ${jobId} ${final.event}, ${(final.payload.totalMs / 1000).toFixed(1)} s after the request`);
      close();
    };

    const send = (event, payload) => (event === 'finished' ? finishIfDone() : write(event, payload));

    // Subscribe BEFORE reading the current state, so a job finishing in between is not missed.
    if (!watchers.has(jobId)) watchers.set(jobId, new Set());
    watchers.get(jobId).add(send);
    const heartbeat = setInterval(() => res.write(': ping\n\n'), HEARTBEAT_MS);
    req.on('close', close);

    const job = await reportQueue.getJob(jobId);
    write('state', { jobId, state: await job.getState(), progress: job.progress });
    await finishIfDone(); // already finished before the client connected
  });
}

module.exports = { addSseRoute };
