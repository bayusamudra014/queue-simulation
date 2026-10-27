// SCENARIO 2 — Service A (API), QUEUE version.
// A only puts a job on the queue and answers immediately with 202 Accepted.
// Only B (the worker) has a concurrency limit; extra jobs simply wait in the queue.
// The client can then watch its job live over SSE until the email is sent.

const express = require('express');
const { Queue } = require('bullmq');
const { parseRows, MAX_ROWS } = require('../shared/report');
const { QUEUE_NAME, DLQ_NAME, DLQ_ENABLED, JOB_ATTEMPTS, connection } = require('./connection');
const { addSseRoute } = require('./sse');

const PORT = Number(process.env.PORT || 3001);

const reportQueue = new Queue(QUEUE_NAME, { connection });
const dlq = new Queue(DLQ_NAME, { connection });

// Adds a report job; used for new requests and for retrying a dead letter.
const addReportJob = (data) =>
  reportQueue.add('generate-report', data, {
    attempts: JOB_ATTEMPTS, // 1 unless the dead-letter queue is enabled
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 1000 },
    removeOnFail: { count: 1000 },
  });

const accepted = (job, message) => ({
  mode: 'queue',
  jobId: job.id,
  status: 'queued',
  statusUrl: `/reports/${job.id}`,
  eventsUrl: `/reports/${job.id}/events`, // Server-Sent Events until the email is sent
  message,
});

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ service: 'A-queue' }));

// 1) Submit a report request -> returns right away with a job id.
app.post('/reports', async (req, res) => {
  const { email = 'student@example.com', reportType = 'sales' } = req.body || {};
  const rows = parseRows(req.body?.rows);
  if (rows === null) {
    return res.status(400).json({ error: `rows must be an integer from 1 to ${MAX_ROWS}` });
  }

  const job = await addReportJob({ email, reportType, rows });
  res.status(202).json(accepted(job, 'Report is being generated, it will be emailed to you.'));
});

// 2) Poll the status of one job.
app.get('/reports/:id', async (req, res) => {
  const job = await reportQueue.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });

  res.json({
    jobId: job.id,
    state: await job.getState(), // waiting | active | completed | failed | delayed
    progress: job.progress,
    attemptsMade: job.attemptsMade,
    queuedMs: job.processedOn ? job.processedOn - job.timestamp : null, // time waiting before a worker picked it
    totalMs: job.finishedOn ? job.finishedOn - job.timestamp : null, // request -> email sent (or failed)
    result: job.returnvalue ?? null,
    failedReason: job.failedReason ?? null,
  });
});

// 3) Watch one job live with Server-Sent Events (see queue/sse.js).
addSseRoute(app, reportQueue);

// 4) Queue overview — nice to show on the projector while the load test runs.
app.get('/stats', async (req, res) => {
  const counts = await reportQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
  const dead = DLQ_ENABLED ? await dlq.getWaitingCount() : null;
  res.json({ queue: QUEUE_NAME, ...counts, dlq: dead });
});

// 5) Dead-letter queue: list jobs that failed every attempt, and send one back for another try.
app.get('/dlq', async (req, res) => {
  if (!DLQ_ENABLED) return res.status(404).json({ error: 'Dead-letter queue is disabled (DLQ_ENABLED=false)' });
  const jobs = await dlq.getWaiting(0, 99);
  res.json({
    queue: DLQ_NAME,
    count: await dlq.getWaitingCount(),
    jobs: jobs.map((j) => ({
      id: j.id,
      email: j.data.email,
      rows: j.data.rows,
      failedReason: j.data.failedReason,
      attemptsMade: j.data.attemptsMade,
      failedAt: new Date(j.data.failedAt).toISOString(),
      retryUrl: `/dlq/${j.id}/retry`,
    })),
  });
});

app.post('/dlq/:id/retry', async (req, res) => {
  if (!DLQ_ENABLED) return res.status(404).json({ error: 'Dead-letter queue is disabled (DLQ_ENABLED=false)' });
  const dead = await dlq.getJob(req.params.id);
  if (!dead) return res.status(404).json({ error: 'Dead letter not found' });

  const { email, reportType, rows } = dead.data;
  const job = await addReportJob({ email, reportType, rows });
  await dead.remove();
  console.log(`[A-queue] dead letter ${dead.id} retried as job ${job.id}`);
  res.status(202).json(accepted(job, `Retrying dead letter ${dead.id} as a new job.`));
});

app.listen(PORT, () =>
  console.log(
    `[A-queue] listening on :${PORT}, Redis ${connection.host}:${connection.port}, ` +
      (DLQ_ENABLED ? `dead-letter queue on (${JOB_ATTEMPTS} attempts per job)` : 'dead-letter queue off (1 attempt per job)')
  )
);
