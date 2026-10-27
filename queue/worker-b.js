// SCENARIO 2 — Service B (report generator), QUEUE version.
// No HTTP server: B pulls jobs from the queue at its own pace.
// Scale it by running more copies (docker compose up --scale worker-b=3).

const os = require('os');
const { Worker, Queue } = require('bullmq');
const { generateReport } = require('../shared/report');
const { QUEUE_NAME, DLQ_NAME, DLQ_ENABLED, connection } = require('./connection');

const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 2);
const WORKER_ID = process.env.WORKER_ID || os.hostname();

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    console.log(`[worker ${WORKER_ID}] picked job ${job.id}`);
    const result = await generateReport(job.data, (pct) => job.updateProgress(pct));

    // job.timestamp = when A added the job, job.processedOn = when this worker picked it up.
    const queuedMs = job.processedOn - job.timestamp;
    const totalMs = Date.now() - job.timestamp;
    console.log(
      `[worker ${WORKER_ID}] job ${job.id} email sent ${(totalMs / 1000).toFixed(1)} s after the request ` +
        `(waited ${(queuedMs / 1000).toFixed(1)} s in queue + ${(result.tookMs / 1000).toFixed(1)} s work)`
    );
    return { ...result, worker: WORKER_ID, queuedMs, totalMs };
  },
  { connection, concurrency: CONCURRENCY }
);

const dlq = DLQ_ENABLED ? new Queue(DLQ_NAME, { connection }) : null;

worker.on('completed', (job) => console.log(`[worker ${WORKER_ID}] job ${job.id} completed`));
worker.on('failed', async (job, err) => {
  if (!job) return;
  const attempts = job.opts.attempts ?? 1;
  // 'failed' fires on every attempt; only the last one leaves the job in the failed state.
  if ((await job.getState()) !== 'failed') {
    console.log(`[worker ${WORKER_ID}] job ${job.id} attempt ${job.attemptsMade}/${attempts} failed: ${err.message} -> will retry`);
    return;
  }
  console.log(`[worker ${WORKER_ID}] job ${job.id} FAILED after ${job.attemptsMade}/${attempts} attempts: ${err.message}`);
  if (!dlq) return;

  // Park a copy in the dead-letter queue so it can be inspected and retried later.
  // (BullMQ does not allow purely numeric custom ids, hence the "dead-" prefix.)
  await dlq.add(
    'dead-report',
    { ...job.data, originalJobId: job.id, failedReason: err.message, attemptsMade: job.attemptsMade, failedAt: Date.now() },
    { jobId: `dead-${job.id}` }
  );
  console.log(`[worker ${WORKER_ID}] job ${job.id} moved to dead-letter queue "${DLQ_NAME}" as dead-${job.id}`);
});

console.log(
  `[worker ${WORKER_ID}] waiting for jobs on "${QUEUE_NAME}" (concurrency ${CONCURRENCY}, ` +
    `dead-letter queue ${DLQ_ENABLED ? `"${DLQ_NAME}"` : 'off'})`
);

// Graceful shutdown: finish current jobs before exiting.
const shutdown = async () => {
  console.log(`[worker ${WORKER_ID}] shutting down...`);
  await worker.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
