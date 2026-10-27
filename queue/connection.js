// Shared Redis connection settings and queue names for A and the worker.

// Dead-letter queue (off by default): jobs that failed every attempt are parked there for a
// human to inspect and retry. Nothing consumes it automatically.
// Off: each job gets one attempt and a failure is final. On: JOB_ATTEMPTS tries, then the DLQ.
const DLQ_ENABLED = process.env.DLQ_ENABLED === 'true';

module.exports = {
  QUEUE_NAME: 'reports',
  DLQ_NAME: 'reports-dlq',
  DLQ_ENABLED,
  JOB_ATTEMPTS: DLQ_ENABLED ? Number(process.env.JOB_ATTEMPTS || 3) : 1,
  connection: {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT || 6379),
    maxRetriesPerRequest: null, // required by BullMQ workers
  },
};
