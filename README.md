# Sync vs Queue — Report Generation Demo

A classroom demo showing why slow work (generating a report + emailing it, ~10 s)
should not be done inside the HTTP request, and how a message queue fixes it.

```
SCENARIO 1 — blocking                    SCENARIO 2 — queue

client ──► A ──HTTP (waits 10s)──► B     client ──► A ──add job──► Redis queue ◄── pull ── B (worker)
        ◄── 200 after 10s ──               ◄── 202 in ~10ms ──                       └─ email ──► Mailpit
```

Stack: Node.js 20+, Express, BullMQ (queue library on top of Redis), Nodemailer +
[Mailpit](https://mailpit.axllent.org/) (mock email server with a web inbox), Docker Compose.

## Project layout

| File | Role |
|---|---|
| `shared/report.js` | Simulated slow job: DB query delay (`REPORT_MS`) + `rows × ROW_MS` to build the CSV, then emails it |
| `shared/email.js` | Sends the email over SMTP to Mailpit (inbox at http://localhost:8025) |
| `sync/service-a.js` | Deployment A (sync): calls B and waits for the result; no limit of its own |
| `sync/service-b.js` | Deployment B (sync): HTTP endpoint that takes ~10 s; same `WORKER_REPLICAS` / `WORKER_CONCURRENCY` as the queue worker, enforced as a socket limit (`server.maxConnections`) |
| `queue/service-a.js` | Deployment A (queue): adds a job, returns **202 Accepted** + job id; no limit of its own |
| `queue/sse.js` | `GET /reports/:id/events`: Server-Sent Events to watch one job until the email is sent |
| `queue/worker-b.js` | Deployment B (queue): worker pulling jobs from Redis |
| `load-test.js` | Sends N requests at a steady rate (optionally with different `rows`); in queue mode listens to each job's SSE until it finishes |
| `watch-stats.js` | Live view of waiting / active / completed jobs |

### Where is the limit?
Only **B** has a concurrency limit, and it is the same in both scenarios:
`WORKER_REPLICAS` containers × `WORKER_CONCURRENCY` reports each (default 1 × 2 = **2**).
A has no limit at all. So the only thing that changes between scenarios is the architecture:

- **Sync**: B uses a socket limit, `server.maxConnections = WORKER_CONCURRENCY`. When all
  sockets are busy, Node drops new connections before reading them (a socket limit can't
  send an HTTP response). A sees the dropped connection and answers the client
  **503 Service Unavailable** (with `Retry-After`). That report is simply lost.
- **Queue**: the worker takes at most `WORKER_CONCURRENCY` jobs at a time; everything
  else waits in Redis. Nobody gets a 503, the work just finishes later.

## Run with Docker

```bash
# Scenario 1
docker compose --profile sync up --build
# in another terminal
node load-test.js localhost:3000 30 10

# stop it, then Scenario 2 (same port 3000 on your machine)
docker compose --profile sync down
docker compose --profile queue up --build
node watch-stats.js http://localhost:3000   # terminal 2
node load-test.js localhost:3000 30 10      # terminal 3
```

Both profiles also start **Mailpit**: every report email lands in the inbox at
**http://localhost:8025** (nothing is delivered to real addresses).

(`load-test.js` and `watch-stats.js` only need Node 18+, no `npm install`.)

## Classroom script

### Part 1 — the problem (blocking)
1. Send one request and let students *feel* the wait:
   `curl -X POST localhost:3000/reports -H "content-type: application/json" -d '{"email":"me@example.com"}'`
   → hangs ~10 s, then `200`.
2. Run `node load-test.js localhost:3000 30 10` (30 users, 10 per second).
3. Expected result with B = 1 container × 2 concurrency:
   ```
   status  count  avgMs
   200       2   ~10000
   503      28       ~7     (B dropped the connection: "All report workers are busy")
   ```
   (Verified in Docker with B = 2 × 2, 1 s query + 1000 rows × 1 ms: 8 × `200`, 22 × `503`.
   A saw the drops as `UND_ERR_SOCKET` / `ECONNRESET`.)
   Ask: *"B has 2 workers — why are 28 users refused instead of waiting?"*
   Because with no queue in front, B can't hold extra work: each report keeps a
   worker for ~10 s, so max throughput = 2 / 10 s = **0.2 reports per second**, and
   everything beyond that is rejected. Adding workers (`WORKER_REPLICAS=3`) only moves
   the line; a bigger spike still gets 503s.
4. Also mention: if the user closes the tab or the request times out (most proxies
   cut at 30–60 s), the work is lost and the user never knows if it succeeded.

### Part 2 — the fix (queue)
(Commands below use port 3001, the port without Docker. In Docker, queue A is on `localhost:3000`.)
1. Single request: returns instantly with `202`, a `jobId` and an `eventsUrl`.
   Watch it live with Server-Sent Events (`-N` turns off curl's buffering):
   `curl -N localhost:3001/reports/1/events`
   ```
   event: state      data: {"state":"waiting",...}
   event: active     ...
   event: progress   data: {"progress":50} ... 100
   event: completed  data: {"totalMs":11234,"queuedMs":3,...}
   ```
   The stream closes once the email is sent. (Polling `curl localhost:3001/reports/1` works too.)
   Open the inbox at http://localhost:8025: the email with the CSV attachment arrives when the job completes.
   The worker log shows how long the user really waited for the email, e.g.
   `job 7 email sent 34.2 s after the request (waited 12.0 s in queue + 22.2 s work)`.
   The same numbers are in the status endpoint as `queuedMs` and `totalMs`. In sync mode,
   A logs `email sent … s after the request` (the client waited that whole time).
2. Start `node watch-stats.js`, then run the same load test against queue A: `node load-test.js localhost:3001 30 10`.
   The load test schedules each job, then listens to its SSE stream and logs when
   the email was sent, so it only ends once every job is done.
3. Expected result (verified in Docker with `WORKER_REPLICAS=2`, `WORKER_CONCURRENCY=2`,
   `REPORT_MS=1000`, `ROW_MS=1`, and `10-2000` rows per request):
   ```
   status  count  avgMs
   202      30      10

   === Jobs (request -> email sent, via SSE) ===
   job        count  avgSec  maxSec
   completed     30    7.5    14
   ```
   Early jobs finish in ~1.5 s; later ones wait up to 11 s in the queue first. With the
   same B settings (and the default 1000 rows), sync mode gave 7 × `200` and 23 × `503`.
   And the watcher shows the queue fill up then drain at a steady pace:
   ```
   waiting= 26  active=2  completed=  7  ##########################
   waiting= 16  active=2  completed= 17  ################
   waiting=  8  active=2  completed= 25  ########
   ```
4. Key points:
   - Same B capacity as in sync, but **every** request is accepted: 30 × 202 instead of 2 × 200 + 28 × 503.
   - The queue is a **buffer**: traffic spikes are absorbed, B works at its own pace.
   - The work still takes time — the queue doesn't make it faster, it makes it
     **not block the user**.

### Part 3 — bonus demos
- **Scale workers horizontally**: set `WORKER_REPLICAS=3` in `.env` (or run
  `WORKER_REPLICAS=3 docker compose --profile queue up`). The worker has no port, so it scales freely. Re-run the load test; the queue drains 3× faster.
- **Resilience**: stop the worker mid-test (`docker compose stop worker-b`). A keeps
  answering 202, jobs pile up in `waiting`. Start it again → nothing is lost.
- **Different report sizes**: every request can send `rows` (1–100000, default `DEFAULT_REPORT_ROWS`):
  `curl -X POST localhost:3001/reports -H "content-type: application/json" -d '{"email":"me@example.com","rows":2000}'`.
  Each report takes `REPORT_MS` (the DB query, same for all) + `rows × ROW_MS` (building the rows).
  Let the load test pick a random size per request with a 4th argument:
  `node load-test.js localhost:3001 30 10 10-5000`. Big reports keep a worker
  busy longer, so watch how small ones get stuck behind them in the queue
  (and, in sync mode, how they keep B's slots busy longer → more 503s).
- **Email server down**: `docker compose stop mailpit`, then send a report.
  Sync: B fails and the user gets `500` — the report is lost. Queue with `DLQ_ENABLED=true`:
  the job fails, BullMQ retries it (`JOB_ATTEMPTS`, backoff 2 s, 4 s). Start Mailpit again
  within the retry window and the email still arrives.
- **Failures, retries and the dead-letter queue**: `SUCCESS_RATE` is the chance that one
  attempt succeeds; the others fail with a simulated DB error (after the query delay, before
  the email). Try it three ways with `SUCCESS_RATE=0.3`:
  1. **Sync**: every failure is a `500` to the user, and that report is gone.
  2. **Queue, `DLQ_ENABLED=false`** (default): 1 attempt per job. Failed jobs stay in
     BullMQ's `failed` set and the SSE stream ends with `event: failed`.
  3. **Queue, `DLQ_ENABLED=true`, `JOB_ATTEMPTS=3`**: each job gets 3 tries with backoff, so
     most now succeed (1 − 0.7³ ≈ 66 %). Jobs that fail all 3 are moved to the
     dead-letter queue `reports-dlq`. Inspect and re-run them:
     ```bash
     curl localhost:3001/dlq                           # list dead letters + failedReason
     curl -X POST localhost:3001/dlq/dead-7/retry      # re-queue one as a new job (202)
     ```
     `watch-stats.js` shows the `dlq=` count live. Discuss: why park failures instead of
     retrying forever? (poison messages, a human needs to look, don't block the queue)
- **Burst vs rate**: `node load-test.js localhost:3001 30 1000` fires
  almost everything at once. The queue version still answers 202 to all of them — the
  burst just makes the queue longer. Good discussion on why queues still need limits
  (max queue length, rate limiting) so they don't grow forever.

## Discussion questions
- How does the user find out the report is ready? (polling `statusUrl`, SSE `eventsUrl`
  as in this demo, webhooks, WebSocket, or the email itself)
- What happens if the same job runs twice? (idempotency)
- What happens to jobs that fail 3 times? (failed set / dead-letter queue)
- When is a queue overkill? (fast operations < ~1 s where the user needs the answer right away)

## Settings

With Docker, set these in [`.env`](.env) (read automatically by `docker compose`)
or override them in the shell: `WORKER_REPLICAS=3 docker compose --profile queue up`.
Without Docker, pass them as normal env vars: `WORKER_CONCURRENCY=4 npm run queue:worker`.

| Variable | Default | Used by |
|---|---|---|
| `REPORT_MS` | 10000 | B / worker — DB query delay, the same for every report |
| `ROW_MS` | 10 | B / worker — time to build one row (total = `REPORT_MS + rows × ROW_MS`) |
| `DEFAULT_REPORT_ROWS` | 100 | B / worker — rows used when the request doesn't send `rows` |
| `MAX_ROWS` | 100000 | A — largest `rows` accepted (otherwise **400**) |
| `WORKER_REPLICAS` | 1 | B / worker — number of containers (Docker only; without Docker, start more terminals) |
| `WORKER_CONCURRENCY` | 2 | B / worker — reports processed at once per container (sync B: max open sockets, extra connections dropped → 503 from A) |
| `SUCCESS_RATE` | 1 | B / worker — chance (0..1) that one attempt succeeds; the rest fail with a simulated DB error |
| `DLQ_ENABLED` | false | queue A and worker — `true` turns on retries + the dead-letter queue `reports-dlq` |
| `JOB_ATTEMPTS` | 3 | queue A — tries per job before it goes to the DLQ; only used when `DLQ_ENABLED=true` (otherwise 1) |
| `B_URL` | http://localhost:4000 | sync A |
| `REDIS_HOST` / `REDIS_PORT` | localhost / 6379 | queue A and worker |
| `SMTP_HOST` / `SMTP_PORT` | localhost / 1025 (`mailpit` / 1025 in Docker) | B / worker — mock mail server |
| `MAIL_FROM` | Reports <reports@queue-demo.local> | B / worker — sender address |
