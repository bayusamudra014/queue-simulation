// SCENARIO 1 — Service B (report generator), SYNCHRONOUS version.
// Same worker settings as the queue worker (WORKER_CONCURRENCY, WORKER_REPLICAS),
// but A calls it directly over HTTP and waits ~10 s for the answer.
// The limit is a socket limit: server.maxConnections = WORKER_CONCURRENCY. Once that many
// requests are open, Node drops new connections before reading them, A sees the connection
// refused/reset and answers the client 503. With no queue in front, that work is lost.

const os = require('os');
const express = require('express');
const { generateReport } = require('../shared/report');

const PORT = Number(process.env.PORT || 4000);
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 2);
const WORKER_ID = process.env.WORKER_ID || os.hostname();

let open = 0; // sockets open right now (= reports running)
let dropped = 0;

const app = express();
app.use(express.json());

// Note: /health needs a socket too, so it is also dropped while B is full.
app.get('/health', (req, res) =>
  res.json({ service: 'B-sync', worker: WORKER_ID, openSockets: open, maxConnections: CONCURRENCY, dropped })
);

app.post('/generate', async (req, res) => {
  // Close the socket after answering, so the slot frees right away (an idle keep-alive
  // socket would still count against maxConnections) and A's next request opens a new
  // connection, which can land on another replica.
  res.set('Connection', 'close');

  console.log(`[worker ${WORKER_ID}] picked request (open sockets ${open}/${CONCURRENCY})`);
  try {
    const result = await generateReport(req.body);
    res.json({ status: 'done', ...result, worker: WORKER_ID });
  } catch (err) {
    // e.g. the mail server is down or a simulated failure: no retry here, the user just gets an error.
    console.log(`[worker ${WORKER_ID}] failed: ${err.message}`);
    res.status(500).json({ status: 'failed', error: err.message, worker: WORKER_ID });
  }
});

const server = app.listen(PORT, () =>
  console.log(`[worker ${WORKER_ID}] B-sync listening on :${PORT} (max ${CONCURRENCY} sockets)`)
);

server.maxConnections = CONCURRENCY;
server.on('connection', (socket) => {
  open++;
  socket.on('close', () => open--);
});
// Emitted when a connection is refused because maxConnections was reached (Node 18.6+).
server.on('drop', () => {
  dropped++;
  console.log(`[worker ${WORKER_ID}] DROP connection (open sockets ${open}/${CONCURRENCY})`);
});
