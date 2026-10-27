// Prints the queue status every second so the class can watch the worker drain the queue.
// Usage: node watch-stats.js [baseUrl]   (default http://localhost:3001)

const base = process.argv[2] || 'http://localhost:3001';

setInterval(async () => {
  try {
    const s = await (await fetch(`${base}/stats`)).json();
    const bar = '#'.repeat(Math.min(s.waiting, 60));
    console.log(
      `${new Date().toLocaleTimeString()}  waiting=${String(s.waiting).padStart(3)}  active=${s.active}  ` +
        `completed=${String(s.completed).padStart(3)}  failed=${s.failed}  ` +
        (s.dlq === null ? '' : `dlq=${s.dlq}  `) +
        bar
    );
  } catch (err) {
    console.log(`cannot reach ${base}/stats: ${err.message}`);
  }
}, 1000);
