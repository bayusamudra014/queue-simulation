// Simulates the slow part: building a report document and emailing it.
// The "work" is just waiting; the email is really sent, to the Mailpit mock server.

const { sendReportEmail } = require('./email');

const REPORT_MS = Number(process.env.REPORT_MS || 10_000); // DB query delay, same for every report
const ROW_MS = Number(process.env.ROW_MS || 10); // time to build one row of the report
const DEFAULT_REPORT_ROWS = Number(process.env.DEFAULT_REPORT_ROWS || 100); // default rows when the request has none
const MAX_ROWS = Number(process.env.MAX_ROWS || 100_000);
// Chance (0..1) that one attempt succeeds. Below 1, some attempts fail on purpose
// to show retries (queue), lost work (sync) and the dead-letter queue.
const SUCCESS_RATE = Math.min(1, Math.max(0, Number(process.env.SUCCESS_RATE ?? 1)));

/** Returns a valid row count from the request, or null if it's invalid. */
function parseRows(value) {
  if (value === undefined || value === null || value === '') return DEFAULT_REPORT_ROWS;
  const rows = Number(value);
  return Number.isInteger(rows) && rows >= 1 && rows <= MAX_ROWS ? rows : null;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * @param {{ email: string, reportType: string, rows?: number }} input
 * @param {(percent: number) => Promise<void> | void} [onProgress]
 */
async function generateReport({ email, reportType, rows = DEFAULT_REPORT_ROWS }, onProgress = () => {}) {
  const started = Date.now();
  const buildMs = rows * ROW_MS;
  console.log(`[report] START  ${reportType} for ${email} (query ${REPORT_MS} ms + ${rows} rows x ${ROW_MS} ms)`);

  // 1) Query the DB: fixed delay, whatever the report size. Progress 0 -> 50%.
  await sleep(REPORT_MS);
  if (Math.random() >= SUCCESS_RATE) {
    console.log(`[report] FAIL   ${reportType} for ${email}: simulated DB error (SUCCESS_RATE=${SUCCESS_RATE})`);
    throw new Error(`Simulated DB error (SUCCESS_RATE=${SUCCESS_RATE})`);
  }
  await onProgress(50);

  // 2) Build the rows: grows with the report size. Progress 50 -> 100% in 5 steps.
  const steps = 5;
  for (let i = 1; i <= steps; i++) {
    await sleep(buildMs / steps); // pretend: format rows, render charts...
    await onProgress(50 + i * 10);
  }

  const fileName = `report-${reportType}-${Date.now()}.csv`;
  const content = buildCsv(reportType, rows);
  const messageId = await sendReportEmail({ email, reportType, rows, fileName, content });
  console.log(`[report] DONE   ${reportType} for ${email}: email sent ${Date.now() - started} ms after B started`);

  return { fileName, email, reportType, rows, messageId, tookMs: Date.now() - started };
}

// Fake report data so the email has a real attachment to open: one line per day.
function buildCsv(reportType, rows) {
  const lines = ['date,' + reportType];
  const day = new Date('2026-01-01');
  for (let i = 0; i < rows; i++) {
    lines.push(`${day.toISOString().slice(0, 10)},${Math.round(Math.random() * 1000)}`);
    day.setDate(day.getDate() + 1);
  }
  return lines.join('\n') + '\n';
}

module.exports = { generateReport, parseRows, REPORT_MS, ROW_MS, DEFAULT_REPORT_ROWS, MAX_ROWS, SUCCESS_RATE };
