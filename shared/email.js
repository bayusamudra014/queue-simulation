// Sends the finished report by SMTP. In this demo the SMTP server is Mailpit, a mock
// mail server: it accepts every email and shows it in a web inbox (http://localhost:8025)
// instead of delivering it, so nothing ever reaches a real address.

const nodemailer = require('nodemailer');

const SMTP_HOST = process.env.SMTP_HOST || 'localhost';
const SMTP_PORT = Number(process.env.SMTP_PORT || 1025);
const MAIL_FROM = process.env.MAIL_FROM || 'Reports <reports@queue-demo.local>';

const transporter = nodemailer.createTransport({
  host: SMTP_HOST,
  port: SMTP_PORT,
  secure: false, // Mailpit speaks plain SMTP, no TLS or login
});

async function sendReportEmail({ email, reportType, rows, fileName, content }) {
  const started = Date.now();
  const info = await transporter.sendMail({
    from: MAIL_FROM,
    to: email,
    subject: `Your ${reportType} report is ready`,
    text: `Hi,\n\nYour ${reportType} report is attached (${fileName}, ${rows} rows).\n\n— queue-demo`,
    attachments: [{ filename: fileName, content, contentType: 'text/csv' }],
  });
  console.log(`[email]  sent ${fileName} to ${email} via ${SMTP_HOST}:${SMTP_PORT} in ${Date.now() - started} ms`);
  return info.messageId;
}

module.exports = { sendReportEmail, SMTP_HOST, SMTP_PORT };
