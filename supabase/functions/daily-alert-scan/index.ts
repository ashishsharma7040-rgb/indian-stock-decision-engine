const BACKEND_URL = Deno.env.get("STOCK_ENGINE_BACKEND_URL") ?? "https://indian-stock-decision-engine-api.onrender.com";
const TELEGRAM_BOT_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN");
const TELEGRAM_CHAT_ID = Deno.env.get("TELEGRAM_CHAT_ID");

async function sendTelegram(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return { skipped: true };
  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text: message,
      parse_mode: "HTML",
      disable_web_page_preview: true,
    }),
  });
  return response.json();
}

Deno.serve(async () => {
  const response = await fetch(`${BACKEND_URL}/api/scheduled/daily`);
  const payload = await response.json();
  const alerts = payload?.alert_scan?.alerts ?? [];
  if (!alerts.length) {
    return Response.json({ ok: true, alerts: 0, message: "No triggered watchlist alerts" });
  }

  const message = alerts.map((alert: Record<string, unknown>) => {
    return [
      `<b>${alert.symbol}</b> ${alert.state}`,
      `Price: ${alert.price}`,
      `Breakout: ${alert.breakout_level}`,
      `Stop: ${alert.stop}`,
    ].join("\n");
  }).join("\n\n");

  const telegram = await sendTelegram(message);
  return Response.json({ ok: true, alerts: alerts.length, telegram });
});
