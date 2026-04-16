const API_BASE_KEY = "stockEngineApiBase";
const LOCAL_API_BASE = "http://127.0.0.1:8000";
const DEPLOYED_API_BASE = window.STOCK_ENGINE_API_BASE || "https://indian-stock-decision-engine-api.onrender.com";
const isLocalPage = ["", "localhost", "127.0.0.1"].includes(window.location.hostname) || window.location.protocol === "file:";
let apiBase = localStorage.getItem(API_BASE_KEY) || (isLocalPage ? LOCAL_API_BASE : DEPLOYED_API_BASE);
let dashboard = null;
let selectedStock = null;
let activeTab = "overview";
let activeFilter = "all";

function qs(id) { return document.getElementById(id); }
function scoreClass(value) { return value >= 75 ? "score-hi" : value >= 55 ? "score-mid" : "score-lo"; }
function scoreColor(value) { return value >= 75 ? "var(--green)" : value >= 55 ? "var(--amber)" : "var(--red)"; }
function money(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "NA";
  return "Rs " + Number(value).toLocaleString("en-IN", {maximumFractionDigits: 2});
}
function zone(value) {
  if (!value) return "Wait";
  if (Array.isArray(value)) return `${money(value[0])} - ${money(value[1])}`;
  return String(value);
}
function percent(value) {
  if (value === null || value === undefined) return "NA";
  const clean = Number(value);
  return `${clean > 0 ? "+" : ""}${clean.toFixed(2)}%`;
}
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[char]));
}
function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
}

async function getJson(path, options = {}) {
  const response = await fetch(`${apiBase}${path}`, options);
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

async function loadDashboard() {
  try {
    dashboard = await getJson("/api/dashboard");
    qs("offlineNotice").classList.add("hidden");
  } catch (error) {
    dashboard = DEMO_DASHBOARD;
    qs("offlineNotice").textContent = `Backend not running at ${apiBase}. Demo mode is active. Start FastAPI for live scoring and API refresh.`;
    qs("offlineNotice").classList.remove("hidden");
  }
  renderAll();
}

function renderAll() {
  renderMarketStrip();
  renderStockList();
  renderHome();
  if (selectedStock) renderStockView(selectedStock);
}

function renderMarketStrip() {
  const market = dashboard.market_regime || {};
  const chips = [
    ["Regime", market.regime || "NA"],
    ["Market score", `${market.score ?? "NA"}/100`],
    ["Nifty", money(market.nifty_close || 0)],
    ["Breadth", `${market.breadth_above_50dma ?? "NA"}% > 50DMA`],
    ["VIX", market.vix ?? "NA"]
  ];
  qs("marketStrip").innerHTML = chips.map(([label, value]) => `
    <div class="market-chip">
      <div class="chip-label">${label}</div>
      <div class="chip-value">${value}</div>
    </div>`).join("");
}

function filteredStocks() {
  const text = qs("searchInput").value.trim().toLowerCase();
  let rows = [...(dashboard.stocks || [])];
  if (activeFilter === "weekly") rows.sort((a,b) => b.weekly_score - a.weekly_score);
  if (activeFilter === "monthly") rows.sort((a,b) => b.monthly_score - a.monthly_score);
  if (activeFilter === "candidate") rows = rows.filter(s => s.candidate);
  if (activeFilter === "avoid") rows = rows.filter(s => s.conviction === "Avoid" || s.risk_score >= 18);
  if (text) rows = rows.filter(s => `${s.symbol} ${s.name} ${s.sector} ${s.industry}`.toLowerCase().includes(text));
  return rows;
}

function renderStockList() {
  const rows = filteredStocks();
  qs("universeCount").textContent = `${dashboard.stocks.length} names`;
  qs("stockList").innerHTML = rows.map(stock => `
    <button class="stock-row ${selectedStock?.symbol === stock.symbol ? "active" : ""}" data-symbol="${stock.symbol}">
      <div>
        <div class="stock-symbol">${stock.symbol}</div>
        <div class="stock-meta">${stock.name} | ${stock.industry || stock.sector}</div>
        <div class="stock-meta">W ${stock.weekly_score} | M ${stock.monthly_score} | Risk ${stock.risk_score}</div>
      </div>
      <div class="score-badge ${scoreClass(stock.weekly_score)}">${stock.weekly_score}</div>
    </button>`).join("");
}

function candidateRows(rows, scoreKey) {
  return rows.map(stock => `
    <button class="candidate-row" data-symbol="${stock.symbol}">
      <div>
        <strong>${stock.symbol}</strong>
        <div class="subtle">${stock.name} | ${stock.sector}</div>
      </div>
      <span class="score-badge ${scoreClass(stock[scoreKey])}">${stock[scoreKey]}</span>
    </button>`).join("");
}

function renderHome() {
  const market = dashboard.market_regime;
  qs("stockView").classList.add("hidden");
  qs("homeView").classList.remove("hidden");
  qs("homeView").innerHTML = `
    <div class="home-head">
      <div>
        <h1>Decision desk</h1>
        <div class="status-line">Universe -> clean data -> scoring -> market filter -> entry plan -> risk -> review</div>
      </div>
      <div class="flat-panel">
        <span class="pill ${market.regime === "Risk-on" ? "green" : market.regime === "Risk-off" ? "red" : "amber"}">${market.regime}</span>
        <div class="status-line">Updated ${new Date(dashboard.generated_at).toLocaleString("en-IN")}</div>
      </div>
    </div>
    <div class="grid three">
      <section class="panel">
        <div class="section-kicker">Top 3 weekly</div>
        ${candidateRows(dashboard.top_weekly_candidates, "weekly_score")}
      </section>
      <section class="panel">
        <div class="section-kicker">Top 3 monthly</div>
        ${candidateRows(dashboard.top_monthly_candidates, "monthly_score")}
      </section>
      <section class="panel">
        <div class="section-kicker">Market support</div>
        ${metric("Nifty trend", market.nifty_close > market.nifty_ema50 ? "Above 50 EMA" : "Below 50 EMA")}
        ${metric("200 DMA filter", market.nifty_close > market.nifty_dma200 ? "Positive" : "Weak")}
        ${metric("Breadth", `${market.breadth_above_50dma}%`)}
        ${metric("VIX", String(market.vix))}
        <div class="bar"><span style="width:${market.score}%;background:${scoreColor(market.score)}"></span></div>
      </section>
    </div>
    <div class="grid two" style="margin-top:14px">
      <section class="panel">
        <div class="section-kicker">Top sectors</div>
        ${(dashboard.top_sectors || []).map(row => `
          <div class="metric">
            <span>${row.sector} | leader ${row.leader}</span>
            <strong>${row.avg_weekly_score}</strong>
          </div>`).join("")}
      </section>
      <section class="panel">
        <div class="section-kicker">Avoid and watch tightly</div>
        ${(dashboard.avoid_list || []).map(stock => `
          <button class="candidate-row" data-symbol="${stock.symbol}">
            <div>
              <strong>${stock.symbol}</strong>
              <div class="subtle">${stock.risk_flags?.join(", ") || stock.conviction}</div>
            </div>
            <span class="pill ${stock.risk_score >= 18 ? "red" : "amber"}">Risk ${stock.risk_score}</span>
          </button>`).join("") || "<div class='status-line'>No high-risk names in this universe.</div>"}
      </section>
    </div>
    <section class="panel" style="margin-top:14px">
      <div class="section-kicker">Latest critical events</div>
      ${(dashboard.latest_critical_events || []).map(eventHtml).join("") || "<div class='status-line'>No critical events loaded.</div>"}
    </section>
    <section class="flat-panel" style="margin-top:14px">
      <strong>Rule:</strong> final score = business quality + sector tailwind + event strength + technical strength + market support - risk penalties.
      <div class="status-line">${dashboard.disclaimer || ""}</div>
    </section>`;
}

async function selectStock(symbol) {
  const summary = dashboard.stocks.find(s => s.symbol === symbol);
  selectedStock = summary;
  activeTab = "overview";
  renderStockList();
  qs("homeView").classList.add("hidden");
  qs("stockView").classList.remove("hidden");
  renderStockView(summary);
  try {
    const detail = await getJson(`/api/stocks/${symbol}`);
    selectedStock = detail;
    renderStockView(detail);
  } catch (error) {
    selectedStock = buildFallbackDetail(summary);
    renderStockView(selectedStock);
  }
}

function buildFallbackDetail(summary) {
  const bars = [];
  let price = summary.price * 0.88;
  for (let i = 90; i >= 0; i--) {
    price *= 1 + (Math.sin(i / 7) * 0.003) + 0.004;
    bars.push({datetime: new Date(Date.now() - i * 86400000).toISOString().slice(0,10), close: Number(price.toFixed(2)), high: Number((price * 1.01).toFixed(2)), low: Number((price * 0.99).toFixed(2)), volume: 1000000 + i * 1000});
  }
  bars[bars.length - 1].close = summary.price;
  return {
    ...summary,
    business_quality: {score: summary.business_score, breakdown: {}},
    sector_tailwind: {score: summary.tailwind_score, breakdown: {}},
    event_strength: {score: summary.event_score, events: dashboard.latest_critical_events.filter(e => e.symbol === summary.symbol)},
    technical_strength: {
      score: summary.technical_score,
      indicators: {close: summary.price, ema20: summary.entry?.pullback?.[1], ema50: summary.entry?.stop, dma200: summary.entry?.stop * 0.9, rsi14: 62, atr14: Math.abs(summary.price - summary.entry.stop) / 2, volume_ratio: 1.5, breakout_level: summary.entry.breakout_level, relative_strength:{state:"Demo",pct:0}},
      checks: {},
      fake_breakout_flags: summary.risk_flags || []
    },
    market_support: dashboard.market_regime,
    risk_penalty: {score: summary.risk_score, breakdown: Object.fromEntries((summary.risk_flags || []).map(flag => [flag, 1]))},
    explanation_json: {five_questions:{good_business: summary.business_score >= 65, sector_helping_now: summary.tailwind_score >= 60, fresh_trigger: summary.event_score >= 55, chart_confirming: summary.technical_score >= 60, where_am_i_wrong_defined: summary.risk_score <= 35}, thesis:[], risk_flags: summary.risk_flags || []},
    exit_rules: {price_stop:`Close below ${summary.entry.stop}`, swing_trend_exit:"Two closes below 20 EMA", positional_trend_exit:"Close below 50 EMA", event_exit:"Exit or reduce on serious negative official event"},
    bars
  };
}

function renderStockView(stock) {
  const full = stock.business_quality ? stock : buildFallbackDetail(stock);
  const scores = [
    ["Business", full.business_quality.score],
    ["Tailwind", full.sector_tailwind.score],
    ["Events", full.event_strength.score],
    ["Technical", full.technical_strength.score],
    ["Market", full.market_support.score],
    ["Risk", full.risk_penalty.score]
  ];
  qs("stockView").classList.remove("hidden");
  qs("stockView").innerHTML = `
    <div class="stock-head">
      <div>
        <div class="section-kicker">${full.sector} | ${full.industry || ""}</div>
        <h1>${full.symbol} - ${full.name}</h1>
        <div class="status-line">Weekly ${full.weekly_score} | Monthly ${full.monthly_score} | ${full.conviction} | Candidate ${full.candidate ? "Yes" : "No"}</div>
      </div>
      <div>
        <div class="price">${money(full.price)}</div>
        <div class="status-line" style="text-align:right">${percent(full.change_pct)}</div>
      </div>
    </div>
    <div class="score-stack">
      ${scores.map(([label, value]) => `
        <div class="score-tile">
          <small>${label}</small>
          <strong style="color:${label === "Risk" ? scoreColor(100 - value) : scoreColor(value)}">${value}</strong>
          <div class="bar"><span style="width:${value}%;background:${label === "Risk" ? "var(--red)" : scoreColor(value)}"></span></div>
        </div>`).join("")}
    </div>
    <div class="tabs">
      ${["overview","fundamentals","events","technical","entry","chart","thesis","data"].map(tab => `<button class="tab ${activeTab === tab ? "active" : ""}" data-tab="${tab}">${tabLabel(tab)}</button>`).join("")}
      <button class="btn blue" data-refresh="${full.symbol}">Refresh live</button>
    </div>
    <div id="tabBody">${renderTab(full)}</div>`;
  if (activeTab === "chart") setTimeout(() => drawChart(full.bars || []), 0);
}

function tabLabel(tab) {
  return {overview:"Overview",fundamentals:"Fundamentals",events:"Events",technical:"Technical",entry:"Entry / Exit",chart:"Chart",thesis:"Thesis Tracker",data:"Free APIs"}[tab];
}

function renderTab(stock) {
  if (activeTab === "overview") return renderOverview(stock);
  if (activeTab === "fundamentals") return renderFundamentals(stock);
  if (activeTab === "events") return renderEvents(stock);
  if (activeTab === "technical") return renderTechnical(stock);
  if (activeTab === "entry") return renderEntry(stock);
  if (activeTab === "chart") return `<div class="chart-wrap"><canvas id="priceChart"></canvas></div>`;
  if (activeTab === "thesis") return renderThesis(stock);
  if (activeTab === "data") return renderApiStack();
  return "";
}

function renderOverview(stock) {
  const questions = stock.explanation_json?.five_questions || {};
  return `
    <div class="grid two">
      <section class="panel">
        <div class="section-kicker">Five-question gate</div>
        ${check("Is this a good business?", questions.good_business)}
        ${check("Is the sector helping it now?", questions.sector_helping_now)}
        ${check("Is there a fresh trigger?", questions.fresh_trigger)}
        ${check("Is the chart confirming?", questions.chart_confirming)}
        ${check("Where am I wrong?", questions.where_am_i_wrong_defined)}
      </section>
      <section class="panel">
        <div class="section-kicker">Thesis</div>
        ${(stock.explanation_json?.thesis || []).map(line => `<div class="check-row"><span class="dot ok"></span><div>${escapeHtml(line)}</div></div>`).join("") || "<div class='status-line'>Start the backend for full thesis lines.</div>"}
      </section>
    </div>
    <div class="grid two" style="margin-top:14px">
      <section class="panel">
        <div class="section-kicker">Sector tailwind</div>
        ${(stock.tailwind_factors || []).map(line => `<div class="check-row"><span class="dot ok"></span><div>${escapeHtml(line)}</div></div>`).join("") || "<div class='status-line'>No sector notes loaded.</div>"}
      </section>
      <section class="panel">
        <div class="section-kicker">Where I am wrong</div>
        ${(stock.explanation_json?.risk_flags || []).map(flag => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(flag)}</div></div>`).join("") || "<div class='status-line'>No active rule-based risk flag.</div>"}
      </section>
    </div>`;
}

function check(label, ok) {
  return `<div class="check-row"><span class="dot ${ok ? "ok" : "bad"}"></span><div><strong>${label}</strong><div class="subtle">${ok ? "Pass" : "Not aligned yet"}</div></div></div>`;
}

function renderFundamentals(stock) {
  const f = stock.fundamentals || {};
  const breakdown = stock.business_quality?.breakdown || {};
  return `
    <div class="grid two">
      <section class="panel">
        <div class="section-kicker">Business quality inputs</div>
        ${metric("Sales CAGR", `${f.sales_cagr ?? "NA"}%`)}
        ${metric("Profit CAGR", `${f.profit_cagr ?? "NA"}%`)}
        ${metric("ROCE", `${f.roce ?? "NA"}%`)}
        ${metric("ROE", `${f.roe ?? "NA"}%`)}
        ${metric("Debt / Equity", f.debt_equity ?? "NA")}
        ${metric("CFO / PAT", f.cfo_pat ?? "NA")}
        ${metric("FCF trend", f.fcf_trend ?? "NA")}
        ${metric("Pledge", `${f.pledge_percent ?? "NA"}%`)}
        ${metric("PE", f.pe ?? "NA")}
      </section>
      <section class="panel">
        <div class="section-kicker">Score breakdown</div>
        ${Object.keys(breakdown).map(key => breakdownRow(key, breakdown[key])).join("") || "<div class='status-line'>Backend scoring breakdown not loaded in demo mode.</div>"}
      </section>
    </div>`;
}

function breakdownRow(key, row) {
  return `<div style="margin:11px 0">
    <div class="metric"><span>${key.replaceAll("_"," ")}</span><strong>${row.points}/${row.weight}</strong></div>
    <div class="bar"><span style="width:${row.score}%;background:${scoreColor(row.score)}"></span></div>
  </div>`;
}

function renderEvents(stock) {
  const events = stock.event_strength?.events || [];
  return `<section class="panel">
    <div class="section-kicker">Event strength = sentiment x freshness x reliability x importance</div>
    ${events.map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>"}
  </section>`;
}

function eventHtml(event) {
  const sentiment = Number(event.sentiment || 0);
  const type = sentiment > 0.1 ? "pos" : sentiment < -0.1 ? "neg" : "neu";
  return `<div class="event ${type}">
    <div style="display:flex;justify-content:space-between;gap:12px">
      <div>
        <strong>${escapeHtml(event.symbol ? `${event.symbol}: ${event.title}` : event.title)}</strong>
        <div class="subtle">${escapeHtml(event.source || "Source")} | ${escapeHtml(event.source_type || "news")} | ${event.days_old ?? "NA"}d old</div>
      </div>
      <span class="pill ${type === "pos" ? "green" : type === "neg" ? "red" : "amber"}">${event.net_score ?? event.importance ?? "NA"}</span>
    </div>
  </div>`;
}

function renderTechnical(stock) {
  const t = stock.technical_strength || {};
  const i = t.indicators || {};
  const checks = t.checks || {};
  return `
    <div class="grid two">
      <section class="panel">
        <div class="section-kicker">Technical snapshot</div>
        ${metric("20 EMA", money(i.ema20))}
        ${metric("50 EMA", money(i.ema50))}
        ${metric("200 DMA", money(i.dma200))}
        ${metric("RSI 14", i.rsi14 ?? "NA")}
        ${metric("ATR 14", money(i.atr14))}
        ${metric("Volume ratio", `${i.volume_ratio ?? "NA"}x`)}
        ${metric("Breakout level", money(i.breakout_level))}
        ${metric("Relative strength", `${i.relative_strength?.state || "NA"} (${i.relative_strength?.pct ?? 0}%)`)}
      </section>
      <section class="panel">
        <div class="section-kicker">Breakout checklist</div>
        ${Object.keys(checks).map(key => check(key.replaceAll("_"," "), checks[key])).join("") || "<div class='status-line'>Backend checklist not loaded in demo mode.</div>"}
        ${(t.fake_breakout_flags || []).map(flag => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(flag)}</div></div>`).join("")}
      </section>
    </div>`;
}

function renderEntry(stock) {
  const entry = stock.entry || {};
  const exits = stock.exit_rules || {};
  return `
    <div class="panel">
      <div class="section-kicker">Entry plan</div>
      <div class="zone-grid">
        <div class="zone"><small>Breakout level</small><strong>${money(entry.breakout_level)}</strong></div>
        <div class="zone"><small>Aggressive entry</small><strong>${zone(entry.aggressive)}</strong></div>
        <div class="zone"><small>Pullback entry</small><strong>${zone(entry.pullback)}</strong></div>
        <div class="zone"><small>Stop</small><strong>${money(entry.stop)}</strong></div>
      </div>
      <div style="margin-top:14px">${metric("Invalidation", entry.invalidation || "NA")}</div>
    </div>
    <section class="panel" style="margin-top:14px">
      <div class="section-kicker">Exit layers</div>
      ${metric("Price stop", exits.price_stop || "NA")}
      ${metric("Swing trend exit", exits.swing_trend_exit || "NA")}
      ${metric("Positional trend exit", exits.positional_trend_exit || "NA")}
      ${metric("Event exit", exits.event_exit || "NA")}
    </section>`;
}

function renderThesis(stock) {
  const saved = JSON.parse(localStorage.getItem(`thesis_${stock.symbol}`) || "{}");
  return `<section class="panel">
    <div class="section-kicker">Thesis tracker</div>
    <div class="grid two">
      <label>Why picked<textarea class="input textarea" id="whyPicked">${escapeHtml(saved.whyPicked || "")}</textarea></label>
      <label>Invalidation<textarea class="input textarea" id="invalidationNote">${escapeHtml(saved.invalidation || stock.entry?.invalidation || "")}</textarea></label>
      <label>What changed after results<textarea class="input textarea" id="changedNote">${escapeHtml(saved.changed || "")}</textarea></label>
      <label>Review note<textarea class="input textarea" id="reviewNote">${escapeHtml(saved.review || "")}</textarea></label>
    </div>
    <div style="margin-top:12px;display:flex;gap:10px;align-items:center">
      <button class="btn primary" id="saveThesisBtn">Save thesis</button>
      <span class="subtle">Last saved: ${saved.savedAt ? new Date(saved.savedAt).toLocaleString("en-IN") : "Never"}</span>
    </div>
  </section>`;
}

function saveThesis() {
  if (!selectedStock) return;
  const payload = {
    whyPicked: qs("whyPicked")?.value || "",
    invalidation: qs("invalidationNote")?.value || "",
    changed: qs("changedNote")?.value || "",
    review: qs("reviewNote")?.value || "",
    savedAt: new Date().toISOString()
  };
  localStorage.setItem(`thesis_${selectedStock.symbol}`, JSON.stringify(payload));
  renderStockView(selectedStock);
}

function renderApiStack() {
  const stack = [
    ["Price OHLCV", "Yahoo chart/yfinance for personal prototype", "Free but unofficial; production needs licensed feed"],
    ["Daily technicals", "Alpha Vantage", "Free key is very limited"],
    ["News", "GDELT + NewsAPI", "GDELT open; NewsAPI developer tier useful locally"],
    ["Official filings", "NSE/BSE corporate filings", "Public pages; respect rate limits"],
    ["Fundamentals", "Annual reports, exchange filings, CSV import", "Free manual import is safest for personal use"]
  ];
  return `<section class="panel">
    <div class="section-kicker">Free and low-cost API stack</div>
    <div class="api-list">
      ${stack.map(row => `<div class="api-line"><strong>${row[0]}</strong><span>${row[1]}</span><span class="subtle">${row[2]}</span></div>`).join("")}
    </div>
    <div style="margin-top:12px" class="notice">For serious intraday Indian equities, fully free official real-time APIs are not realistic. Use free sources for research prototypes and move to licensed feeds before trading automation.</div>
    <label style="display:block;margin-top:12px">Backend URL<input class="input" id="apiBaseInput" value="${escapeHtml(apiBase)}"></label>
    <button class="btn primary" id="saveApiBaseBtn" style="margin-top:10px">Save backend URL</button>
  </section>`;
}

async function refreshLive(symbol) {
  qs("offlineNotice").textContent = `Refreshing ${symbol} from optional live connectors...`;
  qs("offlineNotice").classList.remove("hidden");
  try {
    selectedStock = await getJson(`/api/refresh/${symbol}`, {method: "POST"});
    qs("offlineNotice").textContent = (selectedStock.connector_notes || []).join(" | ");
    renderStockView(selectedStock);
  } catch (error) {
    qs("offlineNotice").textContent = `Live refresh failed: ${error.message}`;
  }
}

function drawChart(bars) {
  const canvas = qs("priceChart");
  if (!canvas) return;
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(320, rect.width * dpr);
  canvas.height = Math.max(220, rect.height * dpr);
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, rect.width, rect.height);
  const series = (bars || []).slice(-120).map(b => Number(b.close)).filter(Number.isFinite);
  if (!series.length) return;
  const pad = 34;
  const min = Math.min(...series);
  const max = Math.max(...series);
  const range = Math.max(max - min, 1);
  ctx.strokeStyle = "#d7dde8";
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i++) {
    const y = pad + (rect.height - pad * 2) * i / 4;
    ctx.beginPath();
    ctx.moveTo(pad, y);
    ctx.lineTo(rect.width - pad, y);
    ctx.stroke();
  }
  ctx.strokeStyle = "#1d4ed8";
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((value, index) => {
    const x = pad + (rect.width - pad * 2) * index / Math.max(series.length - 1, 1);
    const y = rect.height - pad - ((value - min) / range) * (rect.height - pad * 2);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#111827";
  ctx.font = "12px system-ui";
  ctx.fillText(money(max), pad, 18);
  ctx.fillText(money(min), pad, rect.height - 10);
}

document.addEventListener("click", event => {
  const stockButton = event.target.closest("[data-symbol]");
  if (stockButton) selectStock(stockButton.dataset.symbol);
  const filter = event.target.closest("[data-filter]");
  if (filter) {
    activeFilter = filter.dataset.filter;
    document.querySelectorAll("[data-filter]").forEach(btn => btn.classList.toggle("active", btn.dataset.filter === activeFilter));
    renderStockList();
  }
  const tab = event.target.closest("[data-tab]");
  if (tab && selectedStock) {
    activeTab = tab.dataset.tab;
    renderStockView(selectedStock);
  }
  if (event.target.id === "homeBtn") {
    selectedStock = null;
    renderStockList();
    renderHome();
  }
  if (event.target.id === "saveThesisBtn") saveThesis();
  const refresh = event.target.closest("[data-refresh]");
  if (refresh) refreshLive(refresh.dataset.refresh);
  if (event.target.id === "saveApiBaseBtn") {
    apiBase = qs("apiBaseInput").value.trim() || "http://127.0.0.1:8000";
    localStorage.setItem(API_BASE_KEY, apiBase);
    loadDashboard();
  }
});

qs("searchInput").addEventListener("input", renderStockList);
window.addEventListener("resize", () => {
  if (activeTab === "chart" && selectedStock) drawChart(selectedStock.bars || []);
});
loadDashboard();
