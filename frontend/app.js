const API_BASE_KEY = "stockEngineApiBase";
const WATCHLIST_KEY = "stockEngineWatchlist";
const LOCAL_API_BASE = "http://127.0.0.1:8000";
const DEPLOYED_API_BASE = window.STOCK_ENGINE_API_BASE || "https://indian-stock-decision-engine-api.onrender.com";
const isLocalPage = ["", "localhost", "127.0.0.1"].includes(window.location.hostname) || window.location.protocol === "file:";
let apiBase = localStorage.getItem(API_BASE_KEY) || (isLocalPage ? LOCAL_API_BASE : DEPLOYED_API_BASE);
let dashboard = null;
let selectedStock = null;
let activeTab = "overview";
let activeFilter = "all";
let liveSocket = null;
let liveReconnectTimer = null;
let liveStatus = {status: "not_connected", configured: false};
let liveRenderTimer = null;
const DASHBOARD_POLL_MS = 30000;
const SELECTED_STOCK_POLL_MS = 15000;
let dashboardPollTimer = null;
let selectedStockPollTimer = null;
let newsRefreshTimer = null;
let universeSearchRows = null;
let universeSearchMeta = null;
let searchDebounceTimer = null;
let commandSearchTimer = null;
let isDemo = false;
let lastSuccessfulFetch = 0;
let currentDashboardDelay = DASHBOARD_POLL_MS;
let activeChartPeriod = "3m";
let currentChart = null;
let chartResizeObserver = null;
let keyboardFocusIndex = 0;
let activeNewsFilter = "all";
let topCandidateState = {loading: false, lastCalculatedAt: null};
let scanWeeklyResults = null;
let scanMonthlyResults = null;
let scanMeta = null;
let scanRunning = false;
let scanStatusText = "";
let yahooEnrichRunning = false;
let yahooEnrichStatusText = "";
let yahooEnrichPollTimer = null;
let databaseStatus = null;

function qs(id) { return document.getElementById(id); }
function scoreClass(value) { return value >= 75 ? "score-hi" : value >= 55 ? "score-mid" : "score-lo"; }
function scoreColor(value) { return value >= 75 ? "var(--green)" : value >= 55 ? "var(--amber)" : "var(--red)"; }
function stateName(stock) { return stock?.trade_state?.state || (stock?.candidate ? "Watchlist" : "Screened"); }
function slug(value) { return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""); }
function stateClass(value) { return `state-${slug(value || "screened")}`; }
function convictionClass(value) { return `conviction-${slug(value || "avoid")}`; }
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
function countText(value, fallback = "0") {
  if (value === null || value === undefined || value === "") return fallback;
  const clean = Number(value);
  if (!Number.isFinite(clean)) return String(value);
  return clean.toLocaleString("en-IN", {maximumFractionDigits: 0});
}
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[char]));
}
function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
}
function readWatchlist() {
  try {
    const rows = JSON.parse(localStorage.getItem(WATCHLIST_KEY) || "[]");
    return Array.isArray(rows) ? rows.filter(item => item?.symbol) : [];
  } catch {
    return [];
  }
}
function writeWatchlist(rows) {
  localStorage.setItem(WATCHLIST_KEY, JSON.stringify(rows));
}
function isWatchlisted(symbol) {
  const target = String(symbol || "").toUpperCase();
  return readWatchlist().some(item => item.symbol === target);
}
function toggleWatchlist(symbol, note = "") {
  const target = String(symbol || "").toUpperCase();
  if (!target) return false;
  const rows = readWatchlist();
  const exists = rows.some(item => item.symbol === target);
  const next = exists
    ? rows.filter(item => item.symbol !== target)
    : [...rows, {symbol: target, addedAt: new Date().toISOString(), note}];
  writeWatchlist(next);
  return !exists;
}
function watchlistSymbols() {
  return new Set(readWatchlist().map(item => item.symbol));
}
function stockBySymbol(symbol) {
  const target = String(symbol || "").toUpperCase();
  return (dashboard?.stocks || []).find(stock => stock.symbol === target)
    || (dashboard?.top_weekly_candidates || []).find(stock => stock.symbol === target)
    || (dashboard?.top_monthly_candidates || []).find(stock => stock.symbol === target)
    || (dashboard?.avoid_list || []).find(stock => stock.symbol === target)
    || (universeSearchRows || []).find(stock => stock.symbol === target);
}
function badgeForConviction(conviction) {
  return `<span class="conviction-badge ${convictionClass(conviction)}">${escapeHtml(conviction || "Avoid")}</span>`;
}
function scoreValue(value) {
  return Number.isFinite(Number(value)) ? Number(value) : null;
}
function stockScoreLabel(stock) {
  const weekly = scoreValue(stock.weekly_score);
  if (weekly !== null) return weekly;
  return stock.research_covered ? "Research" : "EOD";
}
function dataQualityIcon(stock) {
  const gate = stock?.data_quality_gate;
  if (!gate) return "";
  const warnings = gate.price_data_quality?.warnings || [];
  const issues = gate.price_data_quality?.issues || [];
  const title = gate.pass
    ? `Data quality OK${warnings.length ? ` - ${warnings.join("; ")}` : ""}`
    : `Data quality issue${issues.length ? ` - ${issues.join("; ")}` : ""}`;
  return `<span class="${gate.pass ? "data-quality-ok" : "data-quality-warn"}" title="${escapeHtml(title)}">${gate.pass ? "OK" : "!"}</span>`;
}
function breakoutProximity(stock) {
  const price = Number(stock?.price);
  const breakout = Number(stock?.entry?.breakout_level);
  if (!Number.isFinite(price) || !Number.isFinite(breakout) || breakout <= 0) {
    return {label: "NA", className: ""};
  }
  const pct = (price / breakout - 1) * 100;
  const className = pct >= 0 ? "positive" : pct > -3 ? "amber" : "";
  return {label: `${pct > 0 ? "+" : ""}${pct.toFixed(1)}%`, className};
}
function sparkline(values = []) {
  const clean = values.map(Number).filter(Number.isFinite);
  if (clean.length < 2) return "";
  const width = 92;
  const height = 28;
  const min = Math.min(...clean);
  const max = Math.max(...clean);
  const range = Math.max(max - min, 0.01);
  const points = clean.map((value, index) => {
    const x = (index / Math.max(clean.length - 1, 1)) * width;
    const y = height - ((value - min) / range) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const up = clean[clean.length - 1] >= clean[0];
  return `<svg class="sparkline" viewBox="0 0 ${width} ${height}" aria-hidden="true"><polyline points="${points}" fill="none" stroke="${up ? "#087f5b" : "#b42318"}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"></polyline></svg>`;
}

function scoreHistorySvg(history = []) {
  const values = history.map(item => Number(item.weekly_score)).filter(Number.isFinite);
  if (values.length < 2) return "";
  return `<div class="score-history"><span>Weekly score trend</span>${sparkline(values)}</div>`;
}

function freshnessTone() {
  if (!lastSuccessfulFetch) return "red";
  const age = Date.now() - lastSuccessfulFetch;
  if (age > 120000) return "red";
  if (age > 60000) return "amber";
  return liveStatus.feed_open ? "green" : "amber";
}

function updateFreshnessIndicator() {
  const dot = qs("freshnessDot");
  if (!dot) return;
  const tone = freshnessTone();
  const age = lastSuccessfulFetch ? timeAgo(new Date(lastSuccessfulFetch).toISOString()) : "not refreshed yet";
  dot.className = `freshness-dot ${tone}`;
  dot.title = `Data freshness: ${age}`;
}

function isTriggeredStock(stock, state = stateName(stock)) {
  const price = Number(stock?.price);
  const breakout = Number(stock?.entry?.breakout_level);
  return ["Triggered", "In Trade"].includes(state) || (Number.isFinite(price) && Number.isFinite(breakout) && breakout > 0 && price >= breakout);
}

function stateBadgeHtml(stock, state = stateName(stock)) {
  if (isTriggeredStock(stock, state)) return `<span class="state-badge state-triggered">▲ Triggered</span>`;
  return `<span class="state-badge ${stateClass(state)}">${escapeHtml(state)}</span>`;
}

function confidencePill(confidence = {}) {
  const label = confidence.label || "Watch";
  const margin = Number(confidence.min_margin_above_high ?? confidence.weekly_margin ?? 0);
  const tone = margin >= 8 ? "green" : margin >= 0 ? "amber" : "red";
  return `<span class="confidence-pill ${tone}"><span>${escapeHtml(label)}</span><b>${Number.isFinite(margin) ? margin.toFixed(1) : "NA"}</b></span>`;
}

function scoreTile(label, value, subtitle, options = {}) {
  const clean = Number(value);
  const display = Number.isFinite(clean) ? Math.round(clean) : "NA";
  const scoreForColor = options.risk ? 100 - clean : clean;
  const tone = scoreTone(Number.isFinite(scoreForColor) ? scoreForColor : 0);
  const width = Math.max(0, Math.min(100, Number.isFinite(clean) ? (options.risk ? 100 - clean : clean) : 0));
  const icons = {Weekly: "📈", Monthly: "🧭", Technical: "📊", Business: "💼", Tailwind: "🌊", Risk: "🛡"};
  return `<div class="stile ${tone} ${options.risk ? "risk" : ""}">
    <div class="stile-label"><span class="stile-icon">${icons[label] || "•"}</span>${escapeHtml(label)}</div>
    <div class="stile-num">${display}${options.trend ? `<span class="trend-arrow">${options.trend}</span>` : ""}</div>
    <div class="stile-sub">${escapeHtml(subtitle || "")}</div>
    <div class="stile-bar"><span style="width:${width}%"></span></div>
  </div>`;
}

function renderEntryPlan(stock, compact = false) {
  const entry = stock.entry || {};
  const sizing = entry.position_sizing || {};
  return `<section class="panel entry-panel">
    <div class="panel-title">Entry Plan</div>
    ${entry.candidate_gate ? `<div class="micro-note">${escapeHtml(entry.candidate_gate)}</div>` : ""}
    <div class="entry-zones">
      <div class="ezone target"><small>Breakout</small><strong>${money(entry.breakout_level)}</strong></div>
      <div class="ezone target"><small>Aggressive</small><strong>${zone(entry.aggressive)}</strong></div>
      <div class="ezone"><small>Pullback</small><strong>${zone(entry.pullback)}</strong></div>
      <div class="ezone stop"><small>Stop</small><strong>${money(entry.stop)}</strong></div>
    </div>
    <div class="sizing-grid">
      <div><span>Account</span><strong>${money(sizing.account_size)}</strong></div>
      <div><span>Risk capital</span><strong>${money(sizing.risk_capital)}</strong></div>
      <div><span>Risk / share</span><strong>${money(sizing.risk_per_share)}</strong></div>
      <div><span>Units</span><strong>${sizing.suggested_quantity ?? "NA"}</strong></div>
    </div>
    ${compact ? "" : `<div class="entry-actions"><button class="btn blue" data-toggle-alert="${stock.symbol}">${hasAlert(stock.symbol) ? "Remove breakout alert" : "Alert me at breakout"}</button></div>`}
  </section>`;
}

function renderFiveQuestionGate(stock) {
  const questions = stock.explanation_json?.five_questions || {};
  const rows = [
    ["Is this a good business?", questions.good_business, stock.business_quality?.score],
    ["Is the sector helping it now?", questions.sector_helping_now, stock.sector_tailwind?.score],
    ["Is there a fresh trigger?", questions.fresh_trigger, stock.event_strength?.score],
    ["Is the chart confirming?", questions.chart_confirming, stock.technical_strength?.score],
    ["Where am I wrong?", questions.where_am_i_wrong_defined, 100 - Number(stock.risk_penalty?.score || stock.risk_score || 0)]
  ];
  return `<section class="panel gate-panel">
    <div class="panel-title">Five-Question Gate</div>
    <div class="gate-list">${rows.map(([label, ok, score]) => `
      <div class="gate-row ${ok ? "pass" : "fail"}">
        <span class="gate-dot"></span>
        <span>${escapeHtml(label)}</span>
        <strong>${ok ? `${Math.round(Number(score || 0))}` : "Fail"}</strong>
      </div>`).join("")}</div>
  </section>`;
}

function renderMarketContext(stock) {
  const market = stock.market_support || dashboard?.market_regime || {};
  return `<section class="panel">
    <div class="panel-title">Market Context</div>
    ${metric("Regime", market.regime || "NA")}
    ${metric("Market score", `${market.score ?? "NA"}/100`)}
    ${metric("Nifty trend", Number(market.nifty_close) > Number(market.nifty_ema50) ? "Above 50 EMA" : "Below 50 EMA")}
    ${metric("Breadth", `${market.breadth_above_50dma ?? "NA"}% advancers`)}
    ${metric("VIX", market.vix ?? "NA")}
  </section>`;
}
function websocketBase() {
  const url = new URL(apiBase);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString().replace(/\/$/, "");
}

async function getJson(path, options = {}) {
  const response = await fetch(`${apiBase}${path}`, options);
  if (!response.ok) {
    let detail = "";
    try {
      const payload = await response.json();
      detail = payload.detail || payload.error || payload.message || JSON.stringify(payload);
    } catch {
      try {
        detail = await response.text();
      } catch {
        detail = "";
      }
    }
    throw new Error(`${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`);
  }
  return response.json();
}

function dashboardFromFocus(focus) {
  const allStocks = [
    ...(focus.focus?.triggered || []),
    ...(focus.focus?.stalking || []),
    ...(focus.focus?.watchlist || []),
  ];
  const seen = new Set();
  const stocks = allStocks.filter(stock => {
    if (!stock?.symbol || seen.has(stock.symbol)) return false;
    seen.add(stock.symbol);
    return true;
  });
  return {
    generated_at: focus.generated_at,
    market_regime: focus.market_health?.raw || {},
    top_weekly_candidates: [...stocks].sort((a, b) => (b.weekly_score || 0) - (a.weekly_score || 0)).slice(0, 3),
    top_monthly_candidates: [...stocks].sort((a, b) => (b.monthly_score || 0) - (a.monthly_score || 0)).slice(0, 3),
    top_sectors: focus.sector_map || [],
    avoid_list: focus.focus?.avoid || [],
    latest_critical_events: focus.latest_critical_events || [],
    stocks,
    scored_research_total: focus.scan_meta?.total_scored || stocks.length,
    prices_as_of: focus.generated_at,
    price_refresh: {updated_at: focus.generated_at},
    sector_map: focus.sector_map || [],
    dashboard_mode: "dynamic_focus",
    focus,
    focus_criteria: "Triggered, within 3 percent of breakout, or candidate watchlist from liquid NSE universe.",
    nse_universe: {total: focus.scan_meta?.universe_size || stocks.length},
    weekly_scan: {status: "focus", result_count: stocks.length, enabled: true},
    database: focus.database || {},
    live_feed: focus.live_feed || {},
    latency_strategy: {
      universe: "Dynamic focus dashboard scans top-liquid NSE names and keeps only actionable buckets.",
      prices: "NSE/free quote providers first, then configured fallbacks.",
      scores: "Heavy scoring remains backend-side; frontend renders the structured focus payload.",
    },
    disclaimer: focus.disclaimer,
  };
}

async function fetchDashboardPayload(force = false) {
  try {
    const focus = await getJson(`/api/v1/dashboard/focus?scan_limit=60${force ? "&force=true" : ""}`);
    return dashboardFromFocus(focus);
  } catch (focusError) {
    return getJson("/api/dashboard");
  }
}

async function loadDashboard() {
  try {
    dashboard = await fetchDashboardPayload();
    databaseStatus = dashboard.database || databaseStatus;
    refreshDatabaseStatus(false).catch(() => {});
    isDemo = false;
    lastSuccessfulFetch = Date.now();
    currentDashboardDelay = DASHBOARD_POLL_MS;
    qs("offlineNotice").classList.add("hidden");
  } catch (error) {
    dashboard = makeDemoDashboard();
    isDemo = true;
    setNotice(`Backend is still waking or unavailable at ${apiBase}. Showing the starter dashboard while live data retries. ${error.message}`, "amber");
  }
  renderAll();
  connectLiveFeed();
}

function renderAll() {
  renderMarketStrip();
  renderStockList();
  renderHome();
  if (selectedStock) renderStockView(selectedStock);
}

function mergeDashboard(fresh) {
  if (!fresh || !dashboard) return;
  if (isDemo) {
    dashboard = fresh;
    isDemo = false;
    lastSuccessfulFetch = Date.now();
    currentDashboardDelay = DASHBOARD_POLL_MS;
    return;
  }
  if (fresh.dashboard_mode === "dynamic_focus") {
    dashboard.stocks = fresh.stocks || [];
  } else {
    const currentBySymbol = new Map((dashboard.stocks || []).map(stock => [stock.symbol, stock]));
    (fresh.stocks || []).forEach(freshStock => {
      const existing = currentBySymbol.get(freshStock.symbol);
      if (existing) Object.assign(existing, freshStock);
    });
  }
  dashboard.market_regime = fresh.market_regime || dashboard.market_regime;
  dashboard.generated_at = fresh.generated_at || dashboard.generated_at;
  dashboard.top_weekly_candidates = fresh.top_weekly_candidates || dashboard.top_weekly_candidates;
  dashboard.top_monthly_candidates = fresh.top_monthly_candidates || dashboard.top_monthly_candidates;
  dashboard.top_sectors = fresh.top_sectors || dashboard.top_sectors;
  dashboard.avoid_list = fresh.avoid_list || dashboard.avoid_list;
  dashboard.latest_critical_events = fresh.latest_critical_events || dashboard.latest_critical_events;
  dashboard.live_feed = fresh.live_feed || dashboard.live_feed;
  dashboard.nse_universe = fresh.nse_universe || dashboard.nse_universe;
  dashboard.scored_research_total = fresh.scored_research_total || dashboard.scored_research_total;
  dashboard.prices_as_of = fresh.prices_as_of || dashboard.prices_as_of;
  dashboard.sector_map = fresh.sector_map || dashboard.sector_map;
  dashboard.price_refresh = fresh.price_refresh || dashboard.price_refresh;
  dashboard.focus = fresh.focus || dashboard.focus;
  dashboard.scan_meta = fresh.scan_meta || dashboard.scan_meta;
  dashboard.dashboard_mode = fresh.dashboard_mode || dashboard.dashboard_mode;
  databaseStatus = fresh.database || databaseStatus;
  lastSuccessfulFetch = Date.now();
  currentDashboardDelay = DASHBOARD_POLL_MS;
  if (selectedStock) {
    const liveSelected = (fresh.stocks || []).find(stock => stock.symbol === selectedStock.symbol);
    if (liveSelected) Object.assign(selectedStock, liveSelected);
  }
}

function setNotice(message, tone = "amber") {
  const notice = qs("offlineNotice");
  if (!notice) return;
  notice.textContent = message;
  notice.classList.remove("hidden", "notice-blue", "notice-red", "notice-amber");
  notice.classList.add(`notice-${tone}`);
}

function clearNotice() {
  const notice = qs("offlineNotice");
  if (!notice) return;
  notice.classList.add("hidden");
}

function checkStaleData() {
  if (scanRunning) return;
  if (!lastSuccessfulFetch) return;
  const ageMs = Date.now() - lastSuccessfulFetch;
  if (ageMs > 90000) {
    const mins = Math.max(1, Math.round(ageMs / 60000));
    setNotice(`Data may be stale - last live backend update was ${mins} min ago. Retrying automatically.`, "amber");
  }
}

function pulseLiveUpdate() {
  const strip = qs("marketStrip");
  if (!strip) return;
  strip.style.opacity = "0.55";
  setTimeout(() => { strip.style.opacity = "1"; }, 350);
  const dot = qs("freshnessDot");
  if (dot) {
    dot.classList.add("just-pulsed");
    setTimeout(() => dot.classList.remove("just-pulsed"), 260);
  }
}

async function refreshDatabaseStatus(showNotice = false) {
  const status = await getJson(`/api/database/status?t=${Date.now()}`);
  databaseStatus = status;
  if (dashboard) dashboard.database = status;
  if (showNotice) {
    const counts = status.counts || {};
    const msg = status.enabled
      ? `Supabase connected. Bhavcopy ${counts.latest_bhavcopy_date || "not loaded"}; rows ${countText(counts.daily_ohlcv)}; stale ${counts.bhavcopy_stale ? "yes" : "no"}.`
      : `Supabase is not connected. ${status.error || status.import_error || "Add DATABASE_URL in Render."}`;
    setNotice(msg, status.enabled && !counts.bhavcopy_stale ? "blue" : status.url_kind === "supabase_api_url" ? "red" : "amber");
  }
  renderMarketStrip();
  return status;
}

function startPolling() {
  clearTimeout(dashboardPollTimer);
  const poll = async () => {
    if (scanRunning) {
      dashboardPollTimer = setTimeout(poll, currentDashboardDelay);
      return;
    }
    if (!dashboard) {
      dashboardPollTimer = setTimeout(poll, currentDashboardDelay);
      return;
    }
    try {
      const fresh = await fetchDashboardPayload();
      mergeDashboard(fresh);
      renderMarketStrip();
      renderStockList();
      connectLiveFeed();
      if (selectedStock && !liveStatus.feed_open) renderStockView(selectedStock);
      pulseLiveUpdate();
      clearNotice();
      checkPriceAlerts();
    } catch (error) {
      currentDashboardDelay = Math.min(Math.round(currentDashboardDelay * 1.5), 120000);
      if (!scanRunning) checkStaleData();
    }
    dashboardPollTimer = setTimeout(poll, currentDashboardDelay);
  };
  dashboardPollTimer = setTimeout(poll, currentDashboardDelay);
}

function startSelectedStockPolling(symbol) {
  clearInterval(selectedStockPollTimer);
  clearInterval(newsRefreshTimer);
  selectedStockPollTimer = setInterval(async () => {
    if (!selectedStock || selectedStock.symbol !== symbol || liveStatus.feed_open) return;
    try {
      selectedStock = await getJson(`/api/stocks/${symbol}`);
      if (activeTab !== "chart") renderStockView(selectedStock);
    } catch (error) {
      // Keep the current selected stock if polling fails.
    }
  }, SELECTED_STOCK_POLL_MS);
  newsRefreshTimer = setInterval(() => refreshEvents(symbol, false), 300000);
}

function stopSelectedStockPolling() {
  clearInterval(selectedStockPollTimer);
  clearInterval(newsRefreshTimer);
  selectedStockPollTimer = null;
  newsRefreshTimer = null;
}

function renderMarketStrip() {
  const market = dashboard.market_regime || {};
  const live = liveStatus || dashboard.live_feed || {};
  const db = databaseStatus || dashboard.database || {};
  const dbCounts = db.counts || {};
  const regimeTone = market.regime === "Risk-on" ? "green" : market.regime === "Risk-off" ? "red" : "amber";
  const liveTone = live.feed_open ? "green" : live.configured ? "amber" : "red";
  const dbTone = db.enabled ? (dbCounts.bhavcopy_stale ? "amber" : "green") : "red";
  const dbLabel = db.enabled
    ? dbCounts.latest_bhavcopy_date
      ? `Bhav ${dbCounts.latest_bhavcopy_date}`
      : "Connected"
    : "Not connected";
  const dbBadge = db.enabled
    ? dbCounts.latest_bhavcopy_date
      ? `DB ${String(dbCounts.latest_bhavcopy_date).slice(5)}`
      : "DB ready"
    : "DB off";
  const liveBadge = live.feed_open ? "Live on" : live.configured ? "Live wait" : "Live off";
  const dbTitle = db.enabled
    ? `Supabase/Postgres connected. Latest bhavcopy: ${dbCounts.latest_bhavcopy_date || "not loaded yet"}.`
    : `Supabase/Postgres not connected. ${db.error || db.import_error || "Set DATABASE_URL to the Supabase Postgres connection string."}`;
  const liveTitle = live.feed_open
    ? "Shoonya live feed is connected."
    : live.configured
      ? "Shoonya credentials found; live feed is connecting or waiting."
      : "Shoonya live feed is optional and not configured.";
  const chips = [
    {label: "Regime", value: `<span class="regime-pill ${regimeTone}">${escapeHtml(market.regime || "NA")}</span>`},
    {label: "Market score", value: `${market.score ?? "NA"}/100`},
    {label: "Nifty", value: money(market.nifty_close || 0)},
    {label: "Breadth", value: `${market.breadth_above_50dma ?? "NA"}% adv`},
    {
      label: "Data feeds",
      value: `<span class="feed-stack"><span class="regime-pill ${dbTone}" title="${escapeHtml(dbTitle)}">${escapeHtml(dbBadge)}</span><span class="regime-pill ${liveTone}" title="${escapeHtml(liveTitle)}">${escapeHtml(liveBadge)}</span></span>`
    }
  ];
  qs("marketStrip").innerHTML = chips.map(({label, value}) => `
    <div class="market-chip">
      <div class="chip-label">${label}</div>
      <div class="chip-value">${value}</div>
    </div>`).join("");
  updateFreshnessIndicator();
}

function filteredStocks() {
  const text = qs("searchInput").value.trim().toLowerCase();
  const usingUniverseSearch = text.length >= 2 && Array.isArray(universeSearchRows);
  let rows = usingUniverseSearch ? [...universeSearchRows] : [...(dashboard.stocks || [])];
  if (!usingUniverseSearch) {
    if (activeFilter === "weekly") rows.sort((a,b) => b.weekly_score - a.weekly_score);
    if (activeFilter === "monthly") rows.sort((a,b) => b.monthly_score - a.monthly_score);
    if (activeFilter === "candidate") rows = rows.filter(s => s.candidate);
    if (activeFilter === "watchlist") {
      const pinned = watchlistSymbols();
      rows = rows.filter(s => pinned.has(s.symbol));
    }
    if (activeFilter === "avoid") rows = rows.filter(s => ["Avoid", "Hard Avoid"].includes(s.conviction) || s.risk_score >= 18);
    if (text) rows = rows.filter(s => `${s.symbol} ${s.name} ${s.sector} ${s.industry}`.toLowerCase().includes(text));
  }
  return rows;
}

function moveKeyboardSelection(delta) {
  const rows = filteredStocks();
  if (!rows.length) return;
  const current = selectedStock ? rows.findIndex(row => row.symbol === selectedStock.symbol) : keyboardFocusIndex;
  keyboardFocusIndex = Math.max(0, Math.min(rows.length - 1, (current >= 0 ? current : 0) + delta));
  selectedStock = rows[keyboardFocusIndex];
  renderStockList();
}

function renderStockList() {
  const rows = filteredStocks();
  const searchText = qs("searchInput").value.trim();
  const universeTotal = dashboard.nse_universe?.total || universeSearchMeta?.total;
  const watchCount = readWatchlist().length;
  qs("universeCount").textContent = searchText.length >= 2
    ? `${rows.length} results / ${universeTotal || "NSE"}`
    : activeFilter === "watchlist"
      ? `${rows.length} watched / ${dashboard.stocks.length} focus`
      : `${dashboard.stocks.length} focus / ${universeTotal || dashboard.stocks.length} NSE`;
  const header = activeFilter === "watchlist"
    ? `<div class="watchlist-filter-head"><span>${watchCount} pinned names</span><button class="mini-link" data-clear-watchlist="1">Clear all</button></div>`
    : "";
  const listHtml = rows.map(stock => {
    const state = stateName(stock);
    const weekly = scoreValue(stock.weekly_score);
    const scoreLabel = stockScoreLabel(stock);
    const scoreBadgeClass = weekly === null ? "score-neutral" : scoreClass(weekly);
    const isSearchOnly = weekly === null;
    const triggered = isTriggeredStock(stock, state);
    const pulse = stock._pulse ? `pulse-${stock._pulse}` : "";
    const watched = isWatchlisted(stock.symbol);
    return `
      <button class="stock-row ${selectedStock?.symbol === stock.symbol ? "active" : ""} ${triggered ? "triggered" : ""} ${pulse}" data-symbol="${stock.symbol}">
        <div class="stock-card-main">
          <div class="stock-topline">
            <span class="stock-id"><span class="state-dot ${stateClass(state)}"></span>${dataQualityIcon(stock)}<span class="stock-symbol">${escapeHtml(stock.symbol)}</span></span>
            <span class="stock-actions-line"><span class="watch-star ${watched ? "active" : ""}" data-watch-toggle="${stock.symbol}" title="${watched ? "Remove from watchlist" : "Add to watchlist"}">${watched ? "★" : "☆"}</span>${isSearchOnly ? `<span class="pill blue">${escapeHtml(stock.series || "NSE")}</span>` : badgeForConviction(stock.conviction)}</span>
          </div>
          <div class="stock-meta">${escapeHtml(stock.sector || "NSE")} | ${escapeHtml(stock.industry || stock.name || stock.series || "")}</div>
          <div class="stock-compact-metrics">
            ${isSearchOnly ? `<span>${escapeHtml(stock.name || stock.symbol)}</span>` : stateBadgeHtml(stock, state)}
            ${isSearchOnly ? `<span>${escapeHtml(stock.as_of || "EOD")}</span>` : `<span>W ${stock.weekly_score}</span><span>M ${stock.monthly_score}</span><span>R ${stock.risk_score}</span>`}
          </div>
        </div>
        <div class="score-cluster">
          <div class="score-badge ${scoreBadgeClass}" style="--score:${weekly ?? 55}">${scoreLabel}</div>
          <span class="score-caption">${isSearchOnly ? "search" : percent(stock.change_pct)}</span>
        </div>
      </button>`;
  }).join("");
  qs("stockList").innerHTML = header + (listHtml || (activeFilter === "watchlist" ? "<div class='empty-state'>No watchlist names yet. Star a stock to pin it here.</div>" : ""));
}

function topCandidates(scoreKey = "weekly_score") {
  return [...(dashboard?.stocks || [])]
    .filter(stock => Number.isFinite(Number(stock[scoreKey])))
    .sort((a, b) => Number(b[scoreKey] || 0) - Number(a[scoreKey] || 0))
    .slice(0, 3);
}

function candidateRows(rows, scoreKey) {
  if (topCandidateState.loading) {
    return Array.from({length: 3}).map((_, index) => `
      <div class="candidate-row desk-row candidate-skeleton">
        <span class="rank-pill">${index + 1}</span>
        <div><i class="skeleton-line w70"></i><i class="skeleton-line w45"></i></div>
        <i class="skeleton-ring"></i>
      </div>`).join("");
  }
  return (rows || []).slice(0, 3).map((stock, index) => {
    const state = stateName(stock);
    const score = Number(stock[scoreKey] || 0);
    const proximity = breakoutProximity(stock);
    return `<button class="candidate-row desk-row pro-candidate ${isTriggeredStock(stock, state) ? "triggered" : ""}" data-symbol="${stock.symbol}">
      <span class="rank-pill">${index + 1}</span>
      <div>
        <strong>${escapeHtml(stock.symbol)}</strong>
        <span>${escapeHtml(stock.sector || stock.name || "")} | ${money(stock.price)} | <em class="breakout-proximity ${proximity.className}">${proximity.label}</em></span>
      </div>
      <div class="candidate-tags">${badgeForConviction(stock.conviction)}${stateBadgeHtml(stock, state)}</div>
      <b class="candidate-gauge ${scoreClass(score)}" style="--score:${score}">${Math.round(score)}</b>
    </button>`;
  }).join("") || "<div class='status-line'>No qualified names right now.</div>";
}

function topCandidatePanel(title, rows, scoreKey) {
  const isWeekly = scoreKey === "weekly_score";
  return `<section class="panel desk-card top-candidate-card">
    <div class="panel-headline">
      <div>
        <div class="panel-title">${escapeHtml(title)}</div>
        <div class="status-line">${isWeekly && scanMeta ? `Universe ${countText(scanMeta.universe_size)} | Passed ${countText(scanMeta.passed_liquidity)} | Scored ${countText(scanMeta.total_scored)}` : `Last calculated ${topCandidateState.lastCalculatedAt ? formatIstTime(topCandidateState.lastCalculatedAt) : formatIstTime(dashboard.generated_at)} IST`}</div>
      </div>
      ${isWeekly ? `<button class="btn blue recalc-btn ${topCandidateState.loading ? "loading" : ""}" data-recalculate-best="1">${topCandidateState.loading ? "Scanning NSE" : "Run Full NSE Scan"}</button>` : ""}
    </div>
    <div class="candidate-progress ${topCandidateState.loading ? "active" : ""}"><span></span></div>
    ${isWeekly ? `<div class="scan-status-text">${escapeHtml(scanStatusText || (scanMeta ? `Last full scan ${topCandidateState.lastCalculatedAt ? timeAgo(topCandidateState.lastCalculatedAt) : ""}` : "Full scan ranks the investable NSE universe through the engine."))}</div>` : ""}
    ${candidateRows(rows, scoreKey)}
  </section>`;
}

function focusGrid(rows = []) {
  const topRows = [...rows].sort((a, b) => (b.weekly_score || 0) - (a.weekly_score || 0)).slice(0, 10);
  return `<div class="focus-grid">
    <div class="focus-grid-head">
      <span>Symbol</span><span>Price</span><span>Chg</span><span>W</span><span>M</span><span>Risk</span><span>Breakout</span><span>State</span>
    </div>
    ${topRows.map(stock => {
      const state = stateName(stock);
      const proximity = breakoutProximity(stock);
      return `<button class="focus-grid-row ${stateClass(state)}" data-symbol="${stock.symbol}">
        <strong>${escapeHtml(stock.symbol)}</strong>
        <span>${money(stock.price)}</span>
        <span class="${Number(stock.change_pct || 0) >= 0 ? "positive" : "negative"}">${percent(stock.change_pct)}</span>
        <span class="${scoreClass(stock.weekly_score)}">${stock.weekly_score}</span>
        <span class="${scoreClass(stock.monthly_score)}">${stock.monthly_score}</span>
        <span>${stock.risk_score}</span>
        <span class="breakout-proximity ${proximity.className}">${proximity.label}</span>
        <span class="state-badge ${stateClass(state)}">${escapeHtml(state)}</span>
      </button>`;
    }).join("")}
  </div>`;
}

function formatIstTime(value) {
  if (!value) return "NA";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "NA";
  return parsed.toLocaleTimeString("en-IN", {hour:"2-digit", minute:"2-digit", timeZone:"Asia/Kolkata"});
}

function scoreTone(value) {
  return value >= 75 ? "green" : value >= 55 ? "amber" : "red";
}

function aggregateSectors(stocks = []) {
  const grouped = new Map();
  (stocks || []).forEach(stock => {
    const sector = stock.sector || "Unclassified";
    if (!grouped.has(sector)) grouped.set(sector, []);
    grouped.get(sector).push(stock);
  });
  return [...grouped.entries()].map(([sector, names]) => {
    const ranked = [...names].sort((a, b) => Number(b.weekly_score || 0) - Number(a.weekly_score || 0));
    const avg = ranked.reduce((sum, stock) => sum + Number(stock.weekly_score || 0), 0) / Math.max(ranked.length, 1);
    return {sector, avg_weekly_score: avg, leader: ranked[0]?.symbol || "NA", count: ranked.length, stocks: ranked.slice(0, 7)};
  }).sort((a, b) => b.avg_weekly_score - a.avg_weekly_score);
}

function sectorHeatMap(stocks = []) {
  const sectors = aggregateSectors(stocks?.length ? stocks : (dashboard?.stocks || []));
  if (!sectors.length) return "<div class='status-line'>No sector map loaded.</div>";
  return `<div class="sector-heatmap expanded">${sectors.map(row => {
    const score = Number(row.avg_weekly_score || 0);
    return `<article class="sector-tile ${scoreTone(score)}">
      <button class="sector-tile-head" data-sector-filter="${escapeHtml(row.sector)}">
        <span><strong>${escapeHtml(row.sector)}</strong><small>${row.count} names | leader ${escapeHtml(row.leader)}</small></span>
        <b>${score.toFixed(1)}</b>
      </button>
      <div class="sector-symbol-grid">
        ${row.stocks.map(stock => {
          const state = stateName(stock);
          return `<button class="sector-stock-chip" data-symbol="${stock.symbol}">
            <span class="state-dot ${stateClass(state)}"></span>
            <strong>${escapeHtml(stock.symbol)}</strong>
            <small>${Math.round(Number(stock.weekly_score || 0))}</small>
            <em>${money(stock.price)}</em>
          </button>`;
        }).join("")}
      </div>
    </article>`;
  }).join("")}</div>`;
}

function normalizedEvents(sourceEvents = []) {
  return [...(sourceEvents || [])]
    .filter(Boolean)
    .sort((a, b) => Math.abs(Number(b.net_score ?? b.importance ?? b.sentiment ?? 0)) - Math.abs(Number(a.net_score ?? a.importance ?? a.sentiment ?? 0)));
}

function eventMatchesFilter(event, filter = activeNewsFilter) {
  const sentiment = Number(event.sentiment || 0);
  const type = String(event.source_type || event.type || "").toLowerCase();
  if (filter === "bullish") return sentiment > 0.1 || Number(event.net_score || 0) > 0;
  if (filter === "bearish") return sentiment < -0.1 || Number(event.net_score || 0) < 0;
  if (filter === "filings") return type.includes("filing") || type.includes("exchange") || type.includes("company_ir");
  if (filter === "earnings") return type.includes("earning") || type.includes("result") || String(event.title || "").toLowerCase().includes("result");
  return true;
}

function newsFilters(events = []) {
  const counts = {
    all: events.length,
    bullish: events.filter(event => eventMatchesFilter(event, "bullish")).length,
    bearish: events.filter(event => eventMatchesFilter(event, "bearish")).length,
    filings: events.filter(event => eventMatchesFilter(event, "filings")).length,
    earnings: events.filter(event => eventMatchesFilter(event, "earnings")).length,
  };
  return `<div class="news-tabs">${[
    ["all", "All"],
    ["bullish", "Bullish"],
    ["bearish", "Bearish"],
    ["filings", "Filings"],
    ["earnings", "Earnings"],
  ].map(([key, label]) => `<button class="news-tab ${activeNewsFilter === key ? "active" : ""}" data-news-filter="${key}">${label}<span>${counts[key]}</span></button>`).join("")}</div>`;
}

function renderNewsPanel(events = []) {
  const sorted = normalizedEvents(events);
  const filtered = sorted.filter(event => eventMatchesFilter(event)).slice(0, 8);
  return `<section class="panel news-panel">
    <div class="panel-headline">
      <div>
        <div class="panel-title">Critical News</div>
        <div class="status-line">${sorted.length} scored events, sorted by event impact</div>
      </div>
    </div>
    ${newsFilters(sorted)}
    <div class="event-list compact pro-news-list">${filtered.map(eventHtml).join("") || "<div class='empty-state'>No matching critical events right now.</div>"}</div>
  </section>`;
}

function renderWatchlistPanel() {
  const pinned = readWatchlist();
  const rows = pinned.map(item => stockBySymbol(item.symbol) || item);
  return `<section class="panel watchlist-panel">
    <div class="panel-headline">
      <div>
        <div class="panel-title">My Watchlist</div>
        <div class="status-line">${pinned.length} pinned names stored on this browser</div>
      </div>
      ${pinned.length ? `<button class="mini-link" data-clear-watchlist="1">Clear all</button>` : ""}
    </div>
    <div class="watchlist-table">
      ${rows.map(stock => {
        const state = stateName(stock);
        return `<button class="watch-row" data-symbol="${stock.symbol}">
          <strong>${escapeHtml(stock.symbol)}</strong>
          <span>${Math.round(Number(stock.weekly_score || 0)) || "NA"}</span>
          <span>${money(stock.price)}</span>
          <span class="${Number(stock.change_pct || 0) >= 0 ? "positive" : "negative"}">${stock.change_pct === undefined ? "NA" : percent(stock.change_pct)}</span>
          ${stateBadgeHtml(stock, state)}
        </button>`;
      }).join("") || "<div class='empty-state'>Star stocks from the sidebar or detail page to build your personal queue.</div>"}
    </div>
  </section>`;
}

function loadingStockSkeleton(symbol) {
  return `<section class="panel skeleton-stock">
    <div class="stock-head terminal-head">
      <div>
        <div class="skeleton-line w30"></div>
        <h1>${escapeHtml(symbol)}</h1>
        <div class="skeleton-line w60"></div>
      </div>
      <div class="price-block"><div class="skeleton-line w70"></div><div class="skeleton-line w45"></div></div>
    </div>
    <div class="score-stack terminal-scores">${Array.from({length: 6}).map(() => `<div class="stile skeleton-card"><i class="skeleton-line w45"></i><i class="skeleton-line w70"></i><i class="skeleton-line"></i></div>`).join("")}</div>
    <div class="stock-workspace-grid">
      <div class="stock-left-col"><div class="panel skeleton-card tall"></div><div class="panel skeleton-card chart-skeleton"></div></div>
      <div class="stock-right-col"><div class="panel skeleton-card tall"></div><div class="panel skeleton-card tall"></div></div>
    </div>
  </section>`;
}

function dataPrepPanel() {
  const counts = dashboard?.database?.counts || databaseStatus?.counts || {};
  const enrichedSymbols = counts.enriched_symbols;
  const enrichedRows = counts.enriched_ohlcv;
  const bhavDate = counts.latest_bhavcopy_date;
  const dbOk = dashboard?.database?.enabled || databaseStatus?.enabled;
  return `<section class="panel data-prep-panel">
    <div>
      <div class="panel-title">Data Prep Pipeline</div>
      <div class="body-text">Use this order before a serious full-market scan: latest bhavcopy, Yahoo history, then Full NSE Scan.</div>
      <div class="status-line">DB ${dbOk ? "on" : "off"}${bhavDate ? ` | bhavcopy ${escapeHtml(bhavDate)}` : ""}${enrichedSymbols !== undefined ? ` | enriched symbols ${countText(enrichedSymbols)} | rows ${countText(enrichedRows)}` : ""}</div>
      ${yahooEnrichStatusText ? `<div class="sync-status-text left">${escapeHtml(yahooEnrichStatusText)}</div>` : ""}
    </div>
    <div class="data-prep-actions">
      <button class="btn" data-sync-bhavcopy="1">1. Sync bhavcopy</button>
      <button class="btn blue ${yahooEnrichRunning ? "loading" : ""}" data-sync-yahoo="1">${yahooEnrichRunning ? "2. Syncing Yahoo" : "2. Sync Yahoo data"}</button>
      <button class="btn" data-recalculate-best="1">3. Run Full NSE Scan</button>
    </div>
  </section>`;
}

function renderHome() {
  const market = dashboard.market_regime;
  const triggeredRows = (dashboard.stocks || [])
    .filter(stock => isTriggeredStock(stock))
    .sort((a, b) => (b.weekly_score || 0) - (a.weekly_score || 0))
    .slice(0, 3);
  const alerts = dashboard.latest_critical_events || [];
  const weeklyRows = scanWeeklyResults || topCandidates("weekly_score");
  const monthlyRows = scanMonthlyResults || topCandidates("monthly_score");
  qs("stockView").classList.add("hidden");
  qs("homeView").classList.remove("hidden");
  qs("homeView").innerHTML = `
    <div class="home-head desk-home-head">
      <div>
        <div class="panel-title">Decision Desk</div>
        <h1>Research queue</h1>
        <div class="status-line">${dashboard.stocks.length} researched names from ${dashboard.nse_universe?.total || dashboard.stocks.length} NSE symbols. Prices as of ${formatIstTime(dashboard.prices_as_of || dashboard.generated_at)} IST.</div>
      </div>
      <div class="desk-actions">
        <button class="btn" data-sync-bhavcopy="1">Sync bhavcopy</button>
        <button class="btn ${yahooEnrichRunning ? "loading" : ""}" data-sync-yahoo="1">${yahooEnrichRunning ? "Syncing Yahoo" : "Sync Yahoo data"}</button>
        <button class="btn blue" data-market-refresh="1">Refresh market</button>
        <button class="btn" data-alert-scan="1">Scan alerts</button>
        <button class="btn" data-export-watchlist="1">Export</button>
        ${yahooEnrichStatusText ? `<div class="sync-status-text">${escapeHtml(yahooEnrichStatusText)}</div>` : ""}
      </div>
    </div>
    ${dataPrepPanel()}
    <div class="home-top-grid">
      <section class="panel desk-card">
        <div class="panel-title">Top 3 Triggered</div>
        ${candidateRows(triggeredRows, "weekly_score")}
      </section>
      ${topCandidatePanel("Top 3 Weekly", weeklyRows, "weekly_score")}
      ${topCandidatePanel("Top 3 Monthly", monthlyRows, "monthly_score")}
      <section class="panel desk-card market-card">
        <div class="panel-title">Market Regime</div>
        <div class="market-score-line">
          <span class="regime-pill ${market.regime === "Risk-on" ? "green" : market.regime === "Risk-off" ? "red" : "amber"}">${escapeHtml(market.regime || "NA")}</span>
          <strong>${market.score ?? "NA"}</strong>
        </div>
        ${metric("Nifty trend", market.nifty_close > market.nifty_ema50 ? "Above 50 EMA" : "Below 50 EMA")}
        ${metric("200 DMA filter", market.nifty_close > market.nifty_dma200 ? "Positive" : "Weak")}
        ${metric("Breadth proxy", `${market.breadth_above_50dma}% advancers`)}
        <div class="bar"><span style="width:${market.score}%;background:${scoreColor(market.score)}"></span></div>
      </section>
    </div>
    <div class="home-mid-grid">
      <section class="panel">
        <div class="panel-title">Sector Heat Map</div>
        ${sectorHeatMap(dashboard.stocks || [])}
      </section>
      ${renderNewsPanel(alerts)}
    </div>
    ${renderWatchlistPanel()}
    <section class="panel" style="margin-top:14px">
      <div class="panel-title">Focus Research Grid</div>
      ${focusGrid(dashboard.stocks || [])}
    </section>`;
}

function getAlerts() {
  try { return JSON.parse(localStorage.getItem("price_alerts") || "[]"); }
  catch { return []; }
}

function saveAlerts(alerts) {
  localStorage.setItem("price_alerts", JSON.stringify(alerts));
}

function hasAlert(symbol) {
  return getAlerts().some(alert => alert.symbol === symbol);
}

async function togglePriceAlert(stock) {
  const symbol = stock.symbol;
  let alerts = getAlerts();
  if (alerts.some(alert => alert.symbol === symbol)) {
    alerts = alerts.filter(alert => alert.symbol !== symbol);
  } else {
    if ("Notification" in window && Notification.permission === "default") {
      try { await Notification.requestPermission(); } catch {}
    }
    alerts.push({
      symbol,
      breakoutLevel: Number(stock.entry?.breakout_level || 0),
      stop: Number(stock.entry?.stop || 0),
      createdAt: new Date().toISOString(),
      fired: false
    });
  }
  saveAlerts(alerts);
  renderStockView(stock);
}

function checkPriceAlerts() {
  const alerts = getAlerts();
  if (!alerts.length || !dashboard?.stocks) return;
  let changed = false;
  alerts.forEach(alert => {
    if (alert.fired) return;
    const stock = dashboard.stocks.find(item => item.symbol === alert.symbol);
    const price = Number(stock?.price);
    if (stock && Number.isFinite(price) && alert.breakoutLevel && price >= alert.breakoutLevel) {
      alert.fired = true;
      changed = true;
      if ("Notification" in window && Notification.permission === "granted") {
        new Notification(`${alert.symbol} breakout alert`, {body: `${money(price)} crossed ${money(alert.breakoutLevel)}`});
      }
      setNotice(`${alert.symbol} crossed breakout ${money(alert.breakoutLevel)} at ${money(price)}`, "blue");
    }
  });
  if (changed) saveAlerts(alerts);
}

function exportWatchlist() {
  const rows = dashboard?.stocks || [];
  const headers = ["symbol","name","sector","weekly","monthly","conviction","state","price","breakout","stop","risk"];
  const body = rows.map(stock => [
    stock.symbol,
    stock.name,
    stock.sector,
    stock.weekly_score,
    stock.monthly_score,
    stock.conviction,
    stateName(stock),
    stock.price,
    stock.entry?.breakout_level,
    stock.entry?.stop,
    stock.risk_score,
  ].map(value => `"${String(value ?? "").replaceAll('"', '""')}"`).join(","));
  const blob = new Blob([[headers.join(","), ...body].join("\n")], {type: "text/csv"});
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `stock-watchlist-${new Date().toISOString().slice(0,10)}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function handleSearchInput() {
  const text = qs("searchInput").value.trim();
  clearTimeout(searchDebounceTimer);
  if (text.length < 2) {
    universeSearchRows = null;
    universeSearchMeta = null;
    renderStockList();
    return;
  }
  renderStockList();
  searchDebounceTimer = setTimeout(async () => {
    try {
      const result = await getJson(`/api/universe/search?q=${encodeURIComponent(text)}&limit=60`);
      universeSearchRows = result.rows || [];
      universeSearchMeta = result.meta || null;
      dashboard.nse_universe = result.meta || dashboard.nse_universe;
      renderStockList();
    } catch (error) {
      universeSearchRows = [];
      qs("offlineNotice").textContent = `NSE universe search failed: ${error.message}`;
      qs("offlineNotice").classList.remove("hidden");
      renderStockList();
    }
  }, 280);
}

function openCommandPalette() {
  const palette = qs("commandPalette");
  const input = qs("globalSearchInput");
  if (!palette || !input) return;
  palette.classList.remove("hidden");
  input.value = "";
  qs("searchResults").innerHTML = `<div class="search-empty">Type at least 2 letters. Results come from the NSE universe cache.</div>`;
  setTimeout(() => input.focus(), 0);
}

function closeCommandPalette() {
  const palette = qs("commandPalette");
  if (palette) palette.classList.add("hidden");
}

async function runCommandSearch() {
  const input = qs("globalSearchInput");
  const resultBox = qs("searchResults");
  if (!input || !resultBox) return;
  const query = input.value.trim();
  clearTimeout(commandSearchTimer);
  if (query.length < 2) {
    resultBox.innerHTML = `<div class="search-empty">Type at least 2 letters. Try RELIANCE, POLYCAB, HDFC, IT, power.</div>`;
    return;
  }
  commandSearchTimer = setTimeout(async () => {
    resultBox.innerHTML = `<div class="search-empty">Searching NSE universe...</div>`;
    try {
      const rows = await getJson(`/api/search?q=${encodeURIComponent(query)}&limit=30`);
      resultBox.innerHTML = rows.length ? rows.map(row => `
        <button class="search-result-item" data-symbol="${row.symbol}">
          <strong>${escapeHtml(row.symbol)}</strong>
          <span>${escapeHtml(row.name || row.symbol)}</span>
          <small>${escapeHtml(row.sector || row.series || "NSE")}</small>
        </button>`).join("") : `<div class="search-empty">No NSE result found for ${escapeHtml(query)}.</div>`;
    } catch (error) {
      resultBox.innerHTML = `<div class="search-empty">Search failed: ${escapeHtml(error.message)}</div>`;
    }
  }, 220);
}

async function selectStock(symbol) {
  const summary = dashboard.stocks.find(s => s.symbol === symbol) || (universeSearchRows || []).find(s => s.symbol === symbol);
  selectedStock = summary;
  activeTab = "overview";
  startSelectedStockPolling(symbol);
  renderStockList();
  qs("homeView").classList.add("hidden");
  qs("stockView").classList.remove("hidden");
  qs("stockView").innerHTML = loadingStockSkeleton(symbol);
  try {
    const detail = await getJson(`/api/stocks/${symbol}`);
    selectedStock = detail;
    renderStockView(detail);
  } catch (error) {
    if (summary && summary.weekly_score !== undefined) {
      selectedStock = buildFallbackDetail(summary);
      renderStockView(selectedStock);
    } else {
      qs("stockView").innerHTML = `<section class="panel"><div class="section-kicker">NSE search result</div><h2>${escapeHtml(symbol)}</h2><div class="notice">${escapeHtml(error.message)}</div><div class="status-line">The symbol is searchable in the NSE cache, but the backend needs enough Yahoo history or licensed data before it can produce a full decision score.</div></section>`;
    }
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
      indicators: {close: summary.price, ema20: summary.entry?.pullback?.[1], ema50: summary.entry?.stop, dma200: summary.entry?.stop * 0.9, rsi14: 62, atr14: Math.abs(summary.price - summary.entry.stop) / 2, volume_ratio: 1.5, breakout_level: summary.entry.breakout_level, relative_strength:{state:"Demo",pct:0}, base_quality:{score:50}},
      checks: {},
      fake_breakout_flags: summary.risk_flags || []
    },
    market_support: dashboard.market_regime,
    risk_penalty: {score: summary.risk_score, breakdown: Object.fromEntries((summary.risk_flags || []).map(flag => [flag, 1]))},
    explanation_json: {five_questions:{good_business: summary.business_score >= 65, sector_helping_now: summary.tailwind_score >= 60, fresh_trigger: summary.event_score >= 55, chart_confirming: summary.technical_score >= 60, where_am_i_wrong_defined: summary.risk_score <= 35}, thesis:[], risk_flags: summary.risk_flags || []},
    exit_rules: {price_stop:`Close below ${summary.entry.stop}`, swing_trend_exit:"Two closes below 20 EMA", positional_trend_exit:"Close below 50 EMA", event_exit:"Exit or reduce on serious negative official event"},
    trade_state: summary.trade_state || {state: summary.candidate ? "Watchlist" : "Screened", reason: "Demo state"},
    entry: {...summary.entry, candidate_gate: summary.candidate ? "Pass" : "Blocked in demo gate"},
    confidence_interval: summary.confidence_interval || {label:"Demo", min_margin_above_high:0},
    bars
  };
}

function verdictTone(stock) {
  const text = String(stock.verdict || stock.conviction || "").toLowerCase();
  if (text.includes("strong") || text.includes("candidate - monitor")) return "green";
  if (text.includes("avoid") || text.includes("red") || text.includes("blocked")) return "red";
  return "amber";
}

function stockDataQualityNotice(stock) {
  const notes = stock.connector_notes || [];
  const gate = stock.data_quality_gate || {};
  const warnings = [
    ...(gate.price_data_quality?.warnings || []),
    ...(gate.warning ? [gate.warning] : []),
  ].filter(Boolean);
  const proxy = String(stock.data_mode || "").includes("proxy") || notes.some(note => String(note).toLowerCase().includes("proxy"));
  if (gate.pass && !warnings.length && !proxy) return "";
  const text = [
    proxy ? "Proxy/indicative price history is active." : "",
    warnings.join(" | "),
    notes.slice(0, 3).join(" | "),
  ].filter(Boolean).join(" ");
  return `<div class="notice notice-amber data-quality-banner"><strong>Data quality:</strong> ${escapeHtml(text || "Some scoring inputs are incomplete.")}</div>`;
}

function renderStockView(stock) {
  const full = stock.business_quality ? stock : buildFallbackDetail(stock);
  qs("stockView").classList.remove("hidden");
  const state = stateName(full);
  const trendArrow = Number(full.weekly_score || 0) >= Number(full.monthly_score || 0) ? "↑" : "↓";
  const watched = isWatchlisted(full.symbol);
  const priceSpark = sparkline(full.sparkline || (full.bars || []).slice(-18).map(bar => bar.close));
  const verdict = full.verdict || (full.candidate ? "Candidate - check the entry plan and stop" : "Not a candidate - review failed gates");
  const tone = verdictTone(full);
  qs("stockView").innerHTML = `
    <div class="stock-head terminal-head">
      <div>
        <div class="panel-title">${escapeHtml(full.sector || "Sector")} | ${escapeHtml(full.industry || "Industry")}</div>
        <h1><span class="stock-big-sym">${escapeHtml(full.symbol)}</span> ${escapeHtml(full.name || "")}</h1>
        <div class="headline-badges">${badgeForConviction(full.conviction)}${stateBadgeHtml(full, state)}<span class="pill ${full.candidate ? "green" : "amber"}">Candidate ${full.candidate ? "Yes" : "No"}</span>${confidencePill(full.confidence_interval)}<button class="bookmark-btn ${watched ? "active" : ""}" data-watch-toggle="${full.symbol}">${watched ? "Added to Watchlist ✓" : "Add to Watchlist"}</button></div>
      </div>
      <div class="price-block ${full._pulse ? `pulse-${full._pulse}` : ""}">
        <div class="price price-big">${money(full.price)}</div>
        <div class="${Number(full.change_pct || 0) >= 0 ? "positive" : "negative"}">${percent(full.change_pct)}</div>
        ${priceSpark ? `<div class="price-spark">${priceSpark}</div>` : ""}
        <div class="status-line">Raw W ${full.weekly_raw_score ?? full.score_diagnostics?.weekly_raw ?? "NA"} | Raw M ${full.monthly_raw_score ?? full.score_diagnostics?.monthly_raw ?? "NA"}</div>
      </div>
    </div>
    <div class="verdict-banner ${tone}">
      <strong>Engine verdict</strong>
      <span>${escapeHtml(verdict)}</span>
      <small>${escapeHtml(full.data_mode || "research_scored")}</small>
    </div>
    ${stockDataQualityNotice(full)}
    ${scoreHistorySvg(full.score_history || [])}
    <div class="score-stack terminal-scores">
      ${scoreTile("Weekly", full.weekly_score, "5-15 day setup", {trend: trendArrow})}
      ${scoreTile("Monthly", full.monthly_score, "4-12 week structure")}
      ${scoreTile("Technical", full.technical_strength.score, "price confirms thesis")}
      ${scoreTile("Business", full.business_quality.score, "earnings quality")}
      ${scoreTile("Tailwind", full.sector_tailwind.score, "sector support")}
      ${scoreTile("Risk", full.risk_penalty.score, "penalty load", {risk: true})}
    </div>
    <div class="tabs-row">
      ${["overview","fundamentals","events","technical","entry","chart","thesis","data"].map(tab => `<button class="tab ${activeTab === tab ? "active" : ""}" data-tab="${tab}">${tabLabel(tab)}</button>`).join("")}
      <button class="btn blue" data-refresh="${full.symbol}">Refresh live</button>
    </div>
    <div id="tabBody">${renderTab(full)}</div>`;
  if (activeTab === "chart") setTimeout(() => drawChart(full.bars || [], "priceChart", full.benchmark_bars || []), 0);
  if (activeTab === "overview") setTimeout(() => drawChart(full.bars || [], "overviewChart", full.benchmark_bars || []), 0);
}

function tabLabel(tab) {
  return {overview:"Overview",fundamentals:"Fundamentals",events:"Events",technical:"Technical",entry:"Entry / Exit",chart:"Chart",thesis:"Thesis Tracker",data:"Free APIs"}[tab];
}

function chartIndicatorStrip(stock) {
  const indicators = stock.technical_strength?.indicators || {};
  const base = indicators.base_quality || {};
  const macd = indicators.macd || {};
  const stoch = indicators.stochastic || {};
  const obv = indicators.obv || {};
  const vpt = indicators.vpt || {};
  const idio = indicators.idiosyncratic_momentum || {};
  return `
    <div class="indicator-strip">
      <span>MACD ${escapeHtml(macd.state || "NA")}</span>
      <span>Stoch ${stoch.k ?? "NA"} / ${stoch.d ?? "NA"} ${escapeHtml(stoch.state || "")}</span>
      <span>Idio ${idio.score ?? "NA"} ${escapeHtml(idio.state || "")}</span>
      <span>VPT ${escapeHtml(vpt.state || "NA")}</span>
      <span>OBV ${escapeHtml(obv.state || "NA")}</span>
      <span>RS Rating ${indicators.rs_rating ?? "NA"}</span>
      <span>Base ${base.score ?? "NA"} ${base.last_5_tight ? "tight" : "loose"}</span>
      <span>VCP ${indicators.vcp_pattern?.score ?? "NA"}</span>
      <span>Rel Vol ${indicators.relative_volume ?? indicators.volume_ratio ?? "NA"}x</span>
      <span>ATR BO ${indicators.atr_breakout ? "yes" : "no"}</span>
    </div>`;
}

function renderTab(stock) {
  if (activeTab === "overview") return renderOverview(stock);
  if (activeTab === "fundamentals") return renderFundamentals(stock);
  if (activeTab === "events") return renderEvents(stock);
  if (activeTab === "technical") return renderTechnical(stock);
  if (activeTab === "entry") return renderEntry(stock);
  if (activeTab === "chart") return `<section class="panel"><div class="chart-head"><div><div class="section-kicker">Price confirmation</div><div class="status-line">Fresh chart endpoint: candles, EMAs, volume, RS vs Nifty, breakout, stop, and base context.</div></div><div class="timeframe-row">${["1w","1m","3m","6m","1y"].map(period => `<button class="timeframe-btn ${activeChartPeriod === period ? "active" : ""}" data-chart-period="${period}">${period.toUpperCase()}</button>`).join("")}</div></div>${chartIndicatorStrip(stock)}<div class="chart-wrap"><div id="priceChart" class="tv-chart"></div></div><div id="chartTooltip" class="chart-tooltip hidden"></div><div class="chart-legend"><span class="legend-price">Candles</span><span class="legend-ema">20 EMA</span><span class="legend-dma">200 DMA</span><span class="legend-rs">RS vs Nifty</span><span class="legend-breakout">Breakout</span><span class="legend-stop">Stop</span></div></section>`;
  if (activeTab === "thesis") return renderThesis(stock);
  if (activeTab === "data") return renderApiStack();
  return "";
}

function renderOverview(stock) {
  return `
    <div class="stock-workspace-grid">
      <div class="stock-left-col">
        ${renderEntryPlan(stock, true)}
        <section class="panel">
          <div class="panel-title">Chart Snapshot</div>
          ${chartIndicatorStrip(stock)}
          <div class="chart-wrap compact"><div id="overviewChart" class="tv-chart"></div></div>
        </section>
        <section class="panel">
          <div class="panel-title">Latest Events</div>
          <div class="event-list compact">${(stock.event_strength?.events || []).slice(0, 3).map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>"}</div>
        </section>
      </div>
      <div class="stock-right-col">
        ${renderFiveQuestionGate(stock)}
        ${renderMarketContext(stock)}
        <section class="panel">
          <div class="panel-title">Thesis</div>
          ${stock.trade_state ? `<div class="micro-note">Trade state: ${escapeHtml(stock.trade_state.state)} | ${escapeHtml(stock.trade_state.reason || "")}</div>` : ""}
          <div class="body-text">${(stock.explanation_json?.thesis || []).map(line => `<p>${escapeHtml(line)}</p>`).join("") || "Start the backend for full thesis lines."}</div>
        </section>
        <section class="panel">
          <div class="panel-title">Where I Am Wrong</div>
          ${(stock.explanation_json?.risk_flags || []).map(flag => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(flag)}</div></div>`).join("") || "<div class='status-line'>No active rule-based risk flag.</div>"}
          <div style="margin-top:12px">${renderRiskMatrix(stock)}</div>
        </section>
      </div>
    </div>`;
}

function check(label, ok) {
  return `<div class="check-row"><span class="dot ${ok ? "ok" : "bad"}"></span><div><strong>${label}</strong><div class="subtle">${ok ? "Pass" : "Not aligned yet"}</div></div></div>`;
}

function renderRiskMatrix(stock) {
  const f = stock.fundamentals || {};
  const flags = new Set(stock.explanation_json?.risk_flags || []);
  const cells = [
    ["Debt", Number(f.debt_equity) <= 0.6 ? "green" : Number(f.debt_equity) <= 1.2 ? "amber" : "red", `D/E ${f.debt_equity ?? "NA"}`],
    ["Cash Flow", Number(f.cfo_pat) >= 0.8 ? "green" : Number(f.cfo_pat) >= 0.65 ? "amber" : "red", `CFO/PAT ${f.cfo_pat ?? "NA"}`],
    ["Pledge", Number(f.pledge_percent || 0) === 0 ? "green" : Number(f.pledge_percent || 0) <= 5 ? "amber" : "red", `${f.pledge_percent ?? 0}% pledged`],
    ["Dilution", f.dilution_flag ? "red" : "green", f.dilution_flag ? "Dilution flag" : "No flag"],
    ["Governance", flags.has("negative_governance_event") ? "red" : "green", flags.has("negative_governance_event") ? "Negative event" : "Clean"],
    ["Breakout", flags.has("fake_breakout_risk") || (stock.technical_strength?.fake_breakout_flags || []).length ? "amber" : "green", `${(stock.technical_strength?.fake_breakout_flags || []).length} flags`],
    ["Market", stock.market_support?.regime === "Risk-off" ? "red" : stock.market_support?.regime === "Neutral" ? "amber" : "green", stock.market_support?.regime || "NA"],
  ];
  return `<div class="risk-matrix">${cells.map(([label, tone, detail]) => `
    <div class="risk-cell ${tone}">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(detail)}</span>
    </div>`).join("")}</div>`;
}

function renderFundamentals(stock) {
  const f = stock.fundamentals || {};
  const breakdown = stock.business_quality?.breakdown || {};
  const dataQuality = stock.business_quality?.data_quality || {};
  const reminder = nextQuarterReminder();
  return `
    <div class="grid two">
      <section class="panel">
        <div class="section-kicker">Business quality inputs</div>
        <div class="notice">Quarterly refresh reminder: paste Screener/exported CSV after results. Next review window: ${reminder}</div>
        ${dataQuality.warning ? `<div class="notice">${escapeHtml(dataQuality.warning)} | Completeness ${dataQuality.completeness_pct}%</div>` : ""}
        ${metric("Sales CAGR", `${f.sales_cagr ?? "NA"}%`)}
        ${metric("Profit CAGR", `${f.profit_cagr ?? "NA"}%`)}
        ${metric("ROCE", `${f.roce ?? "NA"}%`)}
        ${metric("ROE", `${f.roe ?? "NA"}%`)}
        ${metric("Debt / Equity", f.debt_equity ?? "NA")}
        ${metric("CFO / PAT", f.cfo_pat ?? "NA")}
        ${metric("FCF trend", f.fcf_trend ?? "NA")}
        ${metric("Pledge", `${f.pledge_percent ?? "NA"}%`)}
        ${metric("PE", f.pe ?? "NA")}
        ${dataQuality.missing_fields?.length ? metric("Missing fields", dataQuality.missing_fields.join(", ")) : ""}
      </section>
      <section class="panel">
        <div class="section-kicker">Score breakdown</div>
        ${Object.keys(breakdown).map(key => breakdownRow(key, breakdown[key])).join("") || "<div class='status-line'>Backend scoring breakdown not loaded in demo mode.</div>"}
      </section>
      <section class="panel">
        <div class="section-kicker">Screener CSV paste workflow</div>
        <div class="status-line">Use backend endpoint POST /api/fundamentals/${stock.symbol}/screener-csv with csv_text after every quarterly result.</div>
        <textarea class="input textarea" readonly>Sales CAGR,22
Profit CAGR,28
ROCE,25
ROE,21
Debt to Equity,0.2
CFO PAT,0.9
FCF trend,positive
Promoter holding trend,stable
Pledge,0
PE,45</textarea>
      </section>
    </div>`;
}

function nextQuarterReminder() {
  const now = new Date();
  const month = now.getMonth();
  const quarterEndMonth = month < 3 ? 2 : month < 6 ? 5 : month < 9 ? 8 : 11;
  const year = now.getFullYear();
  const review = new Date(year, quarterEndMonth + 1, 20);
  if (review < now) review.setMonth(review.getMonth() + 3);
  return review.toLocaleDateString("en-IN", {day:"2-digit", month:"short", year:"numeric"});
}

function breakdownRow(key, row) {
  return `<div style="margin:11px 0">
    <div class="metric"><span>${key.replaceAll("_"," ")}</span><strong>${row.points}/${row.weight}</strong></div>
    <div class="bar"><span style="width:${row.score}%;background:${scoreColor(row.score)}"></span></div>
  </div>`;
}

function renderEvents(stock) {
  const events = stock.event_strength?.events || [];
  const updatedAt = stock.event_strength?.updated_at || stock.events_updated_at;
  const filtered = normalizedEvents(events).filter(event => eventMatchesFilter(event));
  return `<section class="panel">
    <div class="events-head">
      <div>
        <div class="panel-title">Event Strength</div>
        <div class="status-line">Last updated ${updatedAt ? timeAgo(updatedAt) : "when stock opened"}</div>
      </div>
      <button class="btn blue" data-refresh-events="${stock.symbol}">Refresh news</button>
    </div>
    ${newsFilters(events)}
    <div id="eventsList" class="event-list">${filtered.map(eventHtml).join("") || "<div class='status-line'>No matching events loaded.</div>"}</div>
  </section>`;
}

function timeAgo(value) {
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts)) return "NA";
  const mins = Math.max(0, Math.round((Date.now() - ts) / 60000));
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min ago`;
  return `${Math.round(mins / 60)} hr ago`;
}

function updateEventsPane(events, updatedAt) {
  if (!selectedStock) return;
  selectedStock.event_strength = selectedStock.event_strength || {};
  selectedStock.event_strength.events = events;
  selectedStock.event_strength.updated_at = updatedAt || new Date().toISOString();
  if (activeTab === "events") {
    const list = qs("eventsList");
    if (list) list.innerHTML = normalizedEvents(events).filter(event => eventMatchesFilter(event)).map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>";
    const body = qs("tabBody");
    if (body) body.innerHTML = renderEvents(selectedStock);
  }
}

async function refreshEvents(symbol, fresh = false) {
  if (!symbol) return;
  const button = document.querySelector(`[data-refresh-events="${symbol}"]`);
  if (button) button.textContent = "Refreshing...";
  try {
    const payload = await getJson(`/api/events/${symbol}${fresh ? "?fresh=1" : ""}`);
    updateEventsPane(payload.events || [], payload.updated_at);
  } catch (error) {
    setNotice(`News refresh failed: ${error.message}`, "red");
  } finally {
    if (button) button.textContent = "Refresh news";
  }
}

function eventHtml(event) {
  const sentiment = Number(event.sentiment || 0);
  const type = sentiment > 0.1 ? "pos" : sentiment < -0.1 ? "neg" : "neu";
  const sourceType = String(event.source_type || event.type || "news").toLowerCase();
  const icon = sourceType.includes("filing") || sourceType.includes("exchange") ? "📄" : sourceType.includes("earning") || sourceType.includes("result") ? "🎙" : "📰";
  const importance = Math.max(0, Math.min(100, Number(event.importance ?? Math.abs(Number(event.net_score || 0)) ?? 50)));
  const filledStars = Math.max(1, Math.min(5, Math.round(importance / 20)));
  const stars = "★★★★★".slice(0, filledStars) + "☆☆☆☆☆".slice(0, 5 - filledStars);
  const days = Number.isFinite(Number(event.days_old)) ? `${event.days_old}d ago` : "fresh";
  const score = Number(event.net_score ?? event.score ?? sentiment);
  const sentimentLabel = Number.isFinite(score) ? (score > 0 ? `+${score.toFixed(2)}` : score.toFixed(2)) : "NA";
  const barWidth = Math.max(10, Math.min(100, Math.abs(score) || importance));
  return `<article class="event ${type}" style="--event-strength:${barWidth}%">
    <div class="event-impact-bar"></div>
    <div class="event-main">
      <div class="event-top">
        <div class="event-meta-row">
          <span class="source-icon">${icon}</span>
          <span class="source-type">${escapeHtml(event.source_type || event.type || "news")}</span>
          <span class="importance-stars">${stars}</span>
          <span class="days-badge">${escapeHtml(days)}</span>
        </div>
        <span class="sentiment-chip ${type === "pos" ? "green" : type === "neg" ? "red" : "amber"}">${sentimentLabel}</span>
      </div>
      <strong>${escapeHtml(event.title || "Untitled event")}</strong>
      <div class="event-footer">
        ${event.symbol ? `<button class="symbol-chip" data-symbol="${event.symbol}">${escapeHtml(event.symbol)}</button>` : ""}
        <span>${escapeHtml(event.source || "Source")}</span>
      </div>
    </div>
  </article>`;
}

function renderTechnical(stock) {
  const t = stock.technical_strength || {};
  const i = t.indicators || {};
  const checks = t.checks || {};
  const ma = i.moving_average_labels || {fast:"20 EMA", medium:"50 EMA", long:"200 DMA"};
  const base = i.base_quality || {};
  const eventVolume = i.event_volume_context || {};
  return `
    <div class="grid two">
      <section class="panel">
        <div class="section-kicker">Technical snapshot | ${t.timeframe || "weekly"}</div>
        ${metric(ma.fast, money(i.fast_ma ?? i.ema20))}
        ${metric(ma.medium, money(i.medium_ma ?? i.ema50))}
        ${metric(ma.long, money(i.long_ma ?? i.dma200))}
        ${metric("RSI 14", i.rsi14 ?? "NA")}
        ${metric("ATR 14", money(i.atr14))}
        ${metric("Volume ratio", `${i.volume_ratio ?? "NA"}x`)}
        ${metric("Breakout level", money(i.breakout_level))}
        ${metric("Relative strength", `${i.relative_strength?.state || "NA"} (${i.relative_strength?.pct ?? 0}%)`)}
        ${metric("Base quality", `${base.score ?? "NA"}/100`)}
        ${metric("Days in base", base.days_in_base ?? "NA")}
        ${metric("Base tightness", base.tightness_pct === null || base.tightness_pct === undefined ? "NA" : `${base.tightness_pct}%`)}
        ${metric("Event-day volume", eventVolume.fresh_official_event ? "Yes" : "No")}
      </section>
      <section class="panel">
        <div class="section-kicker">Breakout checklist</div>
        ${Object.keys(checks).map(key => check(key.replaceAll("_"," "), checks[key])).join("") || "<div class='status-line'>Backend checklist not loaded in demo mode.</div>"}
        ${(t.fake_breakout_flags || []).map(flag => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(flag)}</div></div>`).join("")}
      </section>
      ${stock.monthly_technical_strength ? `<section class="panel"><div class="section-kicker">Monthly / positional structure</div>
        ${metric("Monthly technical score", stock.monthly_technical_strength.score)}
        ${metric("Timeframe", stock.monthly_technical_strength.timeframe)}
        ${metric("Breakout level", money(stock.monthly_technical_strength.indicators?.breakout_level))}
        ${metric("Relative strength", `${stock.monthly_technical_strength.indicators?.relative_strength?.state || "NA"} (${stock.monthly_technical_strength.indicators?.relative_strength?.pct ?? 0}%)`)}
      </section>` : ""}
    </div>`;
}

function renderEntry(stock) {
  const entry = stock.entry || {};
  const exits = stock.exit_rules || {};
  return `
    ${renderEntryPlan(stock)}
    <section class="panel" style="margin-top:14px">
      <div class="panel-title">Exit Layers</div>
      ${metric("Invalidation", entry.invalidation || "NA")}
      ${metric("Price stop", exits.price_stop || "NA")}
      ${metric("Swing trend exit", exits.swing_trend_exit || "NA")}
      ${metric("Positional trend exit", exits.positional_trend_exit || "NA")}
      ${metric("Event exit", exits.event_exit || "NA")}
    </section>`;
}

function renderThesis(stock) {
  const saved = JSON.parse(localStorage.getItem(`thesis_${stock.symbol}`) || "{}");
  return `<section class="panel">
    <div class="events-head">
      <div>
        <div class="panel-title">Thesis Tracker</div>
        <div class="status-line">Journal the setup, then generate a premium narrative from the scoring JSON.</div>
      </div>
      <button class="btn blue" data-generate-thesis="${stock.symbol}">Generate thesis</button>
    </div>
    <div id="llmThesis" class="premium-thesis ${saved.generatedThesis ? "" : "hidden"}">${escapeHtml(saved.generatedThesis || "")}</div>
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

async function generatePremiumThesis(symbol) {
  const box = qs("llmThesis");
  if (box) {
    box.classList.remove("hidden");
    box.textContent = "Generating thesis from backend scoring JSON...";
  }
  try {
    const payload = await getJson(`/api/thesis/${symbol}/premium`);
    const text = payload.thesis || payload.text || "No thesis returned by backend.";
    if (box) box.textContent = text;
    const saved = JSON.parse(localStorage.getItem(`thesis_${symbol}`) || "{}");
    saved.generatedThesis = text;
    saved.savedAt = new Date().toISOString();
    localStorage.setItem(`thesis_${symbol}`, JSON.stringify(saved));
  } catch (error) {
    if (box) box.textContent = `Premium thesis failed: ${error.message}. Check ANTHROPIC_API_KEY on Render or use the structured thesis above.`;
  }
}

function saveThesis() {
  if (!selectedStock) return;
  const existing = JSON.parse(localStorage.getItem(`thesis_${selectedStock.symbol}`) || "{}");
  const payload = {
    generatedThesis: existing.generatedThesis || "",
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
    ["Real-time prices", "Shoonya WebSocket via FastAPI", "Backend only; no broker key in browser; market-data-only"],
    ["All-NSE search", "NSE equity master + daily bhavcopy", "Free EOD cache for symbol/name search and liquidity screening"],
    ["Database", "Optional PostgreSQL", "DATABASE_URL persists companies, EOD OHLCV, fundamentals, trade state, and market regime"],
    ["Market regime", "Yahoo ^NSEI + NSE advance/decline", "Call /api/market/refresh or /api/scheduled/daily after market close"],
    ["Price OHLCV", "Yahoo Finance chart endpoint", "No key; 5-minute and daily bars for personal research"],
    ["Optional fallback", "Alpha Vantage", "Only needed if you later get a key"],
    ["News", "Yahoo Finance RSS + Google News RSS + GDELT", "No key; cached by backend"],
    ["Official filings", "NSE/BSE corporate filings", "Public pages; respect rate limits"],
    ["Fundamentals", "Annual reports, exchange filings, CSV import", "Free manual import is safest for personal use"]
  ];
  return `<section class="panel">
    <div class="section-kicker">Free and low-cost API stack</div>
    <div class="api-list">
      ${stack.map(row => `<div class="api-line"><strong>${row[0]}</strong><span>${row[1]}</span><span class="subtle">${row[2]}</span></div>`).join("")}
    </div>
    <div style="margin-top:12px" class="notice">Yahoo Finance and RSS news work without keys, but they are not licensed exchange feeds. Good for personal research, not guaranteed for trading automation.</div>
    <div style="margin-top:12px" class="notice">Shoonya status: ${escapeHtml(liveStatus.status || "unknown")} | configured: ${liveStatus.configured ? "yes" : "no"} | feed open: ${liveStatus.feed_open ? "yes" : "no"}</div>
    <section style="margin-top:12px">
      <div class="section-kicker">Shoonya OTP login</div>
      <div class="status-line">Use this only if you do not have a TOTP secret. Enter the current 6-digit OTP and start the live feed without redeploying Render.</div>
      <input class="input" id="shoonyaOtpInput" inputmode="numeric" autocomplete="one-time-code" placeholder="Current Shoonya OTP / TOTP">
      <button class="btn blue" id="submitShoonyaOtpBtn" style="margin-top:10px">Start Shoonya live feed</button>
    </section>
    <div style="margin-top:12px" class="notice">Alert path: schedule GET /api/scheduled/daily, then wire alert_scan.alerts to Telegram/email from Supabase Edge Function, Render cron, or any external cron.</div>
    <label style="display:block;margin-top:12px">Backend URL<input class="input" id="apiBaseInput" value="${escapeHtml(apiBase)}"></label>
    <button class="btn primary" id="saveApiBaseBtn" style="margin-top:10px">Save backend URL</button>
  </section>`;
}

async function refreshMarket() {
  qs("offlineNotice").textContent = "Refreshing Nifty regime, NSE breadth, and sector rotation...";
  qs("offlineNotice").classList.remove("hidden");
  try {
    const result = await getJson("/api/market/refresh", {method: "POST"});
    dashboard.market_regime = result.market;
    qs("offlineNotice").textContent = (result.notes || []).join(" | ") || "Market refresh complete";
    renderAll();
  } catch (error) {
    qs("offlineNotice").textContent = `Market refresh failed: ${error.message}`;
  }
}

async function scanAlerts() {
  qs("offlineNotice").textContent = "Scanning watchlist transitions...";
  qs("offlineNotice").classList.remove("hidden");
  try {
    const result = await getJson("/api/scan/alerts", {method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({refresh_market:false})});
    qs("offlineNotice").textContent = result.alerts?.length ? result.alerts.map(alert => alert.message).join(" | ") : "No Watchlist -> Triggered alerts right now.";
  } catch (error) {
    qs("offlineNotice").textContent = `Alert scan failed: ${error.message}`;
  }
}

async function syncBhavcopy() {
  setNotice("Syncing latest available NSE bhavcopy into Supabase...", "blue");
  try {
    const result = await getJson("/api/database/sync-bhavcopy", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({force:true})
    });
    databaseStatus = result.database || databaseStatus;
    if (dashboard) dashboard.database = databaseStatus;
    const counts = databaseStatus?.counts || {};
    setNotice(`Supabase bhavcopy synced. Latest ${counts.latest_bhavcopy_date || "unknown"}; rows ${countText(counts.daily_ohlcv)}; retained dates ${(counts.bhavcopy_dates || []).join(", ") || "NA"}.`, "blue");
    renderAll();
  } catch (error) {
    setNotice(`Supabase bhavcopy sync failed: ${error.message}`, "red");
  }
}

function setYahooEnrichUi(message) {
  yahooEnrichStatusText = message || "";
  if (!qs("homeView")?.classList.contains("hidden")) renderHome();
}

function yahooEnrichLine(status, startedAt) {
  const pct = Number(status.progress || 0);
  const total = status.total ?? status.requested_symbols ?? "";
  const processed = status.processed ?? "";
  const enriched = status.enriched ?? "";
  const skipped = status.skipped_existing ?? "";
  const skippedUnsupported = status.skipped_unsupported ?? "";
  const rows = status.history_rows ?? "";
  const elapsed = startedAt ? `${Math.max(1, Math.round((Date.now() - startedAt) / 1000))}s elapsed` : "";
  return `[${pct}%] ${elapsed}${elapsed ? " - " : ""}${status.message || "Yahoo enrichment running..."}` +
    (processed !== "" && total !== "" ? ` | processed ${countText(processed)}/${countText(total)}` : "") +
    (enriched !== "" ? ` | enriched ${countText(enriched)}` : "") +
    (rows !== "" ? ` | rows ${countText(rows)}` : "") +
    (skipped ? ` | skipped existing ${countText(skipped)}` : "") +
    (skippedUnsupported ? ` | skipped ETFs/index-like ${countText(skippedUnsupported)}` : "");
}

async function syncYahooData() {
  if (yahooEnrichRunning) return;
  const secret = window.prompt("Enter ADMIN_SECRET to start Yahoo enrichment:");
  if (!secret) return;
  const limitRaw = window.prompt("How many NSE symbols to enrich? Type all for the full universe, or a number for testing.", "all");
  if (limitRaw === null) return;
  const force = window.confirm("Force re-fetch symbols that already have enough Yahoo history? Press Cancel to skip already enriched symbols.");
  yahooEnrichRunning = true;
  const startedAt = Date.now();
  setYahooEnrichUi("Starting Yahoo Finance enrichment in Supabase...");
  setNotice("Yahoo enrichment started. This can take a long time, but it runs in the background and reports progress.", "blue");
  try {
    const trigger = await getJson("/api/admin/enrich-yahoo", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({secret, limit: limitRaw || "all", force})
    });
    setYahooEnrichUi(trigger.message || "Yahoo enrichment started. Polling status...");
    if (yahooEnrichPollTimer) clearInterval(yahooEnrichPollTimer);
    const poll = async () => {
      const status = await getJson(`/api/admin/enrich-yahoo/status?t=${Date.now()}`);
      setYahooEnrichUi(yahooEnrichLine(status, startedAt));
      if (status.status === "complete") {
        if (yahooEnrichPollTimer) clearInterval(yahooEnrichPollTimer);
        yahooEnrichPollTimer = null;
        yahooEnrichRunning = false;
        const result = status.result || status;
        setYahooEnrichUi(`Yahoo sync complete. Enriched ${countText(result.enriched || status.enriched)} symbols, stored ${countText(result.history_rows || status.history_rows)} rows. Now run Full NSE Scan.`);
        setNotice("Yahoo enrichment complete. Run Full NSE Scan again to use the stored history.", "blue");
        try {
          databaseStatus = await getJson("/api/database/status");
          if (dashboard) dashboard.database = databaseStatus;
        } catch {}
        renderAll();
      } else if (status.status === "error") {
        if (yahooEnrichPollTimer) clearInterval(yahooEnrichPollTimer);
        yahooEnrichPollTimer = null;
        yahooEnrichRunning = false;
        setNotice(`Yahoo enrichment failed: ${status.error || status.message || "unknown error"}`, "red");
        renderAll();
      }
    };
    await poll();
    if (yahooEnrichRunning) {
      yahooEnrichPollTimer = setInterval(() => {
        poll().catch(error => {
          const elapsed = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
          setYahooEnrichUi(`Still syncing Yahoo data... ${elapsed}s elapsed. Status polling temporarily failed: ${error.message}`);
        });
      }, 10000);
    }
  } catch (error) {
    yahooEnrichRunning = false;
    setYahooEnrichUi("");
    setNotice(`Yahoo enrichment failed: ${error.message}`, "red");
    renderAll();
  }
}

async function refreshTopCandidates() {
  return runFullNseScan();
}

function setScanUi(message) {
  scanStatusText = message || "";
  if (!qs("homeView")?.classList.contains("hidden")) renderHome();
}

function normalizeFullScanPayload(payload) {
  const isFullScan = payload.dashboard_mode === "full_nse_scan" || payload.scan_meta?.data_mode === "real_nse_only";
  const stocks = payload.stocks || payload.focus?.stocks || (isFullScan ? [] : dashboard?.stocks || []);
  const realRows = stocks.filter(stock => {
    return !isFullScan || ["real_nse_scored", "supabase_yahoo_history_scored", "nse_eod_proxy_scored", "nse_eod_batch_scored", "nse_dynamic_real", "nse_dynamic_focus"].includes(stock.data_mode || "");
  });
  const strictFiltered = realRows.filter(stock => {
    const weekly = Number(stock.weekly_score || 0);
    const risk = Number(stock.risk_score || 0);
    return weekly >= 50 && risk < 25 && !["Avoid", "Hard Avoid"].includes(stock.conviction);
  });
  const payloadWeekly = payload.top_weekly || payload.top_weekly_candidates || [];
  const payloadMonthly = payload.top_monthly || payload.top_monthly_candidates || [];
  const fallbackWeekly = (strictFiltered.length ? strictFiltered : realRows)
    .filter(stock => stock.conviction !== "Hard Avoid")
    .sort((a, b) => Number(b.weekly_score || 0) - Number(a.weekly_score || 0));
  const fallbackMonthly = (strictFiltered.length ? strictFiltered : realRows)
    .filter(stock => stock.conviction !== "Hard Avoid")
    .sort((a, b) => Number(b.monthly_score || 0) - Number(a.monthly_score || 0));
  return {
    stocks,
    topWeekly: (payloadWeekly.length ? payloadWeekly : fallbackWeekly).slice(0, 3),
    topMonthly: (payloadMonthly.length ? payloadMonthly : fallbackMonthly).slice(0, 3),
    scanMeta: payload.scan_meta || payload.scan_status?.scan_meta || {},
    generatedAt: payload.generated_at || new Date().toISOString(),
    marketRegime: payload.market_regime,
    events: payload.latest_critical_events,
    sectorMap: payload.sector_map,
  };
}

async function runFullNseScan() {
  if (scanRunning) return;
  scanRunning = true;
  topCandidateState.loading = true;
  setScanUi("Connecting to NSE data pipeline...");
  setNotice("Full NSE scan started. This can take several minutes. Progress will update automatically.", "blue");
  let pollTimer = null;
  const scanStartTime = Date.now();

  const finishScan = (success) => {
    if (pollTimer) clearInterval(pollTimer);
    scanRunning = false;
    topCandidateState.loading = false;
    if (success) clearNotice();
    renderAll();
    connectLiveFeed();
  };

  const applyFullScanPayload = (payload) => {
    const normalized = normalizeFullScanPayload(payload);
    if (payload.dashboard_mode !== "full_nse_scan" && normalized.scanMeta?.data_mode !== "real_nse_only") {
      throw new Error("Backend returned non-full-scan data. Refusing to show demo/focus picks as full NSE picks.");
    }
    dashboard.stocks = normalized.stocks;
    dashboard.top_weekly_candidates = normalized.topWeekly;
    dashboard.top_monthly_candidates = normalized.topMonthly;
    if (normalized.marketRegime) dashboard.market_regime = normalized.marketRegime;
    if (normalized.events) dashboard.latest_critical_events = normalized.events;
    if (normalized.sectorMap) dashboard.sector_map = normalized.sectorMap;
    dashboard.scan_meta = normalized.scanMeta;
    dashboard.dashboard_mode = "full_nse_scan";
    scanWeeklyResults = normalized.topWeekly;
    scanMonthlyResults = normalized.topMonthly;
    scanMeta = normalized.scanMeta;
    topCandidateState.lastCalculatedAt = normalized.generatedAt;
    lastSuccessfulFetch = Date.now();
    currentDashboardDelay = DASHBOARD_POLL_MS;
    const proxyText = scanMeta.proxy_scored ? `, proxy ${countText(scanMeta.proxy_scored)}` : "";
    const enrichedText = scanMeta.enriched_history_scored ? `, enriched-history ${countText(scanMeta.enriched_history_scored)}` : "";
    const basisText = scanMeta.rank_basis === "watch_only_relaxed" ? " Watch-only fallback shown because strict buy gates found no names." : "";
    const basisMore = scanMeta.rank_basis === "best_available_research_only" ? " Best available research ranking shown because strict gates found no names." : basisText;
    setScanUi(`Scan complete. Universe ${countText(scanMeta.universe_size)}, passed ${countText(scanMeta.passed_liquidity)}, scored ${countText(scanMeta.total_scored)}${enrichedText}${proxyText}, ranked ${countText(scanMeta.ranked_candidates)}.${basisMore}`);
  };

  const pollStatus = async () => {
    const status = await getJson(`/api/admin/full-nse-scan/status?t=${Date.now()}`);
    const meta = status.scan_meta || {};
    scanMeta = {...(scanMeta || {}), ...meta};
    const pct = status.progress || 0;
    const elapsed = Math.max(1, Math.round((Date.now() - scanStartTime) / 1000));
    const scored = meta.scored_so_far ?? meta.total_scored ?? "";
    const passed = meta.passed_liquidity ?? "";
    const skipped = meta.skipped_so_far ?? meta.skipped_insufficient_data ?? "";
    const proxy = meta.proxy_so_far ?? meta.proxy_scored ?? "";
    const enriched = meta.db_history?.symbols_with_history ?? meta.enriched_history_scored ?? "";
    setScanUi(
      `[${pct}%] ${elapsed}s elapsed - ${status.message || "Scanning real NSE universe..."}` +
      (scored !== "" ? ` | scored ${countText(scored)}` : "") +
      (passed !== "" ? ` / ${countText(passed)}` : "") +
      (enriched !== "" ? ` | enriched history ${countText(enriched)}` : "") +
      (proxy !== "" ? ` | proxy ${countText(proxy)}` : "") +
      (skipped !== "" ? ` | skipped ${countText(skipped)}` : "")
    );
    if (status.status === "complete") {
      try {
        const result = await getJson(`/api/admin/full-nse-scan/result?t=${Date.now()}`);
        applyFullScanPayload(result);
        finishScan(true);
      } catch (applyErr) {
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = null;
        setNotice(`Scan result error: ${applyErr.message}`, "red");
        finishScan(false);
      }
    } else if (status.status === "error") {
      setNotice(`Full NSE scan failed: ${status.error || status.message || "unknown error"}`, "red");
      finishScan(false);
    } else if (status.status === "idle" && !status.worker_alive) {
      setNotice("Full NSE scan worker is not running. The backend may have restarted; check Render logs, then start the scan again manually.", "red");
      finishScan(false);
    } else {
      // Still queued/running.
    }
  };

  try {
    const trigger = await getJson("/api/admin/run-full-nse-scan", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({scan_limit: "all", force: true})
    });
    if (trigger.dashboard_mode === "full_nse_scan" || trigger.stocks) {
      applyFullScanPayload(trigger);
      finishScan(true);
      return;
    }
    setScanUi(trigger.message || "Full NSE scan started. Polling status...");
    await pollStatus();
    if (!scanRunning) return;
    pollTimer = setInterval(() => {
      pollStatus().catch(error => {
        const elapsed = Math.max(1, Math.round((Date.now() - scanStartTime) / 1000));
        setScanUi(`Still scanning... ${elapsed}s elapsed. Status polling temporarily failed: ${error.message}`);
      });
    }, 10000);
  } catch (error) {
    setScanUi("");
    setNotice(`Full NSE scan failed: ${error.message}`, "red");
    finishScan(false);
  }
}

async function submitShoonyaOtp() {
  const twofa = qs("shoonyaOtpInput")?.value.trim();
  if (!twofa) {
    qs("offlineNotice").textContent = "Enter the current Shoonya OTP first.";
    qs("offlineNotice").classList.remove("hidden");
    return;
  }
  qs("offlineNotice").textContent = "Sending Shoonya OTP to backend...";
  qs("offlineNotice").classList.remove("hidden");
  try {
    liveStatus = await getJson("/api/live/twofa", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({twofa})
    });
    qs("offlineNotice").textContent = `Shoonya status: ${liveStatus.status || "unknown"}`;
    if (liveSocket) liveSocket.close();
    liveSocket = null;
    connectLiveFeed();
    renderMarketStrip();
  } catch (error) {
    qs("offlineNotice").textContent = `Shoonya OTP login failed: ${error.message}`;
  }
}

function connectLiveFeed() {
  if (!dashboard || !dashboard.stocks || isDemo) return;
  if (liveSocket && [WebSocket.OPEN, WebSocket.CONNECTING].includes(liveSocket.readyState)) return;
  const symbols = dashboard.stocks.map(stock => stock.symbol).join(",");
  const url = `${websocketBase()}/ws/live-prices?symbols=${encodeURIComponent(symbols)}`;
  try {
    liveSocket = new WebSocket(url);
  } catch (error) {
    liveStatus = {status: "socket_error", configured: false, last_error: error.message};
    renderMarketStrip();
    return;
  }
  liveStatus = {status: "connecting", configured: Boolean(dashboard.live_feed?.configured)};
  renderMarketStrip();
  liveSocket.onmessage = event => {
    const payload = JSON.parse(event.data);
    if (payload.type === "status") {
      liveStatus = payload.status || {};
      renderMarketStrip();
      if (!liveStatus.configured) {
        qs("offlineNotice").textContent = "Shoonya live feed is not configured on backend. Add Render environment variables to enable automatic ticks.";
        qs("offlineNotice").classList.remove("hidden");
      }
      return;
    }
    if (payload.type === "ticks") {
      (payload.ticks || []).forEach(updateLiveTick);
      scheduleLiveRender();
    }
  };
  liveSocket.onclose = () => {
    liveStatus = {...liveStatus, feed_open: false, status: "disconnected"};
    renderMarketStrip();
    clearTimeout(liveReconnectTimer);
    liveReconnectTimer = setTimeout(connectLiveFeed, 5000);
  };
  liveSocket.onerror = () => {
    liveStatus = {...liveStatus, status: "socket_error"};
    renderMarketStrip();
  };
}

function updateLiveTick(tick) {
  const symbol = tick.symbol;
  const ltp = Number(tick.ltp);
  if (!symbol || !Number.isFinite(ltp)) return;
  const stock = dashboard.stocks.find(item => item.symbol === symbol);
  if (stock) {
    const previousPrice = Number(stock.price);
    stock.price = ltp;
    if (Number.isFinite(previousPrice) && previousPrice !== ltp) stock._pulse = ltp > previousPrice ? "up" : "down";
    if (Number.isFinite(Number(tick.change_pct))) stock.change_pct = Number(tick.change_pct);
    stock.live_source = tick.source;
    stock.live_timestamp = tick.timestamp;
    if (stock.candidate && stock.entry?.breakout_level && ltp >= Number(stock.entry.breakout_level)) {
      stock.trade_state = {...(stock.trade_state || {}), state: "Triggered", reason: "Live Shoonya tick crossed breakout level"};
    }
  }
  if (selectedStock?.symbol === symbol) {
    const previousSelected = Number(selectedStock.price);
    selectedStock.price = ltp;
    if (Number.isFinite(previousSelected) && previousSelected !== ltp) selectedStock._pulse = ltp > previousSelected ? "up" : "down";
    selectedStock.change_pct = Number.isFinite(Number(tick.change_pct)) ? Number(tick.change_pct) : selectedStock.change_pct;
    selectedStock.live_source = tick.source;
    selectedStock.live_timestamp = tick.timestamp;
    if (selectedStock.candidate && selectedStock.entry?.breakout_level && ltp >= Number(selectedStock.entry.breakout_level)) {
      selectedStock.trade_state = {...(selectedStock.trade_state || {}), state: "Triggered", reason: "Live Shoonya tick crossed breakout level"};
    }
    updateSelectedBarsFromTick(tick);
  }
}

function updateSelectedBarsFromTick(tick) {
  const bars = selectedStock?.bars;
  if (!Array.isArray(bars)) return;
  const ltp = Number(tick.ltp);
  if (!Number.isFinite(ltp)) return;
  const today = new Date().toISOString().slice(0, 10);
  let last = bars[bars.length - 1];
  const lastDay = String(last?.datetime || "").slice(0, 10);
  if (!last || lastDay !== today) {
    const previous = Number(last?.close || ltp);
    bars.push({datetime: today, open: previous, high: ltp, low: ltp, close: ltp, volume: Number(tick.volume || 0)});
  } else {
    last.close = ltp;
    last.high = Math.max(Number(last.high || ltp), Number(tick.high || ltp), ltp);
    last.low = Math.min(Number(last.low || ltp), Number(tick.low || ltp), ltp);
    if (Number.isFinite(Number(tick.volume))) last.volume = Number(tick.volume);
  }
}

function scheduleLiveRender() {
  if (liveRenderTimer) return;
  liveRenderTimer = setTimeout(() => {
    liveRenderTimer = null;
    renderStockList();
    if (selectedStock) renderStockView(selectedStock);
    (dashboard.stocks || []).forEach(stock => { delete stock._pulse; });
    if (selectedStock) delete selectedStock._pulse;
  }, 1200);
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

function emaLine(values, period) {
  const out = [];
  const multiplier = 2 / (period + 1);
  let prev = null;
  values.forEach((value, index) => {
    if (index < period - 1) {
      out.push(null);
      return;
    }
    if (prev === null) prev = values.slice(index - period + 1, index + 1).reduce((sum, item) => sum + item, 0) / period;
    else prev = (value - prev) * multiplier + prev;
    out.push(prev);
  });
  return out;
}

function smaLine(values, period) {
  return values.map((_, index) => {
    if (index < period - 1) return null;
    const slice = values.slice(index - period + 1, index + 1);
    return slice.reduce((sum, item) => sum + item, 0) / period;
  });
}

function barTime(bar) {
  return String(bar.time || bar.datetime || "").slice(0, 10);
}

function lineDataFrom(values, candleData) {
  return values.map((value, index) => Number.isFinite(value) ? {time: candleData[index].time, value: Number(value)} : null).filter(Boolean);
}

function drawLightweightChart(target, rows, benchmarkBars = []) {
  if (!window.LightweightCharts || !target) return false;
  const chartRows = (rows || []).slice(-260).filter(bar =>
    Number.isFinite(Number(bar.open ?? bar.close)) &&
    Number.isFinite(Number(bar.high ?? bar.close)) &&
    Number.isFinite(Number(bar.low ?? bar.close)) &&
    Number.isFinite(Number(bar.close))
  );
  if (!chartRows.length) return false;
  if (currentChart?.remove) currentChart.remove();
  if (chartResizeObserver) chartResizeObserver.disconnect();
  target.innerHTML = "";
  const chart = LightweightCharts.createChart(target, {
    width: Math.max(320, target.clientWidth || 720),
    height: Math.max(220, target.clientHeight || 320),
    layout: {background: {type: "solid", color: "#111520"}, textColor: "#94a3b8", fontFamily: "Inter, system-ui, sans-serif"},
    grid: {vertLines: {color: "#1e2535"}, horzLines: {color: "#1e2535"}},
    crosshair: {mode: LightweightCharts.CrosshairMode.Normal},
    rightPriceScale: {borderColor: "#232b3e"},
    timeScale: {borderColor: "#232b3e", timeVisible: false},
  });
  const candleSeries = chart.addCandlestickSeries({
    upColor: "#10b981",
    downColor: "#f43f5e",
    wickUpColor: "#10b981",
    wickDownColor: "#f43f5e",
    borderVisible: false,
  });
  const candleData = chartRows.map(bar => ({
    time: barTime(bar),
    open: Number(bar.open ?? bar.close),
    high: Number(bar.high ?? bar.close),
    low: Number(bar.low ?? bar.close),
    close: Number(bar.close),
  }));
  candleSeries.setData(candleData);

  const closes = chartRows.map(bar => Number(bar.close));
  const ema20 = emaLine(closes, 20);
  const sma200 = smaLine(closes, 200);
  chart.addLineSeries({color: "#f59e0b", lineWidth: 2, priceLineVisible: false, lastValueVisible: false}).setData(lineDataFrom(ema20, candleData));
  chart.addLineSeries({color: "#10b981", lineWidth: 2, priceLineVisible: false, lastValueVisible: false}).setData(lineDataFrom(sma200, candleData));

  const volumeSeries = chart.addHistogramSeries({
    priceFormat: {type: "volume"},
    priceScaleId: "volume",
    priceLineVisible: false,
    lastValueVisible: false,
  });
  volumeSeries.priceScale().applyOptions({scaleMargins: {top: 0.82, bottom: 0}});
  volumeSeries.setData(chartRows.map((bar, index) => ({
    time: candleData[index].time,
    value: Number(bar.volume || 0),
    color: Number(bar.close) >= Number(bar.open ?? bar.close) ? "rgba(16, 185, 129, 0.28)" : "rgba(244, 63, 94, 0.28)",
  })));

  const benchRows = (benchmarkBars || []).slice(-chartRows.length).filter(bar => Number.isFinite(Number(bar.close)));
  if (benchRows.length >= 20) {
    const aligned = chartRows.slice(-benchRows.length);
    const rsData = aligned.map((bar, index) => ({
      time: barTime(bar),
      value: Number(bar.close) / Math.max(Number(benchRows[index].close), 1) * 100,
    }));
    const rsSeries = chart.addLineSeries({color: "#8b5cf6", lineWidth: 1, priceScaleId: "rs", priceLineVisible: false, lastValueVisible: false});
    rsSeries.priceScale().applyOptions({scaleMargins: {top: 0.66, bottom: 0.18}});
    rsSeries.setData(rsData);
  }

  const entry = selectedStock?.entry || {};
  const high52 = Math.max(...chartRows.map(bar => Number(bar.high || bar.close)));
  [
    {value: Number(entry.breakout_level), title: "Breakout", color: "#f59e0b"},
    {value: Number(entry.stop), title: "Stop", color: "#f43f5e"},
    {value: high52, title: "52W high", color: "#e2e8f0"},
  ].filter(line => Number.isFinite(line.value) && line.value > 0).forEach(line => {
    candleSeries.createPriceLine({
      price: line.value,
      color: line.color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title: line.title,
    });
  });
  chart.timeScale().fitContent();
  const tooltip = qs("chartTooltip");
  chart.subscribeCrosshairMove(param => {
    if (!tooltip || !param?.time || !param.seriesData) {
      if (tooltip) tooltip.classList.add("hidden");
      return;
    }
    const data = param.seriesData.get(candleSeries);
    if (!data) {
      tooltip.classList.add("hidden");
      return;
    }
    tooltip.classList.remove("hidden");
    tooltip.innerHTML = `<strong>${param.time}</strong> O ${money(data.open)} H ${money(data.high)} L ${money(data.low)} C ${money(data.close)}`;
    tooltip.style.left = `${Math.min(Math.max(param.point?.x || 0, 12), target.clientWidth - 260)}px`;
    tooltip.style.top = `${Math.max((param.point?.y || 0) - 42, 8)}px`;
  });
  chartResizeObserver = new ResizeObserver(() => chart.applyOptions({width: target.clientWidth, height: target.clientHeight}));
  chartResizeObserver.observe(target);
  currentChart = chart;
  return true;
}

async function drawChart(bars, canvasId = "priceChart", benchmarkBars = []) {
  const target = qs(canvasId);
  if (!target) return;
  let rows = bars || [];
  let bench = benchmarkBars || [];
  if ((canvasId === "priceChart" || canvasId === "overviewChart") && selectedStock?.symbol && !isDemo) {
    try {
      const payload = await getJson(`/api/chart/${selectedStock.symbol}?range=${activeChartPeriod}&fresh=1`);
      rows = payload.bars || rows;
      bench = payload.benchmark || payload.benchmark_bars || bench;
      selectedStock.bars = rows.map(bar => ({...bar, datetime: bar.time || bar.datetime}));
      selectedStock.benchmark_bars = bench.map(bar => ({...bar, datetime: bar.time || bar.datetime}));
    } catch (error) {
      setNotice(`Chart refresh failed: ${error.message}`, "amber");
    }
  }
  if (!window.LightweightCharts) {
    target.innerHTML = "<div class='status-line'>Chart library did not load. Refresh the browser.</div>";
    return;
  }
  drawLightweightChart(target, rows, bench);
}

document.addEventListener("click", event => {
  const watchToggle = event.target.closest("[data-watch-toggle]");
  if (watchToggle) {
    event.preventDefault();
    event.stopPropagation();
    const symbol = watchToggle.dataset.watchToggle;
    const added = toggleWatchlist(symbol);
    setNotice(`${symbol} ${added ? "added to" : "removed from"} watchlist.`, added ? "blue" : "amber");
    renderStockList();
    if (selectedStock?.symbol === symbol) renderStockView(selectedStock);
    else if (!qs("homeView").classList.contains("hidden")) renderHome();
    return;
  }
  if (event.target.closest("[data-clear-watchlist]")) {
    event.preventDefault();
    writeWatchlist([]);
    setNotice("Watchlist cleared.", "amber");
    renderStockList();
    if (!qs("homeView").classList.contains("hidden")) renderHome();
    if (selectedStock) renderStockView(selectedStock);
    return;
  }
  if (event.target.closest("[data-recalculate-best]")) {
    refreshTopCandidates();
    return;
  }
  if (event.target.closest("[data-sync-bhavcopy]")) {
    syncBhavcopy();
    return;
  }
  if (event.target.closest("[data-sync-yahoo]")) {
    syncYahooData();
    return;
  }
  const newsFilter = event.target.closest("[data-news-filter]");
  if (newsFilter) {
    activeNewsFilter = newsFilter.dataset.newsFilter || "all";
    if (activeTab === "events" && selectedStock) renderStockView(selectedStock);
    else renderHome();
    return;
  }
  const stockButton = event.target.closest("[data-symbol]");
  if (stockButton) {
    closeCommandPalette();
    selectStock(stockButton.dataset.symbol);
  }
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
    stopSelectedStockPolling();
    qs("searchInput").value = "";
    universeSearchRows = null;
    renderStockList();
    renderHome();
  }
  if (event.target.id === "topHomeBtn") qs("homeBtn")?.click();
  if (event.target.id === "commandSearchBtn") openCommandPalette();
  if (event.target.id === "saveThesisBtn") saveThesis();
  if (event.target.id === "submitShoonyaOtpBtn") submitShoonyaOtp();
  const thesisButton = event.target.closest("[data-generate-thesis]");
  if (thesisButton) generatePremiumThesis(thesisButton.dataset.generateThesis);
  const refresh = event.target.closest("[data-refresh]");
  if (refresh) refreshLive(refresh.dataset.refresh);
  if (event.target.closest("[data-market-refresh]")) refreshMarket();
  if (event.target.closest("[data-alert-scan]")) scanAlerts();
  if (event.target.closest("[data-export-watchlist]")) exportWatchlist();
  const sectorFilter = event.target.closest("[data-sector-filter]");
  if (sectorFilter) {
    qs("searchInput").value = sectorFilter.dataset.sectorFilter || "";
    handleSearchInput();
  }
  const eventRefresh = event.target.closest("[data-refresh-events]");
  if (eventRefresh) refreshEvents(eventRefresh.dataset.refreshEvents, true);
  const chartPeriod = event.target.closest("[data-chart-period]");
  if (chartPeriod && selectedStock) {
    activeChartPeriod = chartPeriod.dataset.chartPeriod;
    renderStockView(selectedStock);
  }
  const alertToggle = event.target.closest("[data-toggle-alert]");
  if (alertToggle && selectedStock) togglePriceAlert(selectedStock);
  if (event.target.id === "saveApiBaseBtn") {
    apiBase = qs("apiBaseInput").value.trim() || "http://127.0.0.1:8000";
    localStorage.setItem(API_BASE_KEY, apiBase);
    if (liveSocket) liveSocket.close();
    liveSocket = null;
    loadDashboard().then(startPolling);
  }
});

qs("searchInput").addEventListener("input", handleSearchInput);
qs("globalSearchInput")?.addEventListener("input", runCommandSearch);
document.addEventListener("keydown", event => {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    openCommandPalette();
  } else if (event.key === "Escape") {
    closeCommandPalette();
  } else if (!["INPUT", "TEXTAREA"].includes(document.activeElement?.tagName || "") && event.key === "ArrowDown") {
    event.preventDefault();
    moveKeyboardSelection(1);
  } else if (!["INPUT", "TEXTAREA"].includes(document.activeElement?.tagName || "") && event.key === "ArrowUp") {
    event.preventDefault();
    moveKeyboardSelection(-1);
  } else if (!["INPUT", "TEXTAREA"].includes(document.activeElement?.tagName || "") && event.key === "Enter" && selectedStock) {
    event.preventDefault();
    selectStock(selectedStock.symbol);
  }
});
window.addEventListener("resize", () => {
  if (activeTab === "chart" && selectedStock) drawChart(selectedStock.bars || [], "priceChart", selectedStock.benchmark_bars || []);
  if (activeTab === "overview" && selectedStock) drawChart(selectedStock.bars || [], "overviewChart", selectedStock.benchmark_bars || []);
});
loadDashboard().then(startPolling);
