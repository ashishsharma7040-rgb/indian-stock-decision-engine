const API_BASE_KEY = "stockEngineApiBase";
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
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[char]));
}
function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
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
function websocketBase() {
  const url = new URL(apiBase);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString().replace(/\/$/, "");
}

async function getJson(path, options = {}) {
  const response = await fetch(`${apiBase}${path}`, options);
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

async function loadDashboard() {
  try {
    dashboard = await getJson("/api/dashboard");
    isDemo = false;
    lastSuccessfulFetch = Date.now();
    currentDashboardDelay = DASHBOARD_POLL_MS;
    qs("offlineNotice").classList.add("hidden");
  } catch (error) {
    dashboard = makeDemoDashboard();
    isDemo = true;
    qs("offlineNotice").textContent = `Backend not running at ${apiBase}. Demo mode is active. Start FastAPI for live scoring and API refresh.`;
    qs("offlineNotice").classList.remove("hidden");
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
  const currentBySymbol = new Map((dashboard.stocks || []).map(stock => [stock.symbol, stock]));
  (fresh.stocks || []).forEach(freshStock => {
    const existing = currentBySymbol.get(freshStock.symbol);
    if (existing) Object.assign(existing, freshStock);
  });
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
}

function startPolling() {
  clearTimeout(dashboardPollTimer);
  const poll = async () => {
    if (!dashboard) {
      dashboardPollTimer = setTimeout(poll, currentDashboardDelay);
      return;
    }
    try {
      const fresh = await getJson("/api/dashboard");
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
      checkStaleData();
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
  const chips = [
    ["Regime", market.regime || "NA"],
    ["Market score", `${market.score ?? "NA"}/100`],
    ["Nifty", money(market.nifty_close || 0)],
    ["Breadth", `${market.breadth_above_50dma ?? "NA"}% adv`],
    ["Live", live.feed_open ? "Shoonya on" : live.configured ? "Connecting" : "Not configured"]
  ];
  qs("marketStrip").innerHTML = chips.map(([label, value]) => `
    <div class="market-chip">
      <div class="chip-label">${label}</div>
      <div class="chip-value">${value}</div>
    </div>`).join("");
}

function filteredStocks() {
  const text = qs("searchInput").value.trim().toLowerCase();
  const usingUniverseSearch = text.length >= 2 && Array.isArray(universeSearchRows);
  let rows = usingUniverseSearch ? [...universeSearchRows] : [...(dashboard.stocks || [])];
  if (!usingUniverseSearch) {
    if (activeFilter === "weekly") rows.sort((a,b) => b.weekly_score - a.weekly_score);
    if (activeFilter === "monthly") rows.sort((a,b) => b.monthly_score - a.monthly_score);
    if (activeFilter === "candidate") rows = rows.filter(s => s.candidate);
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
  qs("universeCount").textContent = searchText.length >= 2
    ? `${rows.length} results / ${universeTotal || "NSE"}`
    : `${dashboard.stocks.length} focus / ${universeTotal || dashboard.stocks.length} NSE`;
  qs("stockList").innerHTML = rows.map(stock => {
    const state = stateName(stock);
    const weekly = scoreValue(stock.weekly_score);
    const scoreLabel = stockScoreLabel(stock);
    const scoreBadgeClass = weekly === null ? "score-neutral" : scoreClass(weekly);
    const isSearchOnly = weekly === null;
    return `
      <button class="stock-row ${selectedStock?.symbol === stock.symbol ? "active" : ""}" data-symbol="${stock.symbol}">
        <div class="stock-card-main">
          <div class="stock-topline">
            <span class="stock-id"><span class="state-dot ${stateClass(state)}"></span><span class="stock-symbol">${escapeHtml(stock.symbol)}</span></span>
            ${isSearchOnly ? `<span class="pill blue">${escapeHtml(stock.series || "NSE")}</span>` : badgeForConviction(stock.conviction)}
          </div>
          <div class="stock-meta">${escapeHtml(stock.sector)} | ${escapeHtml(stock.industry || stock.name)}</div>
          <div class="mini-badges">
            ${isSearchOnly ? `<span>${escapeHtml(stock.name || stock.symbol)}</span>` : `<span class="state-badge ${stateClass(state)}">${escapeHtml(state)}</span>`}
            <span>${money(stock.price)}</span>
            <span>${percent(stock.change_pct)}</span>
            ${isSearchOnly ? `<span>${escapeHtml(stock.as_of || "EOD")}</span>` : `<span>W ${stock.weekly_score} / M ${stock.monthly_score}</span><span>Risk ${stock.risk_score}</span>`}
          </div>
          ${sparkline(stock.sparkline)}
        </div>
        <div class="score-cluster">
          <div class="score-badge ${scoreBadgeClass}">${scoreLabel}</div>
          <span class="score-caption">${isSearchOnly ? "search" : "weekly"}</span>
        </div>
      </button>`;
  }).join("");
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

function focusGrid(rows = []) {
  const topRows = [...rows].sort((a, b) => (b.weekly_score || 0) - (a.weekly_score || 0)).slice(0, 10);
  return `<div class="focus-grid">
    <div class="focus-grid-head">
      <span>Symbol</span><span>Price</span><span>Chg</span><span>W</span><span>M</span><span>Risk</span><span>State</span>
    </div>
    ${topRows.map(stock => {
      const state = stateName(stock);
      return `<button class="focus-grid-row ${stateClass(state)}" data-symbol="${stock.symbol}">
        <strong>${escapeHtml(stock.symbol)}</strong>
        <span>${money(stock.price)}</span>
        <span class="${Number(stock.change_pct || 0) >= 0 ? "positive" : "negative"}">${percent(stock.change_pct)}</span>
        <span class="${scoreClass(stock.weekly_score)}">${stock.weekly_score}</span>
        <span class="${scoreClass(stock.monthly_score)}">${stock.monthly_score}</span>
        <span>${stock.risk_score}</span>
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

function sectorHeatMap(rows = []) {
  if (!rows.length) return "<div class='status-line'>No sector map loaded.</div>";
  return `<div class="sector-heatmap">${rows.map(row => {
    const score = Number(row.avg_weekly_score ?? row.avg_weekly ?? 0);
    return `<button class="sector-tile ${scoreTone(score)}" data-sector-filter="${escapeHtml(row.sector)}">
      <strong>${escapeHtml(row.sector)}</strong>
      <span>${score.toFixed ? Number(score).toFixed(1) : score} weekly</span>
      <small>Leader ${escapeHtml(row.leader || "NA")} | ${row.count || 0} names</small>
    </button>`;
  }).join("")}</div>`;
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
        <div class="status-line">Focus list: ${dashboard.stocks.length} researched names from ${dashboard.nse_universe?.total || dashboard.stocks.length} NSE symbols. Search opens the full NSE EOD cache.</div>
      </div>
      <div class="flat-panel">
        <span class="pill ${market.regime === "Risk-on" ? "green" : market.regime === "Risk-off" ? "red" : "amber"}">${market.regime}</span>
        ${market.is_stale ? "<span class='pill red'>Market stale</span>" : "<span class='pill green'>Market fresh</span>"}
        <div class="status-line">Generated ${new Date(dashboard.generated_at).toLocaleString("en-IN")}</div>
        <div class="status-line">Prices as of ${formatIstTime(dashboard.prices_as_of || dashboard.generated_at)} IST</div>
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
        ${metric("Breadth proxy", `${market.breadth_above_50dma}% advancers`)}
        ${metric("Advance / Decline", market.advance_decline_ratio ?? "NA")}
        ${metric("Data source", market.source || "seed")}
        ${metric("VIX", String(market.vix))}
        <div class="bar"><span style="width:${market.score}%;background:${scoreColor(market.score)}"></span></div>
        <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap">
          <button class="btn blue" data-market-refresh="1">Refresh market</button>
          <button class="btn" data-alert-scan="1">Scan alerts</button>
          <button class="btn" data-export-watchlist="1">Export watchlist</button>
        </div>
      </section>
    </div>
    <div class="grid two" style="margin-top:14px">
      <section class="panel">
        <div class="section-kicker">Sector heat map</div>
        ${sectorHeatMap(dashboard.sector_map || dashboard.top_sectors || [])}
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
    <section class="panel" style="margin-top:14px">
      <div class="section-kicker">Focus research grid</div>
      ${focusGrid(dashboard.stocks || [])}
    </section>
    <section class="flat-panel" style="margin-top:14px">
      <strong>Rule:</strong> final score = business quality + sector tailwind + event strength + technical strength + market support - risk penalties.
      <div class="status-line">${dashboard.disclaimer || ""}</div>
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
  qs("stockView").innerHTML = `<section class="panel"><div class="section-kicker">Loading research view</div><h2>${escapeHtml(symbol)}</h2><div class="status-line">Fetching price history, market regime, and scoring rules...</div></section>`;
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
  const state = stateName(full);
  qs("stockView").innerHTML = `
    <div class="stock-head">
      <div>
        <div class="section-kicker">${full.sector} | ${full.industry || ""}</div>
        <h1>${full.symbol} - ${full.name}</h1>
        <div class="headline-badges">${badgeForConviction(full.conviction)}<span class="state-badge ${stateClass(state)}">${escapeHtml(state)}</span><span class="pill ${full.candidate ? "green" : "amber"}">Candidate ${full.candidate ? "Yes" : "No"}</span></div>
        <div class="status-line">Weekly ${full.weekly_score} | Monthly ${full.monthly_score}</div>
        <div class="status-line">Confidence ${full.confidence_interval?.label || "NA"} | margin ${full.confidence_interval?.min_margin_above_high ?? "NA"} above high threshold</div>
        <div class="status-line">Raw W ${full.weekly_raw_score ?? full.score_diagnostics?.weekly_raw ?? "NA"} | Raw M ${full.monthly_raw_score ?? full.score_diagnostics?.monthly_raw ?? "NA"}</div>
      </div>
      <div>
        <div class="price">${money(full.price)}</div>
        <div class="status-line" style="text-align:right">${percent(full.change_pct)}</div>
      </div>
    </div>
    ${scoreHistorySvg(full.score_history || [])}
    <div class="score-stack">
      ${scores.map(([label, value]) => `
        <div class="score-tile ${label === "Risk" ? "risk-tile" : ""}">
          <small>${label}</small>
          <strong style="color:${label === "Risk" ? scoreColor(100 - value) : scoreColor(value)}">${value}${label === "Technical" && full.weekly_score !== undefined && full.monthly_score !== undefined ? `<span class="trend-arrow">${full.weekly_score >= full.monthly_score ? "↑" : "↓"}</span>` : ""}</strong>
          <div class="bar"><span style="width:${value}%;background:${label === "Risk" ? "var(--red)" : scoreColor(value)}"></span></div>
        </div>`).join("")}
    </div>
    <div class="tabs">
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
  const questions = stock.explanation_json?.five_questions || {};
  const entry = stock.entry || {};
  const sizing = entry.position_sizing || {};
  return `
    <section class="panel setup-card">
      <div class="setup-head">
        <div>
          <div class="section-kicker">Trade setup</div>
          <h2>${escapeHtml(stock.symbol)} decision plan</h2>
          <div class="status-line">${escapeHtml(entry.candidate_gate || "Gate status not loaded")}</div>
        </div>
        ${badgeForConviction(stock.conviction)}
      </div>
      <div class="setup-grid">
        <div class="zone"><small>Breakout</small><strong>${money(entry.breakout_level)}</strong></div>
        <div class="zone"><small>Aggressive</small><strong>${zone(entry.aggressive)}</strong></div>
        <div class="zone"><small>Pullback</small><strong>${zone(entry.pullback)}</strong></div>
        <div class="zone"><small>Stop</small><strong>${money(entry.stop)}</strong></div>
        <div class="zone"><small>Risk / share</small><strong>${money(sizing.risk_per_share)}</strong></div>
        <div class="zone"><small>Units</small><strong>${sizing.suggested_quantity ?? "NA"}</strong></div>
      </div>
      <div class="chart-wrap compact"><div id="overviewChart" class="tv-chart"></div></div>
    </section>
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
        ${stock.trade_state ? `<div class="notice">Trade state: ${escapeHtml(stock.trade_state.state)} | ${escapeHtml(stock.trade_state.reason || "")}</div>` : ""}
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
        <div style="margin-top:12px">${renderRiskMatrix(stock)}</div>
      </section>
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
  return `<section class="panel">
    <div class="events-head">
      <div>
        <div class="section-kicker">Event strength = sentiment x freshness x reliability x importance</div>
        <div class="status-line">Last updated ${updatedAt ? timeAgo(updatedAt) : "when stock opened"}</div>
      </div>
      <button class="btn blue" data-refresh-events="${stock.symbol}">Refresh news</button>
    </div>
    <div id="eventsList">${events.map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>"}</div>
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
    if (list) list.innerHTML = events.map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>";
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
  const sizing = entry.position_sizing || {};
  return `
    <div class="panel">
      <div class="section-kicker">Entry plan</div>
      ${entry.candidate_gate ? `<div class="notice">${escapeHtml(entry.candidate_gate)}</div>` : ""}
      <div style="margin-bottom:12px"><button class="btn blue" data-toggle-alert="${stock.symbol}">${hasAlert(stock.symbol) ? "Remove breakout alert" : "Alert me at breakout"}</button></div>
      <div class="zone-grid">
        <div class="zone"><small>Breakout level</small><strong>${money(entry.breakout_level)}</strong></div>
        <div class="zone"><small>Aggressive entry</small><strong>${zone(entry.aggressive)}</strong></div>
        <div class="zone"><small>Pullback entry</small><strong>${zone(entry.pullback)}</strong></div>
        <div class="zone"><small>Stop</small><strong>${money(entry.stop)}</strong></div>
      </div>
      <div style="margin-top:14px">${metric("Invalidation", entry.invalidation || "NA")}</div>
      <div style="margin-top:14px">
        <div class="section-kicker">Position sizing</div>
        ${metric("Account size", money(sizing.account_size))}
        ${metric("Risk capital", money(sizing.risk_capital))}
        ${metric("Risk per share", money(sizing.risk_per_share))}
        ${metric("Suggested quantity", sizing.suggested_quantity ?? "NA")}
        ${metric("Approx position value", money(sizing.approx_position_value))}
      </div>
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
    stock.price = ltp;
    if (Number.isFinite(Number(tick.change_pct))) stock.change_pct = Number(tick.change_pct);
    stock.live_source = tick.source;
    stock.live_timestamp = tick.timestamp;
    if (stock.candidate && stock.entry?.breakout_level && ltp >= Number(stock.entry.breakout_level)) {
      stock.trade_state = {...(stock.trade_state || {}), state: "Triggered", reason: "Live Shoonya tick crossed breakout level"};
    }
  }
  if (selectedStock?.symbol === symbol) {
    selectedStock.price = ltp;
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
    layout: {background: {type: "solid", color: "#ffffff"}, textColor: "#374151", fontFamily: "Inter, system-ui, sans-serif"},
    grid: {vertLines: {color: "#eef2f7"}, horzLines: {color: "#eef2f7"}},
    crosshair: {mode: LightweightCharts.CrosshairMode.Normal},
    rightPriceScale: {borderColor: "#d7dde8"},
    timeScale: {borderColor: "#d7dde8", timeVisible: false},
  });
  const candleSeries = chart.addCandlestickSeries({
    upColor: "#087f5b",
    downColor: "#b42318",
    wickUpColor: "#087f5b",
    wickDownColor: "#b42318",
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
  chart.addLineSeries({color: "#b54708", lineWidth: 2, priceLineVisible: false, lastValueVisible: false}).setData(lineDataFrom(ema20, candleData));
  chart.addLineSeries({color: "#087f5b", lineWidth: 2, priceLineVisible: false, lastValueVisible: false}).setData(lineDataFrom(sma200, candleData));

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
    color: Number(bar.close) >= Number(bar.open ?? bar.close) ? "rgba(8, 127, 91, 0.32)" : "rgba(180, 35, 24, 0.32)",
  })));

  const benchRows = (benchmarkBars || []).slice(-chartRows.length).filter(bar => Number.isFinite(Number(bar.close)));
  if (benchRows.length >= 20) {
    const aligned = chartRows.slice(-benchRows.length);
    const rsData = aligned.map((bar, index) => ({
      time: barTime(bar),
      value: Number(bar.close) / Math.max(Number(benchRows[index].close), 1) * 100,
    }));
    const rsSeries = chart.addLineSeries({color: "#4b5563", lineWidth: 1, priceScaleId: "rs", priceLineVisible: false, lastValueVisible: false});
    rsSeries.priceScale().applyOptions({scaleMargins: {top: 0.66, bottom: 0.18}});
    rsSeries.setData(rsData);
  }

  const entry = selectedStock?.entry || {};
  const high52 = Math.max(...chartRows.map(bar => Number(bar.high || bar.close)));
  [
    {value: Number(entry.breakout_level), title: "Breakout", color: "#b54708"},
    {value: Number(entry.stop), title: "Stop", color: "#b42318"},
    {value: high52, title: "52W high", color: "#111827"},
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
  if (canvasId === "priceChart" && selectedStock?.symbol && !isDemo) {
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
  if (event.target.id === "saveThesisBtn") saveThesis();
  if (event.target.id === "submitShoonyaOtpBtn") submitShoonyaOtp();
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
