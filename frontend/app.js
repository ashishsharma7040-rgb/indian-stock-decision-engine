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
let liveDebugStatus = null;
let candleSeries = null;
let volumeSeries = null;
let ema20Series = null;
let ema50Series = null;
let dma200Series = null;
let rsSeries = null;
let chartPriceLines = [];
let currentChartSymbol = null;
let currentChartTargetId = null;
let currentChartPeriod = null;
let currentChartRows = [];
let currentChartBenchmark = [];
let chartToggleState = loadChartToggleState();
let selectedStockRequestInFlight = false;
let selectedStockRequestController = null;
let selectedStockSlowTimer = null;

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
function loadChartToggleState() {
  const defaults = {
    ema20: true,
    ema50: true,
    dma200: true,
    volume: true,
    rs: true,
    trigger: true,
    stop: true,
    target1: true,
    target2: true,
  };
  try {
    return {...defaults, ...(JSON.parse(localStorage.getItem("stockEngineChartToggles") || "{}") || {})};
  } catch {
    return defaults;
  }
}
function persistChartToggleState() {
  localStorage.setItem("stockEngineChartToggles", JSON.stringify(chartToggleState));
}
function actionDisplayLabel(action, gatePass = true) {
  if (!gatePass) return "WAIT_DATA";
  if (action === "BUY") return "ACTIONABLE";
  return action || "WATCH";
}
function actionTone(label) {
  if (["ACTIONABLE"].includes(label)) return "green";
  if (["STALK", "WATCH", "WATCH_ONLY"].includes(label)) return "amber";
  if (["AVOID"].includes(label)) return "red";
  return "neutral";
}
function istMarketOpenFallback() {
  const stamp = new Date();
  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(formatter.formatToParts(stamp).map(part => [part.type, part.value]));
  if (["Sat", "Sun"].includes(parts.weekday)) return false;
  const totalMinutes = Number(parts.hour || 0) * 60 + Number(parts.minute || 0);
  return totalMinutes >= (9 * 60 + 15) && totalMinutes <= (15 * 60 + 30);
}
function effectiveMarketOpen(stock = selectedStock) {
  if (typeof stock?.market_is_open === "boolean") return stock.market_is_open;
  return istMarketOpenFallback();
}
function selectedStockPollDelay(stock = selectedStock) {
  return effectiveMarketOpen(stock) ? SELECTED_STOCK_POLL_MS : 90000;
}
function mergeSelectedStockUpdate(current, fresh) {
  if (!current) return fresh;
  return {
    ...current,
    ...fresh,
    symbol: current.symbol || fresh.symbol,
    bars: fresh.bars || current.bars,
    benchmark_bars: fresh.benchmark_bars || current.benchmark_bars,
    price: fresh.price ?? current.price,
    change_pct: fresh.change_pct ?? current.change_pct,
    trade_state: fresh.trade_state || current.trade_state,
    action_plan: fresh.action_plan || current.action_plan,
    data_quality_gate: fresh.data_quality_gate || current.data_quality_gate,
    market_is_open: typeof fresh.market_is_open === "boolean" ? fresh.market_is_open : current.market_is_open,
  };
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
  if (scanRunning || yahooEnrichRunning) return "amber";
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
  const gatePass = Boolean(stock.data_quality_gate?.pass);
  return `<section class="panel entry-panel">
    <div class="panel-title">Entry Plan</div>
    ${entry.candidate_gate ? `<div class="micro-note">${escapeHtml(entry.candidate_gate)}</div>` : ""}
    <div class="entry-zones">
      <div class="ezone target"><small>Trigger</small><strong>${gatePass ? money(entry.buy_stop_trigger ?? entry.breakout_level) : "WAIT_DATA"}</strong></div>
      <div class="ezone target"><small>Breakout</small><strong>${gatePass ? money(entry.breakout_level) : "WAIT_DATA"}</strong></div>
      <div class="ezone target"><small>Aggressive</small><strong>${gatePass ? zone(entry.aggressive) : "Gate not open"}</strong></div>
      <div class="ezone"><small>Pullback</small><strong>${zone(entry.pullback)}</strong></div>
      <div class="ezone stop"><small>Stop</small><strong>${money(entry.stop)}</strong></div>
      <div class="ezone"><small>Target 1</small><strong>${money(entry.target_1)}</strong></div>
      <div class="ezone"><small>Target 2</small><strong>${money(entry.target_2)}</strong></div>
    </div>
    <div class="sizing-grid">
      <div><span>Setup</span><strong>${escapeHtml(entry.setup_state || "NA")}</strong></div>
      <div><span>Account</span><strong>${money(sizing.account_size)}</strong></div>
      <div><span>Risk capital</span><strong>${money(sizing.risk_capital)}</strong></div>
      <div><span>Risk / share</span><strong>${money(sizing.risk_per_share)}</strong></div>
      <div><span>Units</span><strong>${sizing.suggested_quantity ?? "NA"}</strong></div>
    </div>
    ${compact ? "" : `<div class="entry-actions"><button class="btn blue" data-toggle-alert="${stock.symbol}">${hasAlert(stock.symbol) ? "Remove breakout alert" : "Alert me at breakout"}</button></div>`}
  </section>`;
}

function renderLevelChips(levels = {}) {
  const rows = Object.entries(levels || {}).filter(([, value]) => Number.isFinite(Number(value)));
  if (!rows.length) return "<div class='status-line'>No level map yet.</div>";
  return `<div class="level-chip-grid">${rows.map(([label, value]) => `
    <div class="level-chip">
      <span>${escapeHtml(label.replaceAll("_", " "))}</span>
      <strong>${money(value)}</strong>
    </div>`).join("")}</div>`;
}

function renderPremiumTags(stock) {
  const tags = stock.premium_tags || [];
  if (!tags.length) return "<div class='status-line'>No premium pattern tags yet.</div>";
  return `<div class="premium-tags-container">${tags.map(tag => `
    <div class="tag-item ${escapeHtml(tag.type || "safe")}">
      <div class="tag-label">${escapeHtml(tag.label || "Tag")}</div>
      <div class="tag-desc">${escapeHtml(tag.description || "")}</div>
    </div>`).join("")}</div>`;
}

function renderForensicAudit(stock) {
  const audit = stock.forensic_audit || {};
  const pass = String(audit.status || "PASS").toUpperCase() === "PASS";
  const metrics = audit.metrics || {};
  return `<section class="panel">
    <div class="panel-title">Forensic Audit</div>
    <div class="audit-status ${pass ? "pass" : "fail"}">
      <strong>${pass ? "Audit cleared" : "Audit failed"}</strong>
      <span>${pass ? "No hard-fail forensic veto is active." : "At least one audit-grade red flag is active."}</span>
    </div>
    ${(audit.warnings || []).length ? `<div class="forensic-warning-list">${audit.warnings.map(item => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(item)}</div></div>`).join("")}</div>` : ""}
    <div class="action-metrics">
      ${metric("Forensic score", audit.score ?? "NA")}
      ${metric("Debt / CFO", metrics.debt_to_cfo ?? "NA")}
      ${metric("Cash tax rate", metrics.cash_tax_rate === null || metrics.cash_tax_rate === undefined ? "NA" : `${(Number(metrics.cash_tax_rate) * 100).toFixed(1)}%`)}
      ${metric("CWIP / gross", metrics.cwip_to_gross === null || metrics.cwip_to_gross === undefined ? "NA" : `${(Number(metrics.cwip_to_gross) * 100).toFixed(1)}%`)}
    </div>
  </section>`;
}

function gateLabel(key) {
  const labels = {
    good_business: "Good business",
    sector_helping_now: "Sector tailwind",
    fresh_trigger: "Fresh trigger",
    chart_confirming: "Chart confirmation",
    where_am_i_wrong_defined: "Risk defined",
    market_support: "Market support",
    market_breadth_master_switch_red: "Market breadth switch",
    rubber_band_overextension_limit: "Overextension control",
    data_quality_or_completeness_failed: "Data quality gate",
    forensic_earnings_quality_fail: "Forensic veto",
    idiosyncratic_momentum_below_0_5: "Relative momentum",
    risk_off_quality_gate: "Risk-off quality gate",
    portfolio_risk_matrix_failed: "Portfolio risk matrix",
  };
  return labels[key] || String(key || "").replaceAll("_", " ");
}

function decisionGateLists(stock) {
  const five = stock.explanation_json?.five_questions || {};
  const passed = [];
  const failed = [];
  Object.entries(five).forEach(([key, value]) => {
    (value ? passed : failed).push(gateLabel(key));
  });
  if (stock.data_quality_gate?.pass) passed.push("Data quality gate");
  else failed.push("Data quality gate");
  if (stock.forensic_gate?.pass !== false) passed.push("Forensic gate");
  else failed.push("Forensic gate");
  return {
    passed: Array.from(new Set(passed)),
    failed: Array.from(new Set([...(stock.action_plan?.failed_gates || []).map(gateLabel), ...failed])),
  };
}

function renderDecisionCard(stock) {
  const plan = stock.action_plan || {};
  const gatePass = Boolean(stock.data_quality_gate?.pass);
  const action = actionDisplayLabel(plan.action, gatePass);
  const tone = actionTone(action);
  const trigger = plan.trigger_price ?? stock.entry?.buy_stop_trigger ?? stock.entry?.breakout_level;
  const stop = plan.stop ?? stock.entry?.stop;
  const target1 = plan.target_1 ?? stock.entry?.target_1;
  const target2 = plan.target_2 ?? stock.entry?.target_2;
  const pullbackZone = plan.pullback_zone ?? stock.entry?.pullback;
  const aggressiveZone = gatePass ? (plan.aggressive_zone ?? stock.entry?.aggressive) : null;
  const riskCapital = plan.risk_capital ?? stock.entry?.position_sizing?.risk_capital;
  const qty = plan.suggested_quantity ?? stock.entry?.position_sizing?.suggested_quantity;
  const positionValue = plan.approx_position_value ?? stock.entry?.position_sizing?.approx_position_value;
  const rr1 = plan.risk_reward_target_1 ?? stock.entry?.risk_reward_target_1;
  const rr2 = plan.risk_reward_target_2 ?? stock.entry?.risk_reward_target_2;
  const horizon = Number(stock.monthly_score || 0) > Number(stock.weekly_score || 0) + 6
    ? "Positional: 4-12 weeks"
    : "Swing: 5-15 trading days";
  const gates = decisionGateLists(stock);
  const riskFlags = Array.from(new Set([...(stock.explanation_json?.risk_flags || []), ...(plan.failed_gates || [])])).slice(0, 5);
  const waitText = stock.data_quality_gate?.warning || "Do not rank/action until data-quality gate passes.";
  const reasonSummary = gatePass ? (plan.reason_summary || plan.summary || stock.verdict || "Research setup under review.") : waitText;
  return `<section class="panel decision-card premium-card sticky-decision">
    <div class="decision-topline">
      <div>
        <div class="section-kicker">Decision Engine</div>
        <h2>Research Decision</h2>
      </div>
      <div class="decision-action ${tone}" data-decision-action>${escapeHtml(action)}</div>
    </div>
    <div class="decision-grid">
      <div class="decision-main">
        <div class="decision-summary" data-decision-summary>${escapeHtml(reasonSummary)}</div>
        <div class="decision-meta">
          ${renderStatusChip(`Confidence ${plan.confidence || "low"}`, tone)}
          ${renderStatusChip(horizon, "neutral")}
          ${renderStatusChip(gatePass ? "Gate pass" : "Gate blocked", gatePass ? "green" : "amber")}
        </div>
      </div>
      <div class="decision-metrics">
        ${metric("Current price", `<span id="selectedStockPrice">${money(stock.price)}</span>`)}
        ${gatePass ? metric("Trigger price", money(trigger)) : metric("Trigger price", "Hidden until gate passes")}
        ${metric("Pullback zone", zone(pullbackZone))}
        ${gatePass && aggressiveZone ? metric("Aggressive zone", zone(aggressiveZone)) : metric("Aggressive zone", "Gate not open")}
        ${metric("Stop", money(stop))}
        ${metric("Target 1", money(target1))}
        ${metric("Target 2", money(target2))}
        ${metric("Risk-reward", rr1 ?? rr2 ?? "NA")}
        ${metric("Suggested quantity", qty ?? "NA")}
        ${metric("Risk capital", money(riskCapital))}
        ${metric("Approx position value", money(positionValue))}
      </div>
    </div>
    ${!gatePass ? `<div class="notice notice-amber">WAIT_DATA: ${escapeHtml(waitText)}</div>` : ""}
    <div class="decision-foot">
      <div>
        <div class="section-kicker">Passed gates</div>
        <div class="decision-chip-row">${gates.passed.map(item => renderStatusChip(item, "green")).join("") || renderStatusChip("No confirmed pass yet", "neutral")}</div>
      </div>
      <div>
        <div class="section-kicker">Failed gates</div>
        <div class="decision-chip-row">${gates.failed.map(item => renderStatusChip(item, "red")).join("") || renderStatusChip("None active", "green")}</div>
      </div>
      <div>
        <div class="section-kicker">Main risks</div>
        <div class="decision-chip-row">${riskFlags.length ? riskFlags.map(item => renderStatusChip(gateLabel(item), "amber")).join("") : renderStatusChip("No major rule-based risk flagged", "green")}</div>
      </div>
    </div>
  </section>`;
}

function renderDataQualityGate(stock) {
  const gate = stock.data_quality_gate || {};
  const tone = gate.pass ? "green" : "amber";
  const issues = [
    ...(gate.price_data_quality?.issues || []),
    ...(gate.price_data_quality?.warnings || []),
    ...(gate.missing_fields || []),
  ].filter(Boolean);
  return `<section class="panel premium-card">
    <div class="panel-title">Data Quality Gate</div>
    <div class="decision-chip-row">
      ${renderStatusChip(gate.pass ? "Pass" : "Blocked", tone)}
      ${renderStatusChip(`Completeness ${gate.actual_completeness_pct ?? "NA"}%`, "neutral")}
      ${gate.corporate_actions_applied ? renderStatusChip("Adjusted history", "green") : renderStatusChip("Adjustment status unknown", "amber")}
    </div>
    ${gate.warning ? `<div class="notice notice-amber">${escapeHtml(gate.warning)}</div>` : ""}
    <div class="status-line">${issues.length ? escapeHtml(issues.slice(0, 8).join(" | ")) : "No active data-quality warning."}</div>
  </section>`;
}

function renderEntryPlanCard(stock) {
  const plan = stock.action_plan || {};
  const gatePass = Boolean(stock.data_quality_gate?.pass);
  return `<section class="panel premium-card">
    <div class="panel-title">Entry Plan</div>
    <div class="entry-zones">
      <div class="ezone target"><small>Trigger</small><strong>${gatePass ? money(plan.trigger_price ?? stock.entry?.buy_stop_trigger ?? stock.entry?.breakout_level) : "WAIT_DATA"}</strong></div>
      <div class="ezone"><small>Pullback</small><strong>${zone(plan.pullback_zone ?? stock.entry?.pullback)}</strong></div>
      <div class="ezone target"><small>Target 1</small><strong>${money(plan.target_1 ?? stock.entry?.target_1)}</strong></div>
      <div class="ezone target"><small>Target 2</small><strong>${money(plan.target_2 ?? stock.entry?.target_2)}</strong></div>
      <div class="ezone stop"><small>Stop</small><strong>${money(plan.stop ?? stock.entry?.stop)}</strong></div>
      <div class="ezone"><small>Invalidation</small><strong>${escapeHtml(plan.invalidation || stock.entry?.invalidation || "NA")}</strong></div>
    </div>
  </section>`;
}

function renderPositionSizingCard(stock) {
  const sizing = stock.entry?.position_sizing || {};
  const plan = stock.action_plan || {};
  return `<section class="panel premium-card">
    <div class="panel-title">Position Sizing</div>
    <div class="action-metrics">
      ${metric("Suggested quantity", plan.suggested_quantity ?? sizing.suggested_quantity ?? "NA")}
      ${metric("Risk capital", money(plan.risk_capital ?? sizing.risk_capital))}
      ${metric("Approx position value", money(plan.approx_position_value ?? sizing.approx_position_value))}
      ${metric("Account size", money(plan.account_size ?? sizing.account_size))}
      ${metric("Risk / share", money(sizing.risk_per_share))}
      ${metric("Stop distance", money(Math.abs(Number((plan.trigger_price ?? stock.entry?.buy_stop_trigger ?? stock.entry?.breakout_level) || 0) - Number((plan.stop ?? stock.entry?.stop) || 0))))}
    </div>
    <div class="status-line">Sizing is research support only. Confirm liquidity, slippage, and your own account rules before acting.</div>
  </section>`;
}

function renderChartToolbar(stock) {
  const toggles = [
    ["ema20", "EMA20"],
    ["ema50", "EMA50"],
    ["dma200", "DMA200"],
    ["volume", "Volume"],
    ["rs", "RS vs Nifty"],
    ["trigger", "Entry Trigger"],
    ["stop", "Stop"],
    ["target1", "Target 1"],
    ["target2", "Target 2"],
  ];
  return `<div class="chart-toolbar">
    <div class="timeframe-row">${["1w","1m","3m","6m","1y"].map(period => `<button class="timeframe-btn ${activeChartPeriod === period ? "active" : ""}" data-chart-period="${period}">${period.toUpperCase()}</button>`).join("")}</div>
    <div class="chart-toolbar-toggles">${toggles.map(([key, label]) => `<button class="timeframe-btn ${chartToggleState[key] ? "active" : ""}" data-chart-toggle="${key}">${label}</button>`).join("")}</div>
    <button class="btn blue" data-refresh-chart="${stock.symbol}">Refresh Chart</button>
  </div>`;
}

function renderActionPlan(stock) {
  const plan = stock.action_plan || {};
  const displayAction = actionDisplayLabel(plan.action, Boolean(stock.data_quality_gate?.pass));
  const tone = actionTone(displayAction);
  const gatePass = Boolean(stock.data_quality_gate?.pass);
  return `<section class="panel action-plan ${tone}">
    <div class="panel-title">Action Plan</div>
    <div class="action-plan-head">
      <div class="action-badge ${tone}">${escapeHtml(displayAction)}</div>
      <div>
        <strong>${escapeHtml(plan.summary || stock.verdict || "No summary yet.")}</strong>
        <div class="status-line">Confidence ${escapeHtml(plan.confidence || "medium")} | Setup ${escapeHtml(plan.setup_state || stock.entry?.setup_state || "NA")}</div>
      </div>
    </div>
    <div class="action-metrics">
      ${metric("Buy stop", gatePass ? money(plan.trigger_price) : "WAIT_DATA")}
      ${metric("Trigger gap", plan.trigger_distance_pct === null || plan.trigger_distance_pct === undefined ? "NA" : `${plan.trigger_distance_pct > 0 ? "+" : ""}${plan.trigger_distance_pct}%`)}
      ${metric("Stop", money(plan.stop))}
      ${metric("Target 1", gatePass ? money(plan.target_1) : "WAIT_DATA")}
      ${metric("Target 2", gatePass ? money(plan.target_2) : "WAIT_DATA")}
      ${metric("R:R T1", plan.risk_reward_target_1 ?? "NA")}
      ${metric("R:R T2", plan.risk_reward_target_2 ?? "NA")}
      ${metric("Invalidation", plan.invalidation || "NA")}
    </div>
    <div class="grid two compact-levels">
      <div>
        <div class="section-kicker">Support map</div>
        ${renderLevelChips(plan.support_levels)}
      </div>
      <div>
        <div class="section-kicker">Resistance map</div>
        ${renderLevelChips(plan.resistance_levels)}
      </div>
    </div>
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

function setStockLoadingStatus(message = "", tone = "blue") {
  const node = qs("stockLoadingStatus");
  if (!node) return;
  if (!message) {
    node.textContent = "";
    node.className = "stock-loading-status hidden";
    return;
  }
  node.textContent = message;
  node.className = `stock-loading-status notice notice-${tone}`;
}

function cancelSelectedStockRequest(message = "Stopped stock detail load.") {
  if (selectedStockRequestController) {
    try { selectedStockRequestController.abort(); } catch {}
  }
  selectedStockRequestController = null;
  selectedStockRequestInFlight = false;
  clearTimeout(selectedStockSlowTimer);
  selectedStockSlowTimer = null;
  setStockLoadingStatus(message, "amber");
}

async function fetchSelectedStockDetail(symbol, {fresh = false, debug = false, background = false} = {}) {
  if (selectedStockRequestInFlight) return null;
  selectedStockRequestInFlight = true;
  const controller = new AbortController();
  selectedStockRequestController = controller;
  const params = new URLSearchParams();
  if (fresh) params.set("fresh", "1");
  if (debug) params.set("debug", "1");
  const path = `/api/stocks/${encodeURIComponent(symbol)}${params.toString() ? `?${params.toString()}` : ""}`;
  if (!background) setStockLoadingStatus("Loading stock detail... fetching price/history/events", "blue");
  clearTimeout(selectedStockSlowTimer);
  selectedStockSlowTimer = setTimeout(() => {
    if (selectedStockRequestController === controller) {
      setStockLoadingStatus("Still waiting for data provider. Try Refresh or use cached result.", "amber");
    }
  }, 20000);
  try {
    return await getJson(path, {signal: controller.signal});
  } finally {
    if (selectedStockRequestController === controller) {
      selectedStockRequestController = null;
      selectedStockRequestInFlight = false;
      clearTimeout(selectedStockSlowTimer);
      selectedStockSlowTimer = null;
      if (!background) setStockLoadingStatus("");
    }
  }
}

async function backgroundRefreshSelectedStock(symbol) {
  try {
    const detail = await fetchSelectedStockDetail(symbol, {fresh: true, background: true});
    if (!detail || !selectedStock || selectedStock.symbol !== symbol) return;
    selectedStock = mergeSelectedStockUpdate(selectedStock, detail);
    renderStockView(selectedStock);
  } catch (error) {
    if (error?.name !== "AbortError") setStockLoadingStatus("Background refresh is still waiting on a provider.", "amber");
  }
}

async function refreshLiveDebugStatus(force = false) {
  if (isDemo && !force) return liveDebugStatus;
  try {
    liveDebugStatus = await getJson("/api/live/debug");
  } catch (error) {
    liveDebugStatus = {
      configured: false,
      status: "unavailable",
      feed_open: false,
      missing_credentials: [],
      last_error: error.message,
      next_step: "Backend live debug endpoint is not reachable right now.",
    };
  }
  if (!qs("homeView")?.classList.contains("hidden")) renderHome();
  return liveDebugStatus;
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
    refreshLiveDebugStatus(false).catch(() => {});
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
  if (scanRunning || yahooEnrichRunning) return;
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
  if (!qs("homeView")?.classList.contains("hidden")) renderHome();
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
      if (selectedStock && !liveStatus.feed_open) {
        updateSelectedStockHeaderUi();
        updateDecisionCardUi();
        if (activeTab === "chart" || activeTab === "overview") updateActiveChartFromSelectedStock(false);
        else renderStockView(selectedStock);
      }
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
  clearTimeout(selectedStockPollTimer);
  clearInterval(newsRefreshTimer);
  const poll = async () => {
    if (!selectedStock || selectedStock.symbol !== symbol) return;
    if (selectedStockRequestInFlight) {
      selectedStockPollTimer = setTimeout(poll, selectedStockPollDelay(selectedStock));
      return;
    }
    if (liveStatus.feed_open) {
      selectedStockPollTimer = setTimeout(poll, selectedStockPollDelay(selectedStock));
      return;
    }
    try {
      const fresh = await fetchSelectedStockDetail(symbol, {background: true});
      if (!fresh) {
        selectedStockPollTimer = setTimeout(poll, selectedStockPollDelay(selectedStock));
        return;
      }
      selectedStock = mergeSelectedStockUpdate(selectedStock, fresh);
      updateSelectedStockHeaderUi();
      updateDecisionCardUi();
      if (activeTab === "chart") {
        updateActiveChartFromSelectedStock(false);
      } else if (activeTab === "overview") {
        updateActiveChartFromSelectedStock(false);
      } else {
        renderStockView(selectedStock);
      }
    } catch (error) {
      // Keep the current selected stock if polling fails.
    } finally {
      if (selectedStock?.symbol === symbol) {
        selectedStockPollTimer = setTimeout(poll, selectedStockPollDelay(selectedStock));
      }
    }
  };
  selectedStockPollTimer = setTimeout(poll, selectedStockPollDelay(selectedStock));
  newsRefreshTimer = setInterval(() => refreshEvents(symbol, false), 300000);
}

function stopSelectedStockPolling() {
  clearTimeout(selectedStockPollTimer);
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
    const gatePass = Boolean(stock?.data_quality_gate?.pass);
    const action = actionDisplayLabel(stock?.action_plan?.action, gatePass);
    const stop = stock?.action_plan?.stop || stock?.entry?.stop;
    const trigger = stock?.action_plan?.trigger_price || stock?.entry?.buy_stop_trigger || stock?.entry?.breakout_level;
    return `<button class="candidate-row desk-row pro-candidate ${isTriggeredStock(stock, state) ? "triggered" : ""}" data-symbol="${stock.symbol}">
      <span class="rank-pill">${index + 1}</span>
      <div>
        <strong>${escapeHtml(stock.symbol)}</strong>
        <span>${escapeHtml(stock.sector || stock.name || "")} | ${money(stock.price)} | <em class="breakout-proximity ${proximity.className}">${proximity.label}</em></span>
        <span class="status-line">${escapeHtml(action)} | Trigger ${money(trigger)} | Stop ${money(stop)}</span>
      </div>
      <div class="candidate-tags">${badgeForConviction(stock.conviction)}${stateBadgeHtml(stock, state)}</div>
      <b class="candidate-gauge ${scoreClass(score)}" style="--score:${score}">${Math.round(score)}</b>
    </button>`;
  }).join("") || `<div class='status-line'>${escapeHtml(scanMeta?.ranking_warning || "No qualified names right now.")}</div>`;
}

function scanRejectedSummary() {
  if (!scanMeta) return "";
  const insufficient = scanMeta.insufficient_history_examples || [];
  const qualityFailed = scanMeta.data_quality_failed_examples || [];
  if (!insufficient.length && !qualityFailed.length) return "";
  const insuffText = insufficient.length
    ? `Awaiting enrichment: ${insufficient.slice(0, 4).map(item => `${item.symbol} (${item.bars_available ?? 0} bars)`).join(", ")}`
    : "";
  const qualityText = qualityFailed.length
    ? `Validation failed: ${qualityFailed.slice(0, 4).map(item => `${item.symbol} (${item.reason || "quality check"})`).join(", ")}`
    : "";
  return `<div class="status-line" style="margin-top:10px">${escapeHtml([insuffText, qualityText].filter(Boolean).join(" | "))}</div>`;
}

function topCandidatePanel(title, rows, scoreKey) {
  const isWeekly = scoreKey === "weekly_score";
  const statusLine = isWeekly && scanMeta
    ? `Universe ${countText(scanMeta.universe_size)} | Seen ${countText(scanMeta.total_symbols_seen || scanMeta.passed_liquidity)} | With history ${countText(scanMeta.symbols_with_history)} | Rankable ${countText(scanMeta.ranking_eligible_count || scanMeta.real_history_rankable)}${scanMeta.excluded_non_equity ? ` | Non-equity skipped ${countText(scanMeta.excluded_non_equity)}` : ""}`
    : `Last calculated ${topCandidateState.lastCalculatedAt ? formatIstTime(topCandidateState.lastCalculatedAt) : formatIstTime(dashboard.generated_at)} IST`;
  return `<section class="panel desk-card top-candidate-card">
    <div class="panel-headline">
      <div>
        <div class="panel-title">${escapeHtml(title)}</div>
        <div class="status-line">${statusLine}</div>
      </div>
      ${isWeekly ? `<button class="btn blue recalc-btn ${topCandidateState.loading ? "loading" : ""}" data-recalculate-best="1">${topCandidateState.loading ? "Scanning NSE" : "Run Full NSE Scan"}</button>` : ""}
    </div>
    <div class="candidate-progress ${topCandidateState.loading ? "active" : ""}"><span></span></div>
    ${isWeekly ? `<div class="scan-status-text">${escapeHtml(scanStatusText || (scanMeta ? `Last full scan ${topCandidateState.lastCalculatedAt ? timeAgo(topCandidateState.lastCalculatedAt) : ""}` : "Full scan ranks the investable NSE universe through the engine."))}</div>` : ""}
    ${candidateRows(rows, scoreKey)}
    ${isWeekly ? scanRejectedSummary() : ""}
  </section>`;
}

function renderStatusChip(label, tone = "neutral") {
  return `<span class="status-chip ${tone}">${escapeHtml(label)}</span>`;
}

function renderMarketHealthCard(market = {}) {
  const tone = market.regime === "Risk-on" ? "green" : market.regime === "Risk-off" ? "red" : "amber";
  return `<section class="panel desk-card market-card premium-card">
    <div class="panel-title">Market Health</div>
    <div class="market-score-line">
      ${renderStatusChip(market.regime || "NA", tone)}
      <strong>${market.score ?? "NA"}</strong>
    </div>
    ${metric("Breadth", `${market.breadth_above_50dma ?? "NA"}% above 50 DMA`)}
    ${metric("Nifty close", money(market.nifty_close))}
    ${metric("Nifty trend", Number(market.nifty_close) > Number(market.nifty_ema50) ? "Above 50 EMA" : "Below 50 EMA")}
    ${metric("VIX", market.vix ?? "NA")}
    <div class="bar"><span style="width:${market.score || 0}%;background:${scoreColor(market.score || 0)}"></span></div>
  </section>`;
}

function renderScanProgressPanel(scan = {}) {
  const latestTime = scan.scan_finished_at || topCandidateState.lastCalculatedAt || dashboard?.generated_at;
  const tone = Number(scan.ranking_eligible_count || 0) > 0 ? "green" : Number(scan.insufficient_history_count || 0) > 0 ? "amber" : "neutral";
  return `<section class="panel desk-card premium-card scan-progress">
    <div class="panel-title">Scan Status</div>
    <div class="status-line">Last scan ${latestTime ? timeAgo(latestTime) : "not run yet"}${latestTime ? ` | ${formatIstTime(latestTime)} IST` : ""}</div>
    <div class="action-metrics">
      ${metric("Seen", countText(scan.total_symbols_seen || scan.universe_size || 0))}
      ${metric("Rankable", countText(scan.ranking_eligible_count || 0))}
      ${metric("Awaiting enrichment", countText(scan.insufficient_history_count || 0))}
      ${metric("Validation failed", countText(scan.data_quality_failed_count || 0))}
    </div>
    <div class="status-line">${renderStatusChip((scan.source_summary || scan.rank_basis || "Stored DB history only"), tone)}</div>
  </section>`;
}

function renderShoonyaStatusCard(status = {}) {
  const tone = status.feed_open ? "green" : status.configured ? "amber" : "red";
  const missing = status.missing_credentials || [];
  return `<section class="panel desk-card premium-card">
    <div class="panel-title">Shoonya Status</div>
    <div class="market-score-line">
      ${renderStatusChip(status.feed_open ? "Live connected" : status.configured ? "Configured / waiting" : "Not configured", tone)}
      <strong>${escapeHtml(status.status || "unknown")}</strong>
    </div>
    ${metric("Feed open", status.feed_open ? "Yes" : "No")}
    ${metric("Subscribed", countText(status.subscribed_count || status.subscribed_symbols?.length || 0))}
    ${metric("Resolved tokens", countText(status.resolved_tokens_count || 0))}
    ${missing.length ? `<div class="status-line">Missing: ${escapeHtml(missing.join(", "))}</div>` : ""}
    ${status.last_error ? `<div class="notice notice-amber">${escapeHtml(status.last_error)}</div>` : ""}
    <div class="status-line">${escapeHtml(status.next_step || status.config_message || "Live feed diagnostics will appear here.")}</div>
  </section>`;
}

function renderHomeDataQualityBanner() {
  if (!scanMeta) return "";
  const rankable = Number(scanMeta.ranking_eligible_count || 0);
  const insufficient = Number(scanMeta.insufficient_history_count || 0);
  const qualityFailed = Number(scanMeta.data_quality_failed_count || 0);
  const tone = rankable > 0 ? "blue" : insufficient > 0 || qualityFailed > 0 ? "amber" : "blue";
  const message = rankable > 0
    ? `Full scan is ranking only stocks with real validated history. Rankable ${countText(rankable)} | awaiting enrichment ${countText(insufficient)} | validation failed ${countText(qualityFailed)}.`
    : (scanMeta.ranking_warning || "Top picks stay blank until 220+ validated daily bars are available.");
  return `<div class="notice notice-${tone} data-quality-banner"><strong>Scan data quality:</strong> ${escapeHtml(message)}</div>`;
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
    <div id="stockLoadingStatus" class="stock-loading-status notice notice-blue">Loading stock detail... fetching price/history/events</div>
    <div class="status-line" style="margin-bottom:12px"><button class="mini-link" data-cancel-stock-load="1">Stop loading</button></div>
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
  const connectionTest = dashboard?.database?.connection_test || databaseStatus?.connection_test;
  const dates = counts.bhavcopy_dates || [];
  const dbError = dashboard?.database?.connection_error || databaseStatus?.connection_error || dashboard?.database?.count_error || databaseStatus?.count_error || "";
  const dbTone = !dbOk ? "red" : connectionTest === "failed" ? "amber" : counts.bhavcopy_stale ? "amber" : "green";
  return `<section class="panel data-prep-panel">
    <div>
      <div class="panel-title">Data Prep Pipeline</div>
      <div class="body-text">Use this order before a serious full-market scan: latest bhavcopy, Yahoo history, then Full NSE Scan.</div>
      <div class="status-line">DB ${dbOk ? "on" : "off"}${connectionTest ? ` | test ${escapeHtml(connectionTest)}` : ""}${bhavDate ? ` | bhavcopy ${escapeHtml(bhavDate)}` : ""}${enrichedSymbols !== undefined ? ` | enriched symbols ${countText(enrichedSymbols)} | rows ${countText(enrichedRows)}` : ""}</div>
      <div class="db-inventory ${dbTone}">
        <strong>Supabase inventory</strong>
        <span>Bhavcopy dates: ${dates.length ? dates.map(item => escapeHtml(item)).join(", ") : "none loaded"}</span>
        <span>Retention ${counts.bhavcopy_retention_dates ?? "NA"} day(s), stale ${counts.bhavcopy_stale ? "yes" : "no"}</span>
        ${dbError ? `<span>${escapeHtml(dbError)}</span>` : ""}
      </div>
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
    ${renderHomeDataQualityBanner()}
    <div class="home-top-grid">
      <section class="panel desk-card">
        <div class="panel-title">Top 3 Triggered</div>
        ${candidateRows(triggeredRows, "weekly_score")}
      </section>
      ${topCandidatePanel("Top 3 Weekly", weeklyRows, "weekly_score")}
      ${topCandidatePanel("Top 3 Monthly", monthlyRows, "monthly_score")}
      ${renderMarketHealthCard(market)}
      ${renderScanProgressPanel(scanMeta || dashboard.scan_meta || {})}
      ${renderShoonyaStatusCard(liveDebugStatus || dashboard.live_feed || liveStatus || {})}
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
  cancelSelectedStockRequest("");
  const summary = dashboard.stocks.find(s => s.symbol === symbol) || (universeSearchRows || []).find(s => s.symbol === symbol);
  selectedStock = summary;
  activeTab = "overview";
  startSelectedStockPolling(symbol);
  renderStockList();
  qs("homeView").classList.add("hidden");
  qs("stockView").classList.remove("hidden");
  qs("stockView").innerHTML = loadingStockSkeleton(symbol);
  try {
    const detail = await fetchSelectedStockDetail(symbol);
    if (!detail) return;
    selectedStock = detail;
    renderStockView(detail);
    if (!detail.business_quality || detail._cache_collection) {
      setStockLoadingStatus("Loaded cached result. Fetching deeper history in background...", "blue");
      void backgroundRefreshSelectedStock(symbol);
    }
  } catch (error) {
    if (error?.name === "AbortError") return;
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
    action_plan: summary.action_plan || {
      action: summary.candidate ? "WATCH" : "WAIT_DATA",
      confidence: "low",
      summary: summary.verdict || "Open the live stock detail for the full action plan.",
      trigger_price: summary.entry?.buy_stop_trigger || summary.entry?.breakout_level,
      stop: summary.entry?.stop,
      target_1: summary.entry?.target_1,
      target_2: summary.entry?.target_2,
      support_levels: summary.entry?.support_levels || {},
      resistance_levels: summary.entry?.resistance_levels || {},
      pivot_levels: summary.entry?.pivot_levels || {},
      fib_levels: summary.entry?.fib_levels || {}
    },
    engine_scores: summary.engine_scores || {final_score: summary.weekly_score, technical_strength: summary.technical_score, fundamental_forensic: summary.business_score},
    premium_tags: summary.premium_tags || [],
    forensic_audit: summary.forensic_audit || {status: "PASS", warnings: [], metrics: {}},
    confidence_interval: summary.confidence_interval || {label:"Demo", min_margin_above_high:0},
    bars
  };
}

function verdictTone(stock) {
  const text = String(stock.verdict || stock.conviction || "").toLowerCase();
  if (text.includes("strong") || text.includes("candidate - monitor") || text.includes("breakout is active") || text.includes("buy")) return "green";
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
        <div id="selectedStockHeadlinePrice" class="price price-big">${money(full.price)}</div>
        <div id="selectedStockHeadlineChange" class="${Number(full.change_pct || 0) >= 0 ? "positive" : "negative"}">${percent(full.change_pct)}</div>
        ${priceSpark ? `<div class="price-spark">${priceSpark}</div>` : ""}
        <div class="status-line">Raw W ${full.weekly_raw_score ?? full.score_diagnostics?.weekly_raw ?? "NA"} | Raw M ${full.monthly_raw_score ?? full.score_diagnostics?.monthly_raw ?? "NA"}</div>
      </div>
    </div>
    ${renderDecisionCard(full)}
    <div id="stockLoadingStatus" class="stock-loading-status hidden"></div>
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
  if (activeTab === "chart") setTimeout(() => drawChart(full.bars || [], "priceChart", full.benchmark_bars || [], {fresh: false, fitContent: true}), 0);
  if (activeTab === "overview") setTimeout(() => drawChart(full.bars || [], "overviewChart", full.benchmark_bars || [], {fresh: false, fitContent: true}), 0);
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
  if (activeTab === "chart") return `<section class="panel"><div class="chart-head"><div><div class="section-kicker">Price confirmation</div><div class="status-line">Cached chart is used by default. Use Refresh Chart only when you want an intentional refetch.</div></div></div>${renderChartToolbar(stock)}${chartIndicatorStrip(stock)}<div class="chart-wrap"><div id="priceChart" class="tv-chart"></div></div><div id="chartTooltip" class="chart-tooltip hidden"></div><div class="chart-legend"><span class="legend-price">Candles</span><span class="legend-ema">20 EMA</span><span class="legend-dma">200 DMA</span><span class="legend-rs">RS vs Nifty</span><span class="legend-breakout">Breakout</span><span class="legend-stop">Stop</span></div></section>`;
  if (activeTab === "thesis") return renderThesis(stock);
  if (activeTab === "data") return renderApiStack();
  return "";
}

function renderOverview(stock) {
  return `
    <div class="stock-workspace-grid">
      <div class="stock-left-col">
        <section class="panel premium-card">
          <div class="panel-title">Chart</div>
          ${renderChartToolbar(stock)}
          ${chartIndicatorStrip(stock)}
          <div class="chart-wrap compact"><div id="overviewChart" class="tv-chart"></div></div>
        </section>
        ${renderEntryPlanCard(stock)}
        ${renderPositionSizingCard(stock)}
        <section class="panel">
          <div class="panel-title">Algorithmic Footprints</div>
          ${renderPremiumTags(stock)}
        </section>
        <section class="panel">
          <div class="panel-title">Latest Events</div>
          <div class="event-list compact">${(stock.event_strength?.events || []).slice(0, 3).map(eventHtml).join("") || "<div class='status-line'>No events loaded.</div>"}</div>
        </section>
      </div>
      <div class="stock-right-col">
        ${renderDataQualityGate(stock)}
        ${renderFiveQuestionGate(stock)}
        ${renderForensicAudit(stock)}
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
  const imputedFields = dataQuality.imputed_fields || [];
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
        ${imputedFields.length ? metric("Sector-median fallback", imputedFields.join(", ")) : ""}
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
        ${metric("Buy stop trigger", money(i.trigger_price))}
        ${metric("Relative strength", `${i.relative_strength?.state || "NA"} (${i.relative_strength?.pct ?? 0}%)`)}
        ${metric("Base quality", `${base.score ?? "NA"}/100`)}
        ${metric("Days in base", base.days_in_base ?? "NA")}
        ${metric("Base tightness", base.tightness_pct === null || base.tightness_pct === undefined ? "NA" : `${base.tightness_pct}%`)}
        ${metric("Setup width", i.setup_width_pct === null || i.setup_width_pct === undefined ? "NA" : `${i.setup_width_pct}%`)}
        ${metric("Setup state", i.setup_state || "NA")}
        ${metric("Event-day volume", eventVolume.fresh_official_event ? "Yes" : "No")}
      </section>
      <section class="panel">
        <div class="section-kicker">Breakout checklist</div>
        ${Object.keys(checks).map(key => check(key.replaceAll("_"," "), checks[key])).join("") || "<div class='status-line'>Backend checklist not loaded in demo mode.</div>"}
        ${(t.fake_breakout_flags || []).map(flag => `<div class="check-row"><span class="dot warn"></span><div>${escapeHtml(flag)}</div></div>`).join("")}
      </section>
      <section class="panel">
        <div class="section-kicker">Support / resistance</div>
        ${renderLevelChips(i.support_levels)}
        <div style="height:10px"></div>
        ${renderLevelChips(i.resistance_levels)}
      </section>
      <section class="panel">
        <div class="section-kicker">Pivot / Fibonacci map</div>
        ${renderLevelChips(i.pivot_levels)}
        <div style="height:10px"></div>
        ${renderLevelChips(i.fib_levels)}
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
    ${renderActionPlan(stock)}
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
      } else if (status.status === "interrupted") {
        if (yahooEnrichPollTimer) clearInterval(yahooEnrichPollTimer);
        yahooEnrichPollTimer = null;
        yahooEnrichRunning = false;
        const resumeMsg = status.message || "Yahoo enrichment was interrupted. Click Sync Yahoo data again with force=false to resume; existing enriched symbols will be skipped.";
        setYahooEnrichUi(resumeMsg);
        setNotice(resumeMsg, "amber");
        try {
          databaseStatus = await getJson("/api/database/status");
          if (dashboard) dashboard.database = databaseStatus;
        } catch {}
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
    return !isFullScan || ["supabase_yahoo_history_scored", "insufficient_history", "data_quality_failed", "nse_dynamic_real", "nse_dynamic_focus"].includes(stock.data_mode || "");
  });
  const rankingReady = payload.scan_meta?.ranking_ready !== false;
  const rankableRows = realRows.filter(stock =>
    (stock.ranking_eligible !== false || ["real_nse_scored", "supabase_yahoo_history_scored", "nse_dynamic_real"].includes(stock.data_mode || ""))
    && !String(stock.data_mode || "").includes("proxy")
    && !String(stock.data_mode || "").includes("batch")
  );
  const strictFiltered = realRows.filter(stock => {
    const weekly = Number(stock.weekly_score || 0);
    const risk = Number(stock.risk_score || 0);
    return weekly >= 50 && risk < 25 && !["Avoid", "Hard Avoid"].includes(stock.conviction) && (stock.ranking_eligible !== false || ["real_nse_scored", "supabase_yahoo_history_scored", "nse_dynamic_real"].includes(stock.data_mode || ""));
  });
  const payloadWeekly = payload.top_weekly || payload.top_weekly_candidates || [];
  const payloadMonthly = payload.top_monthly || payload.top_monthly_candidates || [];
  const fallbackWeekly = (strictFiltered.length ? strictFiltered : rankableRows)
    .filter(stock => stock.conviction !== "Hard Avoid")
    .sort((a, b) => Number(b.weekly_score || 0) - Number(a.weekly_score || 0));
  const fallbackMonthly = (strictFiltered.length ? strictFiltered : rankableRows)
    .filter(stock => stock.conviction !== "Hard Avoid")
    .sort((a, b) => Number(b.monthly_score || 0) - Number(a.monthly_score || 0));
  return {
    stocks,
    topWeekly: rankingReady ? (payloadWeekly.length ? payloadWeekly : fallbackWeekly).slice(0, 3) : [],
    topMonthly: rankingReady ? (payloadMonthly.length ? payloadMonthly : fallbackMonthly).slice(0, 3) : [],
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
    const enrichedText = scanMeta.symbols_with_history ? `, with-history ${countText(scanMeta.symbols_with_history)}` : "";
    const rankableText = scanMeta.ranking_eligible_count ? `, rankable ${countText(scanMeta.ranking_eligible_count)}` : "";
    const insufficientText = scanMeta.insufficient_history_count ? `, awaiting enrichment ${countText(scanMeta.insufficient_history_count)}` : "";
    const qualityFailedText = scanMeta.data_quality_failed_count ? `, validation failed ${countText(scanMeta.data_quality_failed_count)}` : "";
    const excludedText = scanMeta.excluded_non_equity ? `, non-equity skipped ${countText(scanMeta.excluded_non_equity)}` : "";
    const basisText = scanMeta.rank_basis === "watch_only_relaxed" ? " Watch-only fallback shown because strict buy gates found no names." : "";
    const basisMore = scanMeta.rank_basis === "await_history_enrichment"
      ? ` ${scanMeta.ranking_warning || "Top picks are blank until real 220+ bar history is available."}`
      : basisText;
    setScanUi(`Scan complete. Universe ${countText(scanMeta.universe_size)}, seen ${countText(scanMeta.total_symbols_seen || scanMeta.passed_liquidity)}, scored ${countText(scanMeta.total_scored)}${enrichedText}${rankableText}${insufficientText}${qualityFailedText}${excludedText}, ranked ${countText(scanMeta.ranked_candidates)}.${basisMore}`);
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
    const enriched = meta.db_history?.symbols_with_history ?? meta.enriched_history_scored ?? "";
    const insufficient = meta.insufficient_history_count ?? "";
    const qualityFailed = meta.data_quality_failed_count ?? "";
    const excluded = meta.excluded_non_equity ?? "";
    setScanUi(
      `[${pct}%] ${elapsed}s elapsed - ${status.message || "Scanning real NSE universe..."}` +
      (scored !== "" ? ` | scored ${countText(scored)}` : "") +
      (passed !== "" ? ` / ${countText(passed)}` : "") +
      (enriched !== "" ? ` | enriched history ${countText(enriched)}` : "") +
      (insufficient !== "" ? ` | awaiting enrichment ${countText(insufficient)}` : "") +
      (qualityFailed !== "" ? ` | validation failed ${countText(qualityFailed)}` : "") +
      (excluded !== "" ? ` | non-equity skipped ${countText(excluded)}` : "") +
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
    refreshLiveDebugStatus(true).catch(() => {});
    qs("offlineNotice").textContent = `Shoonya status: ${liveStatus.status || "unknown"}`;
    if (liveSocket) liveSocket.close();
    liveSocket = null;
    connectLiveFeed();
    renderMarketStrip();
  } catch (error) {
    qs("offlineNotice").textContent = `Shoonya OTP login failed: ${error.message}`;
  }
}

function liveSubscriptionSymbols() {
  const queue = [
    ...(scanWeeklyResults || []),
    ...(scanMonthlyResults || []),
    ...readWatchlist().map(item => stockBySymbol(item.symbol) || {symbol: item.symbol}),
    ...(dashboard?.stocks || []).filter(stock => stock.candidate || isTriggeredStock(stock)).slice(0, 12),
  ];
  if (selectedStock?.symbol) queue.unshift(selectedStock);
  const seen = new Set();
  return queue
    .map(item => String(item?.symbol || "").toUpperCase())
    .filter(symbol => symbol && !seen.has(symbol) && seen.add(symbol))
    .slice(0, 30);
}

function connectLiveFeed() {
  if (!dashboard || !dashboard.stocks || isDemo) return;
  if (liveSocket && [WebSocket.OPEN, WebSocket.CONNECTING].includes(liveSocket.readyState)) return;
  const symbols = liveSubscriptionSymbols().join(",");
  if (!symbols) return;
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
      refreshLiveDebugStatus(false).catch(() => {});
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

function updateSelectedStockHeaderUi() {
  if (!selectedStock) return;
  const headlinePrice = qs("selectedStockHeadlinePrice");
  if (headlinePrice) headlinePrice.textContent = money(selectedStock.price);
  const headlineChange = qs("selectedStockHeadlineChange");
  if (headlineChange) {
    headlineChange.textContent = percent(selectedStock.change_pct);
    headlineChange.className = Number(selectedStock.change_pct || 0) >= 0 ? "positive" : "negative";
  }
  const decisionPrice = qs("selectedStockPrice");
  if (decisionPrice) decisionPrice.textContent = money(selectedStock.price);
}

function updateDecisionCardUi() {
  if (!selectedStock) return;
  const gatePass = Boolean(selectedStock.data_quality_gate?.pass);
  const action = actionDisplayLabel(selectedStock.action_plan?.action, gatePass);
  const tone = actionTone(action);
  const actionNode = document.querySelector("[data-decision-action]");
  if (actionNode) {
    actionNode.textContent = action;
    actionNode.className = `decision-action ${tone}`;
  }
  const summaryNode = document.querySelector("[data-decision-summary]");
  if (summaryNode) {
    summaryNode.textContent = gatePass
      ? (selectedStock.action_plan?.reason_summary || selectedStock.action_plan?.summary || selectedStock.verdict || "Research setup under review.")
      : (selectedStock.data_quality_gate?.warning || "Do not rank/action until data-quality gate passes.");
  }
}

function syncChartToolbarButtons() {
  document.querySelectorAll("[data-chart-toggle]").forEach(button => {
    const key = button.dataset.chartToggle;
    button.classList.toggle("active", Boolean(chartToggleState[key]));
  });
}

function clearChartPriceLines() {
  if (!candleSeries || !chartPriceLines.length) return;
  chartPriceLines.forEach(line => {
    try { candleSeries.removePriceLine(line); } catch {}
  });
  chartPriceLines = [];
}

function applyChartToggleVisibility() {
  if (ema20Series) ema20Series.applyOptions({visible: Boolean(chartToggleState.ema20)});
  if (ema50Series) ema50Series.applyOptions({visible: Boolean(chartToggleState.ema50)});
  if (dma200Series) dma200Series.applyOptions({visible: Boolean(chartToggleState.dma200)});
  if (volumeSeries) volumeSeries.applyOptions({visible: Boolean(chartToggleState.volume)});
  if (rsSeries) rsSeries.applyOptions({visible: Boolean(chartToggleState.rs)});
  syncChartToolbarButtons();
}

function activeChartCanvasId() {
  if (activeTab === "chart") return "priceChart";
  if (activeTab === "overview") return "overviewChart";
  return null;
}

function updateActiveChartFromSelectedStock(forceFresh = false) {
  if (!selectedStock) return;
  const canvasId = activeChartCanvasId();
  if (!canvasId || !qs(canvasId)) return;
  if (forceFresh) {
    drawChart(selectedStock.bars || [], canvasId, selectedStock.benchmark_bars || [], {fresh: true, fitContent: false});
    return;
  }
  drawLightweightChart(qs(canvasId), selectedStock.bars || [], selectedStock.benchmark_bars || [], {fitContent: false});
}

function updateLiveChartCandleFromSelected() {
  if (!selectedStock || !currentChart || currentChartSymbol !== selectedStock.symbol || !candleSeries) return false;
  const bars = selectedStock.bars || [];
  const last = bars[bars.length - 1];
  if (!last) return false;
  const candle = {
    time: barTime(last),
    open: Number(last.open ?? last.close),
    high: Number(last.high ?? last.close),
    low: Number(last.low ?? last.close),
    close: Number(last.close),
  };
  candleSeries.update(candle);
  if (volumeSeries) {
    volumeSeries.update({
      time: candle.time,
      value: Number(last.volume || 0),
      color: Number(last.close) >= Number(last.open ?? last.close) ? "rgba(16, 185, 129, 0.28)" : "rgba(244, 63, 94, 0.28)",
    });
  }
  return true;
}

function scheduleLiveRender() {
  if (liveRenderTimer) return;
  liveRenderTimer = setTimeout(() => {
    liveRenderTimer = null;
    renderStockList();
    if (selectedStock) {
      updateSelectedStockHeaderUi();
      updateDecisionCardUi();
      if (!updateLiveChartCandleFromSelected()) updateActiveChartFromSelectedStock(false);
    }
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

function drawLightweightChart(target, rows, benchmarkBars = [], options = {}) {
  if (!window.LightweightCharts || !target) return false;
  const chartRows = (rows || []).slice(-260).filter(bar =>
    Number.isFinite(Number(bar.open ?? bar.close)) &&
    Number.isFinite(Number(bar.high ?? bar.close)) &&
    Number.isFinite(Number(bar.low ?? bar.close)) &&
    Number.isFinite(Number(bar.close))
  );
  if (!chartRows.length) return false;
  const benchRows = (benchmarkBars || []).slice(-chartRows.length).filter(bar => Number.isFinite(Number(bar.close)));
  const shouldCreate = !currentChart || currentChartTargetId !== target.id || currentChartSymbol !== selectedStock?.symbol;
  const periodChanged = currentChartPeriod !== activeChartPeriod;
  const shouldFit = Boolean(options.fitContent || shouldCreate || periodChanged);
  const previousRange = !shouldCreate && !shouldFit ? currentChart.timeScale().getVisibleLogicalRange() : null;

  if (shouldCreate) {
    if (currentChart?.remove) currentChart.remove();
    if (chartResizeObserver) chartResizeObserver.disconnect();
    target.innerHTML = "";
    currentChart = LightweightCharts.createChart(target, {
      width: Math.max(320, target.clientWidth || 720),
      height: Math.max(220, target.clientHeight || 320),
      layout: {background: {type: "solid", color: "#111520"}, textColor: "#94a3b8", fontFamily: "Inter, system-ui, sans-serif"},
      grid: {vertLines: {color: "#1e2535"}, horzLines: {color: "#1e2535"}},
      crosshair: {mode: LightweightCharts.CrosshairMode.Normal},
      rightPriceScale: {borderColor: "#232b3e"},
      timeScale: {borderColor: "#232b3e", timeVisible: false},
    });
    candleSeries = currentChart.addCandlestickSeries({
      upColor: "#10b981",
      downColor: "#f43f5e",
      wickUpColor: "#10b981",
      wickDownColor: "#f43f5e",
      borderVisible: false,
    });
    ema20Series = currentChart.addLineSeries({color: "#f59e0b", lineWidth: 2, priceLineVisible: false, lastValueVisible: false});
    ema50Series = currentChart.addLineSeries({color: "#3b82f6", lineWidth: 2, priceLineVisible: false, lastValueVisible: false});
    dma200Series = currentChart.addLineSeries({color: "#10b981", lineWidth: 2, priceLineVisible: false, lastValueVisible: false});
    volumeSeries = currentChart.addHistogramSeries({
      priceFormat: {type: "volume"},
      priceScaleId: "volume",
      priceLineVisible: false,
      lastValueVisible: false,
    });
    volumeSeries.priceScale().applyOptions({scaleMargins: {top: 0.82, bottom: 0}});
    rsSeries = currentChart.addLineSeries({color: "#8b5cf6", lineWidth: 1, priceScaleId: "rs", priceLineVisible: false, lastValueVisible: false});
    rsSeries.priceScale().applyOptions({scaleMargins: {top: 0.66, bottom: 0.18}});
    const tooltip = qs("chartTooltip");
    currentChart.subscribeCrosshairMove(param => {
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
    chartResizeObserver = new ResizeObserver(() => currentChart?.applyOptions({width: target.clientWidth, height: target.clientHeight}));
    chartResizeObserver.observe(target);
  }

  const candleData = chartRows.map(bar => ({
    time: barTime(bar),
    open: Number(bar.open ?? bar.close),
    high: Number(bar.high ?? bar.close),
    low: Number(bar.low ?? bar.close),
    close: Number(bar.close),
  }));
  const closes = chartRows.map(bar => Number(bar.close));
  const ema20 = emaLine(closes, 20);
  const ema50 = emaLine(closes, 50);
  const sma200 = smaLine(closes, 200);
  candleSeries.setData(candleData);
  ema20Series.setData(lineDataFrom(ema20, candleData));
  ema50Series.setData(lineDataFrom(ema50, candleData));
  dma200Series.setData(lineDataFrom(sma200, candleData));
  volumeSeries.setData(chartRows.map((bar, index) => ({
    time: candleData[index].time,
    value: Number(bar.volume || 0),
    color: Number(bar.close) >= Number(bar.open ?? bar.close) ? "rgba(16, 185, 129, 0.28)" : "rgba(244, 63, 94, 0.28)",
  })));

  if (benchRows.length >= 20) {
    const aligned = chartRows.slice(-benchRows.length);
    const rsData = aligned.map((bar, index) => ({
      time: barTime(bar),
      value: Number(bar.close) / Math.max(Number(benchRows[index].close), 1) * 100,
    }));
    rsSeries.setData(rsData);
  } else if (rsSeries) {
    rsSeries.setData([]);
  }

  clearChartPriceLines();
  const entry = selectedStock?.entry || {};
  const plan = selectedStock?.action_plan || {};
  const indicators = selectedStock?.technical_strength?.indicators || {};
  const gatePass = Boolean(selectedStock?.data_quality_gate?.pass);
  const high52 = Math.max(...chartRows.map(bar => Number(bar.high || bar.close)));
  [
    gatePass && chartToggleState.trigger ? {value: Number(plan.trigger_price || entry.buy_stop_trigger || entry.breakout_level), title: "Trigger", color: "#f59e0b"} : null,
    gatePass && chartToggleState.stop ? {value: Number(plan.stop || entry.stop), title: "Stop", color: "#f43f5e"} : null,
    gatePass && chartToggleState.target1 ? {value: Number(plan.target_1 || entry.target_1), title: "Target 1", color: "#10b981"} : null,
    gatePass && chartToggleState.target2 ? {value: Number(plan.target_2 || entry.target_2), title: "Target 2", color: "#22c55e"} : null,
    {value: Number(indicators.pivot_levels?.r1), title: "Pivot R1", color: "#94a3b8"},
    {value: Number(indicators.pivot_levels?.s1), title: "Pivot S1", color: "#64748b"},
    {value: high52, title: "52W high", color: "#e2e8f0"},
  ].filter(line => line && Number.isFinite(line.value) && line.value > 0).forEach(line => {
    chartPriceLines.push(candleSeries.createPriceLine({
      price: line.value,
      color: line.color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title: line.title,
    }));
  });

  applyChartToggleVisibility();
  currentChartRows = chartRows;
  currentChartBenchmark = benchRows;
  currentChartTargetId = target.id;
  currentChartSymbol = selectedStock?.symbol || null;
  currentChartPeriod = activeChartPeriod;
  if (shouldFit) currentChart.timeScale().fitContent();
  else if (previousRange) currentChart.timeScale().setVisibleLogicalRange(previousRange);
  return true;
}

async function drawChart(bars, canvasId = "priceChart", benchmarkBars = [], options = {}) {
  const target = qs(canvasId);
  if (!target) return;
  let rows = bars || [];
  let bench = benchmarkBars || [];
  if ((canvasId === "priceChart" || canvasId === "overviewChart") && selectedStock?.symbol && !isDemo) {
    try {
      const payload = await getJson(`/api/chart/${selectedStock.symbol}?range=${activeChartPeriod}&fresh=${options.fresh ? 1 : 0}`);
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
  drawLightweightChart(target, rows, bench, options);
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
  if (event.target.closest("[data-cancel-stock-load]")) {
    cancelSelectedStockRequest();
    return;
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
    cancelSelectedStockRequest("");
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
  const refreshChartButton = event.target.closest("[data-refresh-chart]");
  if (refreshChartButton && selectedStock) {
    updateActiveChartFromSelectedStock(true);
    return;
  }
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
    if (activeTab === "chart" || activeTab === "overview") {
      document.querySelectorAll("[data-chart-period]").forEach(btn => btn.classList.toggle("active", btn.dataset.chartPeriod === activeChartPeriod));
      drawChart(selectedStock.bars || [], activeTab === "chart" ? "priceChart" : "overviewChart", selectedStock.benchmark_bars || [], {fresh: false, fitContent: true});
    } else {
      renderStockView(selectedStock);
    }
    return;
  }
  const chartToggle = event.target.closest("[data-chart-toggle]");
  if (chartToggle) {
    const key = chartToggle.dataset.chartToggle;
    chartToggleState[key] = !chartToggleState[key];
    persistChartToggleState();
    applyChartToggleVisibility();
    clearChartPriceLines();
    updateActiveChartFromSelectedStock(false);
    return;
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
  const canvasId = activeChartCanvasId();
  const target = canvasId ? qs(canvasId) : null;
  if (currentChart && target) {
    currentChart.applyOptions({width: target.clientWidth, height: target.clientHeight});
  }
});
loadDashboard().then(startPolling);
