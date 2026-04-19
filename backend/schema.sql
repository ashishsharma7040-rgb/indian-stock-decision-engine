create table if not exists companies (
  symbol text primary key,
  name text not null,
  sector text not null,
  industry text,
  market_cap_cr numeric,
  free_float_percent numeric,
  promoter_holding numeric,
  pledge_percent numeric,
  pe numeric,
  ev_ebitda numeric,
  updated_at timestamptz default now()
);

create table if not exists price_bars (
  symbol text references companies(symbol),
  datetime timestamptz not null,
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume numeric not null,
  primary key (symbol, datetime)
);

alter table price_bars add column if not exists data_source text default 'unknown';
alter table price_bars add column if not exists is_adjusted boolean default false;

create table if not exists technical_snapshot (
  symbol text references companies(symbol),
  as_of timestamptz not null,
  ema20 numeric,
  ema50 numeric,
  dma200 numeric,
  sma_200 numeric,
  extension_ratio numeric,
  rsi14 numeric,
  macd numeric,
  atr14 numeric,
  volume_ratio numeric,
  rel_strength numeric,
  breakout_level numeric,
  support_level numeric,
  resistance_level numeric,
  primary key (symbol, as_of)
);

create table if not exists market_regime_log (
  date date primary key,
  total_stocks_measured int not null,
  stocks_above_50dma int not null,
  breadth_percentage numeric(5, 2) not null,
  regime_status text not null,
  position_multiplier numeric(3, 2) not null,
  can_buy boolean not null default true,
  description text,
  raw_json jsonb default '{}'::jsonb
);

create table if not exists nifty500_constituents (
  symbol text primary key,
  name text,
  sector text,
  weight numeric(5, 2),
  as_of date,
  updated_at timestamptz default now()
);

create table if not exists market_breadth_history (
  date date primary key,
  nifty500_breadth_pct numeric(5, 2),
  stocks_above_50dma int,
  total_stocks int,
  source text,
  created_at timestamptz default now()
);

create table if not exists fundamentals_snapshot (
  symbol text references companies(symbol),
  as_of date not null,
  effective_date date,
  knowledge_date date,
  revenue_ttm numeric,
  sales_cagr numeric,
  profit_cagr numeric,
  roce numeric,
  roe numeric,
  debt_equity numeric,
  cfo_pat numeric,
  fcf numeric,
  margin numeric,
  margin_trend_bps numeric,
  forward_pe numeric,
  forward_profit_growth numeric,
  pb numeric,
  roa numeric,
  nim numeric,
  next_earnings_date date,
  net_income numeric,
  operating_cash_flow numeric,
  cash_flow_investing numeric,
  average_total_assets numeric,
  ebitda numeric,
  receivables_growth numeric,
  revenue_growth numeric,
  cash_conversion_cycle_days numeric,
  previous_cash_conversion_cycle_days numeric,
  altman_z_score numeric,
  piotroski_f_score numeric,
  beneish_m_score numeric,
  dilution_flag boolean default false,
  pledge_flag boolean default false,
  primary key (symbol, as_of)
);

create index if not exists idx_fundamentals_snapshot_pit
  on fundamentals_snapshot(symbol, knowledge_date, effective_date);

create table if not exists corporate_actions (
  id bigserial primary key,
  symbol text references companies(symbol),
  ex_date date not null,
  action_type text not null,
  ratio_numerator numeric default 1,
  ratio_denominator numeric default 1,
  price_adjustment_factor numeric default 1,
  volume_adjustment_factor numeric default 1,
  source text,
  raw_json jsonb,
  unique(symbol, ex_date, action_type)
);

create table if not exists sector_tailwind_snapshot (
  symbol text references companies(symbol),
  as_of date not null,
  demand_trend numeric,
  policy_support numeric,
  cost_environment numeric,
  order_visibility numeric,
  sector_momentum numeric,
  tailwind_factors jsonb default '[]'::jsonb,
  source_note text,
  primary key (symbol, as_of)
);

create table if not exists events (
  id bigserial primary key,
  symbol text references companies(symbol),
  timestamp timestamptz not null,
  source text,
  source_type text not null,
  title text not null,
  url text,
  category text,
  sentiment numeric not null,
  reliability numeric not null,
  importance numeric not null,
  freshness numeric not null,
  net_score numeric not null,
  raw_json jsonb
);

create table if not exists market_regime (
  as_of timestamptz primary key,
  nifty_trend numeric,
  breadth numeric,
  advance_decline_ratio numeric,
  advancers numeric,
  decliners numeric,
  vix numeric,
  sector_strength numeric,
  sector_rotation jsonb default '{}'::jsonb,
  source text,
  breadth_source text,
  regime text not null,
  score numeric not null
);

create table if not exists alert_events (
  id bigserial primary key,
  symbol text references companies(symbol),
  created_at timestamptz default now(),
  previous_state text,
  new_state text not null,
  price numeric,
  breakout_level numeric,
  stop numeric,
  delivered_to text,
  delivery_status text,
  raw_json jsonb
);

create table if not exists final_scores (
  symbol text references companies(symbol),
  as_of timestamptz not null,
  weekly_score numeric not null,
  monthly_score numeric not null,
  conviction text not null,
  risk numeric not null,
  explanation_json jsonb not null,
  primary key (symbol, as_of)
);

create table if not exists thesis_tracker (
  symbol text references companies(symbol),
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  picked_reason text,
  invalidation text,
  result_change text,
  review_note text,
  active boolean default true,
  primary key (symbol, created_at)
);

create table if not exists trade_state (
  symbol text primary key references companies(symbol),
  state text not null,
  watched_at timestamptz default now(),
  triggered_at timestamptz,
  entry_at timestamptz,
  exited_at timestamptz,
  breakout_level numeric,
  entry_price numeric,
  stop numeric,
  last_price numeric,
  notes text,
  history jsonb default '[]'::jsonb,
  updated_at timestamptz default now()
);

create table if not exists active_portfolio (
  symbol text primary key references companies(symbol),
  sector text not null,
  shares_held int not null,
  current_value numeric(12, 2) not null,
  updated_at timestamptz default now()
);

create table if not exists overnight_audit_cache (
  as_of date primary key,
  generated_at timestamptz default now(),
  market_health jsonb not null,
  universe_payload jsonb not null
);

create table if not exists backtest_runs (
  id bigserial primary key,
  created_at timestamptz default now(),
  config jsonb not null,
  summary jsonb not null,
  signals jsonb not null,
  warning text
);
