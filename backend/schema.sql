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

create table if not exists technical_snapshot (
  symbol text references companies(symbol),
  as_of timestamptz not null,
  ema20 numeric,
  ema50 numeric,
  dma200 numeric,
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

create table if not exists fundamentals_snapshot (
  symbol text references companies(symbol),
  as_of date not null,
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
  dilution_flag boolean default false,
  pledge_flag boolean default false,
  primary key (symbol, as_of)
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
  vix numeric,
  sector_strength numeric,
  regime text not null,
  score numeric not null
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

create table if not exists backtest_runs (
  id bigserial primary key,
  created_at timestamptz default now(),
  config jsonb not null,
  summary jsonb not null,
  signals jsonb not null,
  warning text
);
