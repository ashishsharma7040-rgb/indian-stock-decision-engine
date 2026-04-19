from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, DateTime, Float, ForeignKey, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.sql import func

from database import Base


class Company(Base):
    __tablename__ = "companies"

    symbol = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    sector = Column(String, default="Unclassified")
    industry = Column(String)
    series = Column(String, default="EQ")
    isin = Column(String)
    market_cap_cr = Column(Float)
    is_candidate = Column(Boolean, default=False, index=True)
    weekly_score = Column(Integer)
    monthly_score = Column(Integer)
    conviction = Column(String)
    source = Column(String)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class DailyOHLCV(Base):
    __tablename__ = "daily_ohlcv"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    turnover = Column(Float)

    __table_args__ = (UniqueConstraint("symbol", "date", name="uix_symbol_date"),)


class Fundamental(Base):
    __tablename__ = "fundamentals"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), index=True)
    as_of = Column(Date, index=True)
    effective_date = Column(Date, index=True)
    knowledge_date = Column(Date, index=True)
    sales_cagr = Column(Float)
    profit_cagr = Column(Float)
    roce = Column(Float)
    roe = Column(Float)
    debt_equity = Column(Float)
    cfo_pat = Column(Float)
    fcf_trend = Column(String)
    promoter_holding_trend = Column(String)
    pledge_percent = Column(Float, default=0.0)
    dilution_flag = Column(Boolean, default=False)
    margin_trend_bps = Column(Float)
    pe = Column(Float)
    forward_pe = Column(Float)
    forward_profit_growth = Column(Float)
    pb = Column(Float)
    roa = Column(Float)
    nim = Column(Float)
    next_earnings_date = Column(Date)
    net_income = Column(Float)
    operating_cash_flow = Column(Float)
    cash_flow_investing = Column(Float)
    average_total_assets = Column(Float)
    ebitda = Column(Float)
    receivables_growth = Column(Float)
    revenue_growth = Column(Float)
    cash_conversion_cycle_days = Column(Float)
    previous_cash_conversion_cycle_days = Column(Float)
    altman_z_score = Column(Float)
    piotroski_f_score = Column(Integer)
    beneish_m_score = Column(Float)

    __table_args__ = (UniqueConstraint("symbol", "as_of", name="uix_fundamental_symbol_as_of"),)


class SectorTailwind(Base):
    __tablename__ = "sector_tailwind"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), index=True)
    as_of = Column(Date, index=True)
    demand_trend = Column(Float, default=50)
    policy_support = Column(Float, default=50)
    cost_environment = Column(Float, default=50)
    order_visibility = Column(Float, default=50)
    sector_momentum = Column(Float, default=50)
    tailwind_factors = Column(JSON, default=list)

    __table_args__ = (UniqueConstraint("symbol", "as_of", name="uix_tailwind_symbol_as_of"),)


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), index=True)
    timestamp = Column(DateTime(timezone=True), index=True)
    source = Column(String)
    source_type = Column(String)
    title = Column(String)
    sentiment = Column(Float)
    reliability = Column(Float)
    importance = Column(Float)
    freshness = Column(Float)
    net_score = Column(Float)
    url = Column(String)
    raw_json = Column(JSON)


class TradeState(Base):
    __tablename__ = "trade_states"

    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), primary_key=True)
    state = Column(String, default="Screened")
    breakout_level = Column(Float)
    stop = Column(Float)
    entry_price = Column(Float)
    last_price = Column(Float)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    history = Column(JSON, default=list)


class MarketRegime(Base):
    __tablename__ = "market_regime"

    id = Column(Integer, primary_key=True)
    as_of = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    nifty_trend = Column(Float)
    breadth = Column(Float)
    advance_decline_ratio = Column(Float)
    advancers = Column(Integer)
    decliners = Column(Integer)
    vix = Column(Float)
    sector_strength = Column(Float)
    regime = Column(String)
    score = Column(Float)
    raw_json = Column(JSON)


class CorporateAction(Base):
    __tablename__ = "corporate_actions"

    id = Column(Integer, primary_key=True)
    symbol = Column(String, ForeignKey("companies.symbol", ondelete="CASCADE"), index=True)
    ex_date = Column(Date, index=True)
    action_type = Column(String)
    ratio_numerator = Column(Float, default=1.0)
    ratio_denominator = Column(Float, default=1.0)
    price_adjustment_factor = Column(Float, default=1.0)
    volume_adjustment_factor = Column(Float, default=1.0)
    source = Column(String)
    raw_json = Column(JSON)

    __table_args__ = (UniqueConstraint("symbol", "ex_date", "action_type", name="uix_corp_action_symbol_date_type"),)


Index("idx_daily_ohlcv_symbol_date", DailyOHLCV.symbol, DailyOHLCV.date.desc())
Index("idx_companies_candidate_score", Company.is_candidate, Company.weekly_score.desc())
Index("idx_companies_symbol_name", Company.symbol, Company.name)
Index("idx_fundamentals_pit", Fundamental.symbol, Fundamental.knowledge_date, Fundamental.effective_date)
