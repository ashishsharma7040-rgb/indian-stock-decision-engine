function makeDemoDashboard() {
  const dashboard = {
    generated_at: new Date().toISOString(),
    market_regime: {
      score: 72,
      regime: "Risk-on",
      nifty_close: 22680,
      nifty_ema50: 22140,
      nifty_dma200: 21380,
      breadth_above_50dma: 61,
      sector_strength: 67,
      vix: 13.6,
      rules: {
        "Risk-on": "Allow breakouts, normal stops, normal candidate count",
        "Neutral": "Prefer pullbacks and leaders, keep risk moderate",
        "Risk-off": "Reduce breakout score, tighten stops, cut candidate count, raise quality threshold"
      }
    },
    prices_as_of: new Date().toISOString(),
    stocks: [
    {symbol:"POLYCAB",name:"Polycab India",sector:"Industrials",industry:"Cables and Wires",price:6850,change_pct:2.1,weekly_score:84,monthly_score:86,business_score:88,tailwind_score:84,event_score:86,technical_score:82,market_score:72,risk_score:8,conviction:"High",candidate:true,entry:{breakout_level:6782,aggressive:[6850,6940],pullback:[6650,6720],stop:6380,invalidation:"Close below 6300"},risk_flags:[]},
    {symbol:"TATAPOWER",name:"Tata Power",sector:"Power",industry:"Utilities and Renewables",price:412,change_pct:1.7,weekly_score:78,monthly_score:76,business_score:69,tailwind_score:82,event_score:78,technical_score:81,market_score:72,risk_score:12,conviction:"High",candidate:true,entry:{breakout_level:407,aggressive:[412,419],pullback:[400,406],stop:391,invalidation:"Close below 385"},risk_flags:["high_debt"]},
    {symbol:"SUZLON",name:"Suzlon Energy",sector:"Renewable Energy",industry:"Wind Equipment",price:61.4,change_pct:3.2,weekly_score:75,monthly_score:77,business_score:78,tailwind_score:84,event_score:80,technical_score:80,market_score:72,risk_score:18,conviction:"Watchlist",candidate:true,entry:{breakout_level:60.2,aggressive:[61.5,63],pullback:[58,60],stop:55.8,invalidation:"Close below 54"},risk_flags:["promoter_pledge"]},
    {symbol:"DIXON",name:"Dixon Technologies",sector:"Manufacturing",industry:"Electronics Manufacturing",price:14800,change_pct:1.2,weekly_score:69,monthly_score:77,business_score:82,tailwind_score:80,event_score:72,technical_score:64,market_score:72,risk_score:16,conviction:"Watchlist",candidate:false,entry:{breakout_level:15120,aggressive:null,pullback:[14200,14500],stop:13600,invalidation:"Close below 13400"},risk_flags:["stretched_price"]},
    {symbol:"KPITTECH",name:"KPIT Technologies",sector:"Technology",industry:"Auto Software",price:1680,change_pct:0.9,weekly_score:68,monthly_score:75,business_score:91,tailwind_score:78,event_score:75,technical_score:61,market_score:72,risk_score:14,conviction:"Watchlist",candidate:false,entry:{breakout_level:1715,aggressive:null,pullback:[1630,1655],stop:1560,invalidation:"Close below 1540"},risk_flags:[]},
    {symbol:"HDFCBANK",name:"HDFC Bank",sector:"Financials",industry:"Private Bank",price:1720,change_pct:0.4,weekly_score:58,monthly_score:70,business_score:80,tailwind_score:63,event_score:50,technical_score:52,market_score:72,risk_score:5,conviction:"Watchlist",candidate:false,entry:{breakout_level:1768,aggressive:null,pullback:[1680,1695],stop:1640,invalidation:"Close below 1620"},risk_flags:[]},
    {symbol:"LTIM",name:"LTIMindtree",sector:"Technology",industry:"IT Services",price:5420,change_pct:-0.8,weekly_score:49,monthly_score:62,business_score:74,tailwind_score:51,event_score:39,technical_score:45,market_score:72,risk_score:8,conviction:"Avoid",candidate:false,entry:{breakout_level:5680,aggressive:null,pullback:[5200,5250],stop:5100,invalidation:"Close below 5050"},risk_flags:["weak_event_layer"]},
    {symbol:"RELIANCE",name:"Reliance Industries",sector:"Diversified",industry:"Energy, Retail and Telecom",price:2847,change_pct:1.4,weekly_score:64,monthly_score:67,business_score:67,tailwind_score:66,event_score:59,technical_score:66,market_score:72,risk_score:9,conviction:"Watchlist",candidate:false,entry:{breakout_level:2910,aggressive:null,pullback:[2790,2810],stop:2761,invalidation:"Close below 2750"},risk_flags:[]}
    ],
    latest_critical_events: [
    {symbol:"POLYCAB",title:"Large power transmission order announced",source:"BSE filing",source_type:"exchange_filing",days_old:2,sentiment:0.88,importance:82,net_score:67},
    {symbol:"SUZLON",title:"Promoter pledge still visible in shareholding data",source:"Exchange shareholding data",source_type:"exchange_filing",days_old:15,sentiment:-0.38,importance:62,net_score:-22},
    {symbol:"LTIM",title:"Cautious demand commentary after recent results",source:"Earnings transcript",source_type:"earnings_transcript",days_old:12,sentiment:-0.48,importance:68,net_score:-28}
    ],
    disclaimer: "Research workflow only. This app does not provide investment advice or guaranteed predictions."
  };

  dashboard.top_weekly_candidates = dashboard.stocks.slice(0, 3);
  dashboard.top_monthly_candidates = [...dashboard.stocks].sort((a, b) => b.monthly_score - a.monthly_score).slice(0, 3);
  dashboard.avoid_list = dashboard.stocks.filter(s => s.conviction === "Avoid" || s.risk_score >= 18);
  dashboard.top_sectors = [
    {sector:"Power",avg_weekly_score:78,avg_tailwind_score:82,leader:"TATAPOWER",count:1},
    {sector:"Industrials",avg_weekly_score:84,avg_tailwind_score:84,leader:"POLYCAB",count:1},
    {sector:"Renewable Energy",avg_weekly_score:75,avg_tailwind_score:84,leader:"SUZLON",count:1},
    {sector:"Technology",avg_weekly_score:58.5,avg_tailwind_score:64.5,leader:"KPITTECH",count:2}
  ];

  dashboard.stocks.forEach(stock => {
    const state = stock.candidate && stock.price >= stock.entry.breakout_level ? "Triggered" : stock.candidate ? "Watchlist" : "Screened";
    const reference = stock.entry.aggressive?.[0] || stock.price;
    const riskPerShare = Math.max(reference - stock.entry.stop, 0);
    stock.weekly_raw_score = stock.weekly_score;
    stock.monthly_raw_score = stock.monthly_score;
    stock.trade_state = {state, reason: state === "Triggered" ? "Demo price is through breakout level" : "Demo state", breakout_level: stock.entry.breakout_level, stop: stock.entry.stop, last_price: stock.price};
    stock.entry.candidate_gate = stock.candidate ? "Pass" : "Blocked in demo gate";
    stock.entry.position_sizing = {
      account_size: 1000000,
      risk_fraction: 0.01,
      risk_capital: 10000,
      entry_reference: reference,
      risk_per_share: Number(riskPerShare.toFixed(2)),
      suggested_quantity: riskPerShare > 0 ? Math.floor(10000 / riskPerShare) : 0,
      approx_position_value: riskPerShare > 0 ? Math.floor(10000 / riskPerShare) * reference : 0
    };
  });

  return JSON.parse(JSON.stringify(dashboard));
}
