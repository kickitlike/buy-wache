import "jsr:@supabase/functions-js/edge-runtime.d.ts";

/**
 * BUY-WACHE V26 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â REGRESSION-GATED / EXECUTION-AWARE AUDIT
 * Replacement candidate for Supabase Edge Function: buy-wache-scan
 *
 * Important:
 * - Heuristic scores are NOT calibrated probabilities.
 * - Uses only closed 1m candles for signal decisions.
 * - Separates EARLY_CANDIDATE, BREAKOUT_CONFIRMED, WATCH and FAILED_BREAKOUT.
 * - Missing data is UNRESOLVED, never converted to zero.
 * - H4 context is fetched natively; 1m history alone cannot produce 8 closed H4 bars.
 * - Provisional trade levels persist separately from ENTRY_READY.
 * - Candidate patch for test project only; not deployed. Do not deploy to production.
 */

const VERSION = "BUY_WACHE_V26_3_DATA_DRIVEN_ENTRY_CANDIDATE";
const UNIVERSE = [
  "FIL","CRV","INJ","HBAR","ASTER","STX","PUMP","TRX","PEPE","POL","LTC",
  "ZEC","UNI","LINK","HYPE","NEAR","WLD","ETC","LDO","ALGO","ONDO","TRUMP",
  "POPCAT","GRASS","APT","ARB","TAO","DOT","JTO","SHIB","PNUT","XPL","BONK",
  "PENGU","AIXBT","AVAX","KAITO","XRP","BNB","MON","S","OP","FARTCOIN","XLM",
  "ENA","WIF","TIA","VIRTUAL","ETHFI","PENDLE","ICP","ZRO","AAVE","RENDER","JUP"
] as const;

const headers = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-client-info, x-buy-wache-secret",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};
const reply = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers });
const n = (v: unknown): number => {
  const x = typeof v === "number" ? v : Number(v);
  return Number.isFinite(x) ? x : NaN;
};
const clamp = (x: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, x));
const pct = (a: number, b: number) => a > 0 ? (b / a - 1) * 100 : NaN;
const iso = (t: number) => new Date(t).toISOString();

async function json(url: string, timeoutMs = 7500): Promise<any> {
  const c = new AbortController();
  const timer = setTimeout(() => c.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: c.signal, headers: { accept: "application/json" } });
    const text = await r.text();
    if (!r.ok) throw new Error(`HTTP_${r.status}:${text.slice(0, 120)}`);
    try { return JSON.parse(text); } catch { throw new Error("INVALID_JSON"); }
  } finally { clearTimeout(timer); }
}
async function retryJson(url: string, timeoutMs = 7500, retries = 1): Promise<any> {
  let last: unknown;
  for (let i = 0; i <= retries; i++) {
    try { return await json(url, timeoutMs); }
    catch (e) { last = e; if (i < retries) await new Promise(r => setTimeout(r, 180 * (i + 1))); }
  }
  throw last instanceof Error ? last : new Error(String(last));
}
async function pool<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      try { out[i] = await fn(items[i]); }
      catch (e) { out[i] = ({ _poolError: String(e) } as unknown) as R; }
    }
  }));
  return out;
}

type Candle = { t: number; o: number; h: number; l: number; c: number; v: number; ct: number };
type Decision = "EARLY_CANDIDATE" | "BREAKOUT_CONFIRMED" | "WATCH" | "FAILED_BREAKOUT" | "NO_BUY" | "UNRESOLVED";
function parseBinance(raw: any[], now = Date.now(), intervalMs = 60000): Candle[] {
  if (!Array.isArray(raw)) return [];
  const rows = raw.filter(k => Array.isArray(k) && n(k[6]) < now && n(k[6]) > 0)
    .map(k => ({ t:n(k[0]), o:n(k[1]), h:n(k[2]), l:n(k[3]), c:n(k[4]), v:n(k[5]), ct:n(k[6]) }))
    .filter(b => [b.t,b.o,b.h,b.l,b.c,b.v,b.ct].every(Number.isFinite) &&
      b.o > 0 && b.c > 0 && b.l > 0 && b.h >= Math.max(b.o,b.c,b.l) &&
      b.l <= Math.min(b.o,b.c) && b.v >= 0 && b.ct - b.t === intervalMs - 1)
    .sort((a,b) => a.t - b.t);
  const unique: Candle[] = [];
  for (const b of rows) if (!unique.length || b.t > unique[unique.length - 1].t) unique.push(b);
  return unique.slice(-1000);
}
function parseBybit(raw: any[], now = Date.now(), intervalMs = 60000): Candle[] {
  if (!Array.isArray(raw)) return [];
  const rows = raw.filter(k => Array.isArray(k) && n(k[0]) + intervalMs - 1 < now)
    .map(k => ({ t:n(k[0]), o:n(k[1]), h:n(k[2]), l:n(k[3]), c:n(k[4]), v:n(k[5]), ct:n(k[0])+intervalMs-1 }))
    .filter(b => [b.t,b.o,b.h,b.l,b.c,b.v,b.ct].every(Number.isFinite) &&
      b.o > 0 && b.c > 0 && b.l > 0 && b.h >= Math.max(b.o,b.c,b.l) && b.v >= 0)
    .sort((a,b) => a.t - b.t);
  const unique: Candle[] = [];
  for (const b of rows) if (!unique.length || b.t > unique[unique.length - 1].t) unique.push(b);
  return unique.slice(-1000);
}
function aggregate(bars: Candle[], minutes: number): Candle[] {
  const groups = new Map<number, Candle[]>();
  for (const b of bars) {
    const key = Math.floor(b.t / (minutes * 60000)) * minutes * 60000;
    const group = groups.get(key) || [];
    group.push(b); groups.set(key, group);
  }
  const out: Candle[] = [];
  for (const [key, g] of groups) {
    if (g.length !== minutes || g[0].t !== key || g[g.length-1].t !== key+(minutes-1)*60000) continue;
    let continuous = true;
    for (let i=1;i<g.length;i++) if (g[i].t-g[i-1].t !== 60000) continuous = false;
    if (!continuous || g[g.length-1].ct >= Date.now()) continue;
    out.push({ t:key, o:g[0].o, h:Math.max(...g.map(x=>x.h)), l:Math.min(...g.map(x=>x.l)),
      c:g[g.length-1].c, v:g.reduce((s,x)=>s+x.v,0), ct:g[g.length-1].ct });
  }
  return out.sort((a,b)=>a.t-b.t);
}
function context(bars: Candle[], timeframe: string, minimum: number) {
  if (bars.length < minimum) return {
    status:"UNRESOLVED", reason:"INSUFFICIENT_CONTEXT_HISTORY", timeframe,
    bars:bars.length, required_bars:minimum
  };
  const last = bars[bars.length-1];
  const prev = bars.slice(0,-1);
  const recent = prev.slice(-12);
  const high = Math.max(...recent.map(x=>x.h));
  const low = Math.min(...recent.map(x=>x.l));
  // Trend is based on several closed candles, not just the colour of the latest candle.
  const trendBars = bars.slice(-Math.min(4,bars.length));
  const first = trendBars[0];
  const trendReturnPct = pct(first.c,last.c);
  const upBars = trendBars.filter(x=>x.c>x.o).length;
  const downBars = trendBars.filter(x=>x.c<x.o).length;
  const higherHighs = trendBars.length>=3 && trendBars[trendBars.length-1].h>trendBars[trendBars.length-2].h &&
    trendBars[trendBars.length-2].h>=trendBars[trendBars.length-3].h;
  const higherLows = trendBars.length>=3 && trendBars[trendBars.length-1].l>trendBars[trendBars.length-2].l &&
    trendBars[trendBars.length-2].l>=trendBars[trendBars.length-3].l;
  const lowerHighs = trendBars.length>=3 && trendBars[trendBars.length-1].h<trendBars[trendBars.length-2].h &&
    trendBars[trendBars.length-2].h<=trendBars[trendBars.length-3].h;
  const lowerLows = trendBars.length>=3 && trendBars[trendBars.length-1].l<trendBars[trendBars.length-2].l &&
    trendBars[trendBars.length-2].l<=trendBars[trendBars.length-3].l;
  const threshold = timeframe==="4h" ? 0.35 : timeframe==="1h" ? 0.15 : 0.10;
  let trend = "SIDEWAYS";
  if (trendReturnPct>threshold && (upBars>=2 || (higherHighs && higherLows))) trend="UP";
  else if (trendReturnPct< -threshold && (downBars>=2 || (lowerHighs && lowerLows))) trend="DOWN";
  return {
    status:"VALID", timeframe, bars:bars.length, return_pct:pct(last.o,last.c),
    trend, trend_return_pct:trendReturnPct, trend_sample_bars:trendBars.length,
    up_bars:upBars, down_bars:downBars, higher_highs:higherHighs, higher_lows:higherLows,
    lower_highs:lowerHighs, lower_lows:lowerLows,
    last_close:last.c, support:low, resistance:high,
    close_position:(last.c-last.l)/Math.max(last.h-last.l,1e-12),
    last_closed_at:iso(last.ct)
  };
}
function evaluate(bars: Candle[], symbol: string, source: string, h4Bars: Candle[] = []) {
  if (bars.length < 40) return null;
  const last = bars[bars.length-1], prev3 = bars.slice(-3);
  const baseline = bars.slice(-33,-3);
  if (prev3.length !== 3 || baseline.length < 20) return null;
  for (let i=0;i<bars.length;i++) {
    const b=bars[i];
    if (![b.t,b.o,b.h,b.l,b.c,b.v,b.ct].every(Number.isFinite) ||
      !(b.o>0 && b.c>0 && b.l>0 && b.h>=Math.max(b.o,b.c,b.l) && b.l<=Math.min(b.o,b.c) && b.v>=0 && b.ct===b.t+59999)) return null;
    if (i>0 && b.t<=bars[i-1].t) return null;
  }
  const avgVol = baseline.reduce((s,x)=>s+x.v,0)/baseline.length;
  const avgRange = baseline.reduce((s,x)=>s+(x.h-x.l),0)/baseline.length;
  const volumeRatio = avgVol > 0 ? last.v/avgVol : null;
  const rangeRatio = avgRange > 0 ? (last.h-last.l)/avgRange : null;
  const closePos = (last.c-last.l)/Math.max(last.h-last.l,1e-12);
  const return3 = pct(prev3[0].o,last.c);
  const return2 = pct(prev3[1].o,last.c);
  const last1mReturn = pct(prev3[1].c,prev3[2].c);
  const previous1mReturn = pct(prev3[0].c,prev3[1].c);
  const acceleration = last1mReturn-previous1mReturn;
  const allGreen = prev3.every(x=>x.c>x.o);
  const allRed = prev3.every(x=>x.c<x.o);
  const candleKinds=prev3.map(x=> {
    const body=Math.abs(x.c-x.o), range=Math.max(x.h-x.l,1e-12);
    if (body/range<=0.08) return "DOJI";
    if ((x.c-x.o)/range>=0.20) return "GREEN";
    if ((x.o-x.c)/range>=0.20) return "RED";
    return "SMALL_BODY";
  });
  const lastBodyPct=(last.c-last.o)/Math.max(last.o,1e-12)*100;
  const higherHighs = prev3[2].h > prev3[1].h && prev3[1].h >= prev3[0].h;
  const higherLows = prev3[2].l > prev3[1].l && prev3[1].l >= prev3[0].l;
  const structure = bars.slice(-33,-3);
  const resistance = Math.max(...structure.map(x=>x.h));
  const support = Math.min(...structure.map(x=>x.l));
  const breakoutBufferPct = 0.05;
  const aboveResistance = last.c > resistance * (1 + breakoutBufferPct/100);
  const nearResistance = last.c >= resistance * 0.996 && last.c <= resistance * 1.006;
  const pullbackFromHighPct = pct(last.c, Math.max(...prev3.map(x=>x.h))) * -1;
  const wickTopPct = last.c > 0 ? (last.h-last.c)/last.c*100 : NaN;
  const lowerWickPct = last.c > 0 ? (Math.min(last.o,last.c)-last.l)/last.c*100 : NaN;
  const volumeSupport = volumeRatio !== null && volumeRatio >= 1.20;
  const rangeExpansion = rangeRatio !== null && rangeRatio >= 1.15;
  const exhaustion = closePos < 0.32 || wickTopPct > 0.45 || (return3 > 0.85 && closePos < 0.55) || pullbackFromHighPct > 0.55;
  const m5 = context(aggregate(bars,5),"5m",12);
  const m15 = context(aggregate(bars,15),"15m",8);
  const h1 = context(aggregate(bars,60),"1h",6);
  const h4 = context(h4Bars,"4h",8);
  const trendBonus = (h1.status==="VALID" ? (h1.trend==="UP"?4:h1.trend==="DOWN"?-4:0) : 0) + (h4.status==="VALID" ? (h4.trend==="UP"?3:h4.trend==="DOWN"?-3:0) : 0);
  const components = {
    positive_return: clamp(return3*16,-8,10), acceleration: clamp(acceleration*22,-7,8),
    close_quality: clamp((closePos-0.5)*12,-6,6),
    candle_structure: (allGreen?4:allRed?-4:0)+(higherHighs?2:0)+(higherLows?2:0),
    volume: volumeRatio===null?0:clamp((volumeRatio-1)*4,-3,7),
    range: rangeRatio===null?0:clamp((rangeRatio-1)*3,-2,5),
    resistance: aboveResistance?8:nearResistance?3:0, trend:trendBonus, exhaustion_penalty:exhaustion?-12:0
  };
  const signalScore = Math.round(clamp(50+Object.values(components).reduce((s,x)=>s+x,0),0,100));
  const directionPositive = return3>0 && last1mReturn>0 && acceleration>0 && lastBodyPct>0 && closePos>=0.55;
  const earlyEvidence = directionPositive && (volumeSupport || rangeExpansion) && (nearResistance || higherHighs || higherLows) && signalScore >= 57 && !exhaustion;
  // A range spike cannot substitute for volume. Confirmed breakout requires rising 5m context
  // and no valid 1h downtrend. Countertrend impulses can remain early candidates, never confirmed.
  const breakoutQuality = aboveResistance && closePos >= 0.70 && volumeSupport && m5.status==="VALID" && m5.trend==="UP" && h1.status==="VALID" && h1.trend!=="DOWN" && !exhaustion && lastBodyPct>0;
  const failedBreakoutRisk = aboveResistance && (closePos < 0.45 || (volumeRatio !== null && volumeRatio < 0.75) || wickTopPct > 0.55 || pullbackFromHighPct > 0.45 || lastBodyPct<=0);
  let decision: Decision;
  if (failedBreakoutRisk) decision = "FAILED_BREAKOUT";
  else if (breakoutQuality) decision = "BREAKOUT_CONFIRMED";
  else if (earlyEvidence) decision = "EARLY_CANDIDATE";
  else if (signalScore >= 54 || nearResistance) decision = "WATCH";
  else decision = "NO_BUY";
  const rejection: string[] = [];
  if (return3 <= 0) rejection.push("NON_POSITIVE_3M_RETURN");
  if (last1mReturn <= 0) rejection.push("NON_POSITIVE_LAST_1M_RETURN");
  if (acceleration <= 0) rejection.push("ACCELERATION_NOT_POSITIVE");
  if (lastBodyPct<=0) rejection.push("LAST_CANDLE_NOT_BULLISH");
  if (closePos < 0.55) rejection.push("CLOSE_POSITION_WEAK");
  if (volumeRatio === null) rejection.push("VOLUME_BASELINE_UNRESOLVED"); else if (volumeRatio < 1.20) rejection.push("VOLUME_CONFIRMATION_MISSING");
  if (rangeRatio === null) rejection.push("RANGE_BASELINE_UNRESOLVED"); else if (rangeRatio < 1.15) rejection.push("RANGE_EXPANSION_MISSING");
  if (!aboveResistance && !nearResistance) rejection.push("NOT_AT_BREAKOUT_LEVEL");
  if (exhaustion) rejection.push("EXHAUSTION_RISK");
  if (failedBreakoutRisk) rejection.push("FAILED_BREAKOUT_RISK");
  if (m5.status!=="VALID") rejection.push("5M_CONTEXT_UNRESOLVED"); else if (m5.trend!=="UP") rejection.push("5M_TREND_NOT_UP");
  if (h1.status!=="VALID") rejection.push("1H_CONTEXT_UNRESOLVED"); else if (h1.trend==="DOWN") rejection.push("1H_TREND_DOWN");
  if (h4.status!=="VALID") rejection.push("4H_CONTEXT_UNRESOLVED"); else if (h4.trend==="DOWN") rejection.push("4H_TREND_DOWN");
  const feeBps = n(Deno.env.get("BUY_WACHE_FEE_BPS_PER_SIDE") ?? "10");
  const spreadBps = n(Deno.env.get("BUY_WACHE_SPREAD_BPS_TOTAL") ?? "5");
  const slippageBps = n(Deno.env.get("BUY_WACHE_SLIPPAGE_BPS_PER_SIDE") ?? "5");
  const costsConfigured=[feeBps,spreadBps,slippageBps].every(x=>Number.isFinite(x)&&x>=0);
  const costPct = costsConfigured ? (2*feeBps+spreadBps+2*slippageBps)/100 : NaN;
  const entry = last.c;
  const riskPct = clamp(Math.max((entry-support)/entry, (avgRange/entry)*1.2, 0.0025),0.0025,0.025);
  const stop = entry*(1-riskPct), risk=entry-stop;
  // Prefer a real overhead resistance from higher-timeframe structure. If none is above
  // market, use a volatility-derived fallback and label it provisional rather than factual resistance.
  const overheadLevels = [m15, h1, h4]
    .filter((x:any) => x.status === "VALID" && Number.isFinite(x.resistance) && x.resistance > entry)
    .map((x:any) => x.resistance)
    .sort((a:number,b:number) => a-b);
  const hasOverheadResistance = overheadLevels.length > 0;
  const provisionalTarget = entry + Math.max(avgRange*1.5, entry*0.003);
  const tp1 = hasOverheadResistance ? overheadLevels[0] : provisionalTarget;
  const tp2 = Math.max(tp1, entry+risk*2.2), tp3 = Math.max(tp2,entry+risk*3);
  const grossRR1=(tp1-entry)/risk;
  const netReward=(tp1-entry)-entry*costPct/100, netRisk=risk+entry*costPct/100;
  const rr1=netRisk>0 ? netReward/netRisk : NaN;

  // For fixed TP1 and SL, solve the entry price that yields at least 1.5 net R
  // after the configured round-trip cost estimate:
  // (TP1 - E - E*c) / (E - SL + E*c) >= R
  // => E <= (TP1 + R*SL) / ((1+R)*(1+c)).
  const rawMaxEntry = costsConfigured ? (tp1 + 1.5*stop) / (2.5 * (1 + costPct/100)) : NaN;
  const priceTolerance = Number.isFinite(rawMaxEntry) ? Math.max(1, Math.abs(rawMaxEntry)) * 1e-10 : 0;
  const rr1Pass = Number.isFinite(rr1) && rr1 >= 1.5 - 1e-9;
  const maxEntry = Number.isFinite(rawMaxEntry) && rawMaxEntry >= entry - priceTolerance ? rawMaxEntry : null;
  const pullbackEntry = Number.isFinite(rawMaxEntry) && rawMaxEntry < entry - priceTolerance &&
    rawMaxEntry > stop + priceTolerance ? rawMaxEntry : null;
  const rrAtPullback = pullbackEntry !== null
    ? (((tp1-pullbackEntry)-pullbackEntry*costPct/100) /
       ((pullbackEntry-stop)+pullbackEntry*costPct/100))
    : NaN;
  const pullbackRRPass = costsConfigured && Number.isFinite(rrAtPullback) && rrAtPullback >= 1.5 - 1e-9;
  const entryStatus = !costsConfigured ? "COST_MODEL_UNRESOLVED"
    : !(tp1 > entry && stop > 0 && stop < entry) ? "INVALID_TP_SL_STRUCTURE"
    : !Number.isFinite(rawMaxEntry) || rawMaxEntry <= stop + priceTolerance ? "NO_FEASIBLE_ENTRY_RANGE"
    : rr1Pass ? "CURRENT_ENTRY_MAY_QUALIFY"
    : pullbackRRPass ? "WAIT_FOR_PULLBACK_RR"
    : "NO_FEASIBLE_ENTRY_RANGE";
  const actionableEntry = rr1Pass ? entry : pullbackRRPass ? pullbackEntry : null;
  const rrActionable = actionableEntry === null ? null
    : (((tp1-actionableEntry)-actionableEntry*costPct/100) /
       ((actionableEntry-stop)+actionableEntry*costPct/100));
  const h4Ready = h4.status === "VALID" && h4.trend !== "DOWN";
  if (decision !== "BREAKOUT_CONFIRMED") rejection.push("BREAKOUT_NOT_CONFIRMED");
  if (!costsConfigured) rejection.push("COST_MODEL_UNRESOLVED");
  if (!rr1Pass) rejection.push("NET_RR_TOO_LOW");
  if (Number.isFinite(rawMaxEntry) && entry > rawMaxEntry + priceTolerance) rejection.push("ENTRY_ABOVE_MAX_ENTRY");
  if (entryStatus === "INVALID_TP_SL_STRUCTURE") rejection.push("INVALID_TP_SL_STRUCTURE");
  if (entryStatus === "NO_FEASIBLE_ENTRY_RANGE") rejection.push("NO_FEASIBLE_ENTRY_RANGE");
  if (entryStatus === "WAIT_FOR_PULLBACK_RR") rejection.push("WAIT_FOR_PULLBACK_RR");
  const entryQuality = entryStatus;
  const tradable = decision === "BREAKOUT_CONFIRMED" && !exhaustion && costsConfigured &&
    rr1Pass && maxEntry !== null && entry <= maxEntry + priceTolerance && entry >= stop && h4Ready;
  return {
    symbol, source, status:"VALID", source_timestamp:iso(last.ct), freshness_ms:Date.now()-last.ct, closed_candles:bars.length,
    outcome_candles:bars.slice(-1000).map(x=>({open_time:iso(x.t),open:x.o,high:x.h,low:x.l,close:x.c,volume:x.v,close_time:iso(x.ct)})),
    last_candles:prev3.map(x=>({open_time:iso(x.t),open:x.o,high:x.h,low:x.l,close:x.c,volume:x.v,close_time:iso(x.ct)})),
    price:last.c, decision, entry_ready:tradable, early_candidate:decision==="EARLY_CANDIDATE", signal_score:signalScore,
    score_type:"HEURISTIC_RANKING_SCORE_NOT_PROBABILITY", probability:null,
    probability_note:"Not calibrated; no percentage probability is emitted.",
    features:{return_3m_pct:return3,return_last_2m_pct:return2,return_last_1m_pct:last1mReturn,previous_1m_return_pct:previous1mReturn,acceleration_pct:acceleration,
      candle_kinds:candleKinds,last_candle_body_pct:lastBodyPct,lower_wick_pct:lowerWickPct,volume_ratio:volumeRatio,range_vs_baseline:rangeRatio,close_location:closePos,
      all_three_green:allGreen,all_three_red:allRed,higher_highs:higherHighs,higher_lows:higherLows,resistance,support,above_resistance:aboveResistance,near_resistance:nearResistance,
      breakout_buffer_pct:breakoutBufferPct,wick_top_pct:wickTopPct,pullback_from_3c_high_pct:pullbackFromHighPct,exhaustion_risk:exhaustion,failed_breakout_risk:failedBreakoutRisk,direction_positive:directionPositive,score_components:components},
    context:{m5,m15,h1,h4},entry_rejection_reasons:[...new Set(tradable?[]:rejection)],
    trade:{entry,market_entry:entry,actionable_entry:actionableEntry,max_entry:maxEntry,raw_max_entry:rawMaxEntry,
      pullback_entry:pullbackEntry,entry_status:entryStatus,stop_loss:stop,take_profit_1:tp1,take_profit_2:tp2,take_profit_3:tp3,
      rr_tp1:rr1,rr_actionable:rrActionable,rr_tp1_gross:grossRR1,
      rr_tp2:(tp2-entry)/risk,rr_tp3:(tp3-entry)/risk,estimated_round_trip_cost_pct:costPct,
      cost_model:"CONFIGURABLE_ESTIMATE_NOT_LIVE_SPREAD",entry_quality:entryQuality,
      target_basis:hasOverheadResistance?"NEAREST_VALID_OVERHEAD_RESISTANCE":"VOLATILITY_FALLBACK_PROVISIONAL",
      overhead_resistance:hasOverheadResistance?overheadLevels[0]:null,
      note:"Entry threshold uses fixed provisional TP1/SL and estimated fees/spread/slippage; costs are assumptions, not live execution quotes."}
  };
}

async function db(base: string, key: string, path: string, method: string, body?: unknown): Promise<any> {
  const r = await fetch(`${base}/rest/v1/${path}`, {
    method,
    headers: { apikey:key, Authorization:`Bearer ${key}`, "Content-Type":"application/json",
      Prefer:"return=representation" },
    body: body===undefined ? undefined : JSON.stringify(body)
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`DB_${r.status}:${text.slice(0,250)}`);
  return text ? JSON.parse(text) : null;
}
async function insertOutcomeEvaluations(base: string, key: string, rows: unknown[]): Promise<any[]> {
  const query = "prediction_outcome_evaluations?on_conflict=prediction_id,evaluator_version,horizon_minutes";
  const r = await fetch(`${base}/rest/v1/${query}`, {
    method:"POST",
    headers:{apikey:key,Authorization:`Bearer ${key}`,"Content-Type":"application/json",
      Prefer:"resolution=ignore-duplicates,return=representation"},
    body:JSON.stringify(rows)
  });
  const text=await r.text();
  if(!r.ok) throw new Error(`OUTCOME_EVALUATION_DB_${r.status}:${text.slice(0,250)}`);
  return text?JSON.parse(text):[];
}
function constantTimeEqual(a: string,b: string) {
  const x=new TextEncoder().encode(a), y=new TextEncoder().encode(b);
  if (!x.length || x.length!==y.length) return false;
  let diff=0; for(let i=0;i<x.length;i++) diff |= x[i]^y[i];
  return diff===0;
}
async function fetchCandles(coin: string, spotSymbols: Set<string>, futuresSymbols: Set<string>) {
  const pair=coin+"USDT", errors:string[]=[];
  const attempts: { source:string; url:string; h4url:string }[]=[];
  if (spotSymbols.has(pair)) attempts.push({source:"BINANCE_SPOT",
    url:`https://data-api.binance.vision/api/v3/klines?symbol=${pair}&interval=1m&limit=1000`,
    h4url:`https://data-api.binance.vision/api/v3/klines?symbol=${pair}&interval=4h&limit=20`});
  if (futuresSymbols.has(pair)) attempts.push({source:"BINANCE_FUTURES",
    url:`https://fapi.binance.com/fapi/v1/klines?symbol=${pair}&interval=1m&limit=1000`,
    h4url:`https://fapi.binance.com/fapi/v1/klines?symbol=${pair}&interval=4h&limit=20`});
  for (const a of attempts) {
    try {
      const bars=parseBinance(await retryJson(a.url,8500,1));
      if (bars.length < 40) { errors.push(`${a.source}:INSUFFICIENT_CANDLES_${bars.length}`); continue; }
      const age=Date.now()-bars[bars.length-1].ct;
      if (age>150000) { errors.push(`${a.source}:STALE_${age}MS`); continue; }
      let h4Bars:Candle[]=[];
      try {
        h4Bars=parseBinance(await retryJson(a.h4url,6500,1),Date.now(),240*60000);
        if (h4Bars.length < 8) errors.push(`${a.source}_H4:INSUFFICIENT_CANDLES_${h4Bars.length}`);
        else if (Date.now()-h4Bars[h4Bars.length-1].ct > 4*60*60*1000 + 5*60*1000) {
          errors.push(`${a.source}_H4:STALE`); h4Bars=[];
        }
      } catch(e) { errors.push(`${a.source}_H4:${String(e).slice(0,100)}`); }
      return {bars,h4Bars,source:a.source,errors};
    } catch(e) { errors.push(`${a.source}:${String(e).slice(0,120)}`); }
  }
  for (const host of ["https://api.bybit.com","https://api.bytick.com","https://api.bybit.nl"]) {
    try {
      const x=await retryJson(`${host}/v5/market/kline?category=linear&symbol=${pair}&interval=1&limit=1000`,6500,1);
      if (n(x?.retCode)!==0) throw new Error(`BYBIT_${x?.retCode}`);
      const bars=parseBybit(x?.result?.list||[]);
      if (bars.length<40) throw new Error(`INSUFFICIENT_CANDLES_${bars.length}`);
      const age=Date.now()-bars[bars.length-1].ct;
      if (age>150000) throw new Error(`STALE_${age}MS`);
      let h4Bars:Candle[]=[];
      try {
        const hx=await retryJson(`${host}/v5/market/kline?category=linear&symbol=${pair}&interval=240&limit=20`,6500,1);
        if (n(hx?.retCode)!==0) throw new Error(`BYBIT_H4_${hx?.retCode}`);
        h4Bars=parseBybit(hx?.result?.list||[],Date.now(),240*60000);
        if (h4Bars.length<8) { errors.push(`${new URL(host).hostname}_H4:INSUFFICIENT_CANDLES_${h4Bars.length}`); h4Bars=[]; }
        else if (Date.now()-h4Bars[h4Bars.length-1].ct > 4*60*60*1000 + 5*60*1000) {
          errors.push(`${new URL(host).hostname}_H4:STALE`); h4Bars=[];
        }
      } catch(e) { errors.push(`${new URL(host).hostname}_H4:${String(e).slice(0,100)}`); }
      return {bars,h4Bars,source:"BYBIT_LINEAR",errors};
    } catch(e) { errors.push(`${new URL(host).hostname}:${String(e).slice(0,100)}`); }
  }
  return {bars:[] as Candle[],h4Bars:[] as Candle[],source:"UNRESOLVED",errors};
}

Deno.serve(async (req: Request) => {
  if (req.method==="OPTIONS") return reply({ok:true});
  if (req.method==="GET") {
    const configured=Deno.env.get("BUY_WACHE_INVOKE_SECRET")||"";
    const supplied=[req.headers.get("x-buy-wache-secret")||"",req.headers.get("apikey")||"",
      req.headers.get("authorization")||"",(req.headers.get("authorization")||"").replace(/^Bearer\s+/i,"")];
    if (!configured || !supplied.some((v)=>v.length>0&&constantTimeEqual(v,configured)))
      return reply({error:"UNAUTHORIZED"},401);
    return reply({service:"buy-wache-scan",version:VERSION,universe_count:UNIVERSE.length,
      probabilities:"not emitted; heuristic scores only",mode:"health"});
  }
  if (req.method!=="POST") return reply({error:"POST_REQUIRED"},405);

  const started=Date.now();
  const base=Deno.env.get("SUPABASE_URL")||"";
  let key=Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")||"";
  if (!key) {
    try {
      const secretKeys=JSON.parse(Deno.env.get("SUPABASE_SECRET_KEYS")||"{}");
      key=typeof secretKeys?.default==="string" ? secretKeys.default : "";
    } catch { /* configuration check below returns a safe error */ }
  }
  const invokeSecret=Deno.env.get("BUY_WACHE_INVOKE_SECRET")||"";
  if (!base || !key || !invokeSecret) return reply({error:"SERVER_CONFIG_MISSING",
    detail:"SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY and BUY_WACHE_INVOKE_SECRET are required."},500);
  // Accept the configured invoke secret in the custom header or common GPT Action API-key header formats.
  // All candidates are still compared against the same server-side secret.
  const authHeader = req.headers.get("authorization") || "";
  const bearerValue = /^Bearer\s+/i.test(authHeader) ? authHeader.replace(/^Bearer\s+/i, "") : "";
  const suppliedSecrets = [
    req.headers.get("x-buy-wache-secret") || "",
    req.headers.get("apikey") || "",
    authHeader,
    bearerValue,
  ];
  if (!suppliedSecrets.some((candidate) => candidate.length > 0 && constantTimeEqual(candidate, invokeSecret)))
    return reply({error:"INVALID_INVOKE_SECRET",auth_diagnostic:{
      custom_header_present:Boolean(req.headers.get("x-buy-wache-secret")),
      apikey_header_present:Boolean(req.headers.get("apikey")),
      authorization_header_present:Boolean(authHeader),
      bearer_format:Boolean(bearerValue)
    }},401);

  // Preserve the current deployment's configured owner until the project auth model is deliberately changed.
  const userId="75875ec8-5e42-4e0a-bb8f-fa8e996c5a79";
  let run: any;
  try {
    const created=await db(base,key,"scan_runs","POST",{
      user_id:userId,status:"RUNNING",universe_count:UNIVERSE.length,
      source_summary:{version:VERSION,primary:"Binance Spot 1m",fallbacks:["Binance Futures 1m","Bybit linear 1m"]},
      audit:{expected_universe:[...UNIVERSE],probability_basis:"NONE_UNCALIBRATED"}
    });
    run=Array.isArray(created)?created[0]:created;
  } catch(e) { return reply({error:"RUN_CREATE_FAILED",detail:String(e)},500); }
  if (!run?.id) return reply({error:"RUN_CREATE_FAILED"},500);

  let spotSymbols=new Set<string>(), futuresSymbols=new Set<string>();
  const universeErrors:string[]=[];
  try {
    const x=await retryJson("https://data-api.binance.vision/api/v3/exchangeInfo",6500,1);
    spotSymbols=new Set((x.symbols||[]).filter((s:any)=>s.status==="TRADING"&&s.isSpotTradingAllowed!==false).map((s:any)=>s.symbol));
  } catch(e) { universeErrors.push("SPOT_EXCHANGE_INFO:"+String(e).slice(0,120)); }
  try {
    const x=await retryJson("https://fapi.binance.com/fapi/v1/exchangeInfo",6500,1);
    futuresSymbols=new Set((x.symbols||[]).filter((s:any)=>s.status==="TRADING").map((s:any)=>s.symbol));
  } catch(e) { universeErrors.push("FUTURES_EXCHANGE_INFO:"+String(e).slice(0,120)); }

  const results:any[]=await pool<any,any>([...UNIVERSE],10,async (coin:any)=>{
    const f=await fetchCandles(coin,spotSymbols,futuresSymbols);
    if (!f.bars.length) return {coin,symbol:coin+"USDT",status:"UNRESOLVED",source:"UNRESOLVED",
      error_code:"ALL_CANDLE_SOURCES_FAILED",detail:f.errors.join(" | ").slice(0,500),provider_errors:f.errors};
    const result=evaluate(f.bars,coin,f.source,f.h4Bars||[]);
    if (!result) return {coin,symbol:coin+"USDT",status:"UNRESOLVED",source:f.source,
      error_code:"INSUFFICIENT_VALID_HISTORY",provider_errors:f.errors};
    return {...result,coin,symbol:coin+"USDT",provider_errors:f.errors};
  });

  const valid=results.filter((x:any)=>x.status==="VALID");
  const candidates=valid.slice().sort((a:any,b:any)=>
    (b.decision==="BREAKOUT_CONFIRMED"?4:b.decision==="EARLY_CANDIDATE"?3:b.decision==="WATCH"?2:0)-
    (a.decision==="BREAKOUT_CONFIRMED"?4:a.decision==="EARLY_CANDIDATE"?3:a.decision==="WATCH"?2:0) ||
    b.signal_score-a.signal_score).slice(0,8);

  const observations=results.map((x:any)=>({
    run_id:run.id,user_id:userId,symbol:x.coin,observed_at:new Date().toISOString(),
    source:x.source||"UNRESOLVED",
    validation_status:x.status==="VALID"?"VALID":"UNAVAILABLE",
    current_price:x.price??null,change_24h_pct:null,
    candles_1m:(x.outcome_candles||x.last_candles||[]).slice(-70),
    features:{
      decision:x.decision||"UNRESOLVED",signal_score:x.signal_score??null,
      score_type:x.score_type||null,features:x.features||null,context:x.context||null,
      trade:x.trade||null,entry_ready:x.entry_ready??false,
      entry_rejection_reasons:x.entry_rejection_reasons||[],
      provider_errors:x.provider_errors||[],error_code:x.error_code||null
    },
    error_code:x.error_code||null,error_message:x.detail||null
  }));
  const errors:string[]=[];
  try { await db(base,key,"coin_observations","POST",observations); }
  catch(e) { errors.push("OBSERVATION_WRITE:"+String(e).slice(0,220)); }

  // Persist one record per horizon, explicitly labelled HEURISTIC. Percent probabilities are not supplied.
  const predictions=valid.flatMap((x:any)=>[15,60].map(h=>({
    run_id:run.id,user_id:userId,symbol:x.coin,prediction_time:x.source_timestamp||new Date().toISOString(),
    horizon_minutes:h,breakout_probability:null,continuation_probability:null,exhaustion_probability:null,
    probability_basis:"HEURISTIC",early_move_score:x.signal_score,
    buy_state:x.decision==="BREAKOUT_CONFIRMED"?"BREAKOUT BUY":
      x.decision==="EARLY_CANDIDATE"?"EARLY BUY":
      x.decision==="FAILED_BREAKOUT"?"NO BUY":
      x.decision==="WATCH"?"WATCH":x.decision==="NO_BUY"?"NO BUY":"UNRESOLVED",
    features:{decision:x.decision,signal_score:x.signal_score,score_type:x.score_type,
      reference_price:x.price,source_timestamp:x.source_timestamp,
      features:x.features,context:x.context,trade:x.trade,entry_ready:x.entry_ready,
      entry_rejection_reasons:x.entry_rejection_reasons,
      prediction_note:"No calibrated probability emitted; evaluate future outcome from later candle observations."}
  })));
  try { if(predictions.length) await db(base,key,"predictions","POST",predictions); }
  catch(e) { errors.push("PREDICTION_WRITE:"+String(e).slice(0,220)); }

  // Persist provisional levels for every valid coin; entry_ready remains a separate permission gate.
  const tradeCandidates=valid.filter((x:any)=>x.trade && Number.isFinite(x.trade.entry));
  try {
    if (tradeCandidates.length) await db(base,key,"trade_levels","POST",tradeCandidates.map((x:any)=>({
      run_id:run.id,user_id:userId,symbol:x.coin,entry:x.trade.entry,max_entry:x.trade.raw_max_entry,
      stop_loss:x.trade.stop_loss,take_profit_1:x.trade.take_profit_1,
      take_profit_2:x.trade.take_profit_2,take_profit_3:x.trade.take_profit_3,
      rr_tp1:x.trade.rr_tp1,rr_tp2:x.trade.rr_tp2,rr_tp3:x.trade.rr_tp3,
      entry_quality:x.trade.entry_quality,entry_ready:x.entry_ready,
      decision:x.decision,rejection_reasons:x.entry_rejection_reasons,provisional:true
    })));
  } catch(e) { errors.push("TRADE_LEVEL_WRITE:"+String(e).slice(0,220)); }

  let readback:any={verified:false};
  try {
    const [o,p,t]=await Promise.all([
      db(base,key,`coin_observations?run_id=eq.${run.id}&select=symbol,validation_status`,"GET"),
      db(base,key,`predictions?run_id=eq.${run.id}&select=symbol,horizon_minutes`,"GET"),
      db(base,key,`trade_levels?run_id=eq.${run.id}&select=symbol,entry,max_entry,stop_loss,take_profit_1,take_profit_2,take_profit_3,rr_tp1,rr_tp2,rr_tp3,entry_ready,decision,provisional` ,"GET")
    ]);
    const obs=Array.isArray(o)?o:[], preds=Array.isArray(p)?p:[], trades=Array.isArray(t)?t:[];
    const set=(xs:string[])=>new Set(xs);
    const obsSymbols=set(obs.map((x:any)=>x.symbol));
    const predPairs=set(preds.map((x:any)=>`${x.symbol}:${x.horizon_minutes}`));
    const expectedPairs=set(valid.flatMap((x:any)=>[`${x.coin}:15`,`${x.coin}:60`]));
    const expectedTrade=set(tradeCandidates.map((x:any)=>x.coin));
    const actualTrade=set(trades.map((x:any)=>x.symbol));
    const tradeRowsValid=trades.every((x:any)=>
      Number.isFinite(Number(x.entry))&&Number(x.entry)>0&&
      Number.isFinite(Number(x.max_entry))&&Number(x.max_entry)>0&&
      Number.isFinite(Number(x.stop_loss))&&Number(x.stop_loss)>0&&Number(x.stop_loss)<Number(x.entry)&&
      Number.isFinite(Number(x.take_profit_1))&&Number(x.take_profit_1)>Number(x.entry)&&
      Number.isFinite(Number(x.take_profit_2))&&Number(x.take_profit_2)>=Number(x.take_profit_1)&&
      Number.isFinite(Number(x.take_profit_3))&&Number(x.take_profit_3)>=Number(x.take_profit_2)&&
      [x.rr_tp1,x.rr_tp2,x.rr_tp3].every((v:any)=>Number.isFinite(Number(v)))&&
      typeof x.entry_ready==="boolean"&&typeof x.decision==="string"&&x.provisional===true);
    const equal=(a:Set<string>,b:Set<string>)=>a.size===b.size&&[...a].every(x=>b.has(x));
    readback={
      observations:obs.length,predictions:preds.length,trade_levels:trades.length,
      unique_observation_symbols:obsSymbols.size,prediction_pairs:predPairs.size,
      observations_verified:obs.length===UNIVERSE.length&&equal(obsSymbols,set([...UNIVERSE])),
      predictions_verified:preds.length===valid.length*2&&equal(predPairs,expectedPairs),
      trade_levels_verified:trades.length===tradeCandidates.length&&equal(actualTrade,expectedTrade)&&tradeRowsValid
    };
    readback.verified=readback.observations_verified&&readback.predictions_verified&&readback.trade_levels_verified;
  } catch(e) { errors.push("READBACK:"+String(e).slice(0,220)); }

  // Evaluate only predictions whose complete future window is available in this run's
  // timestamped 1m history. Missing/partial coverage is deferred, never terminally persisted.
  let outcomeAudit:any={eligible:0,attempted:0,inserted:0,verified_inserted:0,duplicates_ignored:0,unresolved:0,coverage_deferred:0,
    queried_pending_count:0,pending_count:0,not_matured:0,
    errors:[] as string[]};
  try {
    const historyCutoff = new Date(Date.now() - 995*60000).toISOString();
    const pending=await db(base,key,"rpc/buy_wache_pending_predictions_v26_2","POST",{
      p_user_id:userId,p_evaluator_version:VERSION,p_before:new Date().toISOString(),p_after:historyCutoff,p_limit:1000
    });
    const completed=Array.isArray(pending)?pending:[];
    outcomeAudit.queried_pending_count=completed.length;
    outcomeAudit.pending_count=completed.length;
    const nowMs=Date.now();
    const mature=completed.filter((p:any)=>{
      const t=Date.parse(p.prediction_time),h=Number(p.horizon_minutes);
      if(!Number.isFinite(t)||![15,60].includes(h)) return false;
      if(t+h*60000>nowMs){outcomeAudit.not_matured++;return false;}
      return true;
    });
    outcomeAudit.eligible=mature.length;
    const byCoin=new Map<string,any>();
    for (const x of valid) byCoin.set(x.coin,x);
    const outcomeRows:any[]=[];
    const deferred:string[]=[];
    for (const p of mature) {
      const coin=String(p.symbol||"").replace(/USDT$/i,"");
      const current=byCoin.get(coin);
      const predictionTime=Date.parse(p.prediction_time);
      const horizon=Number(p.horizon_minutes);
      const refPrice=n(p.features?.reference_price);
      if (!Number.isFinite(predictionTime) || ![15,60].includes(horizon) || !(refPrice>0)) {
        outcomeAudit.unresolved++;
        outcomeAudit.errors.push(`${coin}_${horizon}:INVALID_PREDICTION_METADATA`);
        continue;
      }

      // prediction_time is the signal candle close (.999); future window starts next minute.
      const firstOpen=Math.floor(predictionTime/60000)*60000+60000;
      const expectedEndClose=firstOpen+horizon*60000-1;
      const history=(current?.outcome_candles||[]).map((c:any)=>({
        t:Date.parse(c.open_time),ct:Date.parse(c.close_time),
        o:n(c.open),h:n(c.high),l:n(c.low),c:n(c.close)
      })).filter((c:any)=>Number.isFinite(c.t)&&Number.isFinite(c.ct)&&c.t>=firstOpen&&c.t<firstOpen+horizon*60000)
        .sort((a:any,b:any)=>a.t-b.t);

      let coverageReason:string|null=null;
      if (!current || history.length===0) coverageReason="CANDLE_COVERAGE_MISSING";
      else if (history.length!==horizon) coverageReason="CANDLE_COUNT_MISMATCH";
      else if (history[0].t!==firstOpen || history[history.length-1].ct!==expectedEndClose)
        coverageReason="HORIZON_BOUNDARY_MISMATCH";
      else if (history.some((c:any)=>![c.o,c.h,c.l,c.c].every(Number.isFinite)||c.o<=0||c.h<c.l||c.l<=0))
        coverageReason="INVALID_OHLC";
      else {
        for(let i=1;i<history.length;i++) if(history[i].t-history[i-1].t!==60000) { coverageReason="CANDLE_GAP"; break; }
      }
      if (coverageReason) {
        outcomeAudit.coverage_deferred++;
        deferred.push(`${coin}_${horizon}:${coverageReason}`);
        continue;
      }

      const candles=history;
      const endCandle=candles[candles.length-1];
      const maxHigh=Math.max(...candles.map((c:any)=>c.h));
      const minLow=Math.min(...candles.map((c:any)=>c.l));
      const favorable=(maxHigh/refPrice-1)*100;
      const adverse=(minLow/refPrice-1)*100;
      const endReturn=(endCandle.c/refPrice-1)*100;
      const isEntryReady=p.features?.entry_ready===true;
      const trade=p.features?.trade||{};
      const feeBps=n(Deno.env.get("BUY_WACHE_FEE_BPS_PER_SIDE")??"10");
      const spreadBps=n(Deno.env.get("BUY_WACHE_SPREAD_BPS_TOTAL")??"5");
      const slippageBps=n(Deno.env.get("BUY_WACHE_SLIPPAGE_BPS_PER_SIDE")??"5");
      const costsConfigured=[feeBps,spreadBps,slippageBps].every((x:number)=>Number.isFinite(x)&&x>=0);
      if (!costsConfigured) {
        outcomeAudit.errors.push(`${coin}_${horizon}:INVALID_COST_CONFIGURATION`);
        continue;
      }
      const costsPct=(2*feeBps+spreadBps+2*slippageBps)/100;
      const adverseBps=slippageBps+spreadBps/2;
      let outcome=endReturn>0.10?"WIN":endReturn< -0.10?"LOSS":"NEUTRAL";
      let exitPrice=endCandle.c, executionMethod="DIRECTIONAL_HORIZON_RETURN_NOT_EXECUTION";
      let executionReason="ENTRY_NOT_READY_DIRECTIONAL_ONLY";
      let executionBars:number|null=null, netReturnPct:number|null=null;
      if(isEntryReady && Number.isFinite(n(trade.stop_loss)) && Number.isFinite(n(trade.take_profit_1)) &&
        n(trade.stop_loss)>0 && n(trade.take_profit_1)>refPrice) {
        const rawFill=candles[0].o;
        const fill=rawFill*(1+adverseBps/10000);
        const stop=n(trade.stop_loss), tp=n(trade.take_profit_1), maxEntry=n(trade.raw_max_entry ?? trade.max_entry);
        if (!(Number.isFinite(fill)&&fill>0)) {
          outcomeAudit.errors.push(`${coin}_${horizon}:INVALID_NEXT_OPEN_FILL`);
          continue;
        }
        executionMethod="NEXT_FUTURE_CANDLE_OPEN_TP_SL_FIRST_PASSAGE";
        executionReason="HORIZON_EXIT";
        if (!(Number.isFinite(maxEntry)&&maxEntry>0)) {
          outcome="UNRESOLVED"; executionReason="MAX_ENTRY_MISSING";
        } else if (fill>maxEntry) {
          outcome="UNRESOLVED"; executionReason="ENTRY_ABOVE_MAX_ENTRY";
        } else if(fill>=tp) {
          outcome="UNRESOLVED"; executionReason="GAP_PAST_TP_NO_VALID_ENTRY";
        } else if(rawFill<=stop) {
          outcome="LOSS"; executionReason="GAP_THROUGH_STOP_AT_NEXT_OPEN";
          exitPrice=Math.min(stop,rawFill)*(1-adverseBps/10000); executionBars=1;
        } else {
          let resolved=false;
          for(let ci=0;ci<candles.length;ci++) {
            const c=candles[ci], hitStop=c.l<=stop, hitTp=c.h>=tp;
            if(hitStop && hitTp) {
              outcome="LOSS"; exitPrice=stop*(1-adverseBps/10000);
              executionReason="BOTH_TOUCHED_STOP_FIRST"; executionBars=ci+1; resolved=true; break;
            }
            if(hitStop) {
              outcome="LOSS"; exitPrice=Math.min(stop,Number.isFinite(c.o)?c.o:stop)*(1-adverseBps/10000);
              executionReason="STOP_FIRST_HIT"; executionBars=ci+1; resolved=true; break;
            }
            if(hitTp) {
              exitPrice=tp*(1-adverseBps/10000);
              executionReason="TP_FIRST_HIT"; executionBars=ci+1; resolved=true; break;
            }
          }
          if(!resolved) {
            exitPrice=endCandle.c*(1-adverseBps/10000);
            executionReason="HORIZON_EXIT_NO_TP_SL_HIT";
          }
        }
        if(outcome!=="UNRESOLVED") {
          netReturnPct=(exitPrice/fill-1)*100-(2*feeBps/100);
          if(executionReason==="TP_FIRST_HIT") outcome=netReturnPct>0?"WIN":"NEUTRAL";
          else if(executionReason==="HORIZON_EXIT_NO_TP_SL_HIT") outcome=netReturnPct>0.10?"WIN":netReturnPct< -0.10?"LOSS":"NEUTRAL";
        }
      }
      outcomeRows.push({
        prediction_id:p.id,run_id:p.run_id,user_id:userId,evaluator_version:VERSION,evaluated_at:new Date().toISOString(),
        horizon_minutes:horizon,start_price:refPrice,end_price:exitPrice,
        max_favorable_pct:favorable,max_adverse_pct:adverse,outcome,net_return_pct:netReturnPct,
        metadata:{method:executionMethod,execution_reason:executionReason,execution_bars:executionBars,
          net_return_pct:netReturnPct,estimated_round_trip_cost_pct:costsPct,
          candles_used:candles.length,coverage_start:iso(candles[0].t),coverage_end:iso(candles[candles.length-1].ct),
          horizon_end:iso(expectedEndClose),decision:p.features?.decision||null,signal_score:p.features?.signal_score??null,
          entry_ready:isEntryReady,entry_fill_rule:"FIRST_FUTURE_CANDLE_OPEN_WITH_ADVERSE_COST_ESTIMATE",
          same_bar_tp_sl_rule:"STOP_FIRST_CONSERVATIVE",mfe_mae_exclude_signal_candle:true,
          note:"Exact future-only closed 1m window. Estimated costs are assumptions, not exchange fills."}
      });
    }
    outcomeAudit.coverage_deferred_examples=deferred.slice(0,30);
    if (outcomeRows.length) {
      outcomeAudit.attempted=outcomeRows.length;
      const insertedRows=await insertOutcomeEvaluations(base,key,outcomeRows);
      const returned=Array.isArray(insertedRows)?insertedRows:[];
      outcomeAudit.inserted=returned.length;
      outcomeAudit.verified_inserted=returned.length;
      outcomeAudit.duplicates_ignored=outcomeRows.length-returned.length;
      if(outcomeAudit.duplicates_ignored<0) throw new Error("OUTCOME_INSERT_READBACK_INVALID_COUNT");
      outcomeAudit.persistence_target="prediction_outcome_evaluations";
      outcomeAudit.idempotency_key="prediction_id,evaluator_version,horizon_minutes";
    }
    outcomeAudit.history_window_minutes=1000;
    outcomeAudit.aged_pending_note="Predictions older than the rolling 1000 closed 1m bars are not evaluated by this worker; reconcile them separately rather than assigning outcomes without coverage.";
  } catch(e) {
    outcomeAudit.errors.push(String(e).slice(0,240));
    errors.push("OUTCOME_EVALUATION:"+String(e).slice(0,220));
  }
  const decisionCounts:Record<string,number>={};
  const rejectionCounts:Record<string,number>={};
  for (const x of valid) {
    decisionCounts[x.decision]=(decisionCounts[x.decision]||0)+1;
    for (const reason of x.entry_rejection_reasons||[]) rejectionCounts[reason]=(rejectionCounts[reason]||0)+1;
  }
  const missing=results.filter((x:any)=>x.status!=="VALID").map((x:any)=>({
    coin:x.coin,status:x.status,error_code:x.error_code||null,detail:x.detail||null
  }));
  const uniqueTimestamps=[...new Set(valid.map((x:any)=>x.source_timestamp))];
  const synchronized=uniqueTimestamps.length===1;
  const status=results.length===UNIVERSE.length&&new Set(results.map((x:any)=>x.coin)).size===UNIVERSE.length&&valid.length===UNIVERSE.length&&errors.length===0&&readback.verified&&synchronized&&outcomeAudit.errors.length===0&&outcomeAudit.verified_inserted===outcomeAudit.inserted?"COMPLETE":"PARTIAL";
  const audit={
    version:VERSION,universe:[...UNIVERSE],universe_unique:new Set(UNIVERSE).size===UNIVERSE.length,
    processed_count:results.length,valid_count:valid.length,missing_count:missing.length,
    unique_candle_end_count:uniqueTimestamps.length,
    synchronized_candle_end:synchronized,
    common_candle_end:uniqueTimestamps.length===1?uniqueTimestamps[0]:null,
    context_validity:{
      m5:valid.filter((x:any)=>x.context.m5.status==="VALID").length,
      m15:valid.filter((x:any)=>x.context.m15.status==="VALID").length,
      h1:valid.filter((x:any)=>x.context.h1.status==="VALID").length,
      h4:valid.filter((x:any)=>x.context.h4.status==="VALID").length
    },
    decision_counts:decisionCounts,rejection_reason_counts:rejectionCounts,outcome_audit:outcomeAudit,
    probabilities_calibrated:false,probability_basis:"NOT_EMITTED",entry_formula:"NET_RR_1_5_WITH_CONFIGURED_COST_ESTIMATE",
    readback,errors,universe_errors:universeErrors
  };
  try {
    await db(base,key,`scan_runs?id=eq.${run.id}`,"PATCH",{
      finished_at:new Date().toISOString(),status,processed_count:results.length,
      valid_count:valid.length,error_count:missing.length+errors.length,
      source_summary:{version:VERSION,markets:[...new Set(valid.map((x:any)=>x.source))],
        persistence_errors:errors,universe_errors:universeErrors},
      audit
    });
  } catch(e) { errors.push("RUN_FINALIZE:"+String(e).slice(0,220)); }

  return reply({
    title:"BUY-WACHE V26 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â REGRESSION-GATED / EXECUTION-AWARE AUDIT",version:VERSION,
    run_id:run.id,run_status:status,universe_count:UNIVERSE.length,
    processed_count:results.length,valid_count:valid.length,missing_count:missing.length,
    probability_basis:"No percentage probabilities emitted; signal_score is a heuristic ranking score.", execution_model:"NEXT_CLOSED_CANDLE_OPEN_REQUIRED; cost estimates configurable; live spread is not measured by this function.", entry_model:"V26_3_DATA_DRIVEN_ENTRY; max_entry is null when above-market entry fails net RR; raw_max_entry retains the solved threshold.",
    elapsed_ms:Date.now()-started,outcome_audit:outcomeAudit,decision_audit:{decision_counts:decisionCounts,
      rejection_reason_counts:rejectionCounts,early_candidates:decisionCounts.EARLY_CANDIDATE||0,
      confirmed_breakouts:decisionCounts.BREAKOUT_CONFIRMED||0,failed_breakout_risks:decisionCounts.FAILED_BREAKOUT||0},
    context_audit:audit.context_validity,synchronized_candle_end:synchronized,readback,errors,missing,
    candidates:candidates.map((x:any)=>({
      coin:x.coin,decision:x.decision,source:x.source,source_timestamp:x.source_timestamp,
      freshness_ms:x.freshness_ms,price:x.price,signal_score:x.signal_score,
      score_type:x.score_type,features:x.features,context:x.context,
      entry_ready:x.entry_ready,entry_rejection_reasons:x.entry_rejection_reasons,
      trade:x.trade,probability:null
    }))
  });
});