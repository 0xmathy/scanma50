// scripts/fetch_and_build.js
// Source: Google Sheet (onglet history uniquement) → enrichi via CoinGecko (prix/MC/ATH/RSI) + DeFiLlama (TVL) → data.json
// Node 20 (fetch natif). CommonJS (require) pour simplicité.

const fs = require('fs');
const { parse } = require('csv-parse/sync');

// ====== CONFIG ======
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv";

const GECKO_BASE = "https://api.coingecko.com/api/v3";
const LLAMA_BASE = "https://api.llama.fi";

// Anti-rate-limit simple
const THROTTLE_MS = 400;
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));

// ====== UTILS ======
function normalizeSymbol(asset){
  if(!asset) return "";
  const core = String(asset).split(':').pop().trim();               // "BINANCE:AVAXUSDT" → "AVAXUSDT"
  return core.replace(/USDT|USD|USDC|PERP|_PERP|\/.*$/i,"").trim().toUpperCase();
}
function parseVenue(asset){
  if(!asset) return "";
  const parts = String(asset).split(':');
  return parts.length>1 ? parts[0].trim().toUpperCase() : "";
}
function ymdParis(d=new Date()){
  const dt = (d instanceof Date) ? d : new Date(d);
  const p = new Intl.DateTimeFormat('fr-FR',{ timeZone:'Europe/Paris', year:'numeric', month:'2-digit', day:'2-digit' })
            .formatToParts(dt).reduce((o,p)=> (o[p.type]=p.value, o), {});
  return `${p.year}-${p.month}-${p.day}`;
}
async function fetchCsv(url, label){
  const res = await fetch(url);
  if(!res.ok) throw new Error(`${label} ${res.status} ${res.statusText}`);
  const text = await res.text();
  return parse(text, { columns:true, skip_empty_lines:true });
}
async function throttledJson(url, label){
  await sleep(THROTTLE_MS);
  const res = await fetch(url);
  if(!res.ok) throw new Error(`${label} ${res.status} ${res.statusText}`);
  return await res.json();
}

// ====== 1) LIRE SHEET "history" (≤ 2 jours) ======
async function collectFromHistory(){
  console.log("➡️  Lecture CSV: history…");
  const recs = await fetchCsv(SHEET_URL_HISTORY, "history");
  const seen = new Set();
  const out = [];

  const today = new Date();
  const cutoff = new Date(today.getTime() - 2*24*3600*1000);
  const cutoffYMD = ymdParis(cutoff);

  for(const row of recs){
    const asset = row["asset"] || row["Asset"] || row["ASSET"];
    if(!asset) continue;
    const symbol = normalizeSymbol(asset);
    if(!symbol) continue;

    const dk = (row["DateKey"] || row["datekey"] || row["date"] || "").toString().slice(0,10);
    if(dk && dk < cutoffYMD) continue;

    if(seen.has(symbol)) continue;
    seen.add(symbol);

    out.push({
      symbol,
      venue: parseVenue(asset),
      alert_date: dk || ymdParis(today)
    });
  }
  console.log(`✅ ${out.length} tokens (history ≤2j)`);
  return out;
}

// ====== 2) COINGECKO ======
async function geckoSearchIdBySymbol(symbol){
  const data = await throttledJson(`${GECKO_BASE}/search?query=${encodeURIComponent(symbol)}`, `Gecko search ${symbol}`);
  const coins = data?.coins || [];
  const exact = coins.filter(c => String(c.symbol||'').toUpperCase() === symbol.toUpperCase());
  const pick = exact[0] || coins[0] || null;
  return pick ? pick.id : null;
}
async function geckoMarkets(ids){
  if(!ids.length) return [];
  const url = `${GECKO_BASE}/coins/markets?vs_currency=usd&ids=${encodeURIComponent(ids.join(','))}&price_change_percentage=24h,7d,30d,1y&per_page=250`;
  return await throttledJson(url, "Gecko markets");
}
async function geckoMarketChart(id, days=90, interval=''){
  const url = `${GECKO_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}${interval?`&interval=${interval}`:''}`;
  return await throttledJson(url, `Gecko chart ${id} ${days}d`);
}
function computeRSI(closes, period=14){
  if(!closes || closes.length < period+1) return null;
  const deltas = [];
  for(let i=1;i<closes.length;i++) deltas.push(closes[i]-closes[i-1]);
  let gain=0, loss=0;
  for(let i=0;i<period;i++){ const d=deltas[i]; if(d>0) gain+=d; else loss-=d; }
  gain/=period; loss/=period;
  for(let i=period;i<deltas.length;i++){
    const d=deltas[i]; const g=d>0?d:0; const l=d<0?-d:0;
    gain=(gain*(period-1)+g)/period; loss=(loss*(period-1)+l)/period;
  }
  if(loss===0) return 100;
  const rs = gain/loss;
  return 100 - (100/(1+rs));
}

// ====== 3) DEFI LLAMA (TVL) ======
let LLAMA_PROTOCOLS_CACHE = null;
async function llamaProtocols(){
  if(LLAMA_PROTOCOLS_CACHE) return LLAMA_PROTOCOLS_CACHE;
  LLAMA_PROTOCOLS_CACHE = await throttledJson(`${LLAMA_BASE}/protocols`, "Llama protocols");
  return LLAMA_PROTOCOLS_CACHE;
}
async function matchTVL(symbol){
  try{
    const protos = await llamaProtocols();
    const sym = symbol.toUpperCase();
    const cand = protos.find(p =>
      (p.symbol && String(p.symbol).toUpperCase() === sym) ||
      (p.symbol && String(p.symbol).toUpperCase().startsWith(sym)) ||
      (p.name && String(p.name).toUpperCase() === sym)
    ) || protos.find(p => p.name && p.name.toUpperCase().startsWith(sym));
    if(!cand) return { tvl:null, source:null };
    const tvl = (typeof cand.tvl === 'number') ? cand.tvl : null;
    return { tvl, source: `protocol:${cand.slug||cand.name}` };
  }catch{ return { tvl:null, source:null }; }
}

// ====== 4) ENRICH PIPELINE ======
async function enrich(tokens){
  // 4.1 Résoudre ids Gecko
  for(const t of tokens){
    try{ t._gid = await geckoSearchIdBySymbol(t.symbol); }
    catch{ t._gid = null; }
    await sleep(150);
  }
  const idMap = tokens.filter(t=>t._gid).reduce((m,t)=>{ m[t._gid]=t; return m; }, {});

  // 4.2 Marchés (prix/MC/variations/vol/supply/ath)
  let markets=[];
  const ids = Object.keys(idMap);
  for(let i=0;i<ids.length;i+=150){
    const batch = ids.slice(i,i+150);
    try{
      const res = await geckoMarkets(batch);
      markets = markets.concat(res||[]);
    }catch(e){ console.warn('markets batch err:', e.message); }
    await sleep(250);
  }
  markets.forEach(m=>{
    const t = idMap[m.id]; if(!t) return;
    t.price = m.current_price ?? null;
    t.mc = m.market_cap ?? null;
    t.rank = m.market_cap_rank ?? null;
    t.d24 = m.price_change_percentage_24h ?? null;
    t.d7  = m.price_change_percentage_7d_in_currency ?? null;
    t.d30 = m.price_change_percentage_30d_in_currency ?? null;
    t.d1y = m.price_change_percentage_1y_in_currency ?? null;

    t.volume_24h = m.total_volume ?? null;
    t.circulating_supply = m.circulating_supply ?? null;
    t.total_supply = m.total_supply ?? null;
    t.max_supply = m.max_supply ?? null;
    t.fdv = m.fully_diluted_valuation ?? null;

    t.ath = m.ath ?? null;
    t.ath_date = m.ath_date ? String(m.ath_date).slice(0,10) : null;

    t.vol_mc_24 = (t.volume_24h && t.mc) ? (t.volume_24h / t.mc * 100) : null;
  });

  // 4.3 TVL
  for(const t of tokens){
    const { tvl, source } = await matchTVL(t.symbol);
    t.tvl = tvl; t.tvl_source = source;
    t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
    await sleep(120);
  }

  // 4.4 RSI + volumes 7j/30j
  for(const t of tokens){
    if(!t._gid){ t.rsi_d=t.rsi_h4=t.vol7_avg=t.vol30_avg=t.var_vol_7_over_30=t.vol7_tvl=null; continue; }

    // Daily 90j
    let daily=null;
    try{
      daily = await geckoMarketChart(t._gid, 90, 'daily');
    }catch(e){ console.warn(`chart daily ${t.symbol}:`, e.message); }

    if(daily){
      const closesD = (daily.prices||[]).map(p=> p[1]).filter(Number.isFinite);
      t.rsi_d = computeRSI(closesD, 14);

      const volsDaily = (daily.total_volumes||[]).map(v=> v[1]).filter(Number.isFinite);
      const last7  = volsDaily.slice(-7);
      const last30 = volsDaily.slice(-30);
      const avg7  = last7.length  ? (last7.reduce((a,b)=>a+b,0)/last7.length)   : null;
      const avg30 = last30.length ? (last30.reduce((a,b)=>a+b,0)/last30.length) : null;
      t.vol7_avg = avg7; t.vol30_avg = avg30;
      t.var_vol_7_over_30 = (avg7 && avg30) ? (avg7/avg30) : null;
      t.vol7_tvl = (avg7 && t.tvl) ? (avg7 / t.tvl * 100) : null;
    }

    // Hourly 7j → RSI H4 (agrégation 4 bougies)
    let hourly=null;
    try{
      hourly = await geckoMarketChart(t._gid, 7, 'hourly');
    }catch(e){ console.warn(`chart hourly ${t.symbol}:`, e.message); }
    if(hourly){
      const closesH = (hourly.prices||[]).map(p=> p[1]).filter(Number.isFinite);
      const h4=[]; for(let i=0;i<closesH.length;i+=4){ h4.push(closesH[Math.min(i+3, closesH.length-1)]); }
      t.rsi_h4 = computeRSI(h4, 14);
    } else {
      t.rsi_h4 = null;
    }

    await sleep(200);
  }

  // 4.5 ATH Mcap & x
  tokens.forEach(t=>{
    if(t.ath!=null && t.price!=null && t.mc!=null){
      const ath_mc = (t.circulating_supply!=null) ? (t.ath * t.circulating_supply) : (t.mc * (t.ath / t.price));
      t.ath_mc = ath_mc ?? null;
      t.x_ath_mc = (ath_mc && t.mc) ? (ath_mc / t.mc) : ((t.price && t.ath) ? (t.ath / t.price) : null);
      t.price_target_ath_mc = (t.x_ath_mc && t.price) ? (t.price * t.x_ath_mc) : null;
    } else {
      t.ath_mc = null; t.x_ath_mc = null; t.price_target_ath_mc = null;
    }
  });

  // 4.6 arrondis doux
  const round = (n, d=6)=> (typeof n==='number' && isFinite(n)) ? +n.toFixed(d) : n;
  tokens.forEach(t=>{
    ['price','mc','tvl','mc_tvl','vol_mc_24','vol7_tvl','var_vol_7_over_30','rsi_d','rsi_h4','ath','ath_mc','x_ath_mc','price_target_ath_mc']
      .forEach(k=> { if(t[k]!=null) t[k]=round(t[k], (k==='mc_tvl'||k==='x_ath_mc')?2:6); });
  });

  return tokens;
}

// ====== 5) MAIN ======
(async function main(){
  try{
    const base = await collectFromHistory();
    const tokens = base.map(x=>({
      symbol: x.symbol,
      venue: x.venue,
      alert_date: x.alert_date,
      // champs enrichis
      price:null, d24:null, d7:null, d30:null, d1y:null,
      mc:null, tvl:null, mc_tvl:null,
      volume_24h:null, vol_mc_24:null,
      vol7_avg:null, vol30_avg:null, vol7_tvl:null, var_vol_7_over_30:null,
      rsi_d:null, rsi_h4:null,
      ath:null, ath_date:null,
      ath_mc:null, x_ath_mc:null, price_target_ath_mc:null,
      circulating_supply:null, total_supply:null, max_supply:null, fdv:null, rank:null,
      website:null, twitter:null, github:null, whitepaper:null,
      tvl_source:null
    }));

    const enriched = await enrich(tokens);
    const data = { updated_at: new Date().toISOString(), tokens: enriched };
    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    console.log(`✅ data.json écrit (${enriched.length} tokens).`);
  }catch(e){
    console.error("❌ Erreur:", e);
    const data = { updated_at: new Date().toISOString(), tokens: [] };
    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    process.exit(1);
  }
})();
