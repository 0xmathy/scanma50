// scripts/fetch_and_build.js
// MIX CMC (métriques marché) + Gecko (charts/RSI/ATH) + DeFiLlama (TVL) avec anti-429.
// Node 18+ (fetch natif).

import fs from 'fs';
import { parse } from 'csv-parse/sync';

/* ====== Config Google Sheets ====== */
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv";
const SHEET_URL_ALERTE =
  process.env.SHEET_URL_ALERTE ||
  "https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=0&range=E:E";

/* ====== APIs ====== */
const CMC_KEY = process.env.CMC_API_KEY || ""; // ⚠️ obligatoire
const CMC_BASE = "https://pro-api.coinmarketcap.com/v1";
const GECKO_BASE = "https://api.coingecko.com/api/v3";
const LLAMA_BASE = "https://api.llama.fi";

/* ====== Anti-rate-limit ====== */
const THROTTLE_MS = Number(process.env.THROTTLE_MS || 1200);
const MAX_TOKENS_PER_RUN = Number(process.env.MAX_TOKENS_PER_RUN || 40);
const ENABLE_RSI_H4 = process.env.RSI_H4 !== '0';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let LAST_CALL_TS = 0;

async function throttledFetch(url, opts={}, label=""){
  const now = Date.now();
  const wait = Math.max(0, LAST_CALL_TS + THROTTLE_MS - now);
  if(wait) await sleep(wait);

  for(let i=0;i<4;i++){
    try{
      const res = await fetch(url, opts);
      if(res.status === 429){
        const ra = parseInt(res.headers.get('retry-after')||'0',10);
        const back = ra ? ra*1000 : THROTTLE_MS*(i+1);
        console.warn(`429 ${label} → pause ${Math.round(back)}ms`);
        await sleep(back);
        continue;
      }
      if(!res.ok){
        if(i<2){
          const back = THROTTLE_MS*(i+1);
          console.warn(`${label} HTTP ${res.status} → retry dans ${back}ms`);
          await sleep(back);
          continue;
        }
        throw new Error(`HTTP ${res.status} ${res.statusText}`);
      }
      LAST_CALL_TS = Date.now();
      return res;
    }catch(e){
      if(i===3) throw e;
      const back = THROTTLE_MS*(i+1);
      console.warn(`${label} erreur "${e.message}" → retry dans ${back}ms`);
      await sleep(back);
    }
  }
}

/* ====== Utils ====== */
function normalizeSymbol(asset){
  if(!asset) return "";
  const core = String(asset).split(':').pop().trim();
  return core.replace(/USDT|USD|USDC|PERP|_PERP|\/.*$/i,"").trim().toUpperCase();
}
function parseVenue(asset){
  if(!asset) return "";
  const parts = String(asset).split(':');
  return parts.length>1 ? parts[0].trim().toUpperCase() : "";
}
function getAssetField(row){
  return row["asset"] || row["Asset"] || row["ASSET"] ||
         row["E"] || row["Col1"] || row[0] || "";
}
function ymdParis(d=new Date()){
  const dt = (d instanceof Date) ? d : new Date(d);
  const p = new Intl.DateTimeFormat('fr-FR',{ timeZone:'Europe/Paris', year:'numeric', month:'2-digit', day:'2-digit' })
            .formatToParts(dt).reduce((o,p)=> (o[p.type]=p.value, o), {});
  return `${p.year}-${p.month}-${p.day}`;
}

async function fetchCsv(url, label){
  if(!url) return [];
  const res = await throttledFetch(url, {}, `CSV ${label}`);
  const text = await res.text();
  let rows = [];
  try { rows = parse(text, { columns: true, skip_empty_lines: true }); } catch(_){}
  if(rows.length && Object.keys(rows[0]).length === 1 && !('asset' in rows[0])) {
    const onlyKey = Object.keys(rows[0])[0];
    return rows.map(r => ({ asset: r[onlyKey] }));
  }
  if(rows.length) return rows;
  const raw = parse(text, { columns: false, skip_empty_lines: true });
  return raw.map(arr => ({ asset: (arr && arr[0]) ? String(arr[0]) : '' }));
}

/* ====== Sheets → tokens ====== */
async function collectAssetsFromSheets(){
  console.log("➡️  Lecture CSV: history…");
  let recH = [];
  try { recH = await fetchCsv(SHEET_URL_HISTORY, "history"); }
  catch(e){ console.warn("❌ history:", e.message); }

  console.log("➡️  Lecture CSV: alerte…");
  let recA = [];
  try { recA = await fetchCsv(SHEET_URL_ALERTE, "alerte"); }
  catch(e){ console.warn("❌ alerte:", e.message); }

  const seen = new Set();
  const out = [];

  const today = new Date();
  const cutoff = new Date(today.getTime() - 2*24*3600*1000);
  const cutoffYMD = ymdParis(cutoff);

  // history ≤ 2j
  for(const row of recH){
    const asset = getAssetField(row);
    if(!asset) continue;
    const symbol = normalizeSymbol(asset);
    if(!symbol) continue;

    const dk = (row["DateKey"] || row["datekey"] || row["date"] || "").toString().slice(0,10);
    if(dk && dk < cutoffYMD) continue;

    if(seen.has(symbol)) continue;
    seen.add(symbol);
    out.push({ symbol, venue: parseVenue(asset), alert_date: dk || ymdParis(today), _src: "history" });
  }

  // alerte (aujourd’hui)
  for(const row of recA){
    const asset = getAssetField(row);
    if(!asset) continue;
    const symbol = normalizeSymbol(asset);
    if(!symbol || seen.has(symbol)) continue;
    seen.add(symbol);
    out.push({ symbol, venue: parseVenue(asset), alert_date: ymdParis(today), _src: "alerte" });
  }

  if(out.length > MAX_TOKENS_PER_RUN){
    console.warn(`⚠️  Trop de tokens (${out.length}) → on limite à ${MAX_TOKENS_PER_RUN}`);
  }
  return out.slice(0, MAX_TOKENS_PER_RUN);
}

/* ====== CoinGecko (charts) ====== */
async function geckoSearchIdBySymbol(symbol){
  const res = await throttledFetch(`${GECKO_BASE}/search?query=${encodeURIComponent(symbol)}`, {}, `Gecko search ${symbol}`);
  const data = await res.json();
  const coins = data?.coins || [];
  const exact = coins.filter(c => String(c.symbol||'').toUpperCase() === symbol.toUpperCase());
  const pick = exact[0] || coins[0] || null;
  return pick ? pick.id : null;
}
async function geckoMarketChart(id, days=90, interval=''){
  const url = `${GECKO_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}${interval?`&interval=${interval}`:''}`;
  const res = await throttledFetch(url, {}, `Gecko chart ${id} ${days}d`);
  if(!res) return { prices:[], total_volumes:[] };
  return await res.json();
}

/* ====== CMC (marché) ====== */
async function cmcQuotesBySymbols(symbols){
  // Batch par 50 pour être safe
  const chunks = [];
  for(let i=0;i<symbols.length;i+=50) chunks.push(symbols.slice(i,i+50));
  const out = {};
  for(const batch of chunks){
    const url = `${CMC_BASE}/cryptocurrency/quotes/latest?symbol=${encodeURIComponent(batch.join(','))}&convert=USD`;
    const res = await throttledFetch(url, { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY } }, `CMC quotes ${batch.length}`);
    if(!res){ continue; }
    const data = await res.json();
    // data.data est un objet keyed par symbol → { symbol: { id, name, symbol, quote: { USD:{...} } } }
    if(data && data.data){
      Object.entries(data.data).forEach(([sym, obj])=>{
        out[sym.toUpperCase()] = obj;
      });
    }
  }
  return out;
}

/* ====== RSI (Wilder) ====== */
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

/* ====== DeFiLlama ====== */
let LLAMA_PROTOCOLS_CACHE = null;
async function llamaProtocols(){
  if(LLAMA_PROTOCOLS_CACHE) return LLAMA_PROTOCOLS_CACHE;
  const res = await throttledFetch(`${LLAMA_BASE}/protocols`, {}, "Llama protocols");
  const data = await res.json();
  LLAMA_PROTOCOLS_CACHE = Array.isArray(data) ? data : [];
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

/* ====== Enrich ====== */
async function enrichTokens(tokens){
  if(!CMC_KEY){
    console.warn("⚠️  CMC_API_KEY manquante → prix/MC via CMC indisponibles.");
  }

  // 0) Gecko IDs
  for(const t of tokens){
    try{
      t._gecko_id = await geckoSearchIdBySymbol(t.symbol);
    }catch(e){
      console.warn(`Gecko search fail ${t.symbol}: ${e.message}`);
      t._gecko_id = null;
    }
  }

  // 1) CMC quotes (prix/MC/vol/%)
  if(CMC_KEY){
    const quotes = await cmcQuotesBySymbols(tokens.map(t=>t.symbol));
    tokens.forEach(t=>{
      const q = quotes[t.symbol.toUpperCase()];
      const usd = q?.quote?.USD;
      if(usd){
        t.price = usd.price ?? null;
        t.mc = usd.market_cap ?? null;
        t.rank = q.cmc_rank ?? null;
        t.d24 = usd.percent_change_24h ?? null;
        t.d7  = usd.percent_change_7d ?? null;
        t.d30 = usd.percent_change_30d ?? null;
        t.volume_24h = usd.volume_24h ?? null;
        t.circulating_supply = q.circulating_supply ?? null;
        t.total_supply = q.total_supply ?? null;
        t.max_supply = q.max_supply ?? null;
        t.fdv = usd.fully_diluted_market_cap ?? null;
        t.vol_mc_24 = (t.volume_24h && t.mc) ? (t.volume_24h / t.mc * 100) : null;
      }
    });
  }

  // 2) TVL (DeFiLlama)
  for(const t of tokens){
    try{
      const { tvl, source } = await matchTVL(t.symbol);
      t.tvl = tvl; t.tvl_source = source;
      t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
    }catch(e){
      t.tvl = null; t.tvl_source = null; t.mc_tvl = null;
    }
  }

  // 3) Charts/RSI/ATH via Gecko
  for(const t of tokens){
    if(!t._gecko_id) { t.rsi_d=t.rsi_h4=t.vol7_avg=t.vol30_avg=t.var_vol_7_over_30=t.vol7_tvl=null; t.ath=null; t.ath_date=null; t.d1y=null; continue; }

    // 370j daily → permet: RSI daily (90 derniers), ATH, d1y, volumes 30j & 7j
    let daily=null;
    try{
      daily = await geckoMarketChart(t._gecko_id, 370, 'daily');
    }catch(e){
      console.warn(`Gecko chart fail ${t.symbol}: ${e.message}`);
    }

    if(daily){
      const closes = (daily.prices||[]).map(p=> p[1]).filter(Number.isFinite);
      const volsDaily = (daily.total_volumes||[]).map(v=> v[1]).filter(Number.isFinite);

      // RSI Daily (14) sur les 90 derniers points
      if(closes.length >= 90){
        const last90 = closes.slice(-90);
        t.rsi_d = computeRSI(last90, 14);
      } else {
        t.rsi_d = computeRSI(closes, 14);
      }

      // RSI H4 (optionnel) — nécessite /hourly (désactivé par défaut)
      if(ENABLE_RSI_H4){
        try{
          const hourly = await geckoMarketChart(t._gecko_id, 7, 'hourly');
          const closesH = (hourly.prices||[]).map(p=> p[1]).filter(Number.isFinite);
          const h4=[]; for(let i=0;i<closesH.length;i+=4){ h4.push(closesH[Math.min(i+3, closesH.length-1)]); }
          t.rsi_h4 = computeRSI(h4, 14);
        }catch{ t.rsi_h4 = null; }
      } else {
        t.rsi_h4 = null;
      }

      // Volumes 7j/30j (journaliers)
      if(volsDaily.length){
        const last7  = volsDaily.slice(-7);
        const last30 = volsDaily.slice(-30);
        const avg7  = last7.length  ? (last7.reduce((a,b)=>a+b,0)/last7.length)   : null;
        const avg30 = last30.length ? (last30.reduce((a,b)=>a+b,0)/last30.length) : null;
        t.vol7_avg = avg7; t.vol30_avg = avg30;
        t.var_vol_7_over_30 = (avg7 && avg30) ? (avg7/avg30) : null;
        t.vol7_tvl = (avg7 && t.tvl) ? (avg7 / t.tvl * 100) : null;
      }

      // ATH (prix) & date côté Gecko (plus fiable universellement)
      if(closes.length){
        let ath = -Infinity, athIdx = -1;
        for(let i=0;i<daily.prices.length;i++){
          const v = daily.prices[i][1];
          if(v>ath){ ath=v; athIdx=i; }
        }
        t.ath = Number.isFinite(ath) ? ath : null;
        t.ath_date = (athIdx>=0) ? new Date(daily.prices[athIdx][0]).toISOString().slice(0,10) : null;
      }

      // Δ 1 an (d1y) = (last / value_≈365j - 1)*100
      if(closes.length > 360){
        const last = closes[closes.length-1];
        const yago = closes[closes.length-366]; // ~1 an
        if(Number.isFinite(last) && Number.isFinite(yago) && yago>0){
          t.d1y = ((last / yago) - 1) * 100;
        }
      }
    }

    // ATH Mcap & x (utilise CMC mc/circ si possible)
    if(t.ath!=null && t.price!=null && t.mc!=null){
      const ath_mc = (t.circulating_supply!=null) ? (t.ath * t.circulating_supply) : (t.mc * (t.ath / t.price));
      t.ath_mc = ath_mc ?? null;
      t.x_ath_mc = (ath_mc && t.mc) ? (ath_mc / t.mc) : ((t.price && t.ath) ? (t.ath / t.price) : null);
      t.price_target_ath_mc = (t.x_ath_mc && t.price) ? (t.price * t.x_ath_mc) : null;
    } else {
      t.ath_mc=null; t.x_ath_mc=null; t.price_target_ath_mc=null;
    }
  }

  // Arrondis doux
  const round = (n, d=6)=> (typeof n==='number' && isFinite(n)) ? +n.toFixed(d) : n;
  tokens.forEach(t=>{
    ['price','mc','tvl','mc_tvl','vol_mc_24','vol7_tvl','var_vol_7_over_30',
     'rsi_d','rsi_h4','ath','ath_mc','x_ath_mc','price_target_ath_mc','d1y']
      .forEach(k=> { if(t[k]!=null) t[k]=round(t[k], (k==='mc_tvl'||k==='x_ath_mc')?2:6); });
  });

  return tokens;
}

/* ====== Main ====== */
async function main(){
  let tokens = [];
  try{
    console.log("➡️  Collect depuis Sheets…");
    const base = await collectAssetsFromSheets();
    tokens = base.map(x => ({
      symbol: x.symbol,
      venue: x.venue,
      alert_date: x.alert_date,
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

    console.log("➡️  Enrich (CMC + Gecko + Llama)…");
    tokens = await enrichTokens(tokens);
  }catch(e){
    console.error("❌ Pipeline erreur:", e.message);
  }finally{
    const data = { updated_at: new Date().toISOString(), tokens };
    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    console.log(`✅ data.json écrit (${tokens.length} tokens).`);
  }
}

main();
