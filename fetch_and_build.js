// scripts/fetch_and_build.js
// Node 18+ (fetch natif). Workflow: .github/workflows/snapshot.yml
// Génère data.json à partir de deux feuilles Google Sheet (history + alerte) puis enrichit via CoinGecko + DeFiLlama.

import fs from 'fs';
import { parse } from 'csv-parse/sync';

// ========== CONFIG SHEETS ==========
// history (publié en CSV : DateKey + asset)
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv";

// alerte (colonne E:E = asset) — via export direct CSV
const SHEET_URL_ALERTE =
  process.env.SHEET_URL_ALERTE ||
  "https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=0&range=E:E";

// ========== API ENDPOINTS ==========
const CG_BASE = "https://api.coingecko.com/api/v3";
const LLAMA_BASE = "https://api.llama.fi";

// ========== UTILS ==========
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function normalizeSymbol(asset){
  if(!asset) return "";
  const core = String(asset).split(':').pop().trim(); // "BINANCE:AVAXUSDT" → "AVAXUSDT"
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
  const res = await fetch(url);
  if(!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} sur ${label}`);
  const text = await res.text();

  // Essai avec colonnes nommées
  let rows = [];
  try {
    rows = parse(text, { columns: true, skip_empty_lines: true });
  } catch(e) {
    // ignore -> retente plus bas
  }
  // Si une seule colonne sans "asset" comme nom, normalise en { asset: valeur }
  if(rows.length && Object.keys(rows[0]).length === 1 && !('asset' in rows[0])) {
    const onlyKey = Object.keys(rows[0])[0];
    rows = rows.map(r => ({ asset: r[onlyKey] }));
    return rows;
  }
  if(rows.length) return rows;

  // Sans entêtes
  const raw = parse(text, { columns: false, skip_empty_lines: true });
  return raw.map(arr => ({ asset: (arr && arr[0]) ? String(arr[0]) : '' }));
}

// ========== 1) COLLECT ASSETS FROM SHEETS ==========
async function collectAssetsFromSheets(){
  console.log("➡️  Lecture CSV: history…");
  const recH = await fetchCsv(SHEET_URL_HISTORY, "history");

  let recA = [];
  if(SHEET_URL_ALERTE){
    console.log("➡️  Lecture CSV: alerte…");
    try {
      recA = await fetchCsv(SHEET_URL_ALERTE, "alerte");
    } catch(e){
      console.warn("⚠️  Alerte non chargée:", e.message);
    }
  } else {
    console.warn("⚠️  SHEET_URL_ALERTE non défini → on ignore l’onglet alerte");
  }

  const seen = new Set();
  const out = [];

  // Filtre history ≤ 2 jours via DateKey (YYYY-MM-DD)
  const today = new Date();
  const cutoff = new Date(today.getTime() - 2*24*3600*1000);
  const cutoffYMD = ymdParis(cutoff);

  for(const row of recH){
    const asset = getAssetField(row);
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
      alert_date: dk || ymdParis(today),
      _source: "history"
    });
  }

  for(const row of recA){
    const asset = getAssetField(row);
    if(!asset) continue;
    const symbol = normalizeSymbol(asset);
    if(!symbol) continue;

    if(seen.has(symbol)) continue;
    seen.add(symbol);

    out.push({
      symbol,
      venue: parseVenue(asset),
      alert_date: ymdParis(today),
      _source: "alerte"
    });
  }

  console.log(`✅ ${out.length} tokens agrégés (history ≤2j + alerte aujourd’hui)`);
  return out;
}

// ========== 2) COINGECKO HELPERS ==========
async function cgSearchIdBySymbol(symbol){
  // CoinGecko /search → retourne des coins avec .symbol, .id, .name
  const url = `${CG_BASE}/search?query=${encodeURIComponent(symbol)}`;
  const res = await fetch(url);
  if(!res.ok) throw new Error(`CG search ${res.status}`);
  const data = await res.json();

  // Meilleur match: symbol exact (case-insensitive) et score max
  const coins = (data.coins||[]).filter(c => c && c.symbol);
  const exact = coins.filter(c => String(c.symbol).toUpperCase() === symbol.toUpperCase());
  const pick = (exact[0] || coins[0] || null);
  return pick ? pick.id : null;
}

async function cgMarkets(ids){
  // /coins/markets?vs_currency=usd&ids=...
  if(!ids.length) return [];
  const url = `${CG_BASE}/coins/markets?vs_currency=usd&ids=${encodeURIComponent(ids.join(','))}&price_change_percentage=24h,7d,30d,1y&per_page=250`;
  const res = await fetch(url);
  if(!res.ok) throw new Error(`CG markets ${res.status}`);
  return await res.json();
}

async function cgMarketChart(id, days=30, interval=''){
  // /coins/{id}/market_chart?vs_currency=usd&days=30&interval=hourly
  const url = `${CG_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}${interval?`&interval=${interval}`:''}`;
  const res = await fetch(url);
  if(!res.ok) throw new Error(`CG chart ${id} ${res.status}`);
  return await res.json();
}

// ========== 3) RSI ==========
function computeRSI(closes, period=14){
  // Wilder’s RSI
  if(!closes || closes.length < period+1) return null;
  const deltas = [];
  for(let i=1;i<closes.length;i++) deltas.push(closes[i]-closes[i-1]);

  let gain=0, loss=0;
  for(let i=0;i<period;i++){
    const d = deltas[i];
    if(d>0) gain += d; else loss -= d;
  }
  gain /= period; loss /= period;

  for(let i=period;i<deltas.length;i++){
    const d=deltas[i];
    const g = d>0 ? d : 0;
    const l = d<0 ? -d : 0;
    gain = (gain*(period-1) + g)/period;
    loss = (loss*(period-1) + l)/period;
  }
  if(loss===0) return 100;
  const rs = gain / loss;
  return 100 - (100/(1+rs));
}

// ========== 4) LLAMA HELPERS (best-effort) ==========
let LLAMA_PROTOCOLS_CACHE = null;
async function llamaProtocols(){
  if(LLAMA_PROTOCOLS_CACHE) return LLAMA_PROTOCOLS_CACHE;
  const res = await fetch(`${LLAMA_BASE}/protocols`);
  if(!res.ok) throw new Error(`Llama protocols ${res.status}`);
  LLAMA_PROTOCOLS_CACHE = await res.json();
  return LLAMA_PROTOCOLS_CACHE;
}
async function matchTVL(symbol){
  try{
    const protos = await llamaProtocols();
    const sym = symbol.toUpperCase();
    // Heuristique: name ou symbol approchant (commence par / égal)
    const cand = protos.find(p =>
      (p.symbol && String(p.symbol).toUpperCase() === sym) ||
      (p.symbol && String(p.symbol).toUpperCase().startsWith(sym)) ||
      (p.name && String(p.name).toUpperCase() === sym)
    ) || protos.find(p => p.name && p.name.toUpperCase().startsWith(sym));
    if(!cand) return { tvl: null, source: null };

    // TVL total
    const tvl = (typeof cand.tvl === 'number') ? cand.tvl : null;
    return { tvl, source: `protocol:${cand.slug||cand.name}` };
  }catch(e){
    return { tvl:null, source:null };
  }
}

// ========== 5) ENRICH PIPELINE ==========
async function enrichTokens(tokens){
  // 5.1 Résolution des ids CoinGecko
  for(const t of tokens){
    try{
      t._cg_id = await cgSearchIdBySymbol(t.symbol);
      await sleep(200); // politeness
    }catch(e){
      console.warn(`CG search fail ${t.symbol}:`, e.message);
      t._cg_id = null;
    }
  }
  const idMap = tokens.filter(t=>t._cg_id).reduce((m,t)=>{ m[t._cg_id]=t; return m; }, {});

  // 5.2 Marché (prix, MC, variations, vol, supply)
  const ids = Object.keys(idMap);
  let markets = [];
  try{
    for(let i=0;i<ids.length;i+=150){
      const batch = ids.slice(i,i+150);
      const res = await cgMarkets(batch);
      markets = markets.concat(res||[]);
      await sleep(300);
    }
  }catch(e){
    console.warn('CG markets error:', e.message);
  }

  // Appliquer markets
  markets.forEach(m=>{
    const t = idMap[m.id];
    if(!t) return;
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
    t.ath_change_pct = (typeof m.ath_change_percentage === 'number') ? m.ath_change_percentage : null;

    // ratios init
    t.vol_mc_24 = (t.volume_24h && t.mc) ? (t.volume_24h / t.mc * 100) : null; // en %
  });

  // 5.3 TVL (best effort via DeFiLlama)
  for(const t of tokens){
    try{
      const { tvl, source } = await matchTVL(t.symbol);
      t.tvl = tvl;
      t.tvl_source = source;
      await sleep(150);
    }catch(e){
      t.tvl = null; t.tvl_source = null;
    }
  }
  tokens.forEach(t=>{
    t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
  });

  // 5.4 ATH Mcap approx + x_ath_mc + price_target_ath_mc
  tokens.forEach(t=>{
    if(t.ath!=null && t.price!=null && t.mc!=null){
      // Approximation: ath_mc ≈ ath_price * circulating_supply_today
      // Comme current_mc = price * circ_today → x_ath_mc ≈ ath_price / price
      const ath_mc = (t.circulating_supply!=null) ? (t.ath * t.circulating_supply) : (t.mc * (t.ath / t.price));
      t.ath_mc = ath_mc ?? null;
      t.x_ath_mc = (ath_mc && t.mc) ? (ath_mc / t.mc) : ((t.price && t.ath) ? (t.ath / t.price) : null);
      t.price_target_ath_mc = (t.x_ath_mc && t.price) ? (t.price * t.x_ath_mc) : null;
    } else {
      t.ath_mc = null;
      t.x_ath_mc = null;
      t.price_target_ath_mc = null;
    }
  });

  // 5.5 Volumes 7j / 30j + RSI Daily & H4
  for(const t of tokens){
    if(!t._cg_id){ t.rsi_d=null; t.rsi_h4=null; t.vol7_avg=null; t.vol30_avg=null; t.vol7_tvl=null; t.var_vol_7_over_30=null; continue; }
    try{
      // Daily closes (90j) pour RSI daily
      const daily = await cgMarketChart(t._cg_id, 90, 'daily'); // interval=daily ok
      const closesD = (daily.prices||[]).map(p=> p[1]).filter(Number.isFinite);
      t.rsi_d = computeRSI(closesD, 14);

      // Hourly (7j) pour RSI H4 (on regroupe 4 bougies/h → 4h)
      const hourly = await cgMarketChart(t._cg_id, 7, 'hourly');
      const closesH = (hourly.prices||[]).map(p=> p[1]).filter(Number.isFinite);
      // agrège par 4 heures (4 * 60min / 60min = 4 points) – approximation
      const h4 = [];
      for(let i=0;i<closesH.length;i+=4){ h4.push(closesH[Math.min(i+3, closesH.length-1)]); }
      t.rsi_h4 = computeRSI(h4, 14);

      // Volumes: /market_chart (30j)
      const chart30 = await cgMarketChart(t._cg_id, 30);
      const vols = (chart30.total_volumes||[]).map(v=> v[1]).filter(Number.isFinite);
      if(vols.length){
        const last7 = vols.slice(-7);
        const last30 = vols.slice(-30);
        const avg7 = last7.length ? (last7.reduce((a,b)=>a+b,0)/last7.length) : null;
        const avg30 = last30.length ? (last30.reduce((a,b)=>a+b,0)/last30.length) : null;

        t.vol7_avg = avg7;
        t.vol30_avg = avg30;
        t.var_vol_7_over_30 = (avg7 && avg30) ? (avg7/avg30) : null;
        t.vol7_tvl = (avg7 && t.tvl) ? (avg7 / t.tvl * 100) : null; // %
      } else {
        t.vol7_avg = null; t.vol30_avg=null; t.var_vol_7_over_30=null; t.vol7_tvl=null;
      }
      await sleep(250);
    }catch(e){
      console.warn(`Chart fail ${t.symbol}:`, e.message);
      t.rsi_d=null; t.rsi_h4=null; t.vol7_avg=null; t.vol30_avg=null; t.var_vol_7_over_30=null; t.vol7_tvl=null;
    }
  }

  // Nettoyage décimales (limiter bruit)
  const round = (n, d=6)=> (typeof n==='number' && isFinite(n)) ? +n.toFixed(d) : n;
  tokens.forEach(t=>{
    ['price','mc','tvl','mc_tvl','vol_mc_24','vol7_tvl','var_vol_7_over_30','rsi_d','rsi_h4','ath','ath_mc','x_ath_mc','price_target_ath_mc']
      .forEach(k=> { if(t[k]!=null) t[k]=round(t[k], k.includes('mc_tvl')||k==='x_ath_mc' ? 2 : 6); });
  });

  return tokens;
}

// ========== 6) MAIN ==========
async function main(){
  try{
    console.log("➡️  Collect tokens depuis Sheets…");
    const base = await collectAssetsFromSheets();

    // Prépare tableau de travail
    const tokens = base.map(x => ({
      symbol: x.symbol,
      venue: x.venue,
      alert_date: x.alert_date,
      // champs enrichis
      price:null, d24:null, d7:null, d30:null, d1y:null,
      mc:null, tvl:null, mc_tvl:null,
      volume_24h:null, vol_mc_24:null,
      vol7_avg:null, vol30_avg:null, vol7_tvl:null, var_vol_7_over_30:null,
      rsi_d:null, rsi_h4:null,
      ath:null, ath_date:null, ath_change_pct:null,
      ath_mc:null, x_ath_mc:null, price_target_ath_mc:null,
      circulating_supply:null, total_supply:null, max_supply:null, fdv:null, rank:null,
      website:null, twitter:null, github:null, whitepaper:null,
      tvl_source:null
    }));

    console.log("➡️  Enrichissement CoinGecko + DeFiLlama…");
    await enrichTokens(tokens);

    const data = {
      updated_at: new Date().toISOString(),
      tokens
    };

    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    console.log(`✅ Écrit ${process.cwd()}/data.json avec ${tokens.length} tokens.`);
  }catch(e){
    console.error("❌ Erreur:", e.message);
    // Écrit tout de même un JSON vide pour que le front ne casse pas
    const data = { updated_at: new Date().toISOString(), tokens: [] };
    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    // Rejette pour status non-0 dans GitHub Actions (utile pour debug)
    throw e;
  }
}

main();
