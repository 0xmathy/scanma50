// scripts/fetch_and_build.js
// Google Sheet (history ≤2j) → CMC (quotes) → DeFiLlama (TVL) → data.json
// Node 20 (fetch natif). CommonJS.

const fs = require('fs');
const { parse } = require('csv-parse/sync');

/* ====== CONFIG ====== */
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv";

const CMC_KEY  = process.env.CMC_API_KEY || "";
const CMC_BASE = "https://pro-api.coinmarketcap.com/v1";
const LLAMA_BASE = "https://api.llama.fi";

// Anti-rate-limit light
const THROTTLE_MS = 500;
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));

/* ====== UTILS ====== */
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
async function throttledJson(url, label, opts={}){
  await sleep(THROTTLE_MS);
  const res = await fetch(url, opts);
  if(!res.ok) throw new Error(`${label} ${res.status} ${res.statusText}`);
  return await res.json();
}

/* ====== 1) FEUILLE history ≤ 2 jours ====== */
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

/* ====== 2) CMC QUOTES (prix/MC/%/vol/supply/FDV/rank) ====== */
async function cmcQuotesBySymbols(symbols){
  if(!CMC_KEY){ console.warn("⚠️  CMC_API_KEY manquante."); return {}; }

  // batch par 50
  const out = {};
  for(let i=0;i<symbols.length;i+=50){
    const batch = symbols.slice(i,i+50);
    const url = `${CMC_BASE}/cryptocurrency/quotes/latest?symbol=${encodeURIComponent(batch.join(','))}&convert=USD`;
    try{
      const data = await throttledJson(url, "CMC quotes", { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY } });
      if(data && data.data){
        Object.entries(data.data).forEach(([sym, obj])=>{
          out[sym.toUpperCase()] = obj;
        });
      }
    }catch(e){
      console.warn('CMC quotes error:', e.message);
    }
  }
  return out;
}

/* ====== 3) DEFI LLAMA TVL (auto-match best-effort) ====== */
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

/* ====== 4) ENRICH ====== */
async function enrich(tokens){
  // 4.1 CMC Market data
  const symList = tokens.map(t=>t.symbol);
  const quotes = await cmcQuotesBySymbols(symList);

  tokens.forEach(t=>{
    const q = quotes[t.symbol.toUpperCase()];
    const usd = q?.quote?.USD;
    if(usd){
      t.price = usd.price ?? null;
      t.mc    = usd.market_cap ?? null;
      t.rank  = q.cmc_rank ?? null;

      t.d24   = usd.percent_change_24h ?? null;
      t.d7    = usd.percent_change_7d ?? null;
      t.d30   = usd.percent_change_30d ?? null;
      // t.d1y : non garanti par CMC free → on laisse null

      t.volume_24h = usd.volume_24h ?? null;
      t.circulating_supply = q.circulating_supply ?? null;
      t.total_supply = q.total_supply ?? null;
      t.max_supply   = q.max_supply ?? null;
      t.fdv = usd.fully_diluted_market_cap ?? null;

      t.vol_mc_24 = (t.volume_24h && t.mc) ? (t.volume_24h / t.mc * 100) : null;
    }else{
      t.price=t.mc=t.rank=t.d24=t.d7=t.d30=t.volume_24h=t.fdv=null;
      t.circulating_supply=t.total_supply=t.max_supply=null;
      t.vol_mc_24=null;
    }

    // champs NON gérés ici (pas d’OHLCV côté CMC free):
    t.rsi_d=null; t.rsi_h4=null;
    t.ath=null; t.ath_date=null; t.ath_mc=null; t.x_ath_mc=null; t.price_target_ath_mc=null;
    t.vol7_avg=null; t.vol30_avg=null; t.var_vol_7_over_30=null; t.vol7_tvl=null;
  });

  // 4.2 TVL
  for(const t of tokens){
    const { tvl, source } = await matchTVL(t.symbol);
    t.tvl = tvl; t.tvl_source = source;
    t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
    await sleep(120);
  }

  // 4.3 Arrondis doux
  const round = (n, d=6)=> (typeof n==='number' && isFinite(n)) ? +n.toFixed(d) : n;
  tokens.forEach(t=>{
    ['price','mc','tvl','mc_tvl','vol_mc_24']
      .forEach(k=> { if(t[k]!=null) t[k]=round(t[k], (k==='mc_tvl')?2:6); });
  });

  return tokens;
}

/* ====== 5) MAIN ====== */
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
