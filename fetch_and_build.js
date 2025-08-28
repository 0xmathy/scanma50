// scripts/fetch_and_build.js
// Sheet (history ≤2j) → CMC (market) + DeFiLlama (TVL) + CEX Klines (RSI/ATH/vol7/30) → data.json
// Node 20 (fetch natif). CommonJS.

const fs = require('fs');
const { parse } = require('csv-parse/sync');

/* ========= CONFIG ========= */
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv";

const CMC_KEY  = process.env.CMC_API_KEY || "";
const CMC_BASE = "https://pro-api.coinmarketcap.com/v1";

const LLAMA_BASE = "https://api.llama.fi";

const THROTTLE_MS = Number(process.env.THROTTLE_MS || 450);
const MAX_TOKENS_PER_RUN = Number(process.env.MAX_TOKENS_PER_RUN || 50);
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));

/* ========= UTILS ========= */
function normalizeSymbolForCMC(asset){
  if(!asset) return "";
  const core = String(asset).split(':').pop().trim(); // "BINANCE:AVAXUSDT" → "AVAXUSDT"
  return core.replace(/USDT|USD|USDC|PERP|_PERP|\/.*$/i,"").trim().toUpperCase();
}
function parseVenue(asset){
  if(!asset) return "";
  const parts = String(asset).split(':');
  return parts.length>1 ? parts[0].trim().toUpperCase() : "";
}
function parseAssetPair(asset){
  if(!asset) return "";
  const parts = String(asset).split(':');
  return parts.length>1 ? parts[1].trim().toUpperCase() : String(asset).trim().toUpperCase();
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
async function jget(url, label, opts={}){
  await sleep(THROTTLE_MS);
  const res = await fetch(url, opts);
  if(!res.ok) throw new Error(`${label} ${res.status} ${res.statusText}`);
  return await res.json();
}

/* ========= SHEET ========= */
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

    const symbolCMC = normalizeSymbolForCMC(asset);
    if(!symbolCMC) continue;

    const dk = (row["DateKey"] || row["datekey"] || row["date"] || "").toString().slice(0,10);
    if(dk && dk < cutoffYMD) continue;

    if(seen.has(symbolCMC)) continue;
    seen.add(symbolCMC);

    out.push({
      symbol_cmc: symbolCMC,                  // pour CMC mapping
      venue: parseVenue(asset),               // ex. BINANCE / MEXC / BYBIT / OKX
      asset_pair: parseAssetPair(asset),      // ex. AVAXUSDT (ou ATHUSDT, etc.)
      alert_date: dk || ymdParis(today)
    });
  }
  if(out.length > MAX_TOKENS_PER_RUN){
    console.warn(`⚠️  Trop de tokens (${out.length}) → on limite à ${MAX_TOKENS_PER_RUN}`);
  }
  const sliced = out.slice(0, MAX_TOKENS_PER_RUN);
  console.log(`✅ ${sliced.length} tokens (history ≤2j)`);
  return sliced;
}

/* ========= CMC (market) ========= */
async function cmcQuotesBySymbols(symbols){
  if(!CMC_KEY){ console.warn("⚠️  CMC_API_KEY manquante."); return {}; }
  const out = {};
  for(let i=0;i<symbols.length;i+=50){
    const batch = symbols.slice(i,i+50);
    const url = `${CMC_BASE}/cryptocurrency/quotes/latest?symbol=${encodeURIComponent(batch.join(','))}&convert=USD`;
    try{
      const data = await jget(url, "CMC quotes", { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY } });
      if(data && data.data){
        Object.entries(data.data).forEach(([sym, obj])=>{
          out[sym.toUpperCase()] = obj;
        });
      }
    }catch(e){ console.warn('CMC quotes error:', e.message); }
  }
  return out;
}

/* ========= DeFiLlama (TVL) ========= */
let LLAMA_PROTOCOLS_CACHE = null;
async function llamaProtocols(){
  if(LLAMA_PROTOCOLS_CACHE) return LLAMA_PROTOCOLS_CACHE;
  LLAMA_PROTOCOLS_CACHE = await jget(`${LLAMA_BASE}/protocols`, "Llama protocols");
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

/* ========= CEX KLINES (OHLCV) ========= */
// output: [{t, o, h, l, c, v}, ...] (t en ms)
// essaie la pair brute (sheet) puis fallback (USDT/USDC/USD). OKX nécessite "AVAX-USDT".
async function fetchKlinesDaily(venue, assetPair){
  if(!venue) return [];
  const v = venue.toUpperCase();

  // helpers
  const asOKX = (p)=> p.includes('-') ? p : p.replace(/(USDT|USDC|USD)$/,'-$1');
  const tryList = (pair)=>{
    const base = pair.replace(/[^A-Z0-9-]/g,'').toUpperCase();
    const variants = new Set([base]);
    if(!/(USDT|USDC|USD)$/.test(base)){
      variants.add(base+'USDT'); variants.add(base+'USDC'); variants.add(base+'USD');
    }else{
      // déjà suffixée : tester aussi USDT/USDC/USD
      variants.add(base.replace(/(USDT|USDC|USD)$/,'USDT'));
      variants.add(base.replace(/(USDT|USDC|USD)$/,'USDC'));
      variants.add(base.replace(/(USDT|USDC|USD)$/,'USD'));
    }
    return Array.from(variants);
  };

  const candidates = tryList(assetPair||"");

  for(const cand of candidates){
    try{
      if(v === 'BINANCE'){
        const symbol = cand.replace(/[^A-Z0-9]/g,'');
        const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1d&limit=1000`;
        const data = await jget(url, `BINANCE klines ${symbol}`);
        const rows = (data||[]).map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]}));
        if(rows.length) return rows;
      } else if(v === 'MEXC'){
        const symbol = cand.replace(/[^A-Z0-9]/g,'');
        const url = `https://api.mexc.com/api/v3/klines?symbol=${symbol}&interval=1d&limit=1000`;
        const data = await jget(url, `MEXC klines ${symbol}`);
        const rows = (data||[]).map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]}));
        if(rows.length) return rows;
      } else if(v === 'BYBIT'){
        const symbol = cand.replace(/[^A-Z0-9]/g,'');
        const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=${symbol}&interval=D&limit=1000`;
        const data = await jget(url, `BYBIT klines ${symbol}`);
        const list = data?.result?.list || [];
        const rows = list.map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]})).reverse();
        if(rows.length) return rows;
      } else if(v === 'OKX'){
        let instId = asOKX(cand);
        const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=1D&limit=1000`;
        const data = await jget(url, `OKX candles ${instId}`);
        const list = data?.data || [];
        const rows = list.map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]})).reverse();
        if(rows.length) return rows;
      }
    }catch(e){
      // try next variant
    }
  }
  return [];
}

async function fetchClosesH4(venue, assetPair){
  if(!venue) return [];
  const v = venue.toUpperCase();

  const asOKX = (p)=> p.includes('-') ? p : p.replace(/(USDT|USDC|USD)$/,'-$1');
  const variants = [assetPair].filter(Boolean);

  for(const candRaw of variants){
    try{
      if(v === 'BINANCE'){
        const symbol = candRaw.replace(/[^A-Z0-9]/g,'').toUpperCase();
        const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=4h&limit=1000`;
        const data = await jget(url, `BINANCE klines 4h ${symbol}`);
        const arr = (data||[]).map(a=>+a[4]).filter(Number.isFinite);
        if(arr.length) return arr;
      } else if(v === 'MEXC'){
        const symbol = candRaw.replace(/[^A-Z0-9]/g,'').toUpperCase();
        const url = `https://api.mexc.com/api/v3/klines?symbol=${symbol}&interval=4h&limit=1000`;
        const data = await jget(url, `MEXC klines 4h ${symbol}`);
        const arr = (data||[]).map(a=>+a[4]).filter(Number.isFinite);
        if(arr.length) return arr;
      } else if(v === 'BYBIT'){
        const symbol = candRaw.replace(/[^A-Z0-9]/g,'').toUpperCase();
        const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=${symbol}&interval=240&limit=1000`;
        const data = await jget(url, `BYBIT klines 4h ${symbol}`);
        const list = data?.result?.list || [];
        const arr = list.map(a=>+a[4]).reverse().filter(Number.isFinite);
        if(arr.length) return arr;
      } else if(v === 'OKX'){
        let instId = asOKX(candRaw.toUpperCase());
        const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=4H&limit=1000`;
        const data = await jget(url, `OKX candles 4H ${instId}`);
        const list = data?.data || [];
        const arr = list.map(a=>+a[4]).reverse().filter(Number.isFinite);
        if(arr.length) return arr;
      }
    }catch(e){
      // try next
    }
  }
  return [];
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

/* ========= ENRICH ========= */
async function enrich(tokens){
  /* 1) CMC */
  const quotes = await cmcQuotesBySymbols(tokens.map(t=>t.symbol_cmc));
  tokens.forEach(t=>{
    const q = quotes[t.symbol_cmc?.toUpperCase()];
    const usd = q?.quote?.USD;
    if(usd){
      t.price = usd.price ?? null;
      t.mc    = usd.market_cap ?? null;
      t.rank  = q.cmc_rank ?? null;
      t.d24   = usd.percent_change_24h ?? null;
      t.d7    = usd.percent_change_7d ?? null;
      t.d30   = usd.percent_change_30d ?? null;
      t.volume_24h = usd.volume_24h ?? null;
      t.circulating_supply = q.circulating_supply ?? null;
      t.total_supply = q.total_supply ?? null;
      t.max_supply   = q.max_supply ?? null;
      t.fdv = usd.fully_diluted_market_cap ?? null;
      t.vol_mc_24 = (t.volume_24h && t.mc) ? (t.volume_24h / t.mc * 100) : null;
    } else {
      t.price=t.mc=t.rank=t.d24=t.d7=t.d30=t.volume_24h=t.fdv=null;
      t.circulating_supply=t.total_supply=t.max_supply=null;
      t.vol_mc_24=null;
    }
    // placeholders
    t.rsi_d=null; t.rsi_h4=null;
    t.ath=null; t.ath_date=null;
    t.vol7_avg=null; t.vol30_avg=null; t.var_vol_7_over_30=null; t.vol7_tvl=null;
    t.ath_mc=null; t.x_ath_mc=null; t.price_target_ath_mc=null;
    t.tvl=null; t.tvl_source=null; t.mc_tvl=null;
  });

  /* 2) TVL (DeFiLlama) */
  for(const t of tokens){
    try{
      const { tvl, source } = await matchTVL(t.symbol_cmc);
      t.tvl = tvl; t.tvl_source = source;
      t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
    }catch{ t.tvl=null; t.tvl_source=null; t.mc_tvl=null; }
    await sleep(120);
  }

  /* 3) Klines → RSI/ATH/vol7/30 */
  for(const t of tokens){
    try{
      const daily = await fetchKlinesDaily(t.venue, t.asset_pair);
      if(daily && daily.length){
        const closes = daily.map(r=>+r.c).filter(Number.isFinite);
        const highs  = daily.map(r=>+r.h).filter(Number.isFinite);
        const vols   = daily.map(r=>+r.v).filter(Number.isFinite);

        // RSI D
        t.rsi_d = computeRSI(closes.slice(-200), 14);

        // ATH (prix + date) sur l'horizon récupéré (1000 jours max)
        let ath = -Infinity, athTs=null;
        for(const r of daily){ if(r.h>ath){ ath=r.h; athTs=r.t; } }
        t.ath = Number.isFinite(ath) ? ath : null;
        t.ath_date = (athTs!=null) ? new Date(athTs).toISOString().slice(0,10) : null;

        // Moy. volumes 7j / 30j et Var
        const last7 = vols.slice(-7);
        const last30= vols.slice(-30);
        const avg7  = last7.length ? last7.reduce((a,b)=>a+b,0)/last7.length : null;
        const avg30 = last30.length? last30.reduce((a,b)=>a+b,0)/last30.length: null;
        t.vol7_avg = avg7;
        t.vol30_avg = avg30;
        t.var_vol_7_over_30 = (avg7 && avg30) ? (avg7/avg30) : null;
        t.vol7_tvl = (avg7 && t.tvl) ? (avg7 / t.tvl * 100) : null;

        // RSI H4
        try{
          const h4closes = await fetchClosesH4(t.venue, t.asset_pair);
          t.rsi_h4 = computeRSI(h4closes.slice(-300), 14);
        }catch{ t.rsi_h4 = null; }

        // ATH Mcap & x
        if(t.ath!=null && t.price!=null && t.mc!=null){
          const ath_mc = (t.circulating_supply!=null)
            ? (t.ath * t.circulating_supply)
            : (t.mc * (t.ath / t.price));
          t.ath_mc = ath_mc ?? null;
          t.x_ath_mc = (ath_mc && t.mc) ? (ath_mc / t.mc) : ((t.price && t.ath) ? (t.ath / t.price) : null);
          t.price_target_ath_mc = (t.x_ath_mc && t.price) ? (t.price * t.x_ath_mc) : null;
        }
      }
    }catch(e){
      console.warn(`Klines enrich fail ${t.symbol_cmc}:`, e.message);
    }
  }

  // Arrondis doux
  const round = (n, d=6)=> (typeof n==='number' && isFinite(n)) ? +n.toFixed(d) : n;
  tokens.forEach(t=>{
    ['price','mc','tvl','mc_tvl','vol_mc_24',
     'vol7_avg','vol30_avg','var_vol_7_over_30','vol7_tvl',
     'rsi_d','rsi_h4','ath','ath_mc','x_ath_mc','price_target_ath_mc'
    ].forEach(k=>{
      if(t[k]!=null) t[k] = round(t[k], (k==='mc_tvl'||k==='x_ath_mc'||k==='var_vol_7_over_30')?2:6);
    });
  });

  return tokens;
}

/* ========= MAIN ========= */
(async function main(){
  let tokens = [];
  try{
    const base = await collectFromHistory();
    tokens = base.map(x=>({
      symbol: x.symbol_cmc,         // pour compat UI ancienne (colonne "symbol")
      symbol_cmc: x.symbol_cmc,
      venue: x.venue,
      asset_pair: x.asset_pair,
      alert_date: x.alert_date,

      // champs enrichis ensuite
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
    const data = { updated_at: new Date().toISOString(), tokens: tokens || [] };
    fs.writeFileSync('data.json', JSON.stringify(data, null, 2));
    process.exit(1);
  }
})();
