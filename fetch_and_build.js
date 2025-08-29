// scripts/fetch_and_build.js
// Sheet (history ≤2j + alertes USDT) → CMC (market + info) + DeFiLlama (TVL + meta) + CEX Klines (RSI/ATH/vol7/30) → data.json
// Node 20 (fetch natif). CommonJS.

const fs = require('fs');
const { parse } = require('csv-parse/sync');

/* ========= CONFIG ========= */
const SHEET_URL_HISTORY =
  "https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=916004394";

const SHEET_URL_ALERTES =
  "https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=0";

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
function stripHtml(s=''){
  return String(s).replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
}
function truncate(s, n=160){
  if(!s) return '';
  const t = s.slice(0, n);
  return (s.length>n) ? t.replace(/\s+\S*$/, '') + '…' : t;
}

/* ========= SHEET ========= */
async function collectFromHistory(){
  console.log("➡️  Lecture CSV: history…");
  const recs = await fetchCsv(SHEET_URL_HISTORY, "history");
  const seen = new Set();
  const out = [];

  const today = new Date();
  const cutoff = new Date(today.getTime() - 2*24*3600*1000); // ≤2 jours
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
      symbol_cmc: symbolCMC,
      venue: parseVenue(asset),
      asset_pair: parseAssetPair(asset),
      alert_date: dk || ymdParis(today),
      source: "history"
    });
  }
  console.log(`✅ ${out.length} tokens (history ≤2j)`);
  return out;
}

async function collectFromAlertes(){
  console.log("➡️  Lecture CSV: alertes…");
  const recs = await fetchCsv(SHEET_URL_ALERTES, "alerte");
  const out = [];
  const seen = new Set();

  for(const row of recs){
    const asset = row["asset"] || row["Asset"] || row["ASSET"];
    if(!asset) continue;

    // on filtre : uniquement les paires USDT (crypto spot)
    if(!/USDT/i.test(asset)) continue;

    const symbolCMC = normalizeSymbolForCMC(asset);
    if(!symbolCMC) continue;
    if(seen.has(symbolCMC)) continue;
    seen.add(symbolCMC);

    out.push({
      symbol_cmc: symbolCMC,
      venue: parseVenue(asset),
      asset_pair: parseAssetPair(asset),
      alert_date: ymdParis(new Date()), // aujourd’hui
      source: "alerte"
    });
  }
  console.log(`✅ ${out.length} tokens (alertes USDT)`);
  return out;
}

/* ========= CMC (market + info) ========= */
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

async function cmcInfoBySymbols(symbols){
  if(!CMC_KEY){ console.warn("⚠️  CMC_API_KEY manquante pour info."); return {}; }
  const out = {};
  for(let i=0;i<symbols.length;i+=50){
    const batch = symbols.slice(i,i+50);
    const url = `${CMC_BASE}/cryptocurrency/info?symbol=${encodeURIComponent(batch.join(','))}`;
    try{
      const data = await jget(url, "CMC info", { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY } });
      const d = data?.data || {};
      Object.keys(d).forEach(sym=>{
        const val = d[sym];
        const first = Array.isArray(val) ? val[0] : val;
        if(first){
          out[sym.toUpperCase()] = {
            category: first.category || null,
            description: first.description || null,
            tags: first.tags || [],
            urls: first.urls || {},
            date_added: first.date_added || null,
            name: first.name || null,
            slug: first.slug || null
          };
        }
      });
    }catch(e){ console.warn('CMC info error:', e.message); }
  }
  return out;
}

/* ========= DeFiLlama (TVL + proto meta) ========= */
let LLAMA_PROTOCOLS_CACHE = null;
async function llamaProtocols(){
  if(LLAMA_PROTOCOLS_CACHE) return LLAMA_PROTOCOLS_CACHE;
  LLAMA_PROTOCOLS_CACHE = await jget(`${LLAMA_BASE}/protocols`, "Llama protocols");
  return LLAMA_PROTOCOLS_CACHE;
}
async function matchTVLAndMeta(symbol){
  try{
    const protos = await llamaProtocols();
    const sym = symbol.toUpperCase();
    const cand = protos.find(p =>
      (p.symbol && String(p.symbol).toUpperCase() === sym) ||
      (p.symbol && String(p.symbol).toUpperCase().startsWith(sym)) ||
      (p.name && String(p.name).toUpperCase() === sym)
    ) || protos.find(p => p.name && p.name.toUpperCase().startsWith(sym));
    if(!cand) return { tvl:null, source:null, category:null, chains:[], protocol_name:null };
    const tvl = (typeof cand.tvl === 'number') ? cand.tvl : null;
    const category = cand.category || null;
    const chains = Array.isArray(cand.chains) ? cand.chains : [];
    const protocol_name = cand.name || null;
    return { tvl, source: `protocol:${cand.slug||cand.name}`, category, chains, protocol_name };
  }catch{
    return { tvl:null, source:null, category:null, chains:[], protocol_name:null };
  }
}

/* ========= CEX KLINES (OHLCV) ========= */
function secondsToMs(ts){ return ts<1e12 ? ts*1000 : ts; }

// ---- BINANCE ----
async function klinesBinanceDaily(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.binance.com/api/v3/klines?symbol=${s}&interval=1d&limit=1000`;
  const data = await jget(url, `BINANCE klines ${s}`);
  return (data||[]).map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]}));
}
async function closesBinanceH4(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.binance.com/api/v3/klines?symbol=${s}&interval=4h&limit=1000`;
  const data = await jget(url, `BINANCE klines 4h ${s}`);
  return (data||[]).map(a=>+a[4]).filter(Number.isFinite);
}

// ---- MEXC ----
async function klinesMexcDaily(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.mexc.com/api/v3/klines?symbol=${s}&interval=1d&limit=1000`;
  const data = await jget(url, `MEXC klines ${s}`);
  return (data||[]).map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]}));
}
async function closesMexcH4(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.mexc.com/api/v3/klines?symbol=${s}&interval=4h&limit=1000`;
  const data = await jget(url, `MEXC klines 4h ${s}`);
  return (data||[]).map(a=>+a[4]).filter(Number.isFinite);
}

// ---- BYBIT ----
async function klinesBybitDaily(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=${s}&interval=D&limit=1000`;
  const data = await jget(url, `BYBIT klines ${s}`);
  const list = data?.result?.list || [];
  return list.map(a=>({t:+a[0], o:+a[1], h:+a[2], l:+a[3], c:+a[4], v:+a[5]})).reverse();
}
async function closesBybitH4(symbol){
  const s = symbol.replace(/[^A-Z0-9]/g,'').toUpperCase();
  const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=${s}&interval=240&limit=1000`;
  const data = await jget(url, `BYBIT klines 4h ${s}`);
  const list = data?.result?.list || [];
  return list.map(a=>+a[4]).reverse().filter(Number.isFinite);
}

// ---- KUCOIN ----
async function klinesKucoinDaily(symbol){
  const s = symbol.toUpperCase().includes('-') ? symbol.toUpperCase()
            : symbol.toUpperCase().replace(/(USDT|USDC|USD)$/,'-$1');
  const url = `https://api.kucoin.com/api/v1/market/candles?type=1day&symbol=${encodeURIComponent(s)}`;
  const data = await jget(url, `KUCOIN candles ${s}`);
  const arr = data?.data || []; // plus récent d'abord
  return arr.map(a=>{
    // a = [time, open, close, high, low, volume, turnover], time en seconds
    const t = secondsToMs(+a[0]);
    return { t, o:+a[1], c:+a[2], h:+a[3], l:+a[4], v:+a[5] };
  }).reverse();
}
async function closesKucoinH4(symbol){
  const s = symbol.toUpperCase().includes('-') ? symbol.toUpperCase()
            : symbol.toUpperCase().replace(/(USDT|USDC|USD)$/,'-$1');
  const url = `https://api.kucoin.com/api/v1/market/candles?type=4hour&symbol=${encodeURIComponent(s)}`;
  const data = await jget(url, `KUCOIN candles 4h ${s}`);
  const arr = data?.data || [];
  return arr.map(a=>+a[2]).reverse().filter(Number.isFinite); // close = index 2
}

/* ==== Orchestrateurs ==== */
const SUPPORTED = ['BINANCE','MEXC','BYBIT','KUCOIN'];

function buildCandidates(pair){
  const base = (pair||'').toUpperCase().replace(/[^A-Z0-9-]/g,'');
  const set = new Set();
  set.add(base);
  if(!/(USDT|USDC|USD)$/.test(base)){
    set.add(base+'USDT'); set.add(base+'USDC'); set.add(base+'USD');
  }else{
    set.add(base.replace(/(USDT|USDC|USD)$/,'USDT'));
    set.add(base.replace(/(USDT|USDC|USD)$/,'USDC'));
    set.add(base.replace(/(USDT|USDC|USD)$/,'USD'));
  }
  return Array.from(set);
}

async function fetchKlinesDailyAny(preferredVenue, pair){
  const order = [];
  const pref = (preferredVenue||'').toUpperCase();
  if(SUPPORTED.includes(pref)) order.push(pref);
  SUPPORTED.forEach(v=>{ if(!order.includes(v)) order.push(v); }); // fallback

  const variants = buildCandidates(pair);

  for(const venue of order){
    for(const cand of variants){
      try{
        if(venue==='BINANCE'){
          const rows = await klinesBinanceDaily(cand);
          if(rows.length) return { venue, pair:cand, rows };
        }else if(venue==='MEXC'){
          const rows = await klinesMexcDaily(cand);
          if(rows.length) return { venue, pair:cand, rows };
        }else if(venue==='BYBIT'){
          const rows = await klinesBybitDaily(cand);
          if(rows.length) return { venue, pair:cand, rows };
        }else if(venue==='KUCOIN'){
          const rows = await klinesKucoinDaily(cand);
          if(rows.length) return { venue, pair:cand, rows };
        }
      }catch(e){
        // try next variant
      }
    }
  }
  return { venue:null, pair:null, rows:[] };
}

async function fetchClosesH4Any(venueHint, pair){
  const order = [];
  const pref = (venueHint||'').toUpperCase();
  if(SUPPORTED.includes(pref)) order.push(pref);
  SUPPORTED.forEach(v=>{ if(!order.includes(v)) order.push(v); });

  for(const venue of order){
    try{
      if(venue==='BINANCE') { const x=await closesBinanceH4(pair); if(x.length) return x; }
      if(venue==='MEXC')    { const x=await closesMexcH4(pair);    if(x.length) return x; }
      if(venue==='BYBIT')   { const x=await closesBybitH4(pair);   if(x.length) return x; }
      if(venue==='KUCOIN')  { const x=await closesKucoinH4(pair);  if(x.length) return x; }
    }catch(e){/* next */}
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
  /* 1) CMC quotes + info */
  const symbols = tokens.map(t=>t.symbol_cmc);
  const quotes = await cmcQuotesBySymbols(symbols);
  const infos  = await cmcInfoBySymbols(symbols);

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
    // contexte (CMC info)
    const info = infos[t.symbol_cmc?.toUpperCase()];
    if(info){
      const urls = info.urls || {};
      t.sector = info.category || null;
      t.tags = Array.isArray(info.tags) ? info.tags.slice(0,3) : [];
      const desc = stripHtml(info.description||'');
      t.blurb = desc ? truncate(desc, 160) : null;
      t.website = Array.isArray(urls.website) && urls.website[0] ? urls.website[0] : null;
      t.twitter = Array.isArray(urls.twitter) && urls.twitter[0] ? urls.twitter[0] : null;
      t.whitepaper = Array.isArray(urls.technical_doc) && urls.technical_doc[0] ? urls.technical_doc[0] : null;
      t.date_added = info.date_added || null;
      t.name = info.name || null;
      t.slug = info.slug || null;
    }else{
      t.sector=null; t.tags=[]; t.blurb=null; t.website=null; t.twitter=null; t.whitepaper=null; t.date_added=null; t.name=null; t.slug=null;
    }

    // placeholders
    t.rsi_d=null; t.rsi_h4=null;
    t.ath=null; t.ath_date=null;
    t.vol7_avg=null; t.vol30_avg=null; t.var_vol_7_over_30=null; t.vol7_tvl=null;
    t.ath_mc=null; t.x_ath_mc=null; t.price_target_ath_mc=null;
    t.tvl=null; t.tvl_source=null; t.mc_tvl=null;
    t.llama_category=null; t.llama_chains=[]; t.protocol_name=null;
  });

  /* 2) TVL (DeFiLlama) + meta fallback */
  for(const t of tokens){
    try{
      const { tvl, source, category, chains, protocol_name } = await matchTVLAndMeta(t.symbol_cmc);
      t.tvl = tvl; t.tvl_source = source;
      t.mc_tvl = (t.mc && t.tvl) ? (t.mc / t.tvl) : null;
      t.llama_category = category || null;
      t.llama_chains = chains || [];
      t.protocol_name = protocol_name || null;

      if(!t.sector && category) t.sector = category;
      if(!t.blurb && protocol_name){
        t.blurb = truncate(`${protocol_name} — protocole ${category || 'DeFi'} sur ${ (chains||[]).slice(0,2).join(', ') }`, 160);
      }
    }catch{ /* ignore */ }
    await sleep(120);
  }

  /* 3) Klines → RSI/ATH/vol7/30 */
  for(const t of tokens){
    try{
      const { venue:usedVenue, pair:usedPair, rows:daily } =
        await fetchKlinesDailyAny(t.venue, t.asset_pair);

      if(daily && daily.length){
        const closes = daily.map(r=>+r.c).filter(Number.isFinite);
        const vols   = daily.map(r=>+r.v).filter(Number.isFinite);

        // RSI D
        t.rsi_d = computeRSI(closes.slice(-200), 14);

        // ATH (prix + date)
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
          const h4closes = await fetchClosesH4Any(usedVenue || t.venue, usedPair || t.asset_pair);
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
      } else {
        console.warn(`ℹ️  Pas de klines pour ${t.symbol_cmc} (venue=${t.venue}, pair=${t.asset_pair})`);
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
    const baseHistory = await collectFromHistory();
    const baseAlertes = await collectFromAlertes();

    // fusion sans doublon (priorité history)
    const merged = [...baseHistory];
    const seen = new Set(baseHistory.map(x=>x.symbol_cmc));
    for(const t of baseAlertes){
      if(!seen.has(t.symbol_cmc)){
        merged.push(t);
        seen.add(t.symbol_cmc);
      }
    }

    // limiter le volume total si besoin
    const slice = merged.slice(0, MAX_TOKENS_PER_RUN);

    tokens = slice.map(x=>({
      symbol: x.symbol_cmc,
      symbol_cmc: x.symbol_cmc,
      venue: x.venue,
      asset_pair: x.asset_pair,
      alert_date: x.alert_date,
      source: x.source,

      // champs enrichis ensuite
      price:null, d24:null, d7:null, d30:null, d1y:null,
      mc:null, tvl:null, mc_tvl:null,
      volume_24h:null, vol_mc_24:null,
      vol7_avg:null, vol30_avg:null, vol7_tvl:null, var_vol_7_over_30:null,
      rsi_d:null, rsi_h4:null,
      ath:null, ath_date:null,
      ath_mc:null, x_ath_mc:null, price_target_ath_mc:null,
      circulating_supply:null, total_supply:null, max_supply:null, fdv:null, rank:null,

      // contexte
      sector:null, tags:[], blurb:null, website:null, twitter:null, whitepaper:null, date_added:null,
      llama_category:null, llama_chains:[], protocol_name:null, name:null, slug:null,

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
