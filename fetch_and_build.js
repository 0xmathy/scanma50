// fetch_and_build.js — Node 20
// Sheet (DateKey ≤2j + fallback 7j) → CMC (prix/MC/%) → DeFiLlama (TVL auto-match, chain-first)
// → CoinGecko (ATH + date, volumes 30j, RSI Daily 14, RSI H4 14)
// → Fallback OHLC (Binance/MEXC/CryptoCompare) pour remplir ATH/RSI en dernier ressort → data.json

const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=916004394';
const OUTPUT = path.resolve('data.json');

const CMC_API_KEY = process.env.CMC_API_KEY || '';

const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

/* ---------- CoinGecko overrides ---------- */
const GECKO_OVERRIDES = {
  BTC:'bitcoin', ETH:'ethereum', BNB:'binancecoin', SOL:'solana', ADA:'cardano', XRP:'ripple',
  DOT:'polkadot', LINK:'chainlink', MATIC:'polygon', POL:'polygon', AVAX:'avalanche-2',
  OP:'optimism', ARB:'arbitrum', ATOM:'cosmos', ETC:'ethereum-classic', NEAR:'near',
  APT:'aptos', SUI:'sui', INJ:'injective-protocol', CRV:'curve-dao-token',
  SNX:'synthetix-network-token', LDO:'lido-dao', AAVE:'aave', UNI:'uniswap', MKR:'maker',
  RUNE:'thorchain', CAKE:'pancakeswap-token', GMX:'gmx', DYDX:'dydx-chain', STETH:'staked-ether',
  MORPHO:'morpho-token',
  ATH:'aethir' // ajuste si besoin
};

/* ---------- Aliases DeFiLlama (favorise les chains) ---------- */
const CHAIN_ALIASES = {
  ETH:'Ethereum', WETH:'Ethereum', STETH:'Ethereum',
  AVAX:'Avalanche', OP:'Optimism', ARB:'Arbitrum', BNB:'BSC',
  MATIC:'Polygon', POL:'Polygon', SOL:'Solana', ADA:'Cardano',
  DOT:'Polkadot', NEAR:'Near', APT:'Aptos', SUI:'Sui',
  ATOM:['Cosmos','Cosmos Hub','CosmosHub'],
  ETC:'Ethereum Classic'
};

/* ================= Utils ================= */
function parseCSV(text){
  const rows=[]; let row=[], col='', inQ=false;
  for(let i=0;i<text.length;i++){
    const c=text[i], n=text[i+1];
    if(inQ){ if(c=='"'&&n=='"'){ col+='"'; i++; } else if(c=='"'){ inQ=false; } else col+=c; }
    else { if(c=='"') inQ=true;
      else if(c==','){ row.push(col); col=''; }
      else if(c=='\n'){ row.push(col); rows.push(row); row=[]; col=''; }
      else if(c!='\r'){ col+=c; } }
  }
  row.push(col); rows.push(row);
  return rows.filter(r=>r.length>1 || (r.length===1 && r[0] !== ''));
}
function normalizeAsset(raw){
  if(!raw) return null;
  let s=String(raw).trim().replace(/[\[\]]/g,'').trim();
  s = s.split(/\s+/)[0];
  let venue='', base=s;
  if(s.includes(':')){ const p=s.split(':'); if(p.length>=2){ venue=(p[0]||'').toUpperCase(); base=p[1]; } }
  const segs=base.split(SEP_RE).filter(Boolean);
  base = segs[segs.length-1] || base;
  if(/^0x[0-9a-fA-F]{4,}$/.test(base)) return { symbol: base.toUpperCase(), venue };
  base = base.toUpperCase().replace(/(PERP|\d+L|\d+S)$/,'');
  for(const q of QUOTES){ if(base.endsWith(q)){ base = base.slice(0, -q.length); break; } }
  base = base.replace(/[^A-Z0-9]/g,'').trim();
  return base ? { symbol: base, venue } : null;
}
function isRecentDateKey(dateStr, days){
  if(!dateStr) return false;
  const iso = String(dateStr).trim().slice(0,10);
  const d = new Date(iso+'T00:00:00Z');
  if(isNaN(d)) return false;
  return (Date.now()-d.getTime())/86400000 <= days;
}
async function fetchTextWithRetry(url){
  const ua={'User-Agent':'Mozilla/5.0'};
  for(let i=0;i<3;i++){
    const u=url+(url.includes('?')?'&':'?')+'rand='+Date.now();
    try{
      const r=await fetch(u,{headers:ua,redirect:'follow'});
      const t=await r.text();
      if(r.ok && t && t.length>0) return t;
    }catch(e){}
    await new Promise(res=>setTimeout(res, 800*(i+1)));
  }
  throw new Error('CSV unreachable');
}
async function fetchJSON(url, init){ const r=await fetch(url,init); const j=await r.json().catch(()=> ({})); if(!r.ok) throw new Error(`HTTP ${r.status}`); return j; }
const avg = a => (!a||!a.length) ? null : a.reduce((x,y)=>x+y,0)/a.length;
const norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,'');

/* ================= 1) Sheet → {symbol, alert_date} ================= */
async function readRecentFromSheet(){
  const csv = await fetchTextWithRetry(SHEET_CSV_URL);
  const rows = parseCSV(csv);
  if(rows.length<2) return [];
  const header = rows[0].map(h=>String(h).trim());
  const idxAsset   = header.map(h=>h.toLowerCase()).indexOf('asset');
  const idxDateKey = header.indexOf('DateKey');
  if(idxAsset===-1) throw new Error('Colonne "asset" introuvable');
  if(idxDateKey===-1) throw new Error('Colonne "DateKey" introuvable');

  const collect=(days)=>{
    const tmp=[];
    for(let i=1;i<rows.length;i++){
      const a=rows[i][idxAsset], d=rows[i][idxDateKey];
      if(!a) continue;
      if(!isRecentDateKey(d, days)) continue;
      const n=normalizeAsset(a);
      if(n) tmp.push({ ...n, alert_date:String(d).slice(0,10) });
    }
    const m=new Map();
    for(const x of tmp){ const k=`${x.symbol}|${x.venue}`; if(!m.has(k)) m.set(k,x); }
    return Array.from(m.values());
  };
  let recent = collect(2);
  if(recent.length===0) recent = collect(7);
  return recent;
}

/* ================= 2) CMC quotes + info ================= */
async function cmcQuotesBySymbol(symbols){
  if(!CMC_API_KEY) return {};
  const headers = { 'X-CMC_PRO_API_KEY': CMC_API_KEY };
  const uniq = [...new Set(symbols)];
  const out={};
  for(let i=0;i<uniq.length;i+=80){
    const chunk=uniq.slice(i,i+80);
    const url='https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol='+encodeURIComponent(chunk.join(','));
    const data=await fetchJSON(url,{headers});
    for(const sym of Object.keys(data.data||{})){
      const arr = Array.isArray(data.data[sym]) ? data.data[sym] : [data.data[sym]];
      const best = arr.filter(x=>x.is_active!==0).sort((a,b)=>(a.cmc_rank||1e9)-(b.cmc_rank||1e9))[0] || arr[0];
      if(best) out[sym.toUpperCase()] = best;
    }
  }
  return out;
}
async function cmcInfoBySymbol(symbols){
  if(!CMC_API_KEY) return {};
  const headers = { 'X-CMC_PRO_API_KEY': CMC_API_KEY };
  const uniq = [...new Set(symbols)];
  const out={};
  for(let i=0;i<uniq.length;i+=200){
    const chunk=uniq.slice(i,i+200);
    const url='https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?symbol='+encodeURIComponent(chunk.join(','));
    const data=await fetchJSON(url,{headers});
    for(const sym of Object.keys(data.data||{})){
      const arr = Array.isArray(data.data[sym]) ? data.data[sym] : [data.data[sym]];
      const best = arr.sort((a,b)=>(a.cmc_rank||1e9)-(b.cmc_rank||1e9))[0] || arr[0];
      if(best) out[sym.toUpperCase()] = best;
    }
  }
  return out;
}

/* ================= 3) DeFiLlama (TVL auto-match) ================= */
let LLAMA_PROTOCOLS=null, LLAMA_CHAINS=null;
async function loadLlamaCatalogs(){
  if(!LLAMA_CHAINS)    LLAMA_CHAINS    = await fetchJSON('https://api.llama.fi/chains');
  if(!LLAMA_PROTOCOLS) LLAMA_PROTOCOLS = await fetchJSON('https://api.llama.fi/protocols');
}
function pickChainForSymbol(symbol){
  if(!Array.isArray(LLAMA_CHAINS)) return null;
  const alias = CHAIN_ALIASES[symbol] || symbol;
  const candidates = Array.isArray(alias)?alias:[alias];
  for(const cand of candidates){
    const c = LLAMA_CHAINS.find(ch => (ch.name||'').toLowerCase() === cand.toLowerCase());
    if(c) return c;
  }
  for(const cand of candidates){
    const c = LLAMA_CHAINS.find(ch => (ch.name||'').toLowerCase().includes(cand.toLowerCase()));
    if(c) return c;
  }
  return null;
}
function pickProtocolForSymbol(symbol){
  if(!Array.isArray(LLAMA_PROTOCOLS)) return null;
  const SYM = symbol.toUpperCase();
  let candidates = LLAMA_PROTOCOLS.filter(p => (p.symbol||'').toUpperCase() === SYM);
  if(candidates.length===0){
    const ns = norm(symbol);
    candidates = LLAMA_PROTOCOLS.filter(p => norm(p.name).includes(ns));
  }
  if(!candidates.length) return null;
  candidates.sort((a,b)=> (b.tvl||0) - (a.tvl||0));
  return candidates[0];
}
async function getTVLForSymbol(symbol){
  await loadLlamaCatalogs();
  const chain = pickChainForSymbol(symbol);
  if(chain && typeof chain.tvl === 'number'){
    return { tvl: chain.tvl, via: `chain:${chain.name}` };
  }
  const proto = pickProtocolForSymbol(symbol);
  if(proto && proto.slug){
    try{
      const n = await fetchJSON('https://api.llama.fi/tvl/'+encodeURIComponent(proto.slug));
      if(typeof n === 'number') return { tvl: n, via: `protocol:${proto.slug}` };
    }catch(e){}
  }
  return { tvl: null, via: null };
}

/* ================= 4) CoinGecko (ATH, vols, RSI D & H4) ================= */
async function geckoFindId(symbol, cmcInfo){
  if(GECKO_OVERRIDES[symbol]) return GECKO_OVERRIDES[symbol];

  try{
    const j1 = await fetchJSON('https://api.coingecko.com/api/v3/search?query='+encodeURIComponent(symbol));
    const hits1 = (j1?.coins||[]).filter(c => (c.symbol||'').toUpperCase()===symbol.toUpperCase());
    if(hits1.length){
      hits1.sort((a,b)=>(a.market_cap_rank||1e9)-(b.market_cap_rank||1e9));
      return hits1[0].id;
    }
  }catch(e){}

  const name = (cmcInfo?.name || cmcInfo?.slug || '').toString().trim();
  if(name){
    try{
      const j2 = await fetchJSON('https://api.coingecko.com/api/v3/search?query='+encodeURIComponent(name));
      const hits2 = (j2?.coins||[]);
      if(hits2.length){
        hits2.sort((a,b)=>(a.market_cap_rank||1e9)-(b.market_cap_rank||1e9));
        return hits2[0].id;
      }
    }catch(e){}
    try{
      const j3 = await fetchJSON('https://api.coingecko.com/api/v3/search?query='+encodeURIComponent(name.replace(/\s+/g,'-').toLowerCase()));
      const hits3 = (j3?.coins||[]);
      if(hits3.length){
        hits3.sort((a,b)=>(a.market_cap_rank||1e9)-(b.market_cap_rank||1e9));
        return hits3[0].id;
      }
    }catch(e){}
  }
  return null;
}
async function geckoDetails(id){
  try{
    const j = await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}?localization=false&tickers=false&market_data=true&community_data=true&developer_data=false&sparkline=false`);
    const md = j?.market_data, comm = j?.community_data;
    return {
      ath: md?.ath?.usd ?? null,
      ath_date: md?.ath_date?.usd ?? null,
      genesis_date: j?.genesis_date ?? null,
      twitter_followers: comm?.twitter_followers ?? null
    };
  }catch(e){ return {}; }
}
async function geckoMarketChart(id, days, interval='daily'){
  try{
    const j = await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=${days}&interval=${interval}`);
    return {
      vols: (j?.total_volumes||[]).map(v=>+v[1]).filter(Number.isFinite),
      closes: (j?.prices||[]).map(p=>+p[1]).filter(Number.isFinite)
    };
  }catch(e){ return { vols:[], closes:[] }; }
}
async function geckoMarketChartHourly(id, days=7){
  try{
    const j=await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=${days}&interval=hourly`);
    const closes=(j?.prices||[]).map(p=>+p[1]).filter(Number.isFinite);
    return closes;
  }catch(e){ return []; }
}
function to4hCloses(hourlyCloses){
  if(!hourlyCloses || hourlyCloses.length < 4) return [];
  const out=[]; for(let i=3;i<hourlyCloses.length;i+=4) out.push(hourlyCloses[i]); return out;
}
function rsi14FromCloses(closes){
  const n=14;
  if(!closes || closes.length < n+1) return null;
  const gains=[], losses=[];
  for(let i=1;i<=n;i++){
    const diff = closes[i]-closes[i-1];
    gains.push(Math.max(diff,0));
    losses.push(Math.max(-diff,0));
  }
  let avgGain = gains.reduce((a,b)=>a+b,0)/n;
  let avgLoss = losses.reduce((a,b)=>a+b,0)/n;
  for(let i=n+1;i<closes.length;i++){
    const diff = closes[i]-closes[i-1];
    const gain = Math.max(diff,0), loss = Math.max(-diff,0);
    avgGain = (avgGain*(n-1)+gain)/n;
    avgLoss = (avgLoss*(n-1)+loss)/n;
  }
  if(avgLoss === 0) return 100;
  const rs = avgGain/avgLoss;
  return 100 - 100/(1+rs);
}

/* ========= Fallback OHLC (Binance/MEXC/CryptoCompare) pour ATH/RSI ========= */
async function binanceKlines(symbol, interval='1d', limit=1000){
  const pairs = [`${symbol}USDT`, `${symbol}USD`];
  for(const p of pairs){
    try{
      const j = await fetchJSON(`https://api.binance.com/api/v3/klines?symbol=${p}&interval=${interval}&limit=${limit}`);
      if(Array.isArray(j) && j.length){
        return j.map(k => +k[4]).filter(Number.isFinite);
      }
    }catch(e){}
  }
  return [];
}
async function mexcKlines(symbol, interval='1d', limit=1000){
  const pairs = [`${symbol}USDT`, `${symbol}USD`];
  for(const p of pairs){
    try{
      const j = await fetchJSON(`https://api.mexc.com/api/v3/klines?symbol=${p}&interval=${interval}&limit=${limit}`);
      if(Array.isArray(j) && j.length){
        return j.map(k => +k[4]).filter(Number.isFinite);
      }
    }catch(e){}
  }
  return [];
}
async function cryptoCompareDaily(symbol, tsym='USD', limit=2000){
  try{
    const j = await fetchJSON(`https://min-api.cryptocompare.com/data/v2/histoday?fsym=${encodeURIComponent(symbol)}&tsym=${tsym}&limit=${limit}`);
    return (j?.Data?.Data||[]).map(x => +x.close).filter(Number.isFinite);
  }catch(e){ return []; }
}
async function cryptoCompareHourly(symbol, tsym='USD', limit=500){
  try{
    const j = await fetchJSON(`https://min-api.cryptocompare.com/data/v2/histohour?fsym=${encodeURIComponent(symbol)}&tsym=${tsym}&limit=${limit}`);
    return (j?.Data?.Data||[]).map(x => +x.close).filter(Number.isFinite);
  }catch(e){ return []; }
}
function to4hFromHourly(hourlyCloses){
  if(!hourlyCloses || hourlyCloses.length < 4) return [];
  const out=[]; for(let i=3;i<hourlyCloses.length;i+=4) out.push(hourlyCloses[i]); return out;
}
async function getCandlesFallback(symbol, venue){
  const VEN = (venue||'').toUpperCase();
  let daily=[], hourly=[];
  if(VEN.includes('BINANCE')){
    daily  = await binanceKlines(symbol, '1d', 1000);
    hourly = await binanceKlines(symbol, '1h',  500);
  }else if(VEN.includes('MEXC')){
    daily  = await mexcKlines(symbol, '1d', 1000);
    hourly = await mexcKlines(symbol, '1h',  500);
  }
  if(daily.length < 20)  daily  = await cryptoCompareDaily(symbol, 'USD', 2000);
  if(hourly.length < 40) hourly = await cryptoCompareHourly(symbol, 'USD', 500);
  return { daily, closes4h: to4hFromHourly(hourly) };
}

/* ================= build helpers ================= */
function baseRow(it, q, info){
  const usd = q?.quote?.USD;
  const urls = info?.urls || {};
  const website    = (urls.website||[])[0] || null;
  const whitepaper = (urls.technical_doc||[])[0] || null;
  const twitter    = (urls.twitter||[])[0] || (info?.twitter_username ? `https://twitter.com/${info.twitter_username}` : null);
  const github     = (urls.source_code||[])[0] || null;
  const tags = Array.isArray(info?.tags) ? info.tags.slice(0,5) : [];

  return {
    symbol: it.symbol,
    venue: it.venue || "",
    alert_date: it.alert_date || null,

    price: usd?.price ?? null,
    d24: usd?.percent_change_24h ?? null,
    d7:  usd?.percent_change_7d ?? null,
    d30: usd?.percent_change_30d ?? null,

    mc: usd?.market_cap ?? null,
    tvl: null,
    mc_tvl: null,

    vol_mc_24: (usd?.volume_24h && usd?.market_cap) ? (usd.volume_24h/usd.market_cap*100) : null,

    vol7_avg: null,
    vol30_avg: null,
    vol7_mc: null,
    vol7_tvl: null,
    var_vol_7_over_30: null,

    rsi_d: null,
    rsi_h4: null,

    ath: null,
    ath_date: null,
    ath_mult: null,

    narrative: tags.length ? tags.join(', ') : null,
    launch_date: info?.date_added || null,

    twitter_followers: null,
    website, whitepaper, github, twitter,

    circulating_supply: q?.circulating_supply ?? null,
    total_supply:       q?.total_supply ?? null,
    max_supply:         q?.max_supply ?? null,
    fdv:                usd?.fully_diluted_market_cap ?? null,
    rank:               q?.cmc_rank ?? null,

    tvl_source: null,
    gecko_id: null
  };
}
function writeDataJSON(rows){
  const out = { updated_at: new Date().toISOString(), tokens: rows };
  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens.`);
}

/* ================= MAIN ================= */
(async()=>{
  try{
    const recent = await readRecentFromSheet();
    const syms = recent.map(x=>x.symbol);

    let cmcQ={}, cmcI={};
    if(syms.length){
      try{ cmcQ = await cmcQuotesBySymbol(syms); } catch(e){ console.warn('CMC quotes:', e.message); }
      try{ cmcI = await cmcInfoBySymbol(syms);   } catch(e){ console.warn('CMC info:', e.message); }
    }

    const rows = recent.map(it => baseRow(it, cmcQ[it.symbol], cmcI[it.symbol]));

    // TVL
    for(const r of rows){
      try{
        const { tvl, via } = await getTVLForSymbol(r.symbol);
        if(typeof tvl === 'number'){ r.tvl = tvl; if(r.mc) r.mc_tvl = r.mc / tvl; }
        r.tvl_source = via;
      }catch(e){}
    }

    // CoinGecko enrich
    for(const r of rows){
      try{
        const id = await geckoFindId(r.symbol, cmcI[r.symbol]);
        r.gecko_id = id || null;
        if(!id) continue;

        const det = await geckoDetails(id);
        if(typeof det.ath === 'number'){ r.ath = det.ath; if(r.price && r.price>0) r.ath_mult = det.ath / r.price; }
        if(det.ath_date) r.ath_date = det.ath_date;
        if(det.twitter_followers != null) r.twitter_followers = det.twitter_followers;
        if(det.genesis_date && !r.launch_date) r.launch_date = det.genesis_date;

        const mc = await geckoMarketChart(id, 30, 'daily');
        const v7 = avg(mc.vols.slice(-7)), v30 = avg(mc.vols.slice(-30));
        if(v7!=null) r.vol7_avg = v7;
        if(v30!=null) r.vol30_avg = v30;
        if(v7 && v30) r.var_vol_7_over_30 = v7 / v30;
        if(r.mc && v7)  r.vol7_mc  = v7 / r.mc * 100;
        if(r.tvl && v7) r.vol7_tvl = v7 / r.tvl * 100;

        const closesD = mc.closes.length ? mc.closes : (await geckoMarketChart(id, 120, 'daily')).closes;
        const rsiD = rsi14FromCloses(closesD);
        if(rsiD!=null) r.rsi_d = rsiD;

        const closesH = await geckoMarketChartHourly(id, 7);
        const c4h = to4hCloses(closesH);
        const rsiH4 = rsi14FromCloses(c4h);
        if(rsiH4!=null) r.rsi_h4 = rsiH4;
      }catch(e){}
    }

    // ===== Fallback OHLC pour compléter si Gecko incomplet =====
    for(const r of rows){
      if((r.ath == null || r.rsi_d == null || r.rsi_h4 == null)){
        try{
          const { daily, closes4h } = await getCandlesFallback(r.symbol, r.venue);

          if(r.ath == null && daily.length){
            const athLocal = Math.max(...daily);
            if(Number.isFinite(athLocal)){
              r.ath = athLocal;
              if(r.price && r.price > 0) r.ath_mult = r.ath / r.price;
            }
          }
          if(r.rsi_d == null && daily.length){
            const rsiD = rsi14FromCloses(daily.slice(-200));
            if(rsiD != null) r.rsi_d = rsiD;
          }
          if(r.rsi_h4 == null && closes4h.length){
            const rsiH4 = rsi14FromCloses(closes4h.slice(-200));
            if(rsiH4 != null) r.rsi_h4 = rsiH4;
          }
        }catch(e){}
      }
    }

    for(const r of rows){
      if(r.tvl && r.mc && !r.mc_tvl) r.mc_tvl = r.mc / r.tvl;
    }

    writeDataJSON(rows);
  }catch(e){
    console.error('❌ Erreur (JSON vide):', e.message||e);
    writeDataJSON([]);
  }
})();
