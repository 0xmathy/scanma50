// fetch_and_build.js — Node 20
// Sheet (DateKey ≤2j + fallback 7j) → CMC (prix/MC/%) → DeFiLlama (TVL auto-match, chain-first)
// → CoinGecko (vols 7/30 + ATH) → data.json pour le front.

const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=916004394';
const OUTPUT = path.resolve('data.json');
const CMC_API_KEY = process.env.CMC_API_KEY || '';

const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

/* ---------- CoinGecko overrides (améliore la couverture ATH/volumes) ---------- */
const GECKO_OVERRIDES = {
  BTC:'bitcoin', ETH:'ethereum', BNB:'binancecoin', SOL:'solana', ADA:'cardano', XRP:'ripple',
  DOT:'polkadot', LINK:'chainlink', MATIC:'polygon', POL:'polygon', AVAX:'avalanche-2',
  OP:'optimism', ARB:'arbitrum', ATOM:'cosmos', ETC:'ethereum-classic', NEAR:'near',
  APT:'aptos', SUI:'sui', INJ:'injective-protocol', CRV:'curve-dao-token',
  SNX:'synthetix-network-token', LDO:'lido-dao', AAVE:'aave', UNI:'uniswap', MKR:'maker',
  RUNE:'thorchain', CAKE:'pancakeswap-token', GMX:'gmx', DYDX:'dydx-chain', STETH:'staked-ether',
  MORPHO:'morpho-token',
  ATH:'aethir' // ajuste/retire si ce n’est pas ton "ATH"
};

/* ---------- Aliases chaînes DeFiLlama (chain-first) ---------- */
const CHAIN_ALIASES = {
  ETH:'Ethereum', WETH:'Ethereum', STETH:'Ethereum',
  AVAX:'Avalanche', OP:'Optimism', ARB:'Arbitrum', BNB:'BSC',
  MATIC:'Polygon', POL:'Polygon', SOL:'Solana', ADA:'Cardano',
  DOT:'Polkadot', NEAR:'Near', APT:'Aptos', SUI:'Sui',
  ATOM:['Cosmos', 'Cosmos Hub', 'CosmosHub'],
  ETC:'Ethereum Classic'
};

/* ================= CSV & dates ================= */
function parseCSV(text){
  const rows=[]; let row=[], col='', inQ=false;
  for(let i=0;i<text.length;i++){
    const c=text[i], n=text[i+1];
    if(inQ){
      if(c=='"'&&n=='"'){ col+='"'; i++; }
      else if(c=='"'){ inQ=false; }
      else col+=c;
    }else{
      if(c=='"') inQ=true;
      else if(c==','){ row.push(col); col=''; }
      else if(c=='\n'){ row.push(col); rows.push(row); row=[]; col=''; }
      else if(c!='\r'){ col+=c; }
    }
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
  if(!base) return null;
  return { symbol: base, venue };
}
function isRecentDateKey(dateStr, days){
  if(!dateStr) return false;
  const iso = String(dateStr).trim().slice(0,10); // YYYY-MM-DD
  const d = new Date(iso + 'T00:00:00Z');
  if(isNaN(d)) return false;
  return (Date.now() - d.getTime())/86400000 <= days;
}

/* ================= HTTP helpers ================= */
async function fetchTextWithRetry(url){
  const ua = {'User-Agent':'Mozilla/5.0'};
  for(let i=0;i<3;i++){
    const u = url + (url.includes('?')?'&':'?') + 'rand=' + Date.now();
    try{
      const r = await fetch(u, { redirect:'follow', headers: ua });
      const t = await r.text();
      if(r.ok && t && t.length>0) return t;
      console.warn('CSV try', i+1, 'HTTP', r.status, r.statusText);
    }catch(e){ console.warn('CSV try', i+1, 'error:', e.message||e); }
    await new Promise(res=>setTimeout(res, 800*(i+1)));
  }
  throw new Error('CSV unreachable after retries');
}
async function fetchJSON(url, init){ const r=await fetch(url, init); const j=await r.json(); if(!r.ok){ console.error('HTTP', r.status, r.statusText, j); throw new Error('Fetch failed'); } return j; }

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
      if(n) tmp.push({ ...n, alert_date: String(d).slice(0,10) });
    }
    const m=new Map();
    for(const x of tmp){ const k=`${x.symbol}|${x.venue}`; if(!m.has(k)) m.set(k,x); }
    return Array.from(m.values());
  };
  let recent = collect(2);
  if(recent.length===0) recent = collect(7); // fallback si pas d’alertes < 48h
  return recent;
}

/* ================= 2) CoinMarketCap (prix/MC/%) ================= */
async function cmcQuotesBySymbol(symbols){
  if(!CMC_API_KEY) return {};
  const headers = { 'X-CMC_PRO_API_KEY': CMC_API_KEY };
  const uniq = Array.from(new Set(symbols));
  const chunks=[]; for(let i=0;i<uniq.length;i+=80) chunks.push(uniq.slice(i,i+80));
  const out={};
  for(const ch of chunks){
    const url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol=' + encodeURIComponent(ch.join(','));
    const data = await fetchJSON(url, { headers });
    for(const sym of Object.keys(data.data||{})){
      const arr = Array.isArray(data.data[sym]) ? data.data[sym] : [data.data[sym]];
      const best = arr.filter(x=>x.is_active!==0).sort((a,b)=>(a.cmc_rank||1e9)-(b.cmc_rank||1e9))[0] || arr[0];
      if(best) out[sym.toUpperCase()] = best;
    }
  }
  return out;
}

/* ================= 3) DeFiLlama AUTO-MATCH (TVL) ================= */
let LLAMA_PROTOCOLS = null;
let LLAMA_CHAINS = null;
const norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,'');

async function loadLlamaCatalogs(){
  if(!LLAMA_CHAINS)    LLAMA_CHAINS    = await fetchJSON('https://api.llama.fi/chains');
  if(!LLAMA_PROTOCOLS) LLAMA_PROTOCOLS = await fetchJSON('https://api.llama.fi/protocols');
}
function pickChainForSymbol(symbol){
  if(!Array.isArray(LLAMA_CHAINS)) return null;
  const alias = CHAIN_ALIASES[symbol] || symbol;
  const candidates = Array.isArray(alias) ? alias : [alias];
  // exact
  for(const cand of candidates){
    const c = LLAMA_CHAINS.find(ch => (ch.name||'').toLowerCase() === cand.toLowerCase());
    if(c) return c;
  }
  // contains
  for(const cand of candidates){
    const c = LLAMA_CHAINS.find(ch => (ch.name||'').toLowerCase().includes(cand.toLowerCase()));
    if(c) return c;
  }
  return null;
}
function pickProtocolForSymbol(symbol){
  if(!Array.isArray(LLAMA_PROTOCOLS)) return null;
  const SYM = symbol.toUpperCase();
  // 1) symbol exact
  let candidates = LLAMA_PROTOCOLS.filter(p => (p.symbol||'').toUpperCase() === SYM);
  // 2) fallback: name ~ symbol
  if(candidates.length===0){
    const ns = norm(symbol);
    candidates = LLAMA_PROTOCOLS.filter(p => norm(p.name).includes(ns));
  }
  if(candidates.length===0) return null;
  candidates.sort((a,b)=> (b.tvl||0) - (a.tvl||0)); // plus gros TVL
  return candidates[0];
}
async function getTVLForSymbol(symbol){
  await loadLlamaCatalogs();
  // CHAÎNE d'abord (évite ATOM → hAtom Lending)
  const chain = pickChainForSymbol(symbol);
  if(chain && typeof chain.tvl === 'number'){
    return { tvl: chain.tvl, via: `chain:${chain.name}` };
  }
  // sinon protocole
  const proto = pickProtocolForSymbol(symbol);
  if(proto && proto.slug){
    try{
      const n = await fetchJSON('https://api.llama.fi/tvl/' + encodeURIComponent(proto.slug));
      if(typeof n === 'number') return { tvl: n, via: `protocol:${proto.slug}` };
    }catch(e){ /* ignore */ }
  }
  return { tvl: null, via: null };
}
async function enrichWithAutoTVL(rows){
  for(const r of rows){
    const { tvl, via } = await getTVLForSymbol(r.symbol);
    if(typeof tvl === 'number'){
      r.tvl = tvl;
      if(r.mc) r.mc_tvl = r.mc / tvl;
    }
    r.tvl_source = via;
  }
}

/* ================= 4) CoinGecko (volumes 7/30 & ATH) ================= */
async function geckoFindId(symbol){
  if(GECKO_OVERRIDES[symbol]) return GECKO_OVERRIDES[symbol];
  try{
    const j = await fetchJSON('https://api.coingecko.com/api/v3/search?query=' + encodeURIComponent(symbol));
    const hits = (j?.coins||[]).filter(c => (c.symbol||'').toUpperCase() === symbol.toUpperCase());
    if(!hits.length) return null;
    hits.sort((a,b)=> (a.market_cap_rank||1e9) - (b.market_cap_rank||1e9));
    return hits[0].id || null;
  }catch(e){ return null; }
}
async function geckoMarketChart30d(id){
  try{
    const j = await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=30&interval=daily`);
    const vols = (j?.total_volumes||[]).map(v=>+v[1]).filter(n=>Number.isFinite(n));
    return vols;
  }catch(e){ return []; }
}
const avg = arr => (!arr.length ? null : arr.reduce((a,b)=>a+b,0)/arr.length);
async function enrichWithGecko(rows){
  // map symbol -> id
  const idsBySym={};
  for(const r of rows){ idsBySym[r.symbol] = await geckoFindId(r.symbol); }

  // markets -> ATH & ath_mult
  const idList = Object.values(idsBySym).filter(Boolean);
  for(let i=0;i<idList.length;i+=200){
    const chunk = idList.slice(i,i+200);
    try{
      const arr = await fetchJSON(
        'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids='
        + chunk.join(',') + '&per_page=250&page=1'
      );
      for(const it of (arr||[])){
        const sym = Object.keys(idsBySym).find(k => idsBySym[k] === it.id);
        const row = rows.find(r => r.symbol === sym);
        if(row){
          row.ath = (typeof it.ath === 'number') ? it.ath : null;
          row.ath_mult = (row.price && row.price>0 && typeof it.ath==='number') ? (it.ath / row.price) : null;
        }
      }
    }catch(e){ /* ignore */ }
  }

  // volumes 7/30
  for(const r of rows){
    const id = idsBySym[r.symbol]; if(!id) continue;
    const vols = await geckoMarketChart30d(id);
    const v30 = avg(vols.slice(-30));
    const v7  = avg(vols.slice(-7));
    r.vol7_avg = v7;
    r.vol30_avg = v30;
    r.var_vol_7_over_30 = (v7 && v30) ? (v7 / v30) : null;   // ex: 1.25×
    r.vol7_mc  = (v7 && r.mc)  ? (v7 / r.mc * 100)  : null;  // %
    r.vol7_tvl = (v7 && r.tvl) ? (v7 / r.tvl * 100) : null;  // %
  }
}

/* ================= build helpers ================= */
function baseRow(it, q){
  const usd = q?.quote?.USD;
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

    vol_mc_24: (usd?.volume_24h && usd?.market_cap) ? (usd.volume_24h / usd.market_cap * 100) : null,

    // remplis après Gecko
    vol7_avg: null,
    vol30_avg: null,
    vol7_mc: null,
    vol7_tvl: null,
    var_vol_7_over_30: null,

    rsi_d: null,
    rsi_h4: null,

    ath: null,
    ath_mult: null,

    circulating_supply: q?.circulating_supply ?? null,
    total_supply:       q?.total_supply ?? null,
    max_supply:         q?.max_supply ?? null,
    fdv:                usd?.fully_diluted_market_cap ?? null,
    rank:               q?.cmc_rank ?? null
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
    let cmc = {};
    if(syms.length){
      try{ cmc = await cmcQuotesBySymbol(syms); }
      catch(e){ console.warn('⚠️ CMC off:', e.message||e); }
    }
    const rows = recent.map(it => baseRow(it, cmc[it.symbol]));

    // TVL auto-match (protocols/chains) — chaîne prioritaire
    await enrichWithAutoTVL(rows);

    // Volumes 7/30 + ATH
    await enrichWithGecko(rows);

    // MC/TVL a déjà été rempli lors du TVL si MC dispo; re-check au cas où
    for(const r of rows){
      if(r.tvl && r.mc && !r.mc_tvl) r.mc_tvl = r.mc / r.tvl;
      // Potentiel ATH = ath / price déjà rempli (ath_mult)
    }

    writeDataJSON(rows);
  }catch(e){
    console.error('❌ Erreur (JSON vide):', e.message||e);
    writeDataJSON([]);
  }
})();
