// fetch_and_build.js — Node 20, CSV export + CMC + DeFiLlama TVL + alert_date
const fs = require('fs');
const path = require('path');

// --- CONFIG ---
const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=916004394';
const OUTPUT = path.resolve('data.json');
const CMC_API_KEY = process.env.CMC_API_KEY || '';

// --- Normalisation tickers ---
const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

// --- Mappings TVL (à compléter au fil du temps) ---
const TVL_MAP = {
  // Protocoles (slug DeFiLlama)
  CRV: { type: 'protocol', slug: 'curve' },
  SNX: { type: 'protocol', slug: 'synthetix' },
  MORPHO: { type: 'protocol', slug: 'morpho' },

  // Chaînes (nom DeFiLlama)
  AVAX: { type: 'chain', name: 'Avalanche' },
  OP:   { type: 'chain', name: 'Optimism' },
  ETC:  { type: 'chain', name: 'Ethereum Classic' },
};

// ---------- Utils CSV ----------
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
function uniqBy(arr, keyFn){ const s=new Set(), out=[]; for(const it of arr){ const k=keyFn(it); if(k && !s.has(k)){ s.add(k); out.push(it); } } return out; }

// ---------- Dates (≤ 2 jours + fallback 7 jours) ----------
function isRecentDateKey(dateStr, days){
  if(!dateStr) return false;
  const iso = String(dateStr).trim().slice(0,10); // YYYY-MM-DD
  const d = new Date(iso + 'T00:00:00Z'); // évite TZ
  if(isNaN(d)) return false;
  const diffDays = (Date.now() - d.getTime())/(1000*60*60*24);
  return diffDays <= days;
}

// ---------- Fetch helpers ----------
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

// ---------- 1) Lire le Sheet (≤ 2 jours; fallback 7 jours) ----------
async function readRecentFromSheet(){
  const csv = await fetchTextWithRetry(SHEET_CSV_URL);
  const rows = parseCSV(csv);
  if(rows.length<2) return [];

  const header = rows[0].map(h=>String(h).trim());
  const headerLower = header.map(h=>h.toLowerCase());

  const idxAsset   = headerLower.indexOf('asset');
  const idxDateKey = header.indexOf('DateKey'); // casse exacte
  if(idxAsset===-1) throw new Error('Colonne "asset" introuvable');
  if(idxDateKey===-1) throw new Error('Colonne "DateKey" introuvable');

  const collect = (days)=> {
    const out=[];
    for(let i=1;i<rows.length;i++){
      const asset = rows[i][idxAsset];
      const dateK = rows[i][idxDateKey];
      if(!asset) continue;
      if(!isRecentDateKey(dateK, days)) continue;
      const norm = normalizeAsset(asset);
      if(norm) out.push({ ...norm, alert_date: String(dateK).slice(0,10) });
    }
    // dédupe symbol|venue
    const map = new Map();
    for(const x of out){ const k=`${x.symbol}|${x.venue}`; if(!map.has(k)) map.set(k,x); }
    return Array.from(map.values());
  };

  let picked = collect(2);             // fenêtre stricte
  if(picked.length === 0) picked = collect(7);  // fallback doux
  return picked;
}

// ---------- 2) CMC quotes (par symbol) ----------
async function cmcQuotesBySymbol(symbols){
  if(!CMC_API_KEY){ console.warn('⚠️  Pas de CMC_API_KEY, métriques null'); return {}; }
  const headers = { 'X-CMC_PRO_API_KEY': CMC_API_KEY };
  const unique = Array.from(new Set(symbols));
  const chunks=[]; for(let i=0;i<unique.length;i+=80) chunks.push(unique.slice(i,i+80));

  const results = {}; // {SYM: bestEntry}
  for(const chunk of chunks){
    const url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol=' + encodeURIComponent(chunk.join(','));
    const data = await fetchJSON(url, { headers });
    if(!data || !data.data) continue;

    for(const sym of Object.keys(data.data)){
      const arr = Array.isArray(data.data[sym]) ? data.data[sym] : [data.data[sym]];
      const best = arr
        .filter(x => x.is_active !== 0)
        .sort((a,b)=> (a.cmc_rank||1e9) - (b.cmc_rank||1e9))[0] || arr[0];
      if(best) results[sym.toUpperCase()] = best;
    }
  }
  return results;
}

// ---------- 3) DeFiLlama TVL ----------
async function fetchChainsTVL(){
  // Liste des chaînes avec leur TVL courant
  const data = await fetchJSON('https://api.llama.fi/chains');
  // data: [{name: 'Ethereum', tvl: 1.2e11}, ...]
  const map = {};
  if(Array.isArray(data)){
    for(const c of data){ if(c && c.name) map[c.name] = c.tvl ?? null; }
  }
  return map;
}
async function fetchProtocolTVL(slug){
  // TVL numérique d'un protocole
  const n = await fetchJSON(`https://api.llama.fi/tvl/${encodeURIComponent(slug)}`);
  return (typeof n === 'number') ? n : null;
}

// ---------- 4) Construire JSON ----------
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
    vol7_mc: null,
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

async function enrichWithTVL(rows){
  // Prépare les jeux à interroger
  const needChains = new Set();
  const needProtocols = new Set();
  for(const r of rows){
    const m = TVL_MAP[r.symbol];
    if(!m) continue;
    if(m.type === 'chain') needChains.add(m.name);
    if(m.type === 'protocol') needProtocols.add(m.slug);
  }

  // Récup chaînes une fois
  let chainTVL = {};
  if(needChains.size){
    try{ chainTVL = await fetchChainsTVL(); }
    catch(e){ console.warn('TVL chains error', e.message||e); }
  }

  // Protocoles (un par un, ils sont peu nombreux en pratique)
  const protoCache = {};
  for(const slug of needProtocols){
    try{ protoCache[slug] = await fetchProtocolTVL(slug); }
    catch(e){ console.warn('TVL protocol error', slug, e.message||e); }
  }

  // Appliquer aux lignes
  for(const r of rows){
    const m = TVL_MAP[r.symbol];
    if(!m) continue;
    if(m.type === 'chain'){
      const tvl = chainTVL[m.name];
      if(typeof tvl === 'number'){ r.tvl = tvl; if(r.mc) r.mc_tvl = r.mc / tvl; }
    } else if(m.type === 'protocol'){
      const tvl = protoCache[m.slug];
      if(typeof tvl === 'number'){ r.tvl = tvl; if(r.mc) r.mc_tvl = r.mc / tvl; }
    }
  }
}

// ---------- MAIN ----------
async function writeDataJSON(rows){
  const out = { updated_at: new Date().toISOString(), tokens: rows };
  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens.`);
}

(async()=>{
  try{
    const recent = await readRecentFromSheet(); // {symbol, venue, alert_date}
    const syms = recent.map(x=>x.symbol);
    let cmcBySym = {};
    if(syms.length){
      try{ cmcBySym = await cmcQuotesBySymbol(syms); }
      catch(e){ console.warn('⚠️  CMC indisponible :', e.message||e); }
    }
    // construire lignes de base
    const rows = recent.map(it => baseRow(it, cmcBySym[it.symbol]));
    // enrichir TVL
    await enrichWithTVL(rows);
    await writeDataJSON(rows);
  }catch(e){
    console.error('❌ Erreur (fallback JSON vide):', e.message || e);
    await writeDataJSON([]);
  }
})();
