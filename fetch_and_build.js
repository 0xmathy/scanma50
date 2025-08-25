// fetch_and_build.js — Node 20, GoogleSheet + CMC quotes
const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv';
const OUTPUT = path.resolve('data.json');
const CMC_API_KEY = process.env.CMC_API_KEY || '';

const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

function parseCSV(text) {
  const rows = []; let row=[], col='', inQ=false;
  for (let i=0;i<text.length;i++){
    const c=text[i], n=text[i+1];
    if (inQ) { if (c === '"' && n === '"'){col+='"';i++;} else if (c === '"'){inQ=false;} else col+=c; }
    else { if (c === '"') inQ=true; else if (c === ','){row.push(col);col='';}
      else if (c === '\n'){row.push(col);rows.push(row);row=[];col='';}
      else if (c !== '\r'){col+=c;}
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
  if (s.includes(':')) { const p=s.split(':'); if (p.length>=2){ venue=(p[0]||'').toUpperCase(); base=p[1]; } }
  const segs=base.split(SEP_RE).filter(Boolean); base=segs[segs.length-1]||base;
  if(/^0x[0-9a-fA-F]{4,}$/.test(base)) return {symbol:base.toUpperCase(), venue};
  base=base.toUpperCase().replace(/(PERP|\d+L|\d+S)$/,'');
  for(const q of QUOTES){ if(base.endsWith(q)){ base=base.slice(0,-q.length); break; } }
  base=base.replace(/[^A-Z0-9]/g,'').trim();
  if(!base) return null;
  return {symbol: base, venue};
}
function uniqBy(arr, keyFn){ const seen=new Set(), out=[]; for (const it of arr){ const k=keyFn(it); if(k && !seen.has(k)){ seen.add(k); out.push(it); } } return out; }

function parseDateKey(s){
  if(!s) return null;
  const d = new Date(String(s).trim()); // YYYY-MM-DD ok
  return isNaN(d) ? null : d;
}
function isRecentByDateKey(dateStr, days=3){
  const d = parseDateKey(dateStr); if(!d) return false;
  return (Date.now() - d.getTime())/(1000*60*60*24) <= days;
}

// fetch helpers
async function fetchText(url, init){ const r=await fetch(url, init); const t=await r.text(); if(!r.ok){ console.error('HTTP', r.status, r.statusText, t.slice(0,200)); throw new Error('Fetch failed'); } return t; }
async function fetchJSON(url, init){ const r=await fetch(url, init); const j=await r.json(); if(!r.ok){ console.error('HTTP', r.status, r.statusText, j); throw new Error('Fetch failed'); } return j; }

// 1) Read assets from sheet
async function readRecentSymbolsFromSheet(){
  const csv = await fetchText(SHEET_CSV_URL, { redirect:'follow' });
  const rows = parseCSV(csv); if (rows.length<2) return [];
  const header = rows[0].map(h=>String(h).trim());
  const headerLower = header.map(h=>h.toLowerCase());

  const idxAsset = headerLower.indexOf('asset');
  const idxDateKey = header.indexOf('DateKey');
  if (idxAsset === -1) throw new Error('Colonne "asset" introuvable');
  if (idxDateKey === -1) throw new Error('Colonne "DateKey" introuvable');

  const raw = [];
  for (let i=1;i<rows.length;i++){
    const asset = rows[i][idxAsset];
    const date  = rows[i][idxDateKey];
    if(!asset) continue;
    if(!isRecentByDateKey(date, 3)) continue;
    const norm = normalizeAsset(asset);
    if (norm) raw.push(norm);
  }
  const uniq = uniqBy(raw, x=>`${x.symbol}|${x.venue}`);
  return uniq.map(x=>x.symbol); // on ne garde que le symbol pour CMC
}

// 2) Map symbols → CMC IDs (best-effort)
async function mapSymbolsToCMCIds(symbols){
  if (!CMC_API_KEY) { console.warn('⚠️ Pas de CMC_API_KEY — les métriques resteront null'); return {}; }
  // L’endpoint /map accepte symbol=CSV
  const unique = Array.from(new Set(symbols));
  const chunks = []; // 120 max par appel pour rester large
  for (let i=0;i<unique.length;i+=100) chunks.push(unique.slice(i,i+100));

  const idMap = {};
  for (const chunk of chunks){
    const url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?symbol=' + encodeURIComponent(chunk.join(','));
    const data = await fetchJSON(url, { headers: { 'X-CMC_PRO_API_KEY': CMC_API_KEY } });
    if (data && Array.isArray(data.data)){
      // S’il y a plusieurs entrées pour un symbol, on prend la première active avec rank si possible
      const bySym = {};
      for (const it of data.data){
        const sym = it.symbol?.toUpperCase();
        if(!sym) continue;
        if(!bySym[sym]) bySym[sym] = [];
        bySym[sym].push(it);
      }
      for (const sym of Object.keys(bySym)){
        const candidates = bySym[sym]
          .filter(x=>x.is_active !== 0)
          .sort((a,b)=> (a.rank||1e9) - (b.rank||1e9));
        const pick = candidates[0] || bySym[sym][0];
        if (pick?.id) idMap[sym] = pick.id;
      }
    }
  }
  return idMap;
}

// 3) Fetch quotes for CMC IDs and build token rows
async function fetchQuotesForIds(idMap){
  if (!CMC_API_KEY || Object.keys(idMap).length===0) return {};
  const ids = Object.values(idMap);
  const chunks = [];
  for (let i=0;i<ids.length;i+=100) chunks.push(ids.slice(i,i+100));

  const out = {};
  for (const chunk of chunks){
    const url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?id=' + chunk.join(',');
    const data = await fetchJSON(url, { headers: { 'X-CMC_PRO_API_KEY': CMC_API_KEY } });
    if (data && data.data){
      Object.assign(out, data.data);
    }
  }
  return out;
}

function buildRows(symbols, idMap, quotesById){
  const rows = [];
  for (const sym of symbols){
    const id = idMap[sym];
    const q = id ? quotesById[id] : null;
    const usd = q?.quote?.USD;

    rows.push({
      symbol: sym,
      venue: "",

      price: usd?.price ?? null,
      d24: usd?.percent_change_24h ?? null,
      d7:  usd?.percent_change_7d ?? null,
      d30: usd?.percent_change_30d ?? null,

      mc: usd?.market_cap ?? null,
      tvl: null,               // (à remplir plus tard via DeFiLlama)
      mc_tvl: (usd?.market_cap && null) ? (usd.market_cap / null) : null,

      vol_mc_24: (usd?.volume_24h && usd?.market_cap) ? (usd.volume_24h / usd.market_cap * 100) : null,
      vol7_mc: null,           // (sera calculé quand on aura l’historique)
      var_vol_7_over_30: null, // (idem)

      rsi_d: null,             // (plus tard via OHLC)
      rsi_h4: null,            // (plus tard via OHLC)

      ath: null,               // (plus tard, source ATH)
      ath_mult: null,          // (sera calculé quand on aura l’ATH)

      // tokenomics additionnelles utiles
      circulating_supply: q?.circulating_supply ?? null,
      total_supply:       q?.total_supply ?? null,
      max_supply:         q?.max_supply ?? null,
      fdv:                usd?.fully_diluted_market_cap ?? null,
      rank:               q?.cmc_rank ?? null
    });
  }
  return rows;
}

// MAIN
(async () => {
  try {
    console.log('➡️ Lecture CSV…');
    const symbols = await readRecentSymbolsFromSheet(); // ex: ["AVAX","ATH","CRV"]
    console.log('➡️ Symbols (≤3j) :', symbols.length);

    let idMap = {};
    let quotesById = {};
    if (CMC_API_KEY && symbols.length){
      console.log('➡️ Mapping symbols → CMC IDs…');
      idMap = await mapSymbolsToCMCIds(symbols);

      console.log('➡️ Fetch quotes…');
      quotesById = await fetchQuotesForIds(idMap);
    } else {
      console.warn('⚠️ Pas de CMC_API_KEY ou 0 symbol — on génère avec valeurs null');
    }

    const rows = buildRows(symbols, idMap, quotesById);

    const out = {
      updated_at: new Date().toISOString(),
      tokens: rows
    };
    fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
    console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens (DateKey ≤ 3 jours).`);
  } catch (e) {
    console.error('❌ Erreur:', e.message || e);
    process.exit(1);
  }
})();
