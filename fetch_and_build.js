// fetch_and_build.js — Node 20
const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/1c2-v0yZdroahwSqKn7yTZ4osQZa_DCf2onTTvPqJnc8/export?format=csv&gid=916004394';
const OUTPUT = path.resolve('data.json');

const CMC_API_KEY = process.env.CMC_API_KEY || '';
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || '';   // optionnel (holders/top10)
const BSCSCAN_API_KEY   = process.env.BSCSCAN_API_KEY   || '';   // optionnel

const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

const GECKO_OVERRIDES = {
  BTC:'bitcoin', ETH:'ethereum', BNB:'binancecoin', SOL:'solana', ADA:'cardano', XRP:'ripple',
  DOT:'polkadot', LINK:'chainlink', MATIC:'polygon', POL:'polygon', AVAX:'avalanche-2',
  OP:'optimism', ARB:'arbitrum', ATOM:'cosmos', ETC:'ethereum-classic', NEAR:'near',
  APT:'aptos', SUI:'sui', INJ:'injective-protocol', CRV:'curve-dao-token',
  SNX:'synthetix-network-token', LDO:'lido-dao', AAVE:'aave', UNI:'uniswap', MKR:'maker',
  RUNE:'thorchain', CAKE:'pancakeswap-token', GMX:'gmx', DYDX:'dydx-chain', STETH:'staked-ether',
  MORPHO:'morpho-token', ATH:'aethir'
};

const CHAIN_ALIASES = {
  ETH:'Ethereum', WETH:'Ethereum', STETH:'Ethereum',
  AVAX:'Avalanche', OP:'Optimism', ARB:'Arbitrum', BNB:'BSC',
  MATIC:'Polygon', POL:'Polygon', SOL:'Solana', ADA:'Cardano',
  DOT:'Polkadot', NEAR:'Near', APT:'Aptos', SUI:'Sui',
  ATOM:['Cosmos','Cosmos Hub','CosmosHub'],
  ETC:'Ethereum Classic'
};

/* ---------- helpers ---------- */
function parseCSV(text){
  const rows=[]; let row=[], col='', inQ=false;
  for(let i=0;i<text.length;i++){
    const c=text[i], n=text[i+1];
    if(inQ){ if(c=='"'&&n=='"'){ col+='"'; i++; } else if(c=='"'){ inQ=false; } else col+=c; }
    else { if(c=='"') inQ=true; else if(c==','){ row.push(col); col=''; }
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
async function fetchJSON(url, init){
  const r=await fetch(url,init);
  const j=await r.json().catch(()=> ({}));
  if(!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
  return j;
}
const avg = a => (!a||!a.length) ? null : a.reduce((x,y)=>x+y,0)/a.length;
const norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,'');

/* ---------- 1) Sheet → symbol/date ---------- */
async function readRecentFromSheet(){
  const csv = await fetchTextWithRetry(SHEET_CSV_URL);
  const rows = parseCSV(csv);
  if(rows.length<2) return [];
  const header = rows[0].map(h=>String(h).trim());
  const idxAsset   = header.map(h=>h.toLowerCase()).indexOf('asset');
  const idxDateKey = header.indexOf('DateKey');
  if(idxAsset===-1) throw new Error('colonne asset manquante');
  if(idxDateKey===-1) throw new Error('colonne DateKey manquante');

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

/* ---------- 2) CMC quotes + info ---------- */
async function cmcQuotesBySymbol(symbols){
  if(!CMC_API_KEY) return {};
  const headers={'X-CMC_PRO_API_KEY':CMC_API_KEY};
  const uniq=[...new Set(symbols)];
  const out={};
  for(let i=0;i<uniq.length;i+=80){
    const chunk=uniq.slice(i,i+80);
    const url='https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol='+encodeURIComponent(chunk.join(','));
    const data=await fetchJSON(url,{headers});
    for(const sym of Object.keys(data.data||{})){
      const arr = Array.isArray(data.data[sym])?data.data[sym]:[data.data[sym]];
      const best = arr.filter(x=>x.is_active!==0).sort((a,b)=>(a.cmc_rank||1e9)-(b.cmc_rank||1e9))[0] || arr[0];
      if(best) out[sym.toUpperCase()] = best;
    }
  }
  return out;
}
async function cmcInfoBySymbol(symbols){
  if(!CMC_API_KEY) return {};
  const headers={'X-CMC_PRO_API_KEY':CMC_API_KEY};
  const uniq=[...new Set(symbols)];
  const out={};
  for(let i=0;i<uniq.length;i+=200){
    const chunk=uniq.slice(i,i+200);
    const url='https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?symbol='+encodeURIComponent(chunk.join(','));
    const data=await fetchJSON(url,{headers});
    for(const sym of Object.keys(data.data||{})){
      const arr = Array.isArray(data.data[sym])?data.data[sym]:[data.data[sym]];
      // même logique: meilleur par rank si dispo
      const best = arr.sort((a,b)=>(a.cmc_rank||1e9)-(b.cmc_rank||1e9))[0] || arr[0];
      if(best) out[sym.toUpperCase()] = best; // contient description, date_added, urls,...
    }
  }
  return out;
}

/* ---------- 3) DeFiLlama TVL (auto-match, chain-first) ---------- */
let LLAMA_PROTOCOLS=null, LLAMA_CHAINS=null;
async function loadLlamaCatalogs(){
  if(!LLAMA_CHAINS)    LLAMA_CHAINS    = await fetchJSON('https://api.llama.fi/chains');
  if(!LLAMA_PROTOCOLS) LLAMA_PROTOCOLS = await fetchJSON('https://api.llama.fi/protocols');
}
function pickChain(symbol){
  if(!Array.isArray(LLAMA_CHAINS)) return null;
  const alias = CHAIN_ALIASES[symbol] || symbol;
  const candidates = Array.isArray(alias)?alias:[alias];
  for(const c of candidates){
    const hit = LLAMA_CHAINS.find(x => (x.name||'').toLowerCase()===c.toLowerCase());
    if(hit) return hit;
  }
  for(const c of candidates){
    const hit = LLAMA_CHAINS.find(x => (x.name||'').toLowerCase().includes(c.toLowerCase()));
    if(hit) return hit;
  }
  return null;
}
function pickProtocol(symbol){
  if(!Array.isArray(LLAMA_PROTOCOLS)) return null;
  const SYM=symbol.toUpperCase();
  let cand = LLAMA_PROTOCOLS.filter(p => (p.symbol||'').toUpperCase()===SYM);
  if(cand.length===0){
    const ns = norm(symbol);
    cand = LLAMA_PROTOCOLS.filter(p => norm(p.name).includes(ns));
  }
  if(!cand.length) return null;
  cand.sort((a,b)=> (b.tvl||0)-(a.tvl||0));
  return cand[0];
}
async function getTVL(symbol){
  await loadLlamaCatalogs();
  const chain = pickChain(symbol);
  if(chain && typeof chain.tvl==='number') return { tvl:chain.tvl, via:`chain:${chain.name}` };
  const proto = pickProtocol(symbol);
  if(proto && proto.slug){
    try{
      const n = await fetchJSON('https://api.llama.fi/tvl/'+encodeURIComponent(proto.slug));
      if(typeof n==='number') return { tvl:n, via:`protocol:${proto.slug}` };
    }catch(e){}
  }
  return { tvl:null, via:null };
}

/* ---------- 4) CoinGecko détails (ATH, genesis, community, volumes) ---------- */
async function geckoFindId(symbol){
  if(GECKO_OVERRIDES[symbol]) return GECKO_OVERRIDES[symbol];
  try{
    const j=await fetchJSON('https://api.coingecko.com/api/v3/search?query='+encodeURIComponent(symbol));
    const hits=(j?.coins||[]).filter(c => (c.symbol||'').toUpperCase()===symbol.toUpperCase());
    if(!hits.length) return null;
    hits.sort((a,b)=>(a.market_cap_rank||1e9)-(b.market_cap_rank||1e9));
    return hits[0].id||null;
  }catch(e){ return null; }
}
async function geckoDetails(id){
  try{
    const j=await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}?localization=false&tickers=false&market_data=true&community_data=true&developer_data=false&sparkline=false`);
    const md=j?.market_data;
    const comm=j?.community_data;
    return {
      ath: md?.ath?.usd ?? null,
      ath_date: md?.ath_date?.usd ?? null,
      genesis_date: j?.genesis_date ?? null,
      twitter_followers: comm?.twitter_followers ?? null,
      // market_chart 30d vols
      _hasId: true
    };
  }catch(e){ return { _hasId:true }; }
}
async function geckoVolumes30d(id){
  try{
    const j=await fetchJSON(`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=30&interval=daily`);
    const vols=(j?.total_volumes||[]).map(v=>+v[1]).filter(Number.isFinite);
    return { v7: avg(vols.slice(-7)), v30: avg(vols.slice(-30)) };
  }catch(e){ return {}; }
}

/* ---------- 5) Optionnel holders (si API keys) ---------- */
async function holdersEth(address){
  if(!ETHERSCAN_API_KEY || !address) return null;
  try{
    const url=`https://api.etherscan.io/api?module=token&action=tokenholderlist&contractaddress=${address}&page=1&offset=1&apikey=${ETHERSCAN_API_KEY}`;
    const j=await fetchJSON(url);
    // Etherscan Pro pour détails top10 → ici on ne bloque pas
    return j?.status==='1' ? (j?.result?.HolderCount || null) : null;
  }catch(e){ return null; }
}

/* ---------- build ---------- */
function baseRow(it, q, info){
  const usd=q?.quote?.USD;
  // narratif depuis CMC tags (catégories) si dispo
  const tags = Array.isArray(info?.tags)? info.tags.slice(0,5) : [];
  const narrative = tags.length ? tags.join(', ') : null;

  // urls
  const urls = info?.urls||{};
  const website = (urls.website||[])[0] || null;
  const whitepaper = (urls.technical_doc||[])[0] || null;
  const twitter = (urls.twitter||[])[0] || (info?.twitter_username ? `https://twitter.com/${info.twitter_username}` : null);
  const github = (urls.source_code||[])[0] || null;

  // desc courte
  const desc = (info?.description||'').replace(/\s+/g,' ').trim();
  const description_short = desc ? (desc.length>260 ? desc.slice(0,257)+'…' : desc) : null;

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

    // Infos
    narrative,
    description_short,
    launch_date: info?.date_added || null,

    // Social / liens
    twitter_followers: null,
    website, whitepaper, github, twitter,

    // Tokenomics
    circulating_supply: q?.circulating_supply ?? null,
    total_supply:       q?.total_supply ?? null,
    max_supply:         q?.max_supply ?? null,
    fdv:                usd?.fully_diluted_market_cap ?? null,
    rank:               q?.cmc_rank ?? null,

    // provenance TVL
    tvl_source: null,

    // adoption (optionnel)
    holders_30d_change: null,
    top10_pct: null
  };
}

function writeDataJSON(rows){
  const out={ updated_at:new Date().toISOString(), tokens:rows };
  fs.writeFileSync(OUTPUT, JSON.stringify(out,null,2),'utf8');
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens.`);
}

/* ---------- MAIN ---------- */
(async()=>{
  try{
    const recent = await readRecentFromSheet();
    const syms = recent.map(x=>x.symbol);

    let cmcQuotes={}, cmcInfo={};
    if(syms.length){
      try{ cmcQuotes = await cmcQuotesBySymbol(syms); } catch(e){ console.warn('CMC quotes:', e.message); }
      try{ cmcInfo   = await cmcInfoBySymbol(syms);   } catch(e){ console.warn('CMC info:', e.message); }
    }

    // base rows
    const rows = recent.map(it => baseRow(it, cmcQuotes[it.symbol], cmcInfo[it.symbol]));

    // TVL
    for(const r of rows){
      try{
        const { tvl, via } = await getTVL(r.symbol);
        if(typeof tvl==='number'){ r.tvl=tvl; if(r.mc) r.mc_tvl=r.mc/tvl; }
        r.tvl_source = via;
      }catch(e){}
    }

    // CoinGecko enrich
    const idsBySym = {};
    for(const r of rows){ try{ idsBySym[r.symbol] = await geckoFindId(r.symbol); }catch(e){ idsBySym[r.symbol]=null; } }

    for(const r of rows){
      const id = idsBySym[r.symbol];
      if(!id) continue;
      try{
        const det = await geckoDetails(id);
        if(det){
          if(typeof det.ath==='number'){ r.ath=det.ath; if(r.price && r.price>0) r.ath_mult=det.ath/r.price; }
          if(det.ath_date) r.ath_date = det.ath_date;
          if(det.genesis_date && !r.launch_date) r.launch_date = det.genesis_date;
          if(det.twitter_followers!=null) r.twitter_followers = det.twitter_followers;
        }
      }catch(e){}
      try{
        const vols = await geckoVolumes30d(id);
        const v7 = vols.v7, v30 = vols.v30;
        r.vol7_avg = v7 ?? r.vol7_avg;
        r.vol30_avg = v30 ?? r.vol30_avg;
        r.var_vol_7_over_30 = (v7 && v30) ? (v7/v30) : r.var_vol_7_over_30;
        if(r.mc && v7)  r.vol7_mc  = v7 / r.mc * 100;
        if(r.tvl && v7) r.vol7_tvl = v7 / r.tvl * 100;
      }catch(e){}
    }

    // Optionnel: holders (seulement si API keys et adress connues → à brancher plus tard)
    // if(ETHERSCAN_API_KEY){ ... }

    writeDataJSON(rows);
  }catch(e){
    console.error('❌ Erreur (JSON vide):', e.message||e);
    writeDataJSON([]);
  }
})();
