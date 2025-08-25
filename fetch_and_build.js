// fetch_and_build.js — version simple, sans dépendances externes (Node 20)
const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv';

const OUTPUT = path.resolve('data.json');
const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

// --- CSV parser minimal (gère les guillemets) ---
function parseCSV(text) {
  const rows = [];
  let row = [], col = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i], n = text[i+1];
    if (inQ) {
      if (c === '"' && n === '"') { col += '"'; i++; }
      else if (c === '"') { inQ = false; }
      else { col += c; }
    } else {
      if (c === '"') inQ = true;
      else if (c === ',') { row.push(col); col = ''; }
      else if (c === '\n') { row.push(col); rows.push(row); row = []; col = ''; }
      else if (c !== '\r') { col += c; }
    }
  }
  row.push(col); rows.push(row);
  return rows.filter(r => r.length > 1 || (r.length === 1 && r[0] !== ''));
}

// --- Normalisation asset → {symbol, venue} ---
function normalizeAsset(raw) {
  if (!raw) return null;
  let s = String(raw).trim().replace(/[\[\]]/g,'').trim();
  s = s.split(/\s+/)[0];

  let venue = '';
  let base = s;

  if (s.includes(':')) {
    const parts = s.split(':');
    if (parts.length >= 2) {
      venue = (parts[0] || '').toUpperCase();
      base  = parts[1];
    }
  }

  const segs = base.split(SEP_RE).filter(Boolean);
  base = segs[segs.length - 1] || base;

  if (/^0x[0-9a-fA-F]{4,}$/.test(base)) {
    return { symbol: base.toUpperCase(), venue };
  }

  base = base.toUpperCase().replace(/(PERP|\d+L|\d+S)$/,'');
  for (const q of QUOTES) {
    if (base.endsWith(q)) { base = base.slice(0, -q.length); break; }
  }
  base = base.replace(/[^A-Z0-9]/g,'').trim();
  if (!base) return null;
  return { symbol: base, venue };
}

function uniqBy(arr, keyFn) {
  const seen = new Set(); const out = [];
  for (const it of arr) {
    const k = keyFn(it);
    if (k && !seen.has(k)) { seen.add(k); out.push(it); }
  }
  return out;
}

async function fetchCSV(url) {
  const res = await fetch(url, { redirect: 'follow' });
  const text = await res.text(); // on lit le corps pour log si erreur
  if (!res.ok) {
    console.error('HTTP status:', res.status, res.statusText);
    console.error('Body preview:', text.slice(0, 200));
    throw new Error(`Fetch CSV failed: ${res.status}`);
  }
  return text;
}

function assetsFromCSV(csvText) {
  const rows = parseCSV(csvText);
  if (rows.length < 2) return [];
  const header = rows[0].map(h => String(h).trim().toLowerCase());
  const idxAsset = header.indexOf('asset');
  if (idxAsset === -1) throw new Error('Colonne "asset" introuvable dans le CSV.');

  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const raw = rows[i][idxAsset];
    if (!raw) continue;
    const norm = normalizeAsset(raw);
    if (norm) out.push(norm);
  }
  return uniqBy(out, x => `${x.symbol}|${x.venue}`);
}

function buildDataJSON(tokens) {
  const rows = tokens.map(t => ({
    symbol: t.symbol,
    venue: t.venue || '',
    price: null, d24: null, d7: null, d30: null,
    mc: null, tvl: null, mc_tvl: null,
    vol_mc_24: null, vol7_mc: null, var_vol_7_over_30: null,
    rsi_d: null, rsi_h4: null, ath: null, ath_mult: null
  }));
  const out = { updated_at: new Date().toISOString(), tokens: rows };
  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens.`);
}

(async () => {
  try {
    console.log('➡️  Lecture du CSV Google Sheet…');
    const csv = await fetchCSV(SHEET_CSV_URL);
    console.log('➡️  Parsing CSV…');
    const tokens = assetsFromCSV(csv);
    console.log(`➡️  Tokens détectés : ${tokens.length}`);
    buildDataJSON(tokens);
  } catch (e) {
    console.error('❌ Erreur:', e.message || e);
    process.exit(1);
  }
})();
