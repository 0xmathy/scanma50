// fetch_and_build.js — Node 20, sans dépendances externes
const fs = require('fs');
const path = require('path');

const SHEET_CSV_URL =
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv';

const OUTPUT = path.resolve('data.json');
const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/;

// --- CSV parser minimal (gère correctement les guillemets) ---
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
  // supprime les lignes vides éventuelles
  return rows.filter(r => r.length > 1 || (r.length === 1 && r[0] !== ''));
}

// --- Normalisation asset → {symbol, venue} ---
function normalizeAsset(raw) {
  if (!raw) return null;
  let s = String(raw).trim().replace(/[\[\]]/g,'').trim();
  s = s.split(/\s+/)[0];

  let venue = '';
  let base = s;

  // EXCHANGE:PAIR
  if (s.includes(':')) {
    const parts = s.split(':');
    if (parts.length >= 2) {
      venue = (parts[0] || '').toUpperCase();
      base  = parts[1];
    }
  }

  // BINANCE-AVAXUSDT / AVAX/USDT / AVAX-USDT
  const segs = base.split(SEP_RE).filter(Boolean);
  base = segs[segs.length - 1] || base;

  // Adresse EVM
  if (/^0x[0-9a-fA-F]{4,}$/.test(base)) {
    return { symbol: base.toUpperCase(), venue };
  }

  base = base.toUpperCase();

  // suffixes levier/perp
  base = base.replace(/(PERP|\d+L|\d+S)$/,'');

  // quotes
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

// --- Parsing robuste de DateKey (plusieurs formats courants) ---
function parseDateKey(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).trim();

  // 1) ISO ou formats natifs : YYYY-MM-DD, YYYY-MM-DDTHH:mm:ss
  const d1 = new Date(s);
  if (!isNaN(d1)) return d1;

  // 2) DD/MM/YYYY [HH:mm]
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T](\d{1,2}):(\d{2}))?$/);
  if (m) {
    const [_, dd, mm, yyyy, HH='0', MM='0'] = m;
    return new Date(Number(yyyy), Number(mm)-1, Number(dd), Number(HH), Number(MM));
  }

  // 3) DD-MM-YYYY
  const m2 = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m2) {
    const [_, dd, mm, yyyy] = m2;
    return new Date(Number(yyyy), Number(mm)-1, Number(dd));
  }

  return null; // inconnu
}

function isRecentByDateKey(dateStr, days = 3) {
  const d = parseDateKey(dateStr);
  if (!d) return false;
  const diffDays = (Date.now() - d.getTime()) / (1000*60*60*24);
  return diffDays <= days;
}

// Node 20 a fetch intégré
async function fetchCSV(url) {
  const res = await fetch(url, { redirect: 'follow' });
  const text = await res.text(); // lis le corps (utile pour logs si erreur)
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

  const header = rows[0].map(h => String(h).trim());
  const headerLower = header.map(h => h.toLowerCase());

  const idxAsset = headerLower.indexOf('asset');
  const idxDateKey = header.indexOf('DateKey'); // respecte la casse exacte si ta colonne est bien "DateKey"

  if (idxAsset === -1) throw new Error('Colonne "asset" introuvable dans le CSV.');
  if (idxDateKey === -1) throw new Error('Colonne "DateKey" introuvable dans le CSV.');

  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const cols = rows[i];
    const rawAsset = cols[idxAsset];
    const dateKey  = cols[idxDateKey];

    if (!rawAsset) continue;
    if (!isRecentByDateKey(dateKey, 3)) continue; // <= 3 jours

    const norm = normalizeAsset(rawAsset);
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
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens (DateKey ≤ 3 jours).`);
}

(async () => {
  try {
    console.log('➡️  Lecture du CSV Google Sheet…');
    const csv = await fetchCSV(SHEET_CSV_URL);
    console.log('➡️  Parsing CSV…');
    const tokens = assetsFromCSV(csv);
    console.log(`➡️  Tokens détectés (récent ≤3j): ${tokens.length}`);
    buildDataJSON(tokens);
  } catch (e) {
    console.error('❌ Erreur:', e.message || e);
    process.exit(1);
  }
})();
