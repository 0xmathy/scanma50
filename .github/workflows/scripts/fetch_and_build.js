// scripts/fetch_and_build.js
// Node 18+ (fetch dispo). Lit la colonne `asset` depuis ton CSV Google Sheet, nettoie et génère data.json

import fs from 'node:fs';
import path from 'node:path';

// 👉 Mets ici ton URL CSV (c'est celle que tu m'as donnée)
const SHEET_CSV_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRz4G8-f_vw017mpvQy9DOl8BhTahfHL5muaKsu8hNPF1U8mC64sU_ec2rs8aKsSMHTVLdaYCNodMpF/pub?gid=916004394&single=true&output=csv';
const OUTPUT = path.resolve('data.json');

// Helpers
const QUOTES = ['USDT','USDC','USD','BTC','ETH','EUR','DAI'];
const SEP_RE = /[:\-_/]/; // séparateurs fréquents MEXC:AVAXUSDT, BINANCE-CRVUSDT, etc.

function normalizeAsset(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  // Enlever crochets/espaces éventuels
  s = s.replace(/[\[\]]/g,'').trim();
  // Split éventuel par espace → garder le premier token
  s = s.split(/\s+/)[0];

  let venue = '';
  let base = s;

  // Cas "EXCHANGE:PAIR"
  if (s.includes(':')) {
    const parts = s.split(':');
    if (parts.length >= 2) {
      venue = parts[0].toUpperCase();
      base = parts[1];
    }
  }

  // Si encore des séparateurs, garder la dernière partie (ex: BINANCE-AVAXUSDT)
  const segs = base.split(SEP_RE).filter(Boolean);
  base = segs[segs.length - 1] || base;

  // Si adresse EVM (0x...) → on garde tel quel
  if (/^0x[0-9a-fA-F]{4,}$/.test(base)) {
    return { symbol: base.toUpperCase(), venue };
  }

  // Uppercase
  base = base.toUpperCase();

  // Enlever suffixes levier / perp fréquents
  base = base.replace(/(PERP|\d+L|\d+S)$/,'');

  // Supprimer les quotes si appariées à la fin (USDT, USDC, etc.)
  for (const q of QUOTES) {
    if (base.endsWith(q)) {
      base = base.slice(0, -q.length);
      break;
    }
  }

  // Nettoyage final
  base = base.replace(/[^A-Z0-9]/g,'');

  if (!base) return null;
  return { symbol: base, venue };
}

function uniqBy(arr, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of arr) {
    const k = keyFn(item);
    if (k && !seen.has(k)) { seen.add(k); out.push(item); }
  }
  return out;
}

async function fetchCSV(url) {
  const res = await fetch(url, { redirect: 'follow' });
  if (!res.ok) throw new Error(`Fetch CSV failed: ${res.status}`);
  return await res.text();
}

function parseCSVAssets(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length === 0) return [];
  const header = lines[0].split(',').map(h => h.trim().toLowerCase());
  const idxAsset = header.indexOf('asset');
  if (idxAsset === -1) {
    console.error('Colonne "asset" introuvable dans le CSV.');
    return [];
  }
  const assets = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    const raw = cols[idxAsset];
    if (!raw) continue;
    const norm = normalizeAsset(raw);
    if (norm) assets.push(norm);
  }
  return uniqBy(assets, x => `${x.symbol}|${x.venue}`);
}

function buildDataJSON(tokens) {
  // 👇 Version minimale : on ne met que les champs nécessaires au rendu
  // (les métriques seront remplies à l’étape suivante via APIs)
  const rows = tokens.map(t => ({
    symbol: t.symbol,
    venue: t.venue || '',
    price: null,
    d24: null,
    d7: null,
    d30: null,
    mc: null,
    tvl: null,
    mc_tvl: null,
    vol_mc_24: null,
    vol7_mc: null,
    var_vol_7_over_30: null,
    rsi_d: null,
    rsi_h4: null,
    ath: null,
    ath_mult: null
  }));

  const out = {
    updated_at: new Date().toISOString(),
    tokens: rows
  };
  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2), 'utf8');
  console.log(`✅ Écrit ${OUTPUT} avec ${rows.length} tokens.`);
}

(async () => {
  try {
    console.log('➡️  Lecture du CSV Google Sheet…');
    const csv = await fetchCSV(SHEET_CSV_URL);
    const tokens = parseCSVAssets(csv);
    console.log(`➡️  Tokens détectés: ${tokens.length}`);
    buildDataJSON(tokens);
  } catch (e) {
    console.error('❌ Erreur:', e);
    process.exit(1);
  }
})();
