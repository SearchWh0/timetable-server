const http = require('http');

// ── Config ─────────────────────────────────────────────────────────────────
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'changeme';
const PORT           = process.env.PORT || 3000;

// Week 1 cycle anchor — Monday 23 Feb 2026
// Day 1=Mon W1, Day 2=Tue W1 ... Day 5=Fri W1, Day 6=Mon W2 ... Day 10=Fri W2
const CYCLE_START = new Date('2026-02-23T00:00:00Z'); // Monday of Week 1

// ── In-memory state ────────────────────────────────────────────────────────
let stored = null; // { date, map, names }

// ── Helpers ────────────────────────────────────────────────────────────────
function todayStr() {
  const d = new Date();
  return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
}

// Returns 'Day 1' … 'Day 10' for a given Date, or null if weekend
function sheetNameForDate(date) {
  const dow = date.getUTCDay(); // 0=Sun,1=Mon...6=Sat
  if (dow === 0 || dow === 6) return null; // weekend

  // Count weekdays from CYCLE_START to date
  const msPerDay = 86400000;
  let weekdayCount = 0;
  const d = new Date(CYCLE_START);
  while (d <= date) {
    const dw = d.getUTCDay();
    if (dw !== 0 && dw !== 6) weekdayCount++;
    d.setUTCDate(d.getUTCDate() + 1);
  }
  // weekdayCount is 1-based from cycle start; mod 10 gives position in cycle
  const dayNum = ((weekdayCount - 1) % 10) + 1;
  return `Day ${dayNum}`;
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Password');
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

// ── Excel row parser (called when Power Automate sends raw rows) ───────────
// Power Automate sends rows as an array of objects keyed by column letter:
// [ { "A": "val", "B": "val", ... "V": "val" }, ... ]
// Columns A-V = 22 columns (indices 0-21)
// We replicate the same colour-group logic as the browser parser but
// here we receive pre-extracted colour+text data from Power Automate.
//
// Power Automate payload format:
// {
//   rows: [
//     { values: ["v1","v2",...], bgColors: ["#fff","#f00",...], fgColors: [...] },
//     ...
//   ],
//   names: { "bg||fg": "Name" }   // optional saved names
// }

const EXCLUDED_COLORS = new Set(['#ffffe1','#ffffe0']);
function isWhiteish(hex) {
  if (!hex || hex === 'none' || hex === '') return true;
  hex = hex.replace('#','');
  if (hex.length === 3) hex = hex.split('').map(c=>c+c).join('');
  if (hex.length !== 6) return true;
  const r = parseInt(hex.slice(0,2),16);
  const g = parseInt(hex.slice(2,4),16);
  const b = parseInt(hex.slice(4,6),16);
  return r > 235 && g > 235 && b > 235;
}
function isBlackish(hex) {
  if (!hex || hex === 'none' || hex === '') return true;
  hex = hex.replace('#','');
  if (hex.length === 3) hex = hex.split('').map(c=>c+c).join('');
  if (hex.length !== 6) return true;
  const r = parseInt(hex.slice(0,2),16);
  const g = parseInt(hex.slice(2,4),16);
  const b = parseInt(hex.slice(4,6),16);
  return r < 20 && g < 20 && b < 20;
}

function parseRows(rows, savedNames) {
  const REF_ROW = 2; // row index 2 = row 3 (0-based)

  // rows[r].values  = array of cell text (22 cols, A-V)
  // rows[r].bgColors = array of bg hex per cell
  // rows[r].fgColors = array of fg hex per cell

  const numCols = 22; // A to V

  // Get period numbers from row 3
  const periodNums = [];
  const refRowData = rows[REF_ROW] || { values: [] };
  for (let c = 0; c < numCols; c++) {
    const txt = (refRowData.values[c] || '').toString().trim();
    const n = parseInt(txt, 10);
    periodNums[c] = (n >= 1 && n <= 6) ? n : null;
  }

  // Find first coloured column
  let dataStartCol = null;
  outer:
  for (let c = 0; c < numCols; c++) {
    for (let r = 0; r < rows.length; r++) {
      if (r === REF_ROW) continue;
      const bg = (rows[r].bgColors || [])[c] || '';
      if (bg && !isWhiteish(bg) && !EXCLUDED_COLORS.has(bg.toLowerCase())) {
        dataStartCol = c;
        break outer;
      }
    }
  }

  if (dataStartCol === null) return null;

  const groupMap = {};
  for (let r = 0; r < rows.length; r++) {
    if (r === REF_ROW) continue;
    const row = rows[r];
    const vals = row.values   || [];
    const bgs  = row.bgColors || [];
    const fgs  = row.fgColors || [];

    for (let c = dataStartCol; c < numCols; c += 3) {
      const bg = bgs[c] || '';
      if (!bg || isWhiteish(bg) || EXCLUDED_COLORS.has(bg.toLowerCase())) continue;

      const d1 = (vals[c]   || '').toString().trim();
      const d2 = (vals[c+1] || '').toString().trim();
      const d3 = (vals[c+2] || '').toString().trim();
      if (!d1 && !d2 && !d3) continue;

      const fgRaw = fgs[c] || '';
      const fg = isBlackish(fgRaw) ? '' : fgRaw;
      const key = bg + '||' + fg;

      const periodNum = periodNums[c];
      if (!periodNum) continue;

      if (!groupMap[key]) groupMap[key] = { bg, fg: fg || null, slots: {} };
      if (!groupMap[key].slots[periodNum]) groupMap[key].slots[periodNum] = [];
      groupMap[key].slots[periodNum].push({ d1, d2, d3 });
    }
  }

  return Object.keys(groupMap).length ? groupMap : null;
}

// ── Server ─────────────────────────────────────────────────────────────────
http.createServer(async (req, res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // GET / — health + today's sheet name
  if (req.method === 'GET' && req.url === '/') {
    const sheet = sheetNameForDate(new Date());
    res.writeHead(200, {'Content-Type':'application/json'});
    res.end(JSON.stringify({ status: 'ok', todaySheet: sheet || 'weekend' }));
    return;
  }

  // GET /timetable — viewers
  if (req.method === 'GET' && req.url === '/timetable') {
    if (!stored || stored.date !== todayStr()) {
      res.writeHead(404, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: 'No timetable for today' }));
      return;
    }
    res.writeHead(200, {'Content-Type':'application/json'});
    res.end(JSON.stringify(stored));
    return;
  }

  // POST /timetable — from admin.html (pre-parsed map)
  if (req.method === 'POST' && req.url === '/timetable') {
    if (req.headers['x-admin-password'] !== ADMIN_PASSWORD) {
      res.writeHead(401, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: 'Unauthorized' })); return;
    }
    try {
      const body = await readBody(req);
      const payload = JSON.parse(body);
      if (!payload.map) throw new Error('Missing map');
      stored = { date: todayStr(), map: payload.map, names: payload.names || {} };
      res.writeHead(200, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ ok: true }));
    } catch(e) {
      res.writeHead(400, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // POST /automate — from Power Automate (raw rows, server parses)
  if (req.method === 'POST' && req.url === '/automate') {
    if (req.headers['x-admin-password'] !== ADMIN_PASSWORD) {
      res.writeHead(401, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: 'Unauthorized' })); return;
    }
    try {
      const body = await readBody(req);
      const payload = JSON.parse(body);
      // payload.rows = array of { values, bgColors, fgColors }
      if (!payload.rows || !Array.isArray(payload.rows)) throw new Error('Missing rows');
      const map = parseRows(payload.rows, payload.names);
      if (!map) throw new Error('No coloured groups found');
      // Preserve existing names if not supplied
      const names = payload.names || (stored ? stored.names : {});
      stored = { date: todayStr(), map, names };
      console.log(`[automate] Stored timetable: ${Object.keys(map).length} groups`);
      res.writeHead(200, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ ok: true, groups: Object.keys(map).length }));
    } catch(e) {
      console.error('[automate] Error:', e.message);
      res.writeHead(400, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // POST /names — update names only
  if (req.method === 'POST' && req.url === '/names') {
    if (req.headers['x-admin-password'] !== ADMIN_PASSWORD) {
      res.writeHead(401, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: 'Unauthorized' })); return;
    }
    try {
      const body = await readBody(req);
      const { names } = JSON.parse(body);
      if (stored) stored.names = names;
      res.writeHead(200, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ ok: true }));
    } catch(e) {
      res.writeHead(400, {'Content-Type':'application/json'});
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  res.writeHead(404); res.end();

}).listen(PORT, () => {
  console.log(`Timetable server running on port ${PORT}`);
  console.log(`Today's sheet: ${sheetNameForDate(new Date()) || 'weekend'}`);
});
