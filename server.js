const http   = require('http');
const https  = require('https');
const { createClient } = require('redis');

// ── Config ─────────────────────────────────────────────────────────────────
const ADMIN_PASSWORD   = process.env.ADMIN_PASSWORD || 'changeme';
const PORT             = process.env.PORT || 3000;
const REDIS_URL        = process.env.REDIS_URL;
const OPENROUTER_KEY   = process.env.OPENROUTER_API_KEY || '';
const CYCLE_START      = new Date('2026-02-23T00:00:00Z');

// Redis keys
const K_TIMETABLE      = 'timetable';
const K_DAILY_CHANGES  = 'daily-changes';
const K_MAP_LAYOUT     = 'map-layout';
const K_PHRASES        = 'obs:phrases';
const K_STATS          = 'obs:stats';
const K_HEARTBEAT      = 'obs:heartbeat';
const K_KILLED         = 'obs:killed';
const K_FIRSTSEEN      = 'obs:firstseen';
const K_EXIT           = 'obs:exit';
const K_VERSIONS       = 'obs:versions';
const K_RESTART        = 'obs:restart';         // per-user restart flags
const K_VERSION_LATEST = 'obs:version-latest';  // latest AHK version string
const K_LSO_SETTINGS   = 'obs:lso-settings';    // per-user email settings (name, domain, cc)

// ── Default phrases ────────────────────────────────────────────────────────
const DEFAULT_PHRASES = {
  base: [
    { key: "1",     label: "Scribed",                 text: "Scribed for written tasks.\n\n" },
    { key: "2",     label: "Read to",                 text: "Read to student.\n\n" },
    { key: "3",     label: "Broke task down",         text: "Broke task into smaller steps.\n\n" },
    { key: "4",     label: "Redirected/Refocused",    text: "The student was redirected or refocused.\n\n" },
    { key: "5",     label: "Re-explained",            text: "Re-explained instructions.\n\n" },
    { key: "6",     label: "Visual Aids",             text: "Provided visual aids like charts and diagrams to help.\n\n" },
    { key: "7",     label: "Moved to quiet space",    text: "Provided a quiet space.\n\n" },
    { key: "8",     label: "Graphic organisers",      text: "Provided graphic organisers.\n\n" },
    { key: "9",     label: "Simplified instructions", text: "Provided simplified instructions.\n\n" },
    { key: "0",     label: "Positive reinforcement",  text: "Provided positive reinforcement.\n\n" },
    { key: "Alt+1", label: "Provided Materials",      text: "Given extra materials/photocopies/notes.\n\n" },
    { key: "Alt+2", label: "Provided IT Support",     text: "Supported with technology issues.\n\n" },
    { key: "Alt+3", label: "Special Provision",       text: "Special provision in Exam/SAC/test.\n\n" },
    { key: "Alt+4", label: "Constructive Feedback",   text: "Provided constructive feedback on draft.\n\n" },
    { key: "Alt+5", label: "Structured Template",     text: "Provided a template with sentence stems and/or structured features.\n\n" },
    { key: "Alt+6", label: "Idea Discussion",         text: "Engaged in discussion to support development of ideas.\n\n" },
    { key: "Alt+7", label: "Goal Setting Tasks",      text: "Provided with goal setting tasks.\n\n" },
    { key: "Alt+8", label: "Practical Task Help",     text: "Assisted student in completing practical activities.\n\n" },
    { key: "Alt+9", label: "Supervised Assessment",   text: "Supervised Assessment (small group room).\n\n" }
  ],
  overrides: {}
};

// ── Redis client ───────────────────────────────────────────────────────────
let redis = null;
const memStore = {};

async function redisGet(key) {
  if (redis) {
    const v = await redis.get(key);
    return v ? JSON.parse(v) : null;
  }
  return memStore[key] ?? null;
}

async function redisSet(key, value) {
  if (redis) {
    await redis.set(key, JSON.stringify(value));
  } else {
    memStore[key] = value;
  }
}

async function redisDel(key) {
  if (redis) await redis.del(key);
  else delete memStore[key];
}

// ── Helpers ────────────────────────────────────────────────────────────────
function todayStr() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function localDateStrFromISO(isoTs) {
  const d = new Date(isoTs);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function sheetNameForDate(date) {
  const dow = date.getUTCDay();
  if (dow === 0 || dow === 6) return null;
  let weekdayCount = 0;
  const d = new Date(CYCLE_START);
  while (d <= date) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) weekdayCount++;
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return `Day ${((weekdayCount - 1) % 10) + 1}`;
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Password');
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function json(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function authFail(res) { json(res, 401, { error: 'Unauthorized' }); }

// ── Timetable row parser ───────────────────────────────────────────────────
const EXCLUDED_COLORS = new Set(['#ffffe1','#ffffe0']);
function isWhiteish(hex) {
  if (!hex||hex==='none'||hex==='') return true;
  hex=hex.replace('#','');
  if(hex.length===3)hex=hex.split('').map(c=>c+c).join('');
  if(hex.length!==6)return true;
  return parseInt(hex.slice(0,2),16)>235&&parseInt(hex.slice(2,4),16)>235&&parseInt(hex.slice(4,6),16)>235;
}
function isBlackish(hex) {
  if (!hex||hex==='none'||hex==='') return true;
  hex=hex.replace('#','');
  if(hex.length===3)hex=hex.split('').map(c=>c+c).join('');
  if(hex.length!==6)return true;
  return parseInt(hex.slice(0,2),16)<20&&parseInt(hex.slice(2,4),16)<20&&parseInt(hex.slice(4,6),16)<20;
}
function parseRows(rows) {
  const REF_ROW=2, numCols=22;
  const refRow=rows[REF_ROW]||{values:[]};
  const periodNums=[];
  for(let c=0;c<numCols;c++){const n=parseInt((refRow.values[c]||'').toString().trim(),10);periodNums[c]=(n>=1&&n<=6)?n:null;}
  let dataStartCol=null;
  outer:for(let c=0;c<numCols;c++)for(let r=0;r<rows.length;r++){if(r===REF_ROW)continue;const bg=(rows[r].bgColors||[])[c]||'';if(bg&&!isWhiteish(bg)&&!EXCLUDED_COLORS.has(bg.toLowerCase())){dataStartCol=c;break outer;}}
  if(dataStartCol===null)return null;
  const groupMap={};
  for(let r=0;r<rows.length;r++){
    if(r===REF_ROW)continue;
    const {values:vals=[],bgColors:bgs=[],fgColors:fgs=[]}=rows[r];
    for(let c=dataStartCol;c<numCols;c+=3){
      const bg=bgs[c]||'';if(!bg||isWhiteish(bg)||EXCLUDED_COLORS.has(bg.toLowerCase()))continue;
      const d1=(vals[c]||'').toString().trim(),d2=(vals[c+1]||'').toString().trim(),d3=(vals[c+2]||'').toString().trim();
      if(!d1&&!d2&&!d3)continue;
      const fg=isBlackish(fgs[c]||'')?'':fgs[c]||'';
      const key=bg+'||'+fg,p=periodNums[c];if(!p)continue;
      if(!groupMap[key])groupMap[key]={bg,fg:fg||null,slots:{}};
      if(!groupMap[key].slots[p])groupMap[key].slots[p]=[];
      groupMap[key].slots[p].push({d1,d2,d3});
    }
  }
  return Object.keys(groupMap).length?groupMap:null;
}

// ── Parse Power Automate / bookmarklet Graph cells ─────────────────────────
function normaliseHex(raw) {
  if (!raw) return null;
  let h = raw.replace('#','').toLowerCase();
  if (h.length === 8) h = h.slice(2);
  if (h.length === 3) h = h.split('').map(c=>c+c).join('');
  if (h.length !== 6) return null;
  return '#' + h;
}

function parseGraphCells(cells) {
  if (!cells || !cells.length) return null;
  const REF_ROW = 2;
  const numCols = Math.max(...cells.map(r => r.length));
  const refRow = cells[REF_ROW] || [];
  const periodNums = [];
  for (let c = 0; c < numCols; c++) {
    const val = refRow[c] ? String(refRow[c].value || '').trim() : '';
    const n = parseInt(val, 10);
    periodNums[c] = (n >= 1 && n <= 6) ? n : null;
  }
  let dataStartCol = null;
  outer: for (let c = 0; c < numCols; c++)
    for (let r = 0; r < cells.length; r++) {
      if (r === REF_ROW) continue;
      const cell = cells[r][c];
      if (!cell) continue;
      const bg = normaliseHex(cell.bg);
      if (bg && !isWhiteish(bg) && !EXCLUDED_COLORS.has(bg)) { dataStartCol = c; break outer; }
    }
  if (dataStartCol === null) return null;
  const groupMap = {};
  for (let r = 0; r < cells.length; r++) {
    if (r === REF_ROW) continue;
    const row = cells[r] || [];
    for (let c = dataStartCol; c < numCols; c += 3) {
      const c0 = row[c], c1 = row[c+1], c2 = row[c+2];
      if (!c0) continue;
      const bg = normaliseHex(c0.bg);
      if (!bg || isWhiteish(bg) || EXCLUDED_COLORS.has(bg)) continue;
      const d1 = String(c0.value || '').trim();
      const d2 = String((c1 && c1.value) || '').trim();
      const d3 = String((c2 && c2.value) || '').trim();
      if (!d1 && !d2 && !d3) continue;
      const rawFg = normaliseHex(c0.fg);
      const fg = (rawFg && !isBlackish(rawFg)) ? rawFg : null;
      const key = bg + '||' + (fg || '');
      const p = periodNums[c];
      if (!p) continue;
      if (!groupMap[key]) groupMap[key] = { bg, fg, slots: {} };
      if (!groupMap[key].slots[p]) groupMap[key].slots[p] = [];
      groupMap[key].slots[p].push({ d1, d2, d3 });
    }
  }
  return Object.keys(groupMap).length ? groupMap : null;
}

// ── HTTP server ────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = req.url.split('?')[0];

  if (req.method==='GET' && url==='/') {
    json(res, 200, {
      status: 'ok',
      todaySheet: sheetNameForDate(new Date()) || 'weekend',
      storage: redis ? 'redis' : 'memory'
    });
    return;
  }

  // ══════════════════════════════════════════════
  //  TIMETABLE
  // ══════════════════════════════════════════════

  if (req.method==='GET' && url==='/timetable') {
    const stored = await redisGet(K_TIMETABLE);
    if (!stored) { json(res, 404, { error: 'No timetable uploaded yet' }); return; }
    json(res, 200, stored);
    return;
  }

  if (req.method==='POST' && url==='/timetable') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const payload = JSON.parse(await readBody(req));
      if (!payload.map) throw new Error('Missing map');
      const stored = { date: todayStr(), map: payload.map, names: payload.names||{} };
      await redisSet(K_TIMETABLE, stored);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='DELETE' && url==='/timetable') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    await redisDel(K_TIMETABLE);
    json(res, 200, { ok: true });
    return;
  }

  if (req.method==='POST' && url==='/automate') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const payload = JSON.parse(await readBody(req));
      if (!Array.isArray(payload.rows)) throw new Error('Missing rows');
      const map = parseRows(payload.rows);
      if (!map) throw new Error('No coloured groups found');
      const existing = await redisGet(K_TIMETABLE);
      const names = payload.names || (existing ? existing.names : {});
      await redisSet(K_TIMETABLE, { date: todayStr(), map, names });
      json(res, 200, { ok: true, groups: Object.keys(map).length });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='POST' && url==='/automate/graph') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const payload = JSON.parse(await readBody(req));
      if (!Array.isArray(payload.cells)) throw new Error('Missing cells array');
      const map = parseGraphCells(payload.cells);
      if (!map) throw new Error('No coloured groups found in cells');
      const existing = await redisGet(K_TIMETABLE);
      const names = payload.names || (existing ? existing.names : {});
      await redisSet(K_TIMETABLE, {
        date: todayStr(),
        sheet: payload.sheet || sheetNameForDate(new Date()) || 'unknown',
        map,
        names,
        source: 'bookmarklet'
      });
      console.log(`[automate/graph] Pushed "${payload.sheet}" — ${Object.keys(map).length} groups`);
      json(res, 200, { ok: true, groups: Object.keys(map).length, sheet: payload.sheet });
    } catch(e) {
      console.error('[automate/graph] Error:', e.message);
      json(res, 400, { error: e.message });
    }
    return;
  }

  if (req.method==='POST' && url==='/names') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { names } = JSON.parse(await readBody(req));
      const stored = await redisGet(K_TIMETABLE);
      if (stored) {
        stored.names = names;
        await redisSet(K_TIMETABLE, stored);
      }
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── Daily Changes ────────────────────────────────────────────────────────

  if (req.method==='GET' && url==='/daily-changes') {
    const data = await redisGet(K_DAILY_CHANGES);
    if (!data) { json(res, 404, { error: 'No daily changes published' }); return; }
    json(res, 200, data);
    return;
  }

  if (req.method==='POST' && url==='/daily-changes') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const payload = JSON.parse(await readBody(req));
      await redisSet(K_DAILY_CHANGES, payload);
      console.log(`[daily-changes] Published for ${payload.date || 'unknown date'}`);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='DELETE' && url==='/daily-changes') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    await redisDel(K_DAILY_CHANGES);
    json(res, 200, { ok: true });
    return;
  }

  // ── Map layout ────────────────────────────────────────────────────────────

  if (req.method==='GET' && url==='/map-layout') {
    const data = await redisGet(K_MAP_LAYOUT);
    if (!data) { json(res, 404, { error: 'No map layout saved yet' }); return; }
    json(res, 200, data);
    return;
  }

  if (req.method==='POST' && url==='/map-layout') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const payload = JSON.parse(await readBody(req));
      await redisSet(K_MAP_LAYOUT, payload);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── Read image via OpenRouter ─────────────────────────────────────────────

  if (req.method==='POST' && url==='/read-image') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    if (!OPENROUTER_KEY) {
      json(res, 503, { error: 'OPENROUTER_API_KEY not set. Get a free key at openrouter.ai and add it as an environment variable in Render.' });
      return;
    }
    try {
      const { imageBase64, imageMime } = JSON.parse(await readBody(req));
      if (!imageBase64 || !imageMime) { json(res, 400, { error: 'imageBase64 and imageMime required' }); return; }

      const prompt = `You are reading a school LSO (Learning Support Officer) daily timetable changes sheet image.

Return ONLY valid JSON — no markdown fences, no explanation, nothing else. Use this exact structure:

{
  "absent": [
    { "name": "H'Onorine", "periods": null },
    { "name": "Kylie", "periods": ["p1", "p2"] }
  ],
  "changes": [
    {
      "lso": "Harpreet",
      "period": "p2",
      "type": "cover",
      "description": "Cover Paige",
      "classCode": "08MMAT260CRAA",
      "room": "BR1",
      "teacherCode": "CRANL"
    }
  ],
  "notes": null
}

HOW TO READ THE IMAGE:

ABSENT LINE: At the very top there is a bold "Absent –" line listing names separated by commas.
- Extract each name into the absent array.
- If a name has a period qualifier like "(P1 & P2)" extract it as periods: ["p1","p2"].
- If no period qualifier, use periods: null (means absent all day).

CHANGES TABLE: A grid with columns labelled P1, P2, P3, P4, P5, P6.
Each table cell can contain one or more change entries. Each entry looks like:

  PersonName – some description text
  CLASSCODE
  [ ROOM     TEACHERCODE ]   ← coloured/highlighted rectangle

For EACH entry in EACH cell create one object in the changes array:
- "lso": the bold person's first name (e.g. "Harpreet", "Michelle", "Beth")
- "period": which column it is in — "p1", "p2", "p3", "p4", "p5", or "p6"
- "type": "cover" if description contains "Cover", "cancelled" if cancelled/group cancelled, "other" for LEC/RCP/MacqLit/TAE admin/etc.
- "description": the text after the dash (e.g. "Cover Paige", "Cover Farishat", "MacqLit for Paige", "LEC", "TAE admin")
- "classCode": the alphanumeric code shown ABOVE the coloured box — copy it EXACTLY.
- "room": the text on the LEFT side of the coloured/highlighted rectangle (e.g. "BR1", "BR3"). Copy exactly.
- "teacherCode": the text on the RIGHT side of the coloured/highlighted rectangle (e.g. "CRANL", "BHATM"). Copy exactly.
- If there is NO coloured box for an entry, set classCode: null, room: null, teacherCode: null.

IMPORTANT:
- Create one changes entry per person per period.
- Extract every single entry from every cell. Do not skip any.
- The "lso" field uses first name only.
- Do not invent data — only extract what is visibly written in the image.`;

      const body = JSON.stringify({
        model: 'google/gemini-2.0-flash-001',
        messages: [{
          role: 'user',
          content: [
            { type: 'image_url', image_url: { url: `data:${imageMime};base64,${imageBase64}` } },
            { type: 'text', text: prompt }
          ]
        }],
        max_tokens: 3000
      });

      const result = await new Promise((resolve, reject) => {
        const opts = {
          hostname: 'openrouter.ai',
          path: '/api/v1/chat/completions',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
            'Authorization': `Bearer ${OPENROUTER_KEY}`,
            'HTTP-Referer': 'https://timetable-server-r6nb.onrender.com',
            'X-Title': 'LSO Timetable'
          }
        };
        const r = https.request(opts, resp => {
          let data = '';
          resp.on('data', c => data += c);
          resp.on('end', () => resolve({ status: resp.statusCode, body: data }));
        });
        r.on('error', reject);
        r.write(body);
        r.end();
      });

      if (result.status !== 200) {
        console.error('[read-image] OpenRouter error', result.status, result.body);
        let detail = result.body;
        try { detail = JSON.parse(result.body)?.error?.message || detail; } catch(e) {}
        json(res, 502, { error: `OpenRouter error: ${detail}` });
        return;
      }

      const parsed = JSON.parse(result.body);
      let text = parsed.choices?.[0]?.message?.content || '';
      text = text.replace(/^```json\s*/,'').replace(/^```\s*/,'').replace(/\s*```$/,'').trim();
      console.log('[read-image] Cleaned response:', text.slice(0, 200));
      json(res, 200, { text });
    } catch(e) {
      console.error('[read-image]', e);
      json(res, 500, { error: e.message });
    }
    return;
  }

  // ══════════════════════════════════════════════
  //  PHRASES
  // ══════════════════════════════════════════════

  if (req.method==='GET' && url==='/phrases') {
    const qs   = new URLSearchParams(req.url.split('?')[1]||'');
    const user = (qs.get('user')||'').trim();
    const data = (await redisGet(K_PHRASES)) || DEFAULT_PHRASES;
    const base = data.base;
    const overrides = user && data.overrides[user] ? data.overrides[user] : [];
    const overrideMap = {};
    overrides.forEach(o => overrideMap[o.key] = o);
    const merged = base.map(p => overrideMap[p.key] || p);
    overrides.forEach(o => { if (!base.find(b => b.key===o.key)) merged.push(o); });
    json(res, 200, { base, overrides, merged, user: user||null });
    return;
  }

  if (req.method==='POST' && url==='/phrases/base') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { base } = JSON.parse(await readBody(req));
      if (!Array.isArray(base)) throw new Error('base must be array');
      const data = (await redisGet(K_PHRASES)) || DEFAULT_PHRASES;
      data.base = base;
      await redisSet(K_PHRASES, data);
      json(res, 200, { ok: true, count: base.length });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='POST' && url==='/phrases/override') {
    try {
      const { user, overrides: ov } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      if (!Array.isArray(ov)) throw new Error('overrides must be array');
      const data = (await redisGet(K_PHRASES)) || DEFAULT_PHRASES;
      data.overrides[user] = ov;
      await redisSet(K_PHRASES, data);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/phrases/users') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const data = (await redisGet(K_PHRASES)) || DEFAULT_PHRASES;
    json(res, 200, { users: Object.keys(data.overrides) });
    return;
  }

  // ══════════════════════════════════════════════
  //  STATS / LOGGING
  // ══════════════════════════════════════════════

  if (req.method==='POST' && url==='/log') {
    try {
      const { user, key, label } = JSON.parse(await readBody(req));
      if (!user||!key) throw new Error('user and key required');
      const data = (await redisGet(K_STATS)) || { events: [], byUserDay: {} };

      const ts  = new Date().toISOString();
      const day = localDateStrFromISO(ts);

      data.events.push({ ts, user, key, label: label||key });
      if (data.events.length > 50000) data.events = data.events.slice(-50000);

      const UTILITY_KEYS = new Set(['Ctrl+Alt+E','Ctrl+Alt+T','Ctrl+Alt+P','Ctrl+Alt+F','Ctrl+Alt+W','Ctrl+Alt+S','Ctrl+Alt+M','__heartbeat__']);
      if (!data.byUserDay) data.byUserDay = {};
      if (!data.byUserDay[user]) data.byUserDay[user] = {};
      if (!data.byUserDay[user][day]) data.byUserDay[user][day] = { phrases: 0, utility: 0 };
      if (UTILITY_KEYS.has(key)) {
        if (key !== '__heartbeat__') data.byUserDay[user][day].utility++;
      } else {
        data.byUserDay[user][day].phrases++;
      }

      await redisSet(K_STATS, data);

      const firstSeen = (await redisGet(K_FIRSTSEEN)) || {};
      if (!firstSeen[user]) {
        firstSeen[user] = ts;
        await redisSet(K_FIRSTSEEN, firstSeen);
      }
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/stats') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const data = (await redisGet(K_STATS)) || { events: [], byUserDay: {} };
    const byKey={}, byUser={}, byDay={};

    data.events.forEach(({ ts, user, key, label }) => {
      if (!byKey[key]) byKey[key]={ label, total:0, byUser:{} };
      byKey[key].total++;
      byKey[key].byUser[user]=(byKey[key].byUser[user]||0)+1;
      if (!byUser[user]) byUser[user]={ total:0, byKey:{} };
      byUser[user].total++;
      byUser[user].byKey[key]=(byUser[user].byKey[key]||0)+1;
      const day=ts.slice(0,10);
      byDay[day]=(byDay[day]||0)+1;
    });

    json(res, 200, {
      totalEvents: data.events.length,
      byKey,
      byUser,
      byDay,
      byUserDay: data.byUserDay || {},
      recentEvents: data.events.slice(-2000).reverse()
    });
    return;
  }

  if (req.method==='GET' && url==='/stats/me') {
    const qs   = new URLSearchParams(req.url.split('?')[1]||'');
    const user = (qs.get('user')||'').trim();
    if (!user) { json(res, 400, { error: 'user required' }); return; }
    const data  = (await redisGet(K_STATS)) || { events: [] };
    const mine  = data.events.filter(e => e.user===user);
    const byKey = {};
    mine.forEach(({ key, label }) => {
      if (!byKey[key]) byKey[key]={ label, count:0 };
      byKey[key].count++;
    });
    json(res, 200, { user, total: mine.length, byKey });
    return;
  }

  // ══════════════════════════════════════════════
  //  HEARTBEAT
  // ══════════════════════════════════════════════

  if (req.method==='POST' && url==='/heartbeat') {
    try {
      const { user, version } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const beats = (await redisGet(K_HEARTBEAT)) || {};
      beats[user] = new Date().toISOString();
      await redisSet(K_HEARTBEAT, beats);
      if (version) {
        const versions = (await redisGet(K_VERSIONS)) || {};
        versions[user] = version;
        await redisSet(K_VERSIONS, versions);
      }
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/heartbeat') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const beats    = (await redisGet(K_HEARTBEAT)) || {};
    const versions = (await redisGet(K_VERSIONS))  || {};
    const exits    = (await redisGet(K_EXIT))       || {};
    const restarts = (await redisGet(K_RESTART))    || {};
    const now = Date.now();
    const result = {};
    for (const [user, ts] of Object.entries(beats)) {
      const ageMs = now - new Date(ts).getTime();
      result[user] = {
        lastSeen:       ts,
        online:         ageMs < 3 * 60 * 1000,
        version:        versions[user] || null,
        exitPending:    !!exits[user],
        restartPending: !!restarts[user]
      };
    }
    json(res, 200, result);
    return;
  }

  // ══════════════════════════════════════════════
  //  USER MANAGEMENT
  // ══════════════════════════════════════════════

  if (req.method==='GET' && url==='/stats/firstseen') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const data = (await redisGet(K_FIRSTSEEN)) || {};
    json(res, 200, data);
    return;
  }

  if (req.method==='POST' && url==='/user/kill') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { user, killed } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const data = (await redisGet(K_KILLED)) || {};
      if (killed) data[user] = true;
      else delete data[user];
      await redisSet(K_KILLED, data);
      json(res, 200, { ok: true, user, killed: !!killed });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/user/status') {
    try {
      const qs      = new URLSearchParams(req.url.split('?')[1]||'');
      const user    = (qs.get('user')||'').trim();
      if (!user) throw new Error('user required');
      const killed        = (await redisGet(K_KILLED))        || {};
      const exits         = (await redisGet(K_EXIT))          || {};
      const restarts      = (await redisGet(K_RESTART))       || {};
      const versionData   = await redisGet(K_VERSION_LATEST);
      const latestVersion = (versionData && versionData.version) ? versionData.version : null;
      const shouldExit    = !!exits[user];
      const shouldRestart = !!restarts[user];
      // Consume flags — clear them on read so they only fire once
      if (shouldExit)    { delete exits[user];    await redisSet(K_EXIT, exits); }
      if (shouldRestart) { delete restarts[user]; await redisSet(K_RESTART, restarts); }
      json(res, 200, { user, killed: !!killed[user], exit: shouldExit, restart: shouldRestart, latestVersion });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/users/killed') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const data = (await redisGet(K_KILLED)) || {};
    json(res, 200, data);
    return;
  }

  if (req.method==='POST' && url==='/exit/user') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { user } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const exits = (await redisGet(K_EXIT)) || {};
      exits[user] = true;
      await redisSet(K_EXIT, exits);
      json(res, 200, { ok: true, user });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='POST' && url==='/exit/all') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const body = req.headers['content-length'] > 0 ? JSON.parse(await readBody(req)) : {};
      const beats  = (await redisGet(K_HEARTBEAT)) || {};
      const stats  = (await redisGet(K_STATS))     || { events: [] };
      const allUsers = new Set([
        ...Object.keys(beats),
        ...stats.events.map(e => e.user).filter(Boolean)
      ]);
      if (body.users) body.users.forEach(u => allUsers.add(u));
      const updates = {};
      allUsers.forEach(u => { updates[u] = true; });
      await redisSet(K_EXIT, updates);
      json(res, 200, { ok: true, flagged: [...allUsers] });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── NEW: Per-user restart signal ──────────────────────────────────────────

  if (req.method==='POST' && url==='/restart/user') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { user } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const restarts = (await redisGet(K_RESTART)) || {};
      restarts[user] = true;
      await redisSet(K_RESTART, restarts);
      console.log(`[restart] Signal sent to ${user}`);
      json(res, 200, { ok: true, user });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── NEW: Latest AHK version (read by script, set by admin) ───────────────

  if (req.method==='GET' && url==='/version/latest') {
    const stored = await redisGet(K_VERSION_LATEST);
    const version = (stored && stored.version) ? stored.version : 'v3.5';
    json(res, 200, { version });
    return;
  }

  if (req.method==='POST' && url==='/version/latest') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const { version } = JSON.parse(await readBody(req));
      if (!version || typeof version !== 'string') throw new Error('version string required');
      await redisSet(K_VERSION_LATEST, { version: version.trim() });
      console.log(`[version] Latest set to ${version.trim()}`);
      json(res, 200, { ok: true, version: version.trim() });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── LSO settings (per-user name / domain / cc for email) ──────────────────

  if (req.method==='GET' && url==='/lso-settings') {
    try {
      const qs   = new URLSearchParams(req.url.split('?')[1]||'');
      const user = (qs.get('user')||'').trim();
      if (!user) throw new Error('user required');
      const all = (await redisGet(K_LSO_SETTINGS)) || {};
      json(res, 200, all[user] || {});
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='POST' && url==='/lso-settings') {
    try {
      const { user, name, domain, cc } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const all = (await redisGet(K_LSO_SETTINGS)) || {};
      all[user] = { name: name||'', domain: domain||'stpeters.vic.edu.au', cc: cc||'' };
      await redisSet(K_LSO_SETTINGS, all);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── LSO email builder — called by AHK to get a ready-to-open mailto URL ───
  // GET /lso-email?user=JohnSmith  (matches by Windows username against tt.names)

  if (req.method==='GET' && url==='/lso-email') {
    try {
      const qs   = new URLSearchParams(req.url.split('?')[1]||'');
      const user = (qs.get('user')||'').trim();
      const key  = (qs.get('key')||'').trim();
      if (!user) throw new Error('user required');

      const tt = await redisGet(K_TIMETABLE);
      if (!tt || !tt.map) throw new Error('No timetable loaded on server');

      const allSettings = (await redisGet(K_LSO_SETTINGS)) || {};
      const settings = allSettings[user] || {};
      const domain   = settings.domain || 'stpeters.vic.edu.au';
      const yourName = settings.name   || user;
      const cc       = settings.cc     || '';

      // Find this user's timetable group.
      // Priority: explicit key param -> stored display name -> raw Windows username
      let grp = null, grpName = '';
      const searchTerms = [];
      if (settings.name && settings.name.trim()) searchTerms.push(settings.name.trim().toLowerCase());
      if (!searchTerms.includes(user.toLowerCase())) searchTerms.push(user.toLowerCase());

      if (key && tt.map[key]) {
        grp = tt.map[key]; grpName = tt.names?.[key] || key;
      } else {
        outer: for (const term of searchTerms) {
          for (const [k, g] of Object.entries(tt.map)) {
            const n = (tt.names?.[k] || '').toLowerCase();
            if (n === term || n.replace(/\s+/g,'') === term.replace(/\s+/g,'')) {
              grp = g; grpName = tt.names?.[k] || k; break outer;
            }
          }
        }
      }
      if (!grp) throw new Error(`No timetable entry found for "${settings.name || user}". Open the Settings dialog in the LSO tool and set your display name to exactly match what appears on the timetable (e.g. "Nathan").`);

      // Get DC changes for this person
      const dc = await redisGet(K_DAILY_CHANGES);
      const dcChanges = [];
      if (dc && dc.changes) {
        const lsoLower = grpName.toLowerCase();
        dc.changes.forEach(ch => { if ((ch.lso||'').toLowerCase() === lsoLower) dcChanges.push(ch); });
      }

      const slots = grp.slots || {};
      const d = new Date();
      const dd = `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getFullYear()).slice(-2)}`;
      const subject = `LSO ${dd}`;

      function codeToPrefix(code) {
        const lower = code.toLowerCase().trim();
        return lower[lower.length-1] + lower.slice(0, lower.length-1);
      }

      const dcByPeriod = {};
      dcChanges.forEach(ch => {
        const pNum = parseInt((ch.period||'').replace('p',''), 10);
        if (pNum >= 1 && pNum <= 6) { if (!dcByPeriod[pNum]) dcByPeriod[pNum] = []; dcByPeriod[pNum].push(ch); }
      });

      const periodLines = [];
      const teacherMap  = new Map();
      for (let p = 1; p <= 6; p++) {
        const dcFP = dcByPeriod[p] || [];
        const entries = slots[p];
        if (dcFP.length > 0) {
          dcFP.forEach(ch => {
            periodLines.push(`P${p} - ${ch.classCode||ch.description||''}   ${ch.room||''}   ${ch.teacherCode||''}`.trimEnd());
            if (ch.teacherCode) { const code = ch.teacherCode.trim().toUpperCase(); if (!teacherMap.has(code)) teacherMap.set(code, codeToPrefix(code)); }
          });
        } else if (entries && entries.length > 0) {
          entries.forEach(({d1,d2,d3}) => {
            if (d1||d2||d3) {
              periodLines.push(`P${p} - ${d1||''}   ${d2||''}   ${d3||''}`.trimEnd());
              if (d3) { const code = d3.trim().toUpperCase(); if (!teacherMap.has(code)) teacherMap.set(code, codeToPrefix(code)); }
            }
          });
        }
      }

      const teacherEmails = [...teacherMap.values()].map(prefix => `${prefix}@${domain}`);
      const body = [
        'Good morning teachers,', '',
        'Today I will be attending and supporting the students within your classes.',
        '', ...periodLines, '',
        `Please introduce me to the students as ${yourName}.`,
        'I look forward to working with you today and providing effective support to our students.',
        '', 'Kind regards,', yourName
      ].join('\n');

      const toStr  = teacherEmails.join(';');
      const mailto = `mailto:${toStr}?cc=${encodeURIComponent(cc)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
      json(res, 200, { subject, body, mailto, teacherEmails, toStr, cc, yourName, grpName });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // ── Delete user (removes from all data stores) ────────────────────────────

  if (req.method==='DELETE' && url.startsWith('/user/')) {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    try {
      const user = decodeURIComponent(url.slice('/user/'.length)).trim();
      if (!user) throw new Error('user required');
      const beats    = (await redisGet(K_HEARTBEAT)) || {}; delete beats[user];    await redisSet(K_HEARTBEAT, beats);
      const versions = (await redisGet(K_VERSIONS))  || {}; delete versions[user]; await redisSet(K_VERSIONS, versions);
      const killed   = (await redisGet(K_KILLED))    || {}; delete killed[user];   await redisSet(K_KILLED, killed);
      const fs       = (await redisGet(K_FIRSTSEEN)) || {}; delete fs[user];       await redisSet(K_FIRSTSEEN, fs);
      const exits    = (await redisGet(K_EXIT))       || {}; delete exits[user];    await redisSet(K_EXIT, exits);
      const restarts = (await redisGet(K_RESTART))    || {}; delete restarts[user]; await redisSet(K_RESTART, restarts);
      const stats    = (await redisGet(K_STATS))      || { events: [] };
      stats.events = (stats.events||[]).filter(e => e.user !== user);
      if (stats.byUser)    delete stats.byUser[user];
      if (stats.byUserDay) delete stats.byUserDay[user];
      await redisSet(K_STATS, stats);
      console.log(`[delete-user] Removed all data for ${user}`);
      json(res, 200, { ok: true, user });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  res.writeHead(404); res.end();
});

// ── Startup ────────────────────────────────────────────────────────────────
async function start() {
  if (REDIS_URL) {
    try {
      redis = createClient({ url: REDIS_URL });
      redis.on('error', e => console.error('[redis] error:', e.message));
      await redis.connect();
      console.log('[redis] connected ✓');
    } catch(e) {
      console.error('[redis] failed to connect — falling back to in-memory:', e.message);
      redis = null;
    }
  } else {
    console.warn('[redis] REDIS_URL not set — using in-memory (data lost on restart)');
  }

  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Storage: ${redis ? 'Redis (persistent ✓)' : 'in-memory (temporary)'}`);
    console.log(`Sheet today: ${sheetNameForDate(new Date()) || 'weekend'}`);
  });
}

start();
