const http   = require('http');
const { createClient } = require('redis');

// ── Config ─────────────────────────────────────────────────────────────────
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'changeme';
const PORT           = process.env.PORT || 3000;
const REDIS_URL      = process.env.REDIS_URL;       // injected by Render when Redis is linked
const CYCLE_START    = new Date('2026-02-23T00:00:00Z');

// Redis keys
const K_TIMETABLE  = 'timetable';
const K_PHRASES    = 'obs:phrases';
const K_STATS      = 'obs:stats';
const K_HEARTBEAT  = 'obs:heartbeat';  // { user: isoTimestamp }

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
// Falls back to in-memory if REDIS_URL not set (local dev / before linking)
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

// ── Timetable row parser (Power Automate) ──────────────────────────────────
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

// ── HTTP server ────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = req.url.split('?')[0];

  // GET / — health
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
      console.log(`[timetable] saved to ${redis?'Redis':'memory'} — ${Object.keys(payload.map).length} groups`);
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
      console.log(`[automate] saved to ${redis?'Redis':'memory'} — ${Object.keys(map).length} groups`);
      json(res, 200, { ok: true, groups: Object.keys(map).length });
    } catch(e) { json(res, 400, { error: e.message }); }
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
      const data = (await redisGet(K_STATS)) || { events: [] };
      data.events.push({ ts: new Date().toISOString(), user, key, label: label||key });
      if (data.events.length > 50000) data.events = data.events.slice(-50000);
      await redisSet(K_STATS, data);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  if (req.method==='GET' && url==='/stats') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const data = (await redisGet(K_STATS)) || { events: [] };
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
      byKey, byUser, byDay,
      recentEvents: data.events.slice(-200).reverse()
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

  // POST /heartbeat — AHK pings this every 2 min while script is open
  // Body: { user }  — no password needed, low-value data
  if (req.method==='POST' && url==='/heartbeat') {
    try {
      const { user } = JSON.parse(await readBody(req));
      if (!user) throw new Error('user required');
      const beats = (await redisGet(K_HEARTBEAT)) || {};
      beats[user] = new Date().toISOString();
      await redisSet(K_HEARTBEAT, beats);
      json(res, 200, { ok: true });
    } catch(e) { json(res, 400, { error: e.message }); }
    return;
  }

  // GET /heartbeat — dashboard polls this to show who's online
  if (req.method==='GET' && url==='/heartbeat') {
    if (req.headers['x-admin-password']!==ADMIN_PASSWORD) { authFail(res); return; }
    const beats = (await redisGet(K_HEARTBEAT)) || {};
    // Mark anyone seen in last 3 minutes as online
    const now = Date.now();
    const result = {};
    for (const [user, ts] of Object.entries(beats)) {
      const ageMs = now - new Date(ts).getTime();
      result[user] = { lastSeen: ts, online: ageMs < 3 * 60 * 1000 };
    }
    json(res, 200, result);
    return;
  }

  res.writeHead(404); res.end();
});

// ── Startup — connect Redis first, then listen ─────────────────────────────
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
