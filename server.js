const http = require('http');

// ── In-memory state ────────────────────────────────────────────────────────
let stored = null; // { date: 'YYYY-M-D', map: {...}, names: {...} }

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'changeme';
const PORT = process.env.PORT || 3000;

function todayStr() {
  const d = new Date();
  return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
}

// ── CORS helper ────────────────────────────────────────────────────────────
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Password');
}

// ── Server ─────────────────────────────────────────────────────────────────
http.createServer((req, res) => {
  cors(res);

  // Preflight
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // GET /timetable — viewers fetch the current timetable
  if (req.method === 'GET' && req.url === '/timetable') {
    if (!stored || stored.date !== todayStr()) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'No timetable for today' }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(stored));
    return;
  }

  // POST /timetable — admin uploads a new timetable
  if (req.method === 'POST' && req.url === '/timetable') {
    const pw = req.headers['x-admin-password'];
    if (pw !== ADMIN_PASSWORD) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body);
        if (!payload.map) throw new Error('Missing map');
        stored = { date: todayStr(), map: payload.map, names: payload.names || {} };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Bad payload' }));
      }
    });
    return;
  }

  // POST /names — admin updates names without re-uploading timetable
  if (req.method === 'POST' && req.url === '/names') {
    const pw = req.headers['x-admin-password'];
    if (pw !== ADMIN_PASSWORD) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { names } = JSON.parse(body);
        if (stored) stored.names = names;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Bad payload' }));
      }
    });
    return;
  }

  // Health check
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Timetable server running');
    return;
  }

  res.writeHead(404); res.end();

}).listen(PORT, () => console.log(`Timetable server on port ${PORT}`));
