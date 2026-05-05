/**
 * TransportX — Live Location Tracker Server
 */

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const path      = require('path');
const os        = require('os');
const fs        = require('fs');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: '/ws' });

app.use(express.json());

// ── Static files ─────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (_req, res) => res.redirect('/viewer/'));

// ── Log directory ─────────────────────────────────────────────────────
const LOGS_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR);

function appendLog(deviceId, entry) {
  try {
    const file = path.join(LOGS_DIR, `${deviceId}.jsonl`);
    fs.appendFileSync(file, JSON.stringify(entry) + '\n');
  } catch (_) {}
}

function readLog(deviceId) {
  try {
    const file = path.join(LOGS_DIR, `${deviceId}.jsonl`);
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8')
      .trim().split('\n').filter(Boolean)
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean);
  } catch (_) { return []; }
}

// ── Device registry ───────────────────────────────────────────────────
const COLORS = ['#6c63ff', '#f59e0b', '#22c55e', '#ef4444', '#06b6d4', '#f97316'];
let colorCursor = 0;
const devices = {};   // live device state
const viewers = new Set();
const kickedSessions = new Set(); // sessionIds that have been kicked

function nextColor() { return COLORS[colorCursor++ % COLORS.length]; }

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const ws of viewers) {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}

function deviceSummary(id) {
  const d = devices[id];
  return {
    deviceId: id, name: d.name, color: d.color,
    online: d.online, lastSeen: d.lastSeen, sessionId: d.sessionId,
    trail: d.trail, lastLocation: d.trail.at(-1) ?? null,
  };
}

// ── Shared helpers ────────────────────────────────────────────────────
function ensureDevice(deviceId, name, sessionId) {
  if (!devices[deviceId]) {
    devices[deviceId] = {
      name: name || 'Unknown Device',
      color: nextColor(),
      online: true, lastSeen: Date.now(),
      sessionId: sessionId || null,
      trail: [],
    };
    appendLog(deviceId, {
      type: 'session_start', deviceId,
      name: devices[deviceId].name, color: devices[deviceId].color,
      sessionId, timestamp: Date.now(),
    });
    broadcast({ type: 'device_online', ...deviceSummary(deviceId) });
  } else {
    const dev = devices[deviceId];
    const wasOffline = !dev.online;
    dev.name      = name || dev.name;
    dev.online    = true;
    dev.lastSeen  = Date.now();
    dev.sessionId = sessionId || dev.sessionId;
    if (wasOffline) {
      appendLog(deviceId, {
        type: 'session_start', deviceId,
        name: dev.name, color: dev.color,
        sessionId, timestamp: Date.now(),
      });
      broadcast({ type: 'device_online', ...deviceSummary(deviceId) });
    }
  }
}

function recordPoint(deviceId, point) {
  const dev = devices[deviceId];
  dev.trail.push(point);
  if (dev.trail.length > 500) dev.trail.shift();
  dev.lastSeen = Date.now();
  appendLog(deviceId, { type: 'location', ...point });
  broadcast({ type: 'location_update', deviceId, name: dev.name, color: dev.color, ...point });
}

function markOffline(deviceId) {
  if (!devices[deviceId]) return;
  devices[deviceId].online = false;
  appendLog(deviceId, { type: 'session_end', timestamp: Date.now() });
  broadcast({ type: 'device_offline', deviceId });
}

// ── WebSocket handler ─────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let role = null, deviceId = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'register_viewer') {
      role = 'viewer';
      viewers.add(ws);
      ws.send(JSON.stringify({
        type: 'initial_state',
        devices: Object.keys(devices).map(deviceSummary),
      }));

    } else if (msg.type === 'register_tracker') {
      role     = 'tracker';
      deviceId = msg.deviceId;
      ensureDevice(deviceId, msg.name, msg.sessionId || null);
      ws.send(JSON.stringify({
        type: 'registered',
        deviceId,
        color:     devices[deviceId].color,
        name:      devices[deviceId].name,
        sessionId: devices[deviceId].sessionId,
      }));

    } else if (msg.type === 'location') {
      if (!deviceId || !devices[deviceId]) return;
      recordPoint(deviceId, {
        lat: msg.lat, lng: msg.lng,
        accuracy:  msg.accuracy  ?? null,
        speed:     msg.speed     ?? null,
        heading:   msg.heading   ?? null,
        timestamp: msg.timestamp ?? Date.now(),
      });
    }
  });

  ws.on('close', () => {
    if (role === 'viewer') {
      viewers.delete(ws);
    } else if (role === 'tracker' && deviceId) {
      markOffline(deviceId);
    }
  });

  ws.on('error', () => {});
});

// ── Heartbeat ─────────────────────────────────────────────────────────
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25_000);
wss.on('close', () => clearInterval(heartbeat));

// ── REST: live devices ────────────────────────────────────────────────
app.get('/api/devices', (_req, res) => {
  res.json(Object.keys(devices).map(id => ({
    deviceId: id, name: devices[id].name, color: devices[id].color,
    online: devices[id].online, lastSeen: devices[id].lastSeen,
    sessionId: devices[id].sessionId,
    pointCount: devices[id].trail.length,
    lastLocation: devices[id].trail.at(-1) ?? null,
  })));
});

// ── REST: log summaries ───────────────────────────────────────────────
app.get('/api/logs', (_req, res) => {
  if (!fs.existsSync(LOGS_DIR)) return res.json([]);
  const files = fs.readdirSync(LOGS_DIR).filter(f => f.endsWith('.jsonl'));
  const summaries = files.map(file => {
    const deviceId = file.replace('.jsonl', '');
    const entries  = readLog(deviceId);
    const locs     = entries.filter(e => e.type === 'location');
    const sessions = entries.filter(e => e.type === 'session_start');
    const last     = sessions.at(-1);
    const dev      = devices[deviceId];
    return {
      deviceId,
      name:         dev?.name  || last?.name  || deviceId,
      color:        dev?.color || last?.color || '#7c85a2',
      online:       dev?.online || false,
      pingCount:    locs.length,
      sessionCount: sessions.length,
      firstSeen:    entries[0]?.timestamp || null,
      lastSeen:     entries.at(-1)?.timestamp || null,
    };
  });
  res.json(summaries.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0)));
});

// ── REST: full log for one device ─────────────────────────────────────
app.get('/api/devices/:id/log', (req, res) => {
  res.json(readLog(req.params.id));
});

// ── REST: HTTP ping (Scriptable / Capacitor app / generic clients) ────
app.post('/api/ping', (req, res) => {
  const { deviceId, name, sessionId, lat, lng, accuracy, speed, heading, timestamp } = req.body || {};
  if (!deviceId || lat == null || lng == null) {
    return res.status(400).json({ error: 'deviceId, lat, lng required' });
  }

  // Session kick check
  if (sessionId && kickedSessions.has(sessionId)) {
    return res.status(403).json({ error: 'session_kicked', message: 'This session has been ended by an admin. Press Start Tracking to begin a new session.' });
  }

  ensureDevice(deviceId, name, sessionId);
  recordPoint(deviceId, {
    lat, lng,
    accuracy:  accuracy  ?? null,
    speed:     speed     ?? null,
    heading:   heading   ?? null,
    timestamp: timestamp ?? Date.now(),
  });
  res.json({ ok: true, color: devices[deviceId].color, name: devices[deviceId].name });
});

// ── REST: mark device offline ─────────────────────────────────────────
app.post('/api/offline', (req, res) => {
  const { deviceId } = req.body || {};
  if (!deviceId) return res.status(400).json({ error: 'deviceId required' });
  markOffline(deviceId);
  res.json({ ok: true });
});

// ── REST: kick a device offline for its current session ───────────────
// This ends the current session only. The device can reconnect with a new sessionId.
app.post('/api/kick/:deviceId', (req, res) => {
  const id  = req.params.deviceId;
  const dev = devices[id];
  if (!dev) return res.status(404).json({ error: 'Device not found or not online' });

  // Record the kicked sessionId so future pings from this session get 403
  if (dev.sessionId) kickedSessions.add(dev.sessionId);

  markOffline(id);
  broadcast({ type: 'device_kicked', deviceId: id });

  res.json({ ok: true, kickedSession: dev.sessionId });
});

// ── REST: OwnTracks HTTP endpoint ─────────────────────────────────────
app.post('/api/owntracks/:deviceId', (req, res) => {
  const deviceId = req.params.deviceId;
  const b = req.body || {};
  if (b._type !== 'location' || b.lat == null || b.lon == null) return res.json([]);

  const name = deviceId.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  ensureDevice(deviceId, name, null);
  recordPoint(deviceId, {
    lat:      b.lat,
    lng:      b.lon,
    accuracy: b.acc ?? null,
    speed:    b.vel ?? null,
    heading:  b.cog ?? null,
    timestamp: b.tst ? b.tst * 1000 : Date.now(),
  });
  res.json([]);
});

// ── REST: clear trail ─────────────────────────────────────────────────
app.delete('/api/devices/:id/trail', (req, res) => {
  const dev = devices[req.params.id];
  if (!dev) return res.status(404).json({ error: 'Not found' });
  dev.trail = [];
  broadcast({ type: 'trail_cleared', deviceId: req.params.id });
  res.json({ ok: true });
});

// ── Start ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4747;
server.listen(PORT, '0.0.0.0', () => {
  const localIP = getLocalIP();
  console.log('\n  🚀  TransportX Location Tracker');
  console.log('  ─────────────────────────────────────────');
  console.log(`  🖥   Viewer    →  http://localhost:${PORT}/viewer/`);
  console.log(`  📱  PWA       →  http://${localIP}:${PORT}/tracker/`);
  console.log(`  🍎  OwnTracks →  https://<tunnel>/api/owntracks/<device-id>`);
  console.log(`  🤖  Samsung   →  http://${localIP}:${PORT}/api/ping`);
  console.log(`  📂  Logs      →  ${LOGS_DIR}`);
  console.log('  ─────────────────────────────────────────');
  console.log('  For iOS: cloudflared tunnel --url http://localhost:' + PORT);
  console.log('  ─────────────────────────────────────────\n');
});

function getLocalIP() {
  for (const iface of Object.values(os.networkInterfaces()))
    for (const a of iface)
      if (a.family === 'IPv4' && !a.internal) return a.address;
  return 'localhost';
}
