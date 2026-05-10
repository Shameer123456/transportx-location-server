/**
 * TransportX Server
 *
 * Two transports on the same HTTP server:
 *   /ws        — legacy raw WebSocket (Capacitor / PWA tracker apps)
 *   /socket.io — Socket.IO (React Native app + customer web map)
 *
 * REST:
 *   Legacy  — /api/devices, /api/ping, /api/kick, /api/logs, etc.
 *   Product — /api/drivers/online, /api/jobs, /api/route
 */

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const { Server: SocketIO } = require('socket.io');
const path      = require('path');
const os        = require('os');
const fs        = require('fs');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: '/ws' });
const io     = new SocketIO(server, { cors: { origin: '*' }, path: '/socket.io' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── File logs (ephemeral on Render free tier) ────────────────────────────────
const LOGS_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR);

function appendLog(deviceId, entry) {
  try {
    fs.appendFileSync(path.join(LOGS_DIR, `${deviceId}.jsonl`), JSON.stringify(entry) + '\n');
  } catch (_) {}
}
function readLog(deviceId) {
  try {
    const file = path.join(LOGS_DIR, `${deviceId}.jsonl`);
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean)
      .map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  } catch (_) { return []; }
}

// ── In-memory state ──────────────────────────────────────────────────────────
const devices        = {};   // legacy tracker: deviceId → device object
const viewers        = new Set();
const kickedSessions = new Set();
const onlineDrivers  = {};   // driverId → driver object
const jobs           = {};   // jobId    → job object

let colorCursor = 0;
const COLORS = ['#6c63ff', '#f59e0b', '#22c55e', '#ef4444', '#06b6d4', '#f97316'];
function nextColor() { return COLORS[colorCursor++ % COLORS.length]; }

// ── Legacy WS helpers ────────────────────────────────────────────────────────
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const ws of viewers) if (ws.readyState === WebSocket.OPEN) ws.send(data);
}
function deviceSummary(id) {
  const d = devices[id];
  return {
    deviceId: id, name: d.name, color: d.color,
    online: d.online, lastSeen: d.lastSeen,
    sessionId: d.sessionId, trail: d.trail,
    lastLocation: d.trail.at(-1) ?? null,
  };
}
function ensureDevice(deviceId, name, sessionId) {
  if (!devices[deviceId]) {
    devices[deviceId] = {
      name: name || 'Unknown Device', color: nextColor(),
      online: true, lastSeen: Date.now(),
      sessionId: sessionId || null, trail: [],
    };
    appendLog(deviceId, { type: 'session_start', deviceId, name: devices[deviceId].name, color: devices[deviceId].color, sessionId, timestamp: Date.now() });
    broadcast({ type: 'device_online', ...deviceSummary(deviceId) });
  } else {
    const dev = devices[deviceId];
    const wasOffline = !dev.online;
    dev.name      = name || dev.name;
    dev.online    = true;
    dev.lastSeen  = Date.now();
    dev.sessionId = sessionId || dev.sessionId;
    if (wasOffline) {
      appendLog(deviceId, { type: 'session_start', deviceId, name: dev.name, color: dev.color, sessionId, timestamp: Date.now() });
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

// ── Helpers ──────────────────────────────────────────────────────────────────
function activeJobs() {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  return Object.values(jobs).filter(j => j.postedAt > cutoff);
}

function driverPayload(id, d) {
  return {
    driverId:    id,
    name:        d.name,
    phone:       d.phone        || null,
    lat:         d.lat,
    lng:         d.lng,
    destination: d.destination  || null,   // { name, lat, lng }
    heading:     d.heading      ?? null,
    speed:       d.speed        ?? null,
  };
}

// ── Socket.IO — driver & customer ────────────────────────────────────────────
io.on('connection', (socket) => {

  // Driver goes online — carries phone + destination now
  socket.on('driver:online', ({ driverId, name, phone, lat, lng, destination }) => {
    if (!driverId || !name) return;
    onlineDrivers[driverId] = {
      name,
      phone:       phone       || null,
      lat:         lat         || 0,
      lng:         lng         || 0,
      destination: destination || null,
      heading:     null,
      speed:       null,
      lastSeen:    Date.now(),
      socketId:    socket.id,
    };
    socket.data.driverId = driverId;
    socket.data.role     = 'driver';
    io.emit('driver:online', driverPayload(driverId, onlineDrivers[driverId]));
  });

  // Driver location update (no phone/destination — those don't change mid-session)
  socket.on('driver:location', ({ driverId, lat, lng, heading, speed }) => {
    const d = onlineDrivers[driverId];
    if (!d) return;
    Object.assign(d, { lat, lng, heading: heading ?? null, speed: speed ?? null, lastSeen: Date.now() });
    io.emit('driver:location', { driverId, lat, lng, heading, speed });
  });

  socket.on('driver:offline', ({ driverId }) => {
    delete onlineDrivers[driverId];
    io.emit('driver:offline', { driverId });
  });

  // Customer joins — receive current drivers + active jobs
  socket.on('customer:join', () => {
    socket.data.role = 'customer';

    const drivers = Object.entries(onlineDrivers)
      .filter(([, d]) => d.lat || d.lng)
      .map(([id, d]) => driverPayload(id, d));

    socket.emit('drivers:initial', drivers);
    socket.emit('jobs:initial', activeJobs());
  });

  socket.on('disconnect', () => {
    const { role, driverId } = socket.data || {};
    if (role === 'driver' && driverId && onlineDrivers[driverId]) {
      onlineDrivers[driverId].socketId       = null;
      onlineDrivers[driverId].disconnectedAt = Date.now();
    }
  });
});

// Expire drivers silent for > 3 minutes
setInterval(() => {
  const cutoff = Date.now() - 3 * 60 * 1000;
  for (const [driverId, d] of Object.entries(onlineDrivers)) {
    if (d.lastSeen < cutoff) {
      delete onlineDrivers[driverId];
      io.emit('driver:offline', { driverId });
    }
  }
}, 30_000);

// Expire jobs older than 24 h
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [id, j] of Object.entries(jobs)) {
    if (j.postedAt < cutoff) delete jobs[id];
  }
}, 60 * 60 * 1000);

// ── Legacy raw WebSocket ─────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let role = null, deviceId = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'register_viewer') {
      role = 'viewer';
      viewers.add(ws);
      ws.send(JSON.stringify({ type: 'initial_state', devices: Object.keys(devices).map(deviceSummary) }));

    } else if (msg.type === 'register_tracker') {
      role = 'tracker';
      deviceId = msg.deviceId;
      ensureDevice(deviceId, msg.name, msg.sessionId || null);
      ws.send(JSON.stringify({ type: 'registered', deviceId, color: devices[deviceId].color, name: devices[deviceId].name }));

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
    if (role === 'viewer') viewers.delete(ws);
    else if (role === 'tracker' && deviceId) markOffline(deviceId);
  });
  ws.on('error', () => {});
});

// WS heartbeat
const heartbeat = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25_000);
wss.on('close', () => clearInterval(heartbeat));

// ── REST — legacy tracker ────────────────────────────────────────────────────
app.get('/api/devices', (_req, res) => {
  res.json(Object.entries(devices).map(([id, d]) => ({
    deviceId: id, name: d.name, color: d.color,
    online: d.online, lastSeen: d.lastSeen,
    pointCount: d.trail.length, lastLocation: d.trail.at(-1) ?? null,
  })));
});

app.get('/api/logs', (_req, res) => {
  if (!fs.existsSync(LOGS_DIR)) return res.json([]);
  const summaries = fs.readdirSync(LOGS_DIR).filter(f => f.endsWith('.jsonl')).map(file => {
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
      firstSeen:    entries[0]?.timestamp    || null,
      lastSeen:     entries.at(-1)?.timestamp || null,
    };
  });
  res.json(summaries.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0)));
});

app.get('/api/devices/:id/log', (req, res) => res.json(readLog(req.params.id)));

app.post('/api/ping', (req, res) => {
  const { deviceId, name, sessionId, lat, lng, accuracy, speed, heading, timestamp } = req.body || {};
  if (!deviceId || lat == null || lng == null)
    return res.status(400).json({ error: 'deviceId, lat, lng required' });
  if (sessionId && kickedSessions.has(sessionId))
    return res.status(403).json({ error: 'session_kicked', message: 'Session ended. Press Start Tracking to begin a new session.' });
  ensureDevice(deviceId, name, sessionId);
  recordPoint(deviceId, { lat, lng, accuracy: accuracy ?? null, speed: speed ?? null, heading: heading ?? null, timestamp: timestamp ?? Date.now() });
  res.json({ ok: true, color: devices[deviceId].color, name: devices[deviceId].name });
});

app.post('/api/offline', (req, res) => {
  const { deviceId } = req.body || {};
  if (!deviceId) return res.status(400).json({ error: 'deviceId required' });
  markOffline(deviceId);
  res.json({ ok: true });
});

app.post('/api/kick/:deviceId', (req, res) => {
  const id  = req.params.deviceId;
  const dev = devices[id];
  if (!dev) return res.status(404).json({ error: 'Device not found' });
  if (dev.sessionId) kickedSessions.add(dev.sessionId);
  markOffline(id);
  broadcast({ type: 'device_kicked', deviceId: id });
  res.json({ ok: true, kickedSession: dev.sessionId });
});

app.post('/api/owntracks/:deviceId', (req, res) => {
  const deviceId = req.params.deviceId;
  const b = req.body || {};
  if (b._type !== 'location' || b.lat == null || b.lon == null) return res.json([]);
  const name = deviceId.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  ensureDevice(deviceId, name, null);
  recordPoint(deviceId, { lat: b.lat, lng: b.lon, accuracy: b.acc ?? null, speed: b.vel ?? null, heading: b.cog ?? null, timestamp: b.tst ? b.tst * 1000 : Date.now() });
  res.json([]);
});

app.delete('/api/devices/:id/trail', (req, res) => {
  const dev = devices[req.params.id];
  if (!dev) return res.status(404).json({ error: 'Not found' });
  dev.trail = [];
  broadcast({ type: 'trail_cleared', deviceId: req.params.id });
  res.json({ ok: true });
});

// ── REST — product: drivers ──────────────────────────────────────────────────
app.get('/api/drivers/online', (_req, res) => {
  res.json(Object.entries(onlineDrivers).map(([id, d]) => driverPayload(id, d)));
});

// ── REST — product: jobs ─────────────────────────────────────────────────────
app.get('/api/jobs', (_req, res) => {
  res.json(activeJobs());
});

app.post('/api/jobs', (req, res) => {
  const { pickup, dropoff, phone, description } = req.body || {};
  if (!pickup?.lat || !pickup?.lng || !dropoff?.lat || !dropoff?.lng || !phone)
    return res.status(400).json({ error: 'pickup (lat/lng), dropoff (lat/lng), and phone are required' });

  const jobId = 'job-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  const job = {
    jobId,
    pickup:      { name: (pickup.name  || '').trim(), lat: pickup.lat,  lng: pickup.lng  },
    dropoff:     { name: (dropoff.name || '').trim(), lat: dropoff.lat, lng: dropoff.lng },
    phone:       phone.trim(),
    description: (description || '').trim(),
    postedAt:    Date.now(),
  };
  jobs[jobId] = job;
  io.emit('job:new', job);
  res.json({ ok: true, jobId });
});

// ── REST — product: routing (OSRM proxy) ────────────────────────────────────
app.get('/api/route', async (req, res) => {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from and to required' });
  const [fLat, fLng] = from.split(',').map(Number);
  const [tLat, tLng] = to.split(',').map(Number);
  if ([fLat, fLng, tLat, tLng].some(isNaN)) return res.status(400).json({ error: 'Invalid coordinates' });
  try {
    const url  = `https://router.project-osrm.org/route/v1/driving/${fLng},${fLat};${tLng},${tLat}?overview=full&geometries=geojson`;
    const resp = await fetch(url);
    const data = await resp.json();
    if (!data.routes?.length) return res.status(404).json({ error: 'No route found' });
    const route  = data.routes[0];
    const distKm = (route.distance / 1000).toFixed(1);
    const mins   = Math.ceil(route.duration / 60);
    res.json({
      geometry:       route.geometry,
      distance_m:     Math.round(route.distance),
      duration_s:     Math.round(route.duration),
      distance_label: `${distKm} km`,
      eta_label:      mins < 60 ? `${mins} min` : `${Math.floor(mins / 60)}h ${mins % 60}m`,
    });
  } catch (err) {
    console.error('[route]', err.message);
    res.status(502).json({ error: 'Routing service unavailable' });
  }
});

// ── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4747;
server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('\n  TransportX');
  console.log(`  Landing  → http://localhost:${PORT}/`);
  console.log(`  Driver   → http://localhost:${PORT}/driver/`);
  console.log(`  Customer → http://localhost:${PORT}/customer/`);
  console.log(`  Tracker  → http://${ip}:${PORT}/tracker/`);
  console.log(`  Viewer   → http://localhost:${PORT}/viewer/\n`);
});

function getLocalIP() {
  for (const iface of Object.values(os.networkInterfaces()))
    for (const a of iface)
      if (a.family === 'IPv4' && !a.internal) return a.address;
  return 'localhost';
}
