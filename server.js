/**
 * TransportX v2 Server
 * - Raw WS (/ws): backward compat with existing tracker PWA
 * - Socket.IO (/socket.io): new driver & customer pages
 * - REST API: location, routing, sessions, chat
 */
const express  = require('express');
const http     = require('http');
const WebSocket = require('ws');
const { Server: SocketIO } = require('socket.io');
const path     = require('path');
const os       = require('os');
const fs       = require('fs');
const crypto   = require('crypto');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: '/ws' });
const io     = new SocketIO(server, { cors: { origin: '*' }, path: '/socket.io' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// Logs (server-side only, never shown in product UI)
const LOGS_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR);
function appendLog(deviceId, entry) {
  try { fs.appendFileSync(path.join(LOGS_DIR, deviceId + '.jsonl'), JSON.stringify(entry) + '\n'); } catch (_) {}
}
function readLog(deviceId) {
  try {
    const file = path.join(LOGS_DIR, deviceId + '.jsonl');
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean)
      .map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  } catch (_) { return []; }
}

// In-memory stores
const devices        = {};
const viewers        = new Set();
const kickedSessions = new Set();
const onlineDrivers  = {}; // driverId -> { name, lat, lng, heading, speed, lastSeen, socketId }
const customerSessions = {}; // sessionId -> { createdAt, lastActive, expiresAt }
const conversations  = {}; // convId -> { driverId, sessionId, status, messages }
const SESSION_TTL    = 24 * 60 * 60 * 1000;
let colorCursor = 0;
const COLORS = ['#6c63ff','#f59e0b','#22c55e','#ef4444','#06b6d4','#f97316'];
function nextColor() { return COLORS[colorCursor++ % COLORS.length]; }

// Legacy WS helpers
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const ws of viewers) { if (ws.readyState === WebSocket.OPEN) ws.send(data); }
}
function deviceSummary(id) {
  const d = devices[id];
  return { deviceId: id, name: d.name, color: d.color, online: d.online, lastSeen: d.lastSeen, sessionId: d.sessionId, trail: d.trail, lastLocation: d.trail.at(-1) ?? null };
}
function ensureDevice(deviceId, name, sessionId) {
  if (!devices[deviceId]) {
    devices[deviceId] = { name: name || 'Unknown Device', color: nextColor(), online: true, lastSeen: Date.now(), sessionId: sessionId || null, trail: [] };
    appendLog(deviceId, { type: 'session_start', deviceId, name: devices[deviceId].name, color: devices[deviceId].color, sessionId, timestamp: Date.now() });
    broadcast({ type: 'device_online', ...deviceSummary(deviceId) });
  } else {
    const dev = devices[deviceId];
    const wasOffline = !dev.online;
    dev.name = name || dev.name; dev.online = true; dev.lastSeen = Date.now(); dev.sessionId = sessionId || dev.sessionId;
    if (wasOffline) { appendLog(deviceId, { type: 'session_start', deviceId, name: dev.name, color: dev.color, sessionId, timestamp: Date.now() }); broadcast({ type: 'device_online', ...deviceSummary(deviceId) }); }
  }
}
function recordPoint(deviceId, point) {
  const dev = devices[deviceId];
  dev.trail.push(point); if (dev.trail.length > 500) dev.trail.shift();
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
function validSession(sessionId) {
  const s = customerSessions[sessionId];
  if (!s) return false;
  if (Date.now() > s.expiresAt) { delete customerSessions[sessionId]; return false; }
  s.lastActive = Date.now(); return true;
}

// Socket.IO — new driver + customer pages
io.on('connection', (socket) => {
  socket.on('driver:online', ({ driverId, name, lat, lng }) => {
    if (!driverId || !name) return;
    onlineDrivers[driverId] = { name, lat: lat || 0, lng: lng || 0, lastSeen: Date.now(), socketId: socket.id };
    socket.data.driverId = driverId; socket.data.role = 'driver';
    io.emit('driver:online', { driverId, name, lat: lat || 0, lng: lng || 0 });
  });
  socket.on('driver:location', ({ driverId, lat, lng, heading, speed }) => {
    const d = onlineDrivers[driverId]; if (!d) return;
    d.lat = lat; d.lng = lng; d.heading = heading ?? null; d.speed = speed ?? null; d.lastSeen = Date.now();
    if (devices[driverId]) recordPoint(driverId, { lat, lng, heading, speed, timestamp: Date.now() });
    io.emit('driver:location', { driverId, lat, lng, heading, speed });
  });
  socket.on('driver:offline', ({ driverId }) => {
    delete onlineDrivers[driverId]; io.emit('driver:offline', { driverId });
  });
  socket.on('customer:join', () => {
    socket.data.role = 'customer';
    const drivers = Object.entries(onlineDrivers).map(([id, d]) => ({ driverId: id, name: d.name, lat: d.lat, lng: d.lng }));
    socket.emit('drivers:initial', drivers);
  });
  socket.on('chat:join', ({ conversationId }) => { socket.join('conv:' + conversationId); });
  socket.on('chat:message', ({ conversationId, body, senderType, senderId }) => {
    const conv = conversations[conversationId]; if (!conv || conv.status !== 'open') return;
    const msg = { id: crypto.randomUUID(), body, senderType, senderId, sentAt: Date.now() };
    conv.messages.push(msg);
    io.to('conv:' + conversationId).emit('chat:message', { conversationId, ...msg });
  });
  socket.on('disconnect', () => {
    const { role, driverId } = socket.data || {};
    if (role === 'driver' && driverId && onlineDrivers[driverId]) {
      // Don't remove immediately — driver may just be navigating between pages.
      // TTL cleanup will expire them if they don't reconnect within 3 minutes.
      onlineDrivers[driverId].socketId = null;
      onlineDrivers[driverId].disconnectedAt = Date.now();
    }
  });
});

// Auto-expire drivers that haven't sent a location update in 3 minutes
const DRIVER_EXPIRE_MS = 3 * 60 * 1000;
setInterval(() => {
  const now = Date.now();
  for (const [driverId, d] of Object.entries(onlineDrivers)) {
    if (now - d.lastSeen > DRIVER_EXPIRE_MS) {
      delete onlineDrivers[driverId];
      io.emit('driver:offline', { driverId });
    }
  }
}, 30000);

// Legacy raw WS handler
wss.on('connection', (ws) => {
  let role = null, deviceId = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'register_viewer') {
      role = 'viewer'; viewers.add(ws);
      ws.send(JSON.stringify({ type: 'initial_state', devices: Object.keys(devices).map(deviceSummary) }));
    } else if (msg.type === 'register_tracker') {
      role = 'tracker'; deviceId = msg.deviceId;
      ensureDevice(deviceId, msg.name, msg.sessionId || null);
      ws.send(JSON.stringify({ type: 'registered', deviceId, color: devices[deviceId].color, name: devices[deviceId].name, sessionId: devices[deviceId].sessionId }));
    } else if (msg.type === 'location') {
      if (!deviceId || !devices[deviceId]) return;
      recordPoint(deviceId, { lat: msg.lat, lng: msg.lng, accuracy: msg.accuracy ?? null, speed: msg.speed ?? null, heading: msg.heading ?? null, timestamp: msg.timestamp ?? Date.now() });
    }
  });
  ws.on('close', () => { if (role === 'viewer') viewers.delete(ws); else if (role === 'tracker' && deviceId) markOffline(deviceId); });
  ws.on('error', () => {});
});
const heartbeat = setInterval(() => { wss.clients.forEach(ws => { if (!ws.isAlive) { ws.terminate(); return; } ws.isAlive = false; ws.ping(); }); }, 25000);
wss.on('close', () => clearInterval(heartbeat));

// REST — legacy
app.get('/api/devices', (_req, res) => {
  res.json(Object.keys(devices).map(id => ({ deviceId: id, name: devices[id].name, color: devices[id].color, online: devices[id].online, lastSeen: devices[id].lastSeen, sessionId: devices[id].sessionId, pointCount: devices[id].trail.length, lastLocation: devices[id].trail.at(-1) ?? null })));
});
app.get('/api/logs', (_req, res) => {
  if (!fs.existsSync(LOGS_DIR)) return res.json([]);
  const files = fs.readdirSync(LOGS_DIR).filter(f => f.endsWith('.jsonl'));
  const summaries = files.map(file => {
    const deviceId = file.replace('.jsonl', '');
    const entries = readLog(deviceId);
    const locs = entries.filter(e => e.type === 'location');
    const sessions = entries.filter(e => e.type === 'session_start');
    const last = sessions.at(-1); const dev = devices[deviceId];
    return { deviceId, name: dev?.name || last?.name || deviceId, color: dev?.color || last?.color || '#7c85a2', online: dev?.online || false, pingCount: locs.length, sessionCount: sessions.length, firstSeen: entries[0]?.timestamp || null, lastSeen: entries.at(-1)?.timestamp || null };
  });
  res.json(summaries.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0)));
});
app.get('/api/devices/:id/log', (req, res) => res.json(readLog(req.params.id)));
app.post('/api/ping', (req, res) => {
  const { deviceId, name, sessionId, lat, lng, accuracy, speed, heading, timestamp } = req.body || {};
  if (!deviceId || lat == null || lng == null) return res.status(400).json({ error: 'deviceId, lat, lng required' });
  if (sessionId && kickedSessions.has(sessionId)) return res.status(403).json({ error: 'session_kicked', message: 'Session ended. Press Start Tracking to begin a new session.' });
  ensureDevice(deviceId, name, sessionId);
  recordPoint(deviceId, { lat, lng, accuracy: accuracy ?? null, speed: speed ?? null, heading: heading ?? null, timestamp: timestamp ?? Date.now() });
  res.json({ ok: true, color: devices[deviceId].color, name: devices[deviceId].name });
});
app.post('/api/offline', (req, res) => {
  const { deviceId } = req.body || {};
  if (!deviceId) return res.status(400).json({ error: 'deviceId required' });
  markOffline(deviceId); res.json({ ok: true });
});
app.post('/api/kick/:deviceId', (req, res) => {
  const id = req.params.deviceId; const dev = devices[id];
  if (!dev) return res.status(404).json({ error: 'Device not found or not online' });
  if (dev.sessionId) kickedSessions.add(dev.sessionId);
  markOffline(id); broadcast({ type: 'device_kicked', deviceId: id });
  res.json({ ok: true, kickedSession: dev.sessionId });
});
app.post('/api/owntracks/:deviceId', (req, res) => {
  const deviceId = req.params.deviceId; const b = req.body || {};
  if (b._type !== 'location' || b.lat == null || b.lon == null) return res.json([]);
  const name = deviceId.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  ensureDevice(deviceId, name, null);
  recordPoint(deviceId, { lat: b.lat, lng: b.lon, accuracy: b.acc ?? null, speed: b.vel ?? null, heading: b.cog ?? null, timestamp: b.tst ? b.tst * 1000 : Date.now() });
  res.json([]);
});
app.delete('/api/devices/:id/trail', (req, res) => {
  const dev = devices[req.params.id];
  if (!dev) return res.status(404).json({ error: 'Not found' });
  dev.trail = []; broadcast({ type: 'trail_cleared', deviceId: req.params.id }); res.json({ ok: true });
});

// REST — new product endpoints
app.get('/api/drivers/online', (_req, res) => {
  res.json(Object.entries(onlineDrivers).map(([id, d]) => ({ driverId: id, name: d.name, lat: d.lat, lng: d.lng, heading: d.heading ?? null, lastSeen: d.lastSeen })));
});
app.get('/api/route', async (req, res) => {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: 'from and to required' });
  const [fLat, fLng] = from.split(',').map(Number);
  const [tLat, tLng] = to.split(',').map(Number);
  if ([fLat, fLng, tLat, tLng].some(isNaN)) return res.status(400).json({ error: 'Invalid coordinates' });
  try {
    const url = 'https://router.project-osrm.org/route/v1/driving/' + fLng + ',' + fLat + ';' + tLng + ',' + tLat + '?overview=full&geometries=geojson';
    const resp = await fetch(url);
    const data = await resp.json();
    if (!data.routes?.length) return res.status(404).json({ error: 'No route found' });
    const route = data.routes[0];
    const distKm = (route.distance / 1000).toFixed(1);
    const mins = Math.ceil(route.duration / 60);
    res.json({ geometry: route.geometry, distance_m: Math.round(route.distance), duration_s: Math.round(route.duration), distance_label: distKm + ' km', eta_label: mins < 60 ? mins + ' min' : Math.floor(mins/60) + 'h ' + (mins%60) + 'm' });
  } catch (err) {
    console.error('[route]', err.message);
    res.status(502).json({ error: 'Routing service unavailable' });
  }
});
app.post('/api/sessions', (_req, res) => {
  const sessionId = crypto.randomUUID();
  customerSessions[sessionId] = { createdAt: Date.now(), lastActive: Date.now(), expiresAt: Date.now() + SESSION_TTL };
  res.json({ sessionId });
});
app.get('/api/sessions/:id', (req, res) => {
  if (!validSession(req.params.id)) return res.status(404).json({ error: 'Session not found or expired' });
  res.json({ ok: true });
});
app.post('/api/conversations', (req, res) => {
  const { driverId, sessionId } = req.body || {};
  if (!driverId || !sessionId) return res.status(400).json({ error: 'driverId and sessionId required' });
  if (!validSession(sessionId)) return res.status(401).json({ error: 'Invalid or expired session' });
  const existing = Object.entries(conversations).find(([, c]) => c.driverId === driverId && c.sessionId === sessionId && c.status === 'open');
  if (existing) return res.json({ conversationId: existing[0] });
  const convId = crypto.randomUUID();
  conversations[convId] = { driverId, sessionId, status: 'open', messages: [], createdAt: Date.now() };
  res.status(201).json({ conversationId: convId });
});
app.get('/api/conversations/:id/messages', (req, res) => {
  const conv = conversations[req.params.id];
  if (!conv) return res.status(404).json({ error: 'Conversation not found' });
  res.json(conv.messages);
});

// Start
const PORT = process.env.PORT || 4747;
server.listen(PORT, '0.0.0.0', () => {
  const localIP = getLocalIP();
  console.log('\n  TransportX v2');
  console.log('  Landing   -> http://localhost:' + PORT + '/');
  console.log('  Customer  -> http://localhost:' + PORT + '/customer/');
  console.log('  Driver    -> http://localhost:' + PORT + '/driver/');
  console.log('  Tracker   -> http://' + localIP + ':' + PORT + '/tracker/');
  console.log('  Viewer    -> http://localhost:' + PORT + '/viewer/\n');
});
function getLocalIP() {
  for (const iface of Object.values(os.networkInterfaces()))
    for (const a of iface)
      if (a.family === 'IPv4' && !a.internal) return a.address;
  return 'localhost';
}
