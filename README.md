# TransportX — Live Location Tracker

Real-time GPS tracker. Phones ping their location to your laptop server. Watch movement live on a map.

---

## How it works

```
iPhone / Samsung  →  WebSocket ping  →  Node.js server  →  WebSocket broadcast  →  Browser map viewer
```

---

## Setup (one-time)

### 1. Install Node.js
Download from https://nodejs.org (LTS version). Run the installer.

### 2. Install dependencies
Open a terminal in this folder (`location-tracker/`) and run:
```
npm install
```

### 3. Start the server
```
npm start
```

You'll see output like:
```
  🖥   Viewer  →  http://localhost:3000/viewer/
  📱  Tracker  →  http://192.168.1.45:3000/tracker/
```

### 4. Open the map viewer
On your laptop, open: **http://localhost:3000/viewer/**

---

## Setting up the phones

### Android (Samsung Galaxy S26 Ultra)
Chrome handles this perfectly over your local network:

1. Make sure the phone is on the **same WiFi** as the laptop
2. Open Chrome on the phone
3. Navigate to the Tracker URL shown in the terminal, e.g.:
   `http://192.168.1.45:3000/tracker/`
4. Enter a device name (e.g. "Diamond Samsung")
5. Choose your ping interval
6. Tap **Start Tracking**
7. To install as an app: tap the Chrome menu → **"Add to Home screen"**

### iPhone 15 Pro Max
⚠️ iOS requires **HTTPS** for GPS access. You need ngrok (free):

**Step 1 — Install ngrok** (one-time)
```
npm install -g ngrok
```
or download from https://ngrok.com/download

**Step 2 — Start ngrok** (every session, in a second terminal)
```
npx ngrok http 3000
```
This gives you a URL like: `https://abc123.ngrok-free.app`

**Step 3 — On the iPhone**
1. Open Safari
2. Go to: `https://abc123.ngrok-free.app/tracker/`
3. Enter a device name (e.g. "Diamond iPhone")
4. Tap **Start Tracking**
5. To install: tap the Share button → **"Add to Home Screen"**

> The server still runs on your laptop. ngrok just creates a secure tunnel so iOS can access it.

---

## Ping intervals

| Interval | Use case             | Battery impact |
|----------|----------------------|----------------|
| 10s      | Testing / debugging  | High           |
| 30s      | Active monitoring    | Medium-high    |
| 1 min    | General use          | Medium         |
| 2 min    | Normal tracking      | Low-medium     |
| 5 min    | Occasional check-in  | Low            |
| 10 min   | Battery saver        | Very low       |

---

## Map viewer features

- **Live markers** — each device has a unique colour and initials badge
- **Movement trail** — dashed line showing the path taken (last 200 points)
- **Accuracy circle** — shows GPS precision around the device
- **Device panel** — shows name, coordinates, speed, accuracy, last seen
- **Click to follow** — click a device card to auto-pan the map as it moves
- **3 map styles** — Standard, Night, Satellite

---

## File structure

```
location-tracker/
├── server.js              ← Node.js server (Express + WebSocket)
├── package.json
├── README.md
└── public/
    ├── tracker/
    │   ├── index.html     ← Phone PWA app
    │   ├── manifest.json  ← PWA manifest (enables "Add to Home Screen")
    │   └── sw.js          ← Service worker (offline support)
    └── viewer/
        └── index.html     ← Live map viewer (open on laptop)
```

---

## Troubleshooting

**"Location access denied" on Android**
→ Go to Chrome Settings → Site Settings → Location → allow the site

**"Location access denied" on iPhone**
→ Settings → Safari → Location → Allow

**Phone can't reach the server**
→ Check both devices are on the same WiFi
→ Check Windows Firewall isn't blocking port 3000:
  `netsh advfirewall firewall add rule name="TX Tracker" dir=in action=allow protocol=TCP localport=3000`

**Map viewer shows "Disconnected"**
→ Make sure `npm start` is still running in the terminal
