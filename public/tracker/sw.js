/* TransportX Tracker — Service Worker
   Enables "Add to Home Screen" / PWA install on Android & iOS.
   Caches the app shell so it loads even when the server is briefly
   unreachable (location sending will resume when connected). */

const CACHE = 'tx-tracker-v1';
const SHELL = ['/tracker/', '/tracker/index.html', '/tracker/manifest.json'];

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  // Only cache GET requests for the app shell; let WS and API calls pass through
  if (e.request.method !== 'GET') return;
  if (e.request.url.includes('/ws') || e.request.url.includes('/api/')) return;

  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request))
  );
});
