/* Culé Live service worker: lets the app open instantly and work without internet,
   and shows match notifications (sent by the push server, push-worker.js) even when the app is closed.
   The app itself (index.html) is always fetched fresh when online, so new uploads show up
   straight away; the saved copy is only used when there's no connection.
   Match data isn't stored here: the app saves that itself. */
const CACHE = 'cule-live-v2';
const SHELL = ['./', './index.html', './manifest.webmanifest', './icon-192.png', './icon-512.png', './icon-maskable-512.png', './icon-180.png', './badge-96.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const req = e.request, url = new URL(req.url);
  if (req.method !== 'GET' || url.origin !== location.origin) return; // data, photos and videos go straight to the internet
  if (req.mode === 'navigate' || url.pathname.endsWith('/') || url.pathname.endsWith('index.html')) {
    // the app page: newest version when online, saved copy when offline
    e.respondWith(fetch(req).then(res => { const copy = res.clone(); caches.open(CACHE).then(c => c.put('./index.html', copy)); return res; })
      .catch(() => caches.match('./index.html')));
    return;
  }
  e.respondWith(caches.match(req).then(hit => hit || fetch(req))); // icons and the manifest
});

// A match alert arrived: show it (phones require every alert to be shown)
self.addEventListener('push', e => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch (err) { d = { body: e.data ? e.data.text() : '' }; }
  e.waitUntil(self.registration.showNotification(d.title || 'Culé Live', {
    body: d.body || '',
    icon: 'icon-192.png',
    badge: 'badge-96.png', // the small white ball in the Android status bar
    tag: d.tag || undefined, // the same alert never shows twice
    data: { url: d.url || './' },
    timestamp: d.ts || Date.now(),
    lang: d.lang || undefined,
    dir: d.lang === 'ar' ? 'rtl' : 'auto'
  }));
});

// Tapping an alert opens the match (or brings the open app to the front and goes there)
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = new URL((e.notification.data && e.notification.data.url) || './', self.registration.scope);
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
    const open = list.find(c => c.url.startsWith(self.registration.scope));
    if (open) { open.postMessage({ open: url.hash }); return open.focus(); }
    return self.clients.openWindow(url.href);
  }));
});
