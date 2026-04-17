// Orion PWA Service Worker v1.0
const CACHE_NAME = 'orion-v1';
const ASSETS = ['/mobile.html', '/manifest.json'];

// Sons
const SOUNDS = {
  pix_generated: 'https://e-volutionn.com/wp-content/uploads/2026/04/ding-sound-effect_2_Sfdd45L.mp3',
  payment: 'https://e-volutionn.com/wp-content/uploads/2026/04/u_byub5wd934-cashier-quotka-chingquot-sound-effect-129698.mp3'
};

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE_NAME).then(c => c.addAll(ASSETS)).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(clients.claim());
});

self.addEventListener('fetch', e => {
  // Cache apenas assets estáticos
  if (e.request.url.includes('/mobile.html') || e.request.url.includes('/manifest.json')) {
    e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
  }
});

// Notificações push
self.addEventListener('push', e => {
  if (!e.data) return;
  const data = e.data.json();
  const options = {
    body: data.body,
    icon: 'https://e-volutionn.com/wp-content/uploads/2026/04/Gemini_Generated_Image_b05m0ob05m0ob05m.jpeg',
    badge: 'https://e-volutionn.com/wp-content/uploads/2026/04/Gemini_Generated_Image_b05m0ob05m0ob05m.jpeg',
    tag: data.tag || 'orion',
    requireInteraction: data.type === 'instance_down',
    vibrate: data.type === 'instance_down' ? [300, 100, 300, 100, 300] : [200, 50, 200],
    data: { url: data.url || '/mobile.html', type: data.type }
  };
  e.waitUntil(self.registration.showNotification(data.title, options));
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = e.notification.data?.url || '/mobile.html';
  e.waitUntil(clients.matchAll({ type: 'window' }).then(list => {
    const existing = list.find(c => c.url.includes('mobile.html'));
    if (existing) return existing.focus();
    return clients.openWindow(url);
  }));
});

// Mensagens do cliente
self.addEventListener('message', e => {
  if (e.data?.type === 'SKIP_WAITING') self.skipWaiting();
});
