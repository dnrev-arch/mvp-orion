// Orion PWA Service Worker v2.0
const CACHE_NAME = 'orion-v2';

self.addEventListener('install', e => {
    self.skipWaiting();
});

self.addEventListener('activate', e => {
    e.waitUntil(clients.claim());
});

// Push notifications
self.addEventListener('push', e => {
    if (!e.data) return;
    let data;
    try { data = e.data.json(); } catch { data = { title: 'Orion', body: e.data.text() }; }

    const icons = {
        pix_generated: '💰',
        payment: '✅',
        card: '💳',
        instance_down: '🔴',
        instance_up: '🟢'
    };

    const options = {
        body: data.body,
        icon: 'https://e-volutionn.com/wp-content/uploads/2026/04/Gemini_Generated_Image_b05m0ob05m0ob05m.jpeg',
        badge: 'https://e-volutionn.com/wp-content/uploads/2026/04/Gemini_Generated_Image_b05m0ob05m0ob05m.jpeg',
        tag: data.type || 'orion-notif',
        requireInteraction: data.type === 'instance_down',
        vibrate: data.type === 'instance_down' ? [400, 100, 400, 100, 400] : [200, 50, 200],
        silent: false,
        data: { url: '/mobile.html', type: data.type, timestamp: data.timestamp }
    };

    e.waitUntil(self.registration.showNotification(data.title, options));
});

self.addEventListener('notificationclick', e => {
    e.notification.close();
    const url = e.notification.data?.url || '/mobile.html';
    e.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
            const orionClient = list.find(c => c.url.includes('mobile.html') || c.url.includes(self.location.origin));
            if (orionClient) return orionClient.focus();
            return clients.openWindow(url);
        })
    );
});

self.addEventListener('message', e => {
    if (e.data?.type === 'SKIP_WAITING') self.skipWaiting();
});
