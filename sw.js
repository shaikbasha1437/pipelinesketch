/* PipelineSketch service worker: network-first for app files, per-file cache, offline fallback.
   Bump CACHE on each release; old caches are removed on activate. */
const CACHE = 'pipelinesketch-v1';
const ASSETS = ['./', './index.html', './styles.css', './manifest.webmanifest', './icons/icon.svg', './icons/icon-192.png', './icons/icon-512.png',
  './js/cron.js', './js/nodes.js', './js/core.js', './js/gen-common.js', './js/gen-py.js', './js/gen-airflow.js', './js/gen-dagster.js', './js/gen-prefect.js',
  './js/gen-dbt.js', './js/gen-docs.js', './js/import.js', './js/templates.js', './js/zip.js', './js/highlight.js', './js/modal.js', './js/app.js'];
self.addEventListener('install', (e) => {
  self.skipWaiting();
  // Cache each file on its own so one failed request can't break install.
  e.waitUntil(caches.open(CACHE).then((c) => Promise.all(ASSETS.map((a) => c.add(new Request(a, { cache: 'reload' })).catch(() => {})))));
});
self.addEventListener('activate', (e) => e.waitUntil(
  caches.keys().then((keys) => Promise.all(keys.filter((k) => k.startsWith('pipelinesketch-') && k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())
));
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || new URL(req.url).origin !== location.origin) return;
  // no-cache: always revalidate with the server, so a new release shows up on the next visit.
  e.respondWith(fetch(req, { cache: 'no-cache' }).then((res) => {
    if (res && res.ok && res.type === 'basic') { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
    return res;
  }).catch(() => caches.match(req, { ignoreSearch: true })
    .then((r) => r || (req.mode === 'navigate' ? caches.match('./index.html').then((i) => i || caches.match('./')) : undefined))
    .then((r) => r || Response.error())));
});
