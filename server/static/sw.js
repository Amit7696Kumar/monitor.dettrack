const SW_VERSION = "det-static-v4";
const STATIC_CACHE = SW_VERSION;
const STATIC_ASSETS = [
  "/static/app.css",
  "/static/app.js",
  "/static/tokens.css",
  "/static/manifest.webmanifest",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png",
  "/static/icons/icon-192-maskable.png",
  "/static/icons/apple-touch-icon.png",
  "/static/icons/favicon-32.png",
  "/static/icons/favicon-16.png",
  "/static/icons/newicon.png",
  "/static/offline.html",
];

function isStaticAsset(url) {
  return url.origin === self.location.origin && url.pathname.startsWith("/static/");
}

function isCriticalStatic(url) {
  return (
    url.origin === self.location.origin &&
    (url.pathname === "/static/app.css" || url.pathname === "/static/app.js")
  );
}

function isSensitiveOrDynamic(url) {
  if (url.origin !== self.location.origin) return false;
  const path = url.pathname;
  return (
    path.startsWith("/api/") ||
    path.startsWith("/admin") ||
    path.startsWith("/coadmin") ||
    path.startsWith("/tasks") ||
    path.startsWith("/uploads/") ||
    path.startsWith("/download/") ||
    path.startsWith("/alerts/") ||
    path.startsWith("/chat") ||
    path.startsWith("/login") ||
    path.startsWith("/logout")
  );
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.map((key) => {
          if (key !== STATIC_CACHE) return caches.delete(key);
          return Promise.resolve();
        })
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);

  // Do not cache time-sensitive or authenticated dynamic routes.
  if (isSensitiveOrDynamic(url)) {
    event.respondWith(fetch(req));
    return;
  }

  // Cache-first for static versioned assets.
  if (isStaticAsset(url)) {
    // Keep auth/dashboard styles/scripts fresh to avoid stale UI after deploy.
    if (isCriticalStatic(url)) {
      event.respondWith(
        fetch(req)
          .then((res) => {
            const clone = res.clone();
            caches.open(STATIC_CACHE).then((cache) => cache.put(req, clone));
            return res;
          })
          .catch(() => caches.match(req))
      );
      return;
    }

    event.respondWith(
      caches.match(req).then((cached) => {
        if (cached) return cached;
        return fetch(req).then((res) => {
          const clone = res.clone();
          caches.open(STATIC_CACHE).then((cache) => cache.put(req, clone));
          return res;
        });
      })
    );
    return;
  }

  // Network-first for navigation with safe offline fallback.
  if (req.mode === "navigate") {
    event.respondWith(
      fetch(req).catch(() => caches.match("/static/offline.html"))
    );
    return;
  }
});
