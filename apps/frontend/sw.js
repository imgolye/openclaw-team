const CACHE_NAME = "mission-control-v3";
const LOCAL_HOSTNAMES = new Set(["localhost", "127.0.0.1", "::1"]);
const SHOULD_DISABLE = LOCAL_HOSTNAMES.has(self.location.hostname);
const APP_SHELL = [
  "/manifest.webmanifest",
  "/app-icon.svg",
];
const APP_SHELL_SET = new Set(APP_SHELL);

self.addEventListener("install", (event) => {
  if (SHOULD_DISABLE) {
    self.skipWaiting();
    return;
  }
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)),
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  if (SHOULD_DISABLE) {
    event.waitUntil(
      caches.keys()
        .then((keys) => Promise.all(keys.map((key) => caches.delete(key))))
        .then(() => self.registration.unregister())
        .then(() => self.clients.claim()),
    );
    return;
  }
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))),
    ),
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  if (SHOULD_DISABLE) {
    return;
  }
  const request = event.request;
  if (request.method !== "GET") {
    return;
  }

  const url = new URL(request.url);

  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/events")) {
    return;
  }

  if (url.pathname.startsWith("/assets/")) {
    return;
  }

  if (!APP_SHELL_SET.has(url.pathname)) {
    return;
  }

  event.respondWith(
    caches.match(request).then((cached) => {
      if (cached) {
        return cached;
      }
      return fetch(request).then((response) => {
        if (!response || response.status !== 200) {
          return response;
        }
        const clone = response.clone();
        caches.open(CACHE_NAME).then((cache) => {
          cache.put(request, clone);
        });
        return response;
      });
    }),
  );
});
