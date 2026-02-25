self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open("seguritec-v1").then((cache) =>
      cache.addAll(["/login", "/nueva", "/historial", "/static/manifest.json"])
    )
  );
});

self.addEventListener("fetch", (event) => {
  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request))
  );
});