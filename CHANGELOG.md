# Changelog

## 0.2.1-kiln1 (unreleased): hardening for Kiln OS

Fork of microspool 0.2.1 for Kiln OS, where the web UI is reachable on the LAN without a login.

Connections
- 32 connection slots instead of 8. Local clients (127.0.0.1 or the machine's own address, so
  Moonraker) can always connect: LAN clients share 28 slots, at most 8 per address and 4 websockets.
  A new LAN client pushes out the oldest LAN connection that has not finished its request after 1 s.
- One 10 s deadline per HTTP request, from connect to answer sent; trickling bytes no longer keeps a
  connection open (slowloris).
- Websockets are closed after 3 ping intervals without any data from the client; Moonraker answers
  pings, so it stays connected. The ping timer now really uses `-k`.
- Send buffers are capped (256 KB unsent per websocket client, 8 MB in total): a client that never
  reads is dropped instead of growing memory. Large buffers are freed when a connection ends.

Requests
- `Host` must be an IP address, `localhost`, the hostname or `<hostname>.local`, or a name given with
  the new `-H` flag (DNS rebinding), otherwise 403.
- Writes and websocket upgrades from a browser must come from the page's own origin or one given with
  the new `-o` flag, otherwise 403. Clients without `Origin` (Moonraker) are not affected.
- POST/PATCH/PUT bodies must be `Content-Type: application/json` (415), valid UTF-8 (400), and numbers
  must be finite and at most 1e9 (`1e999` used to become infinity).
- `extra` values must be strings, like in Spoolman.
- The web UI is sent with a strict Content-Security-Policy (only its own script, by hash),
  `X-Frame-Options: DENY` and `X-Content-Type-Options: nosniff`. The Moonraker badge still works.

Data file
- Create, change and delete answer only after the data is on disk; if writing fails (disk full), the
  change is undone and the answer is 500. A failed save is retried every second.
- Usage reports are written within 5 s (was 30 s), on `SIGHUP` and on exit.
- LAN clients can make about one save per second after a burst of 5, so a write loop cannot wear out the
  eMMC; local clients are not limited.
- The previous version is kept as `<datafile>.bak`. A damaged data file falls back to it (with a loud
  warning; the damaged file is kept as `<datafile>.corrupt`). If both are damaged, microspool refuses to
  start and leaves both alone. Only a missing file means "start empty"; other read errors are fatal.
- Temp files are created safely (no symlink following), stale ones are removed at start, and the
  directory is synced after each save.
- Ids in the data file are checked (duplicates, out of range).

API
- `limit` and `offset` on the vendor, filament and spool lists (`X-Total-Count` is the total), and the
  `vendor.id` filter on filaments, as HelixScreen uses them.
- `GET /api/v1/field/<entity>` returns `[]`; field definitions are acknowledged but not stored.
- Everything under `/api/v1/external/` returns `[]`, including `filament/search`.
- `/api/v1/info` no longer shows the absolute data path.
- Websocket paths work like Spoolman's (`/api/v1/`, `/api/v1/<resource>`, `/api/v1/<resource>/<id>`),
  and vendor and filament changes send events too.

Build
- cJSON updated to 1.7.19 (`vendor/README` records the source).
- Built with 64-bit `time_t`/`off_t` on 32-bit glibc targets; fixed the WebSocket frame length for
  frames over 64 KB on 32-bit CPUs, a gcc 14 warning, and small leaks when loading the data file.

## 0.2.1 — 2026-09-17

- Web UI: clearing the location/comment fields now actually clears them (empty values were skipped).
- `-k <seconds>` sets the server-side WebSocket ping interval (`-k 0` disables it); the default stays 20 s.
- README: Fluidd verified on a real printer; note that its “Manage spools” button resolves a loopback
  Spoolman host to the printer's address by itself.

## 0.2.0 — 2026-09-15

- Built-in web UI (single `ui/index.html`, gzip-embedded in the binary): spool cards with
  search/archived filter, quick actions (set/use weight, location/comment, archive, delete,
  `extra` editor), add/edit spool with inline filament+brand creation, filament/brand list
  management, optional Moonraker active-spool badge. Served at `GET /`/`/index.html`
  (`Content-Encoding: gzip`; `406` if the client doesn't advertise `Accept-Encoding: gzip`);
  `-U` disables it (`404`). Adds armv7 ~10.8 KB.
- `GET /api/v1/setting/currency` (for Fluidd); any other `/api/v1/setting/*` → 404.
- `GET /api/v1/material`, `/api/v1/location`, `/api/v1/lot-number` — sorted unique values
  in use, for Fluidd/Mainsail/HelixScreen pickers.

## 0.1.0 — 2026-09-15

First public version.

- Spoolman API subset: vendor / filament / spool CRUD, `spool/{id}/use`, info, health, empty external catalog.
- WebSocket `/api/v1/spool` with ping/pong and spool events (required by Moonraker `[spoolman]`).
- Atomic JSON persistence with deferred writes for usage reports.
- Static musl builds for armv7, aarch64, x86_64 (~95–105 KB).
- Verified on FlashForge AD5M (Z‑Mod 1.7.3): Moonraker connects, proxy and usage reporting work, RSS 124–224 KB.
