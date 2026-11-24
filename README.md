# microspool

Track your 3D printing filament spools — material, colour and grams left — **on the printer itself**.
microspool speaks the [Spoolman](https://github.com/Donkie/Spoolman) REST API, so Moonraker subtracts
filament usage while you print and Fluidd, Mainsail and HelixScreen show your spools as usual.

A single static C binary: **~106 KB on disk, ~0.2 MB RAM**. No Python, no database, no second host.
That is the point: a printer like the FlashForge Adventurer 5M running
[Z-Mod](https://github.com/ghzserg/zmod) has 128 MB of RAM for Klipper, Moonraker and the touchscreen —
nothing left over for a filament inventory service, so this one had to be tiny.

It implements the part of the Spoolman API that Moonraker and the interfaces actually call:
`vendor` / `filament` / `spool` CRUD, `spool/{id}/use`, `info`, `health`, materials / locations /
lot numbers and the `/api/v1/spool` WebSocket. The inventory lives in one JSON file. A built-in web UI
(~8 KB gzipped) is served at `http://<host>:7912/` when started with `-l 0.0.0.0` — there is no auth, so
keep it on a trusted network; `-U` disables it. No SpoolmanDB mirror, no multi-printer inventory: if your
host can run Spoolman, run Spoolman. Independent project, not affiliated with it.

## Status

- Moonraker `[spoolman]` — tested on a real AD5M (connection, proxy, usage reports).
- HelixScreen — should work (same API calls), on-device check pending.
- Fluidd — tested on a real printer (spool panel, spool selection). Mainsail — not tested.
- Fluidd's “Manage spools” button opens the built-in UI even with `server: http://127.0.0.1:7912` —
  it swaps a loopback host for the printer's address. Keep the loopback setting; only the UI port is on the LAN.

## Build

```sh
make release   # build/microspool-{armv7,aarch64,x86_64}, static musl (needs zig)
make test      # needs uv
```

## Use

```sh
microspool -l 127.0.0.1 -p 7912 -d microspool.json
```

```ini
# moonraker.conf
[spoolman]
server: http://127.0.0.1:7912
```

AD5M / Z-Mod install: [contrib/zmod/INSTALL.md](contrib/zmod/INSTALL.md).

### On the LAN (`-l 0.0.0.0`)

There is still no login, but hostile LAN clients and web pages are kept in check:

- Local clients (127.0.0.1 or the machine's own address, i.e. Moonraker) always get a connection slot;
  LAN clients share 28 of the 32, at most 8 per address and 4 websockets.
- Each HTTP request must arrive and be answered within 10 s; websockets silent for 3 ping intervals
  (`-k`, default 20 s) are closed; a client that does not read its websocket is dropped.
- The `Host` header must be an IP address, `localhost`, the machine's hostname or `<hostname>.local`
  (DNS rebinding). Allow other names with `-H name` (repeatable, `-H '*'` allows any).
- Writes and websockets from a browser must come from the same origin as the page; allow another origin
  with `-o http://name[:port]`. Clients without an `Origin` header (Moonraker, curl) are not affected.
- Request bodies must be `Content-Type: application/json`, valid UTF-8, finite numbers.
- LAN clients can save at most about once per second after a burst of 5 writes.

### Data file

Create/patch/delete answer only after the change is on disk (500 and nothing changed if the write fails).
Usage reports are written within 5 s, on `SIGHUP` and on exit. The previous version is kept as
`<datafile>.bak`; if the main file is damaged, microspool starts from the `.bak`, keeps the damaged file as
`<datafile>.corrupt` and says so loudly in its log. If both are damaged it refuses to start.

## License

MIT. Bundles [cJSON](https://github.com/DaveGamble/cJSON) (MIT).
