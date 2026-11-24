# kiln-microspool

A spool inventory that runs on the printer itself: material, colour, brand
and grams left for each spool. A Kiln OS plugin around
[microspool](https://github.com/sntgl/microspool) by sntgl, a small server
that speaks the [Spoolman](https://github.com/Donkie/Spoolman) API, so
Moonraker counts the filament each print uses and Fluidd, HelixScreen and
[kiln-spoolman](https://github.com/djvalic/kiln-spoolman) work with it the
way they work with Spoolman.

Real Spoolman needs far more memory than a Kiln OS printer can spare
(about 60-100 MB on the printer, with a part in Rust that isn't built for
its MIPS processor). microspool is one small program: about 130 KB, well
under 1 MB of memory. If you already run Spoolman on another computer,
keep using that instead (Moonraker's `[spoolman] server:`); this plugin is
for printers without one.

**Status: in development, not in the Kiln OS catalog yet.**

## With the IFS: also install Spoolman (kiln-spoolman)

microspool keeps the inventory; Moonraker takes each print's filament off
the *active* spool. With the IFS, the active spool has to follow the slot
in use, and that is what [kiln-spoolman](https://github.com/djvalic/kiln-spoolman)
does (`spoolman` in the Kiln OS catalog): install it from Settings >
Plugins on the touch screen (or `INSTALL_PLUGIN NAME=spoolman ENABLE=1`),
then link each slot to its spool (the slot's spool picker on the touch
screen, or `SPOOLMAN_SET_SLOT SLOT=1 SPOOL=3`). Each print then counts
against the spool in the slot it used, and a linked slot takes its spool's
material and colour.

Without kiln-spoolman, microspool still works: pick the active spool in
Fluidd or on the touch screen, as on a printer with one spool. On a printer
with the IFS, Fluidd shows a reminder until kiln-spoolman is on.

## What you get

- Fluidd's spool panel, HelixScreen's Spoolman screens and kiln-spoolman's
  IFS slot links, with the usage of every print taken off the spool.
- A web page for the inventory at `http://<printer>:7912` (Fluidd's
  "Manage spools" opens it): add vendors, filaments and spools, set grams
  left, archive empty spools.
- The data in one file, `/home/klipper/printer_data/microspool/microspool.json`
  (on the data partition, kept by Kiln OS updates), with the previous
  version next to it as `microspool.json.bak`.

## Install

Until it is in the Kiln OS catalog, clone it once over SSH, as user
`klipper` so Fluidd's Software Updates can keep it up to date:

```
cd /data/ad5x/plugins
su klipper -s /bin/sh -c 'git clone https://github.com/djvalic/kiln-microspool.git microspool'
```

Then switch it on: Settings > Plugins on the touch screen, or
`ENABLE_PLUGIN NAME=microspool` in Fluidd's console. Moonraker restarts
(a few seconds) and starts microspool; Fluidd then lists kiln-microspool
under Software Updates.

With IFS slots: also enable kiln-spoolman (`spoolman` in the catalog) and
link each slot to its spool (`SPOOLMAN_SET_SLOT SLOT=1 SPOOL=3`, or the
slot's spool picker on the touch screen).

## Settings

In your `moonraker.conf`, a `[microspool]` section of your own changes these
(the plugin's section comes after yours, so set only what you change):

| Option | Default | |
|---|---|---|
| `host` | `0.0.0.0` | `127.0.0.1` keeps the web page off the network; Fluidd's and HelixScreen's spool panels still work |
| `port` | `7912` | |
| `data_file` | `/home/klipper/printer_data/microspool/microspool.json` | |
| `binary` | the plugin's `bin/microspool` | |
| `extra_args` | | more microspool options |

`GET /server/microspool/status` shows whether it runs, its restarts and
the last error.

If your own `moonraker.conf` already has a `[spoolman] server:` (Spoolman on
another computer), the plugin's `[spoolman]` replaces it while the plugin is
on, and Fluidd shows a warning saying so. Use one or the other.

## Security

Like Fluidd on Kiln OS, the web page has no login: anyone on your network
can change the inventory (nothing else; it can't reach files or run
anything). Kiln's fork limits connections, sizes and time per client, and
refuses requests from other web sites, so a device or a web page on the
network can't lock Moonraker out of it. Use `host: 127.0.0.1` if your
network isn't trusted.

## How it runs

The plugin's Moonraker component starts `bin/microspool` as user
`klipper`, at low priority and first in line if memory runs out, restarts
it if it stops, and stops it (it saves first) when Moonraker stops or the
plugin is switched off.

`bin/microspool` is built from this repository's source with Kiln OS's own
toolchain (`tools/build_mipsel.sh`; `bin/SOURCES` says from which commit).

## Licence and credits

- microspool (`microspool.c`, `ui/`, `tools/embed_ui.py`, the tests):
  MIT, by sntgl (`LICENSE`); its own README is `README-microspool.md`.
  Kiln's changes are listed in `CHANGELOG.md`.
- cJSON (`vendor/`): MIT, Dave Gamble and contributors.
- The Moonraker component (`moonraker/components/microspool.py`): GPLv3,
  like Moonraker.
