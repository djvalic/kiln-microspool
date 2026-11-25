# kiln-microspool: runs microspool, a small Spoolman-compatible spool
# server, next to Moonraker on Kiln OS
#
# Kiln OS plugin (plugin API 1.4): Moonraker loads this component from the
# plugin's moonraker/components while the plugin is enabled, and restarts
# when the plugin is switched on or off. The component starts the plugin's
# microspool binary as a child process (user klipper, like Moonraker),
# restarts it if it stops, and stops it (SIGTERM: it saves first) when
# Moonraker stops. The plugin's moonraker.conf points [spoolman] at it.
#
# [microspool] options (all optional):
#   host        address to listen on (0.0.0.0: the web page on the LAN;
#               127.0.0.1: only Moonraker and the printer itself)
#   port        7912 (Spoolman's port; Fluidd's "Manage spools" opens it)
#   data_file   the spool inventory, one JSON file
#   binary      the microspool program
#   allow_hosts more names the web page may be opened by (comma or space
#               separated, e.g. ad5x.home, ad5x.tailnet-name.ts.net); IP
#               addresses, localhost and the printer's own hostname always
#               work (microspool -H, its DNS rebinding check)
#   extra_args  more command line options for microspool
#
# Endpoint: GET /server/microspool/status
#
# Warnings (Fluidd shows Moonraker's warnings in its notifications):
# - the printer has the IFS but kiln-spoolman ("spoolman" in the catalog)
#   is off, so Moonraker can't tell which slot's spool a print uses;
# - your own moonraker.conf names another [spoolman] server, which this
#   plugin's [spoolman] replaces while it is on.
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import asyncio
import ctypes
import logging
import os
import re
import shlex
import signal
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..common import WebRequest

PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
DEFAULT_DATA = "/home/klipper/printer_data/microspool/microspool.json"
# allow_hosts: plain DNS names only (no wildcards, ports or addresses)
HOST_NAME_RE = re.compile(
    r"^(?=.{1,253}\.?$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?$")
# Restart back-off: 2 s, doubling to 5 min; reset after 10 min of running
BACKOFF_MIN = 2.
BACKOFF_MAX = 300.
STABLE_AFTER = 600.
STOP_WAIT = 10.
# Below Moonraker's priority, and first in line for the OOM killer
# before Klipper, Moonraker and the screen
NICE = 10
OOM_SCORE_ADJ = 500
PR_SET_PDEATHSIG = 1


class Microspool:
    def __init__(self, config: ConfigHelper) -> None:
        self.server = config.get_server()
        self.host = config.get("host", "0.0.0.0")
        self.port = config.getint("port", 7912, minval=1, maxval=65535)
        # Default: printer_data/microspool/ (Moonraker's data path, which is
        # /home/klipper/printer_data on Kiln OS)
        data_path = self.server.get_app_args().get("data_path", "")
        default_data = (os.path.join(data_path, "microspool", "microspool.json")
                        if data_path else DEFAULT_DATA)
        self.data_file = config.get("data_file", default_data)
        self.binary = config.get(
            "binary", os.path.join(PLUGIN_DIR, "bin", "microspool"))
        self.allow_hosts: List[str] = []
        self.bad_hosts: List[str] = []
        for name in re.split(r"[\s,]+", config.get("allow_hosts", "")):
            if not name:
                continue
            if HOST_NAME_RE.match(name):
                self.allow_hosts.append(name.lower().rstrip("."))
            else:
                self.bad_hosts.append(name)
        self.extra_args = shlex.split(config.get("extra_args", ""))
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.task: Optional[asyncio.Task] = None
        self.closing = False
        self.started_at = 0.
        self.restarts = 0
        self.last_exit: Optional[int] = None
        self.last_error = ""
        self.data_path = data_path
        self.server.register_endpoint(
            "/server/microspool/status", ["GET"], self._handle_status)
        self.server.register_event_handler(
            "server:klippy_ready", self._on_klippy_ready)

    def _command(self) -> List[str]:
        cmd = [self.binary, "-l", self.host, "-p", str(self.port),
               "-d", self.data_file]
        for name in self.allow_hosts:
            cmd += ["-H", name]
        return cmd + self.extra_args

    async def component_init(self) -> None:
        self._check_own_spoolman()
        if self.bad_hosts:
            self.server.add_warning(
                "kiln-microspool: allow_hosts takes host names such as "
                "ad5x.home; left out: %s" % (", ".join(self.bad_hosts),),
                warn_id="microspool_allow_hosts")
        self.task = asyncio.create_task(self._supervise())

    def _check_own_spoolman(self) -> None:
        # The user's own moonraker.conf (printer_data/config), read as text:
        # Moonraker's merged config only shows the plugin's value
        if not self.data_path:
            return
        path = os.path.join(self.data_path, "config", "moonraker.conf")
        server = own_spoolman_server(path)
        if server is None:
            return
        mine = ("127.0.0.1:%d" % self.port, "localhost:%d" % self.port)
        bare = server.split("://", 1)[-1].rstrip("/")
        if bare in mine:
            return
        self.server.add_warning(
            "kiln-microspool: your moonraker.conf has [spoolman] server: "
            "%s. While the microspool plugin is on, Moonraker uses "
            "microspool on this printer instead. Use one or the other: "
            "switch the plugin off (DISABLE_PLUGIN NAME=microspool) to go "
            "back to %s, or remove that [spoolman] section." % (
                server, server), warn_id="microspool_own_spoolman")

    async def _on_klippy_ready(self) -> None:
        klippy_apis = self.server.lookup_component("klippy_apis")
        try:
            objects = await klippy_apis.get_object_list()
        except Exception:
            return
        if "ifs" in objects and "spoolman_slots" not in objects:
            self.server.add_warning(
                "kiln-microspool: this printer has the IFS. To take each "
                "print's filament off the spool in its own slot, also "
                "install Spoolman (Settings > Plugins on the touch screen, "
                "or INSTALL_PLUGIN NAME=spoolman ENABLE=1) and link each "
                "slot to its spool.", warn_id="microspool_ifs_slots",
                log=False)
            logging.info("microspool: IFS without kiln-spoolman: notice shown")
        else:
            self.server.remove_warning("microspool_ifs_slots")

    def _prepare(self) -> bool:
        if not os.access(self.binary, os.X_OK):
            self.last_error = "%s is missing or not executable" % self.binary
            return False
        try:
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        except OSError as e:
            self.last_error = "data directory: %s" % e
            return False
        return True

    async def _supervise(self) -> None:
        backoff = BACKOFF_MIN
        while not self.closing:
            if not self._prepare():
                logging.info("microspool: %s; retry in %d s",
                             self.last_error, int(backoff))
            else:
                await self._run_once()
                if self.closing:
                    break
                if time.monotonic() - self.started_at > STABLE_AFTER:
                    backoff = BACKOFF_MIN
                self.restarts += 1
                logging.info("microspool: stopped (exit %s); restart in %d s",
                             self.last_exit, int(backoff))
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break
            backoff = min(backoff * 2, BACKOFF_MAX)

    async def _run_once(self) -> None:
        cmd = self._command()
        try:
            self.proc = await asyncio.create_subprocess_exec(
                *cmd, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                preexec_fn=self._child_setup)
        except Exception as e:
            self.last_error = "start failed: %s" % e
            logging.info("microspool: %s", self.last_error)
            return
        self.started_at = time.monotonic()
        self.last_error = ""
        logging.info("microspool: started (pid %d): %s", self.proc.pid,
                     " ".join(shlex.quote(c) for c in cmd))
        assert self.proc.stdout is not None
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                logging.info("microspool: %s", text[:500])
        self.last_exit = await self.proc.wait()
        self.proc = None

    @staticmethod
    def _child_setup() -> None:
        # Runs in the child before exec (no locks, no logging here).
        # Its own session, and SIGTERM when Moonraker dies, so a killed
        # Moonraker doesn't leave it holding the port
        os.setsid()
        try:
            ctypes.CDLL(None, use_errno=True).prctl(
                PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)
        except Exception:
            pass
        try:
            os.nice(NICE)
        except OSError:
            pass
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write(str(OOM_SCORE_ADJ))
        except OSError:
            pass
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGPIPE):
            signal.signal(sig, signal.SIG_DFL)

    async def _handle_status(self, web_request: WebRequest) -> Dict[str, Any]:
        running = self.proc is not None and self.proc.returncode is None
        return {
            "running": running,
            "pid": self.proc.pid if running and self.proc else None,
            "uptime": (round(time.monotonic() - self.started_at, 1)
                       if running else 0),
            "host": self.host,
            "port": self.port,
            "data_file": self.data_file,
            "restarts": self.restarts,
            "last_exit": self.last_exit,
            "error": self.last_error,
        }

    async def close(self) -> None:
        self.closing = True
        proc = self.proc
        if proc is not None and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), STOP_WAIT)
            except asyncio.TimeoutError:
                logging.info("microspool: no exit after SIGTERM, killing")
                proc.kill()
                await proc.wait()
        if self.task is not None:
            self.task.cancel()


def own_spoolman_server(path: str) -> Optional[str]:
    # "server:" of a [spoolman] section in one config file, or None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    section = ""
    for line in lines:
        text = line.split("#", 1)[0].strip()
        if not text:
            continue
        if text.startswith("[") and text.endswith("]"):
            section = text[1:-1].strip().lower()
            continue
        if section == "spoolman" and ":" in text:
            key, value = text.split(":", 1)
            if key.strip().lower() == "server" and value.strip():
                return value.strip()
    return None


def load_component(config: ConfigHelper) -> Microspool:
    return Microspool(config)
