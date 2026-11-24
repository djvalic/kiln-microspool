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
#   extra_args  more command line options for microspool
#
# Endpoint: GET /server/microspool/status
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import asyncio
import ctypes
import logging
import os
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
        self.data_file = config.get("data_file", DEFAULT_DATA)
        self.binary = config.get(
            "binary", os.path.join(PLUGIN_DIR, "bin", "microspool"))
        self.extra_args = shlex.split(config.get("extra_args", ""))
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.task: Optional[asyncio.Task] = None
        self.closing = False
        self.started_at = 0.
        self.restarts = 0
        self.last_exit: Optional[int] = None
        self.last_error = ""
        self.server.register_endpoint(
            "/server/microspool/status", ["GET"], self._handle_status)

    def _command(self) -> List[str]:
        return [self.binary, "-l", self.host, "-p", str(self.port),
                "-d", self.data_file] + self.extra_args

    async def component_init(self) -> None:
        self.task = asyncio.create_task(self._supervise())

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


def load_component(config: ConfigHelper) -> Microspool:
    return Microspool(config)
