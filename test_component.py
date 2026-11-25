#!/usr/bin/env python3
"""Tests for the Kiln OS Moonraker component (moonraker/components/microspool.py)
with a stand-in for Moonraker: the warnings and the own-[spoolman] check.
Starting and supervising the microspool binary is covered by Kiln's
Moonraker test (tools/test_moonraker.sh with this plugin) and on a printer.
"""
import asyncio
import importlib.util
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "microspool_component",
    os.path.join(HERE, "moonraker", "components", "microspool.py"))
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)

FAILS = []
PASSES = 0


def check(name, cond):
    global PASSES
    if cond:
        PASSES += 1
        print("[PASS] " + name)
    else:
        FAILS.append(name)
        print("[FAIL] " + name)


class KlippyApis:
    def __init__(self, objects):
        self.objects = objects

    async def get_object_list(self):
        return list(self.objects)


class Server:
    def __init__(self, data_path, objects=()):
        self.data_path = data_path
        self.warnings = {}
        self.events = {}
        self.klippy = KlippyApis(objects)

    def get_app_args(self):
        return {"data_path": self.data_path}

    def register_endpoint(self, *a, **k):
        pass

    def register_event_handler(self, name, cb):
        self.events[name] = cb

    def add_warning(self, msg, warn_id=None, log=True, exc_info=None):
        self.warnings[warn_id] = msg

    def remove_warning(self, warn_id):
        self.warnings.pop(warn_id, None)

    def lookup_component(self, name):
        assert name == "klippy_apis"
        return self.klippy


class Config:
    def __init__(self, server, opts=None):
        self.server, self.opts = server, opts or {}

    def get_server(self):
        return self.server

    def get(self, key, default=None):
        return self.opts.get(key, default)

    def getint(self, key, default=None, **kw):
        return int(self.opts.get(key, default))


def user_conf(d, text):
    os.makedirs(os.path.join(d, "config"), exist_ok=True)
    with open(os.path.join(d, "config", "moonraker.conf"), "w") as f:
        f.write(text)


def main():
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "m.conf")
        cases = [
            ("", None),
            ("[server]\nport: 7125\n", None),
            ("[spoolman]\nserver: http://192.0.2.5:7912\n",
             "http://192.0.2.5:7912"),
            ("[Spoolman]\n  server : http://nas:7912/  # comment\n",
             "http://nas:7912/"),
            ("[spoolman]\n# server: http://x:1\n", None),
            ("[spoolman]\nsync_rate: 5\n[other]\nserver: http://y:1\n", None),
        ]
        for text, want in cases:
            with open(p, "w") as f:
                f.write(text)
            check("parse %r -> %r" % (text[:30], want),
                  ms.own_spoolman_server(p) == want)
        check("missing file -> None",
              ms.own_spoolman_server(os.path.join(d, "nope")) is None)

    def warnings_for(conf_text, objects, port=7912):
        with tempfile.TemporaryDirectory() as d:
            if conf_text is not None:
                user_conf(d, conf_text)
            srv = Server(d, objects)
            comp = ms.load_component(Config(srv, {"port": port}))
            comp._check_own_spoolman()
            asyncio.run(srv.events["server:klippy_ready"]())
            return srv.warnings

    w = warnings_for(None, ["ifs", "toolhead"])
    check("IFS, no kiln-spoolman -> IFS notice", "microspool_ifs_slots" in w)
    check("no user conf -> no own-spoolman warning",
          "microspool_own_spoolman" not in w)
    w = warnings_for(None, ["ifs", "spoolman_slots"])
    check("IFS with kiln-spoolman -> no IFS notice",
          "microspool_ifs_slots" not in w)
    w = warnings_for(None, ["toolhead"])
    check("no IFS -> no IFS notice", "microspool_ifs_slots" not in w)
    w = warnings_for("[spoolman]\nserver: http://192.0.2.5:7912\n", ["toolhead"])
    check("own Spoolman elsewhere -> warning", "microspool_own_spoolman" in w)
    check("warning names that server",
          "192.0.2.5:7912" in w.get("microspool_own_spoolman", ""))
    w = warnings_for("[spoolman]\nserver: http://127.0.0.1:7912\n", ["toolhead"])
    check("own [spoolman] pointing at microspool -> no warning",
          "microspool_own_spoolman" not in w)
    w = warnings_for("[spoolman]\nserver: localhost:7912\n", ["toolhead"])
    check("localhost without scheme -> no warning",
          "microspool_own_spoolman" not in w)
    w = warnings_for("[spoolman]\nserver: http://127.0.0.1:7912\n", ["toolhead"],
                     port=7913)
    check("microspool on another port -> warning",
          "microspool_own_spoolman" in w)

    # allow_hosts -> one -H per name; bad names left out with a warning
    with tempfile.TemporaryDirectory() as d:
        srv = Server(d)
        comp = ms.load_component(Config(srv, {"port": 7912}))
        cmd = comp._command()
        check("no allow_hosts -> no -H", "-H" not in cmd)
        srv = Server(d)
        comp = ms.load_component(Config(srv, {
            "port": 7912, "allow_hosts":
            "ad5x.home, AD5X.tail-net.ts.net.\n  printer-2 *  1.2.3.4:80 a/b",
            "extra_args": "-v"}))
        cmd = comp._command()
        hs = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-H"]
        check("allow_hosts names -> -H each, lower case, no trailing dot",
              hs == ["ad5x.home", "ad5x.tail-net.ts.net", "printer-2"])
        check("extra_args still last", cmd[-1] == "-v")
        check("wildcard, port and slash refused",
              comp.bad_hosts == ["*", "1.2.3.4:80", "a/b"])

        async def init_warns():
            comp.task = None
            orig = comp._supervise
            async def nothing():
                return None
            comp._supervise = nothing
            await comp.component_init()
            await comp.task
            comp._supervise = orig
        asyncio.run(init_warns())
        check("bad names -> warning naming them",
              "*" in srv.warnings.get("microspool_allow_hosts", ""))

    print("\n%d passed, %d failed" % (PASSES, len(FAILS)))
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
