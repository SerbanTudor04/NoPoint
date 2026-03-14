"""
nopoint.cli.main
=================
Entry point for the NoPoint Drive CLI.

Usage
-----
    python run_cli.py                        # uses saved config
    python run_cli.py --host 10.0.0.1 --port 9876 --user alice
    python run_cli.py --sync ~/MyDrive       # override sync root for this session
"""

import argparse
import asyncio
import getpass
import sys
from pathlib import Path

from rich.prompt import Prompt

from cli.config import Config
from cli.render import console, ok, err, info, banner, C
from cli.repl   import run_repl

from client import NoPointClient
from protocol.message import AuthError


# ── Arg parsing ────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="nopoint",
        description="NoPoint Drive — interactive CLI client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python run_cli.py
  python run_cli.py --host 192.168.1.10 --user alice
  python run_cli.py --sync ~/Documents/NoPoint
        """,
    )
    p.add_argument("--host", metavar="HOST",               help="Server host (overrides saved config)")
    p.add_argument("--port", metavar="PORT", type=int,     help="Server port")
    p.add_argument("--user", metavar="USER",               help="Username")
    p.add_argument("--sync", metavar="DIR",                help="Local sync root directory")
    p.add_argument("--save", action="store_true",          help="Save CLI flags to config and exit")
    return p.parse_args()


# ── Main ───────────────────────────────────────────────────────────────────────
async def main() -> int:
    args = _parse_args()

    # ── Load + patch config ────────────────────────────────────────────────────
    cfg = Config.load()
    if args.host:  cfg.host      = args.host
    if args.port:  cfg.port      = args.port
    if args.user:  cfg.username  = args.user
    if args.sync:
        cfg.sync_root = str(Path(args.sync).expanduser().resolve())

    if args.save:
        cfg.save()
        ok("Config saved to ~/.config/nopoint/config.json")
        return 0

    # ── Prompt for anything missing ────────────────────────────────────────────
    if not cfg.username:
        cfg.username = Prompt.ask(f"  [{C['accent']}]Username[/]")

    password = getpass.getpass(f"  Password for {cfg.username}: ")

    # ── Connect ────────────────────────────────────────────────────────────────
    banner(cfg.host, cfg.port, cfg.username, cfg.sync_root)
    info(f"Connecting to {cfg.host}:{cfg.port} …")

    client = NoPointClient(cfg.host, cfg.port)
    try:
        await client.connect(cfg.username, password)
    except AuthError as e:
        err(f"Authentication failed: {e}")
        return 1
    except OSError as e:
        err(f"Cannot reach {cfg.host}:{cfg.port} — {e}")
        return 1

    ok(f"Connected as {cfg.username}")
    info(f"Sync root: {cfg.sync_root}")
    console.print(f"  [{C['dim']}]Type  help  for available commands[/]")
    console.print()

    # ── REPL ───────────────────────────────────────────────────────────────────
    try:
        await run_repl(client, cfg)
    finally:
        await client.disconnect()

    return 0


def run() -> None:
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    run()