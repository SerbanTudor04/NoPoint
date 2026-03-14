"""
nopoint.cli.repl
=================
Interactive REPL loop.
Also starts the sync_listener as a background asyncio task so
server-pushed changes are applied automatically while you type.
"""

import asyncio
import shlex

from rich.prompt import Prompt

from cli.config   import Config
from cli.render   import console, err, info, dim, C
from cli.commands import COMMANDS

try:
    from client.sync_listener import listen_for_changes
except ImportError:
    try:
        from sync_listener import listen_for_changes
    except ImportError:
        listen_for_changes = None


_EXIT_CMDS = {"exit", "quit", "q", "bye"}


def _on_sync_event(event: dict) -> None:
    """Print a one-line notification when a peer changes something."""
    op = event.get("op", "?")
    by = event.get("by", "?")
    match op:
        case "upsert":
            console.print(
                f"\n  [bold bright_black]↓  {event.get('path')}[/]"
                f"  [bright_black]← {by}[/]"
            )
        case "delete":
            console.print(
                f"\n  [bold bright_black]🗑  {event.get('path')}[/]"
                f"  [bright_black]← {by}[/]"
            )
        case "move":
            console.print(
                f"\n  [bold bright_black]↔  {event.get('src')} → {event.get('dst')}[/]"
                f"  [bright_black]← {by}[/]"
            )
        case "mkdir":
            console.print(
                f"\n  [bold bright_black]📁  {event.get('path')}[/]"
                f"  [bright_black]← {by}[/]"
            )


async def run_repl(client, cfg: Config) -> None:
    cwd = "."

    # ── Start background sync listener ─────────────────────────────────────────
    sync_task = None
    if listen_for_changes is not None:
        sync_task = asyncio.create_task(
            listen_for_changes(client, cfg, on_event=_on_sync_event),
            name="sync-listener",
        )
        info("Live sync active — changes from peers appear automatically")
    else:
        dim("sync_listener not found — live sync disabled")

    try:
        await _repl_loop(client, cfg, cwd)
    finally:
        if sync_task and not sync_task.done():
            sync_task.cancel()
            try:
                await sync_task
            except asyncio.CancelledError:
                pass


async def _repl_loop(client, cfg: Config, cwd: str) -> None:
    while True:
        remote_display = "/" + cwd.strip("./") if cwd not in (".", "") else "/"
        prompt = (
            f"[{C['dim']}]{cfg.username}@{cfg.host}[/]"
            f"[{C['accent']}]:{remote_display}[/] "
            f"[{C['dim']}]›[/] "
        )

        try:
            line = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: Prompt.ask(prompt, default="", show_default=False),
            )
        except (EOFError, KeyboardInterrupt):
            console.print()
            dim("Disconnected")
            break

        line = line.strip()
        if not line:
            continue

        try:
            parts = shlex.split(line)
        except ValueError as e:
            err(f"Parse error: {e}")
            continue

        cmd_name, *args = parts

        if cmd_name.lower() in _EXIT_CMDS:
            dim("Goodbye")
            break

        handler = COMMANDS.get(cmd_name.lower())
        if handler is None:
            err(f"Unknown command: {cmd_name!r}  — type  help  for a list")
            continue

        try:
            cwd = await handler(client, cfg, cwd, args)
        except KeyboardInterrupt:
            console.print()
            dim("Interrupted")
        except Exception as e:
            err(str(e))