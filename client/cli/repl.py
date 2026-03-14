"""
nopoint.cli.repl
=================
Interactive REPL loop.
Handles the prompt, input parsing, command dispatch,
and error display.  No business logic lives here.
"""

import asyncio
import shlex

from rich.prompt import Prompt

from cli.config   import Config
from cli.render   import console, err, dim, C
from cli.commands import COMMANDS


_EXIT_CMDS = {"exit", "quit", "q", "bye"}


async def run_repl(client, cfg: Config) -> None:
    """
    Run the interactive shell until the user types exit/quit
    or hits Ctrl-C / Ctrl-D.
    """
    cwd = "."

    while True:
        # ── Build prompt ───────────────────────────────────────────────────────
        remote_display = "/" + cwd.strip("./") if cwd not in (".", "") else "/"
        prompt = (
            f"[{C['dim']}]{cfg.username}@{cfg.host}[/]"
            f"[{C['accent']}]:{remote_display}[/] "
            f"[{C['dim']}]›[/] "
        )

        # ── Read input ─────────────────────────────────────────────────────────
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

        # ── Parse ──────────────────────────────────────────────────────────────
        try:
            parts = shlex.split(line)
        except ValueError as e:
            err(f"Parse error: {e}")
            continue

        cmd_name, *args = parts

        # ── Exit ───────────────────────────────────────────────────────────────
        if cmd_name.lower() in _EXIT_CMDS:
            dim("Goodbye")
            break

        # ── Dispatch ───────────────────────────────────────────────────────────
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