"""
nopoint.cli.render
===================
All Rich formatting helpers — tables, progress bars, status
messages — kept in one place so commands stay clean.
"""

import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()
err_console = Console(stderr=True)


# ── Colour palette (matches the dark GUI theme) ────────────────────────────────
C = {
    "accent":  "bright_magenta",
    "ok":      "bright_green",
    "warn":    "yellow",
    "err":     "bright_red",
    "dim":     "bright_black",
    "file":    "cyan",
    "dir":     "bright_blue",
    "size":    "white",
    "date":    "bright_black",
    "head":    "bold bright_magenta",
}


# ── Status lines ───────────────────────────────────────────────────────────────
def ok(msg: str)   -> None: console.print(f"  [bold {C['ok']}]✓[/]  {msg}")
def warn(msg: str) -> None: console.print(f"  [bold {C['warn']}]![/]  {msg}")
def err(msg: str)  -> None: err_console.print(f"  [bold {C['err']}]✗[/]  {msg}")
def info(msg: str) -> None: console.print(f"  [bold {C['accent']}]·[/]  {msg}")
def dim(msg: str)  -> None: console.print(f"    [{C['dim']}]{msg}[/]")


# ── Directory listing table ────────────────────────────────────────────────────
def file_table(entries: list[dict], cwd: str) -> None:
    title = Text(f"  /{cwd.strip('./')}", style=C["head"])
    t = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style=f"bold {C['dim']}",
        title=title,
        title_justify="left",
        pad_edge=False,
        show_edge=False,
        expand=False,
    )
    t.add_column("",        width=2,  no_wrap=True)
    t.add_column("Name",    min_width=28, no_wrap=True)
    t.add_column("Size",    width=10, justify="right")
    t.add_column("Modified",width=18)

    for e in sorted(entries, key=lambda x: (x["type"] != "dir", x["name"].lower())):
        is_dir  = e["type"] == "dir"
        icon    = "📁" if is_dir else "📄"
        name    = Text(e["name"], style=C["dir"] if is_dir else C["file"])
        size    = Text("—" if is_dir else _fmt_size(e.get("size", 0)),
                       style=C["dim"] if is_dir else C["size"])
        modified = Text(_fmt_ts(e.get("modified", 0)), style=C["date"])
        t.add_row(icon, name, size, modified)

    console.print()
    console.print(t)
    console.print()


# ── Stat card ──────────────────────────────────────────────────────────────────
def stat_card(info: dict) -> None:
    console.print()
    console.rule(f"[{C['head']}]{info['path']}[/]")
    rows = [
        ("Type",     info.get("type", "?")),
        ("Size",     _fmt_size(info["size"]) if info.get("size") else "—"),
        ("Modified", _fmt_ts(info.get("modified", 0))),
        ("SHA-256",  (info.get("sha256") or "—")[:16] + "…" if info.get("sha256") else "—"),
    ]
    for label, value in rows:
        console.print(f"  [{C['dim']}]{label:<12}[/] {value}")
    console.print()


# ── Inline progress bar (used with rich.progress externally) ──────────────────
def progress_bar(description: str):
    """Return a configured rich Progress context manager."""
    from rich.progress import (
        Progress, BarColumn, DownloadColumn,
        TransferSpeedColumn, TimeRemainingColumn, TextColumn,
    )
    return Progress(
        TextColumn(f"  [bold {C['accent']}]{{task.description}}[/]"),
        BarColumn(bar_width=36, style=C["dim"], complete_style=C["accent"]),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


# ── Connection banner ──────────────────────────────────────────────────────────
def banner(host: str, port: int, user: str, sync_root: str) -> None:
    console.print()
    console.rule(f"[{C['head']}]◈  NoPoint Drive[/]")
    console.print(f"  [{C['dim']}]server   [/] {host}:{port}")
    console.print(f"  [{C['dim']}]user     [/] {user}")
    console.print(f"  [{C['dim']}]sync dir [/] [{C['file']}]{sync_root}[/]")
    console.print()


# ── Help table ─────────────────────────────────────────────────────────────────
def help_table(commands: list[tuple[str, str, str]]) -> None:
    """commands = [(name, args, description), ...]"""
    t = Table(box=box.SIMPLE, show_header=False, pad_edge=False,
              show_edge=False, expand=False)
    t.add_column(style=f"bold {C['accent']}", no_wrap=True, min_width=12)
    t.add_column(style=C["dim"],              no_wrap=True, min_width=24)
    t.add_column(style="white")

    for name, args, desc in commands:
        t.add_row(name, args, desc)

    console.print()
    console.print(t)
    console.print()


# ── Formatting utils ───────────────────────────────────────────────────────────
def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _fmt_ts(ts: float) -> str:
    if not ts:
        return "—"
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d  %H:%M")