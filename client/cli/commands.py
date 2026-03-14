"""
nopoint.cli.commands
=====================
One async function per CLI command.
Each receives the connected NoPointClient and the current Config.
Returns the new cwd (or the same one if it didn't change).
"""

import asyncio
from pathlib import Path

from rich.prompt import Confirm

from cli.config import Config
from cli.render import (
    console, ok, warn, err, info, dim,
    file_table, stat_card, progress_bar, help_table,
)

from client import NoPointClient


# ── ls ─────────────────────────────────────────────────────────────────────────
async def cmd_ls(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    path = args[0] if args else cwd
    entries = await client.list_dir(path)
    file_table(entries, path)
    return cwd


# ── cd ─────────────────────────────────────────────────────────────────────────
async def cmd_cd(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        return "."
    target = args[0]
    if target == "..":
        parts = [p for p in cwd.split("/") if p and p != "."]
        return "/".join(parts[:-1]) or "."
    new_cwd = (cwd.strip("./") + "/" + target).lstrip("/") if cwd not in (".", "") else target
    try:
        await client.list_dir(new_cwd)
        return new_cwd
    except Exception as e:
        err(f"cd: {e}")
        return cwd


# ── pwd ────────────────────────────────────────────────────────────────────────
async def cmd_pwd(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    info(f"/{cwd.strip('./')}")
    return cwd


# ── mkdir ──────────────────────────────────────────────────────────────────────
async def cmd_mkdir(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        err("mkdir: folder name required")
        return cwd
    base   = cwd.strip("./")
    remote = (base + "/" + args[0]).lstrip("/") if base else args[0]
    await client.mkdir(remote)
    ok(f"Created  {remote}")
    return cwd


# ── rm ─────────────────────────────────────────────────────────────────────────
async def cmd_rm(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        err("rm: path required")
        return cwd
    base   = cwd.strip("./")
    remote = (base + "/" + args[0]).lstrip("/") if base else args[0]
    if not Confirm.ask(f"  Delete [bright_red]{remote}[/]?", default=False):
        dim("Cancelled")
        return cwd
    await client.delete(remote)
    ok(f"Deleted  {remote}")
    return cwd


# ── mv ─────────────────────────────────────────────────────────────────────────
async def cmd_mv(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if len(args) < 2:
        err("mv: usage  mv <src> <dst>")
        return cwd
    base = cwd.strip("./")

    def _resolve(p: str) -> str:
        return (base + "/" + p).lstrip("/") if (base and not p.startswith("/")) else p.lstrip("/")

    src, dst = _resolve(args[0]), _resolve(args[1])
    await client.move(src, dst)
    ok(f"{src}  →  {dst}")
    return cwd


# ── stat ───────────────────────────────────────────────────────────────────────
async def cmd_stat(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        err("stat: path required")
        return cwd
    base   = cwd.strip("./")
    remote = (base + "/" + args[0]).lstrip("/") if base else args[0]
    result = await client.stat(remote)
    stat_card(result)
    return cwd


# ── ping ───────────────────────────────────────────────────────────────────────
async def cmd_ping(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    count = int(args[0]) if args else 3
    for i in range(count):
        rtt = await client.ping()
        console.print(f"  [{_rtt_colour(rtt)}]{rtt * 1000:.1f} ms[/]")
        if i < count - 1:
            await asyncio.sleep(0.5)
    return cwd


# ── upload ─────────────────────────────────────────────────────────────────────
async def cmd_upload(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        err("upload: local path required")
        return cwd

    import glob as _glob
    locals_ = _glob.glob(args[0], recursive=True)
    if not locals_:
        err(f"upload: no files matching  {args[0]!r}")
        return cwd

    base = cwd.strip("./")

    for local in locals_:
        p = Path(local)
        if not p.is_file():
            warn(f"Skipping non-file: {local}")
            continue

        remote_name = args[1] if (len(args) > 1 and len(locals_) == 1) else p.name
        remote      = (base + "/" + remote_name).lstrip("/") if base else remote_name

        with progress_bar(p.name) as prog:
            task = prog.add_task(p.name, total=p.stat().st_size)
            last = [0]

            def _progress(sent: int, total: int, _task=task, _prog=prog, _last=last) -> None:
                _prog.update(_task, advance=sent - _last[0])
                _last[0] = sent

            await client.upload(p, remote, on_progress=_progress)

        ok(f"Uploaded  {remote}")

    return cwd


# ── download ───────────────────────────────────────────────────────────────────
async def cmd_download(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        err("download: remote path required")
        return cwd

    base       = cwd.strip("./")
    remote     = (base + "/" + args[0]).lstrip("/") if (base and not args[0].startswith("/")) else args[0].lstrip("/")
    local_path = Path(args[1]) if len(args) > 1 else cfg.local_path_for(remote)

    try:
        meta  = await client.stat(remote)
        total = meta.get("size", 0)
    except Exception:
        total = 0

    with progress_bar(Path(remote).name) as prog:
        task = prog.add_task(Path(remote).name, total=total or None)
        last = [0]

        def _progress(recv: int, t: int, _task=task, _prog=prog, _last=last) -> None:
            _prog.update(_task, total=t, advance=recv - _last[0])
            _last[0] = recv

        await client.download(remote, local_path, on_progress=_progress)

    ok(f"Saved  →  {local_path}")
    return cwd


# ── sync ───────────────────────────────────────────────────────────────────────
async def cmd_sync(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    remote_dir = args[0] if args else cwd

    async def _sync_dir(remote: str) -> tuple[int, int]:
        skipped = downloaded = 0
        entries = await client.list_dir(remote)
        for e in entries:
            child_remote = (remote.strip("./") + "/" + e["name"]).lstrip("/") or e["name"]
            if e["type"] == "dir":
                s, d = await _sync_dir(child_remote)
                skipped += s
                downloaded += d
            else:
                local = cfg.local_path_for(child_remote)
                if local.exists() and local.stat().st_size == e.get("size", -1):
                    dim(f"skip  {child_remote}")
                    skipped += 1
                    continue
                info(f"pull  {child_remote}")
                local.parent.mkdir(parents=True, exist_ok=True)
                await client.download(child_remote, local)
                downloaded += 1
        return skipped, downloaded

    info(f"Syncing  /{remote_dir.strip('./')}  →  {cfg.sync_root}")
    skipped, downloaded = await _sync_dir(remote_dir)
    ok(f"Done  —  {downloaded} downloaded,  {skipped} already up-to-date")
    return cwd


# ── set ────────────────────────────────────────────────────────────────────────
async def cmd_set(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    if not args:
        console.print()
        console.print(f"  [bold bright_black]host     [/] {cfg.host}")
        console.print(f"  [bold bright_black]port     [/] {cfg.port}")
        console.print(f"  [bold bright_black]username [/] {cfg.username}")
        console.print(f"  [bold bright_black]sync_root[/] [cyan]{cfg.sync_root}[/]")
        console.print()
        return cwd

    if len(args) < 2:
        err("set: usage  set <key> <value>")
        return cwd

    key, value = args[0], " ".join(args[1:])
    match key:
        case "sync_root" | "sync":
            cfg.sync_root = str(Path(value).expanduser().resolve())
            cfg.save()
            ok(f"sync_root  →  {cfg.sync_root}")
        case "host":
            cfg.host = value
            cfg.save()
            ok(f"host  →  {cfg.host}")
        case "port":
            cfg.port = int(value)
            cfg.save()
            ok(f"port  →  {cfg.port}")
        case _:
            err(f"set: unknown key  {key!r}  (host / port / sync_root)")
    return cwd


# ── help ───────────────────────────────────────────────────────────────────────
async def cmd_help(client: NoPointClient, cfg: Config, cwd: str, args: list[str]) -> str:
    help_table([
        ("ls",       "[path]",           "List remote directory"),
        ("cd",       "<dir>",            "Change remote directory"),
        ("pwd",      "",                 "Print remote working directory"),
        ("mkdir",    "<dir>",            "Create remote directory"),
        ("rm",       "<path>",           "Delete remote file or directory"),
        ("mv",       "<src> <dst>",      "Move or rename"),
        ("stat",     "<path>",           "Show file metadata"),
        ("upload",   "<local> [remote]", "Upload file(s) to current remote dir"),
        ("download", "<remote> [local]", "Download file to sync_root or local path"),
        ("sync",     "[remote_dir]",     "Pull entire remote dir into sync_root"),
        ("ping",     "[count]",          "Measure round-trip time"),
        ("set",      "[key value]",      "View / update config (sync_root, host, port)"),
        ("help",     "",                 "Show this help"),
        ("exit",     "",                 "Disconnect and quit"),
    ])
    return cwd


# ── Command registry ───────────────────────────────────────────────────────────
COMMANDS: dict[str, callable] = {
    "ls":       cmd_ls,
    "ll":       cmd_ls,
    "dir":      cmd_ls,
    "cd":       cmd_cd,
    "pwd":      cmd_pwd,
    "mkdir":    cmd_mkdir,
    "rm":       cmd_rm,
    "del":      cmd_rm,
    "mv":       cmd_mv,
    "rename":   cmd_mv,
    "stat":     cmd_stat,
    "info":     cmd_stat,
    "ping":     cmd_ping,
    "upload":   cmd_upload,
    "put":      cmd_upload,
    "download": cmd_download,
    "get":      cmd_download,
    "sync":     cmd_sync,
    "pull":     cmd_sync,
    "set":      cmd_set,
    "config":   cmd_set,
    "help":     cmd_help,
    "?":        cmd_help,
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def _rtt_colour(rtt: float) -> str:
    if rtt < 0.010: return "bright_green"
    if rtt < 0.050: return "yellow"
    return "bright_red"