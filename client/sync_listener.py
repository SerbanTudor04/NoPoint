"""
nopoint.client.sync_listener
=============================
Consumes server-pushed SYNC_DELTA frames from client.iter_pushes()
and applies each change to the local sync_root automatically.

Does NOT touch the transport directly — all reads go through
the client's demultiplexing queue, so commands and the listener
can run concurrently without fighting over the socket.

Server push events:
    {"op": "upsert", "path": "docs/report.pdf", "sha256": "...", "by": "alice"}
    {"op": "delete", "path": "docs/old.txt",                     "by": "alice"}
    {"op": "move",   "src":  "a.txt",            "dst": "b.txt", "by": "alice"}
    {"op": "mkdir",  "path": "photos",                           "by": "alice"}
"""

import hashlib
import logging
import shutil
from pathlib import Path

from protocol.message import MAX_CHUNK

log = logging.getLogger("nopoint.sync")


async def listen_for_changes(
    client,
    cfg,
    on_event=None,
) -> None:
    """
    Drain client.iter_pushes() forever, applying each change locally.
    Run as an asyncio.create_task() — cancellation stops it cleanly.
    """
    log.info("[sync] Listener started")
    async for frame in client.iter_pushes():
        event = frame.meta
        op    = event.get("op")
        by    = event.get("by", "?")

        try:
            await _apply(op, event, client, cfg)
        except Exception as e:
            log.error("[sync] Failed to apply %s from %s: %s", op, by, e)
            continue

        if on_event:
            try:
                on_event(event)
            except Exception:
                pass

    log.info("[sync] Listener stopped")


async def _apply(op: str, event: dict, client, cfg) -> None:
    match op:
        case "upsert": await _apply_upsert(event, client, cfg)
        case "delete": _apply_delete(event, cfg)
        case "move":   _apply_move(event, cfg)
        case "mkdir":  _apply_mkdir(event, cfg)
        case _:        log.debug("[sync] Unknown op %r — ignored", op)


async def _apply_upsert(event: dict, client, cfg) -> None:
    remote_path = event["path"]
    remote_sha  = event.get("sha256", "")
    local_path  = cfg.local_path_for(remote_path)

    if local_path.exists() and _local_sha256(local_path) == remote_sha:
        log.debug("[sync] upsert %s — already current", remote_path)
        return

    log.info("[sync] ↓  %s  (by %s)", remote_path, event.get("by", "?"))
    local_path.parent.mkdir(parents=True, exist_ok=True)
    await client.download(remote_path, local_path)
    log.info("[sync] ✓  saved  %s", local_path)


def _apply_delete(event: dict, cfg) -> None:
    local = cfg.local_path_for(event["path"])
    if local.exists():
        if local.is_dir():
            shutil.rmtree(local)
        else:
            local.unlink()
        log.info("[sync] 🗑  %s  (by %s)", event["path"], event.get("by", "?"))


def _apply_move(event: dict, cfg) -> None:
    src = cfg.local_path_for(event["src"])
    dst = cfg.local_path_for(event["dst"])
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.replace(dst)
        log.info("[sync] ↔  %s → %s  (by %s)", event["src"], event["dst"], event.get("by", "?"))


def _apply_mkdir(event: dict, cfg) -> None:
    local = cfg.local_path_for(event["path"])
    local.mkdir(parents=True, exist_ok=True)
    log.debug("[sync] mkdir  %s  (by %s)", event["path"], event.get("by", "?"))


def _local_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(MAX_CHUNK):
            h.update(chunk)
    return h.hexdigest()