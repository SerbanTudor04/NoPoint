"""
NoPoint Drive Server — Shared + Live Sync
==========================================
All users read/write the same storage root (./storage/shared/).
When any client modifies a file the server pushes a SYNC_DELTA
notification to every *other* connected client so they can pull
the change immediately.

Storage layout (flat, shared across all users):
    ./storage/shared/
        docs/
            report.pdf
        photos/
            vacation.jpg

Run:
    python server.py
    python server.py --host 0.0.0.0 --port 9876 --storage ./storage --verbose
"""

import asyncio
import hashlib
import logging
import shutil
import uuid
from pathlib import Path
from typing import Optional
from weakref import WeakSet

from protocol.message import (
    Frame, MsgType, ProtocolError,
    ack, error, MAX_CHUNK,
)
from protocol.transport import Transport

log = logging.getLogger("nopoint.server")

DEFAULT_USERS = {
    "tudors": "tudors",
    "rzv":    "rzv",
    "ts":     "ts",
    "rog":    "rog",
}


# ── Chunked SHA-256 — no full file in RAM ──────────────────────────────────────
def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(MAX_CHUNK):
            h.update(chunk)
    return h.hexdigest()


# ── Live-session registry ──────────────────────────────────────────────────────
class SessionRegistry:
    """
    Tracks every authenticated ClientSession.
    Broadcasts change events to all clients except the one that caused the change.
    """

    def __init__(self) -> None:
        self._sessions: WeakSet["ClientSession"] = WeakSet()

    def register(self, session: "ClientSession") -> None:
        self._sessions.add(session)

    def unregister(self, session: "ClientSession") -> None:
        self._sessions.discard(session)

    def count(self) -> int:
        return sum(1 for s in self._sessions if s.authenticated)

    async def notify_change(self, changed_by: "ClientSession", event: dict) -> None:
        """
        Push a SYNC_DELTA frame to every session except the sender.
        Errors are swallowed — disconnected clients re-sync on reconnect.
        """
        frame = Frame(MsgType.SYNC_DELTA, event)
        targets = [s for s in list(self._sessions)
                   if s is not changed_by and s.authenticated]
        if not targets:
            return
        log.info(
            "[notify] op=%s  path=%s  by=%s  →  %d client(s)",
            event.get("op"), event.get("path", event.get("src", "?")),
            changed_by.username, len(targets),
        )
        for session in targets:
            try:
                await session.transport.send(frame)
            except Exception:
                pass


# ── In-progress uploads ────────────────────────────────────────────────────────
class PendingUpload:
    def __init__(self, path: Path, total_size: int, expected_sha256: str) -> None:
        self.path         = path
        self.total_size   = total_size
        self.expected_sha = expected_sha256
        self.received     = 0
        self.fh           = open(path, "wb")

    def write_chunk(self, data: bytes) -> None:
        self.fh.write(data)
        self.received += len(data)

    def finalize(self) -> bool:
        self.fh.close()
        return _file_hash(self.path) == self.expected_sha

    def abort(self) -> None:
        self.fh.close()
        self.path.unlink(missing_ok=True)


# ── Client session ─────────────────────────────────────────────────────────────
class ClientSession:
    def __init__(
        self,
        transport:   Transport,
        shared_root: Path,
        users:       dict[str, str],
        registry:    SessionRegistry,
    ) -> None:
        self.transport     = transport
        self.root          = shared_root        # same path for every user
        self.users         = users
        self.registry      = registry
        self.authenticated = False
        self.username:     Optional[str] = None
        self._uploads:     dict[str, PendingUpload] = {}

    # ── Shared path resolution (no per-user prefix) ────────────────────────────
    def _resolve(self, rel: str) -> Path:
        target = (self.root / rel.lstrip("/")).resolve()
        if not str(target).startswith(str(self.root.resolve())):
            raise ProtocolError("Path traversal detected")
        return target

    # ── Main loop ──────────────────────────────────────────────────────────────
    async def run(self) -> None:
        peer = self.transport.peer
        log.info("[+] Connected: %s", peer)
        try:
            while True:
                frame = await self.transport.recv()
                await self._dispatch(frame)
        except asyncio.IncompleteReadError:
            log.info("[-] Disconnected: %s  (%s)", peer, self.username or "unauthed")
        except ProtocolError as e:
            log.warning("[!] Protocol error from %s: %s", peer, e)
            try:
                await self.transport.send(error(400, str(e)))
            except Exception:
                pass
        except Exception:
            log.exception("[!] Unexpected error from %s", peer)
        finally:
            self.registry.unregister(self)
            await self.transport.close()

    async def _dispatch(self, frame: Frame) -> None:
        if not self.authenticated and frame.msg_type not in (MsgType.AUTH_REQ, MsgType.PING):
            await self.transport.send(error(401, "Not authenticated"))
            return

        handlers = {
            MsgType.AUTH_REQ:     self._handle_auth,
            MsgType.PING:         self._handle_ping,
            MsgType.UPLOAD_START: self._handle_upload_start,
            MsgType.UPLOAD_CHUNK: self._handle_upload_chunk,
            MsgType.UPLOAD_DONE:  self._handle_upload_done,
            MsgType.DOWNLOAD_REQ: self._handle_download,
            MsgType.LIST_DIR:     self._handle_list,
            MsgType.MKDIR:        self._handle_mkdir,
            MsgType.DELETE:       self._handle_delete,
            MsgType.MOVE:         self._handle_move,
            MsgType.STAT:         self._handle_stat,
            MsgType.SYNC_REQUEST: self._handle_sync_request,
            MsgType.DISCONNECT:   self._handle_disconnect,
        }
        handler = handlers.get(frame.msg_type)
        if handler is None:
            await self.transport.send(error(400, f"Unknown type: {frame.msg_type}"))
            return
        await handler(frame)

    # ── Auth ───────────────────────────────────────────────────────────────────
    async def _handle_auth(self, frame: Frame) -> None:
        user = frame.meta.get("username", "")
        pw   = frame.meta.get("password", "")
        if self.users.get(user) == pw:
            self.authenticated = True
            self.username = user
            self.registry.register(self)
            online = self.registry.count()
            log.info("[auth] %s authenticated  (%d online)", user, online)
            await self.transport.send(Frame(MsgType.AUTH_OK, {"username": user}))
        else:
            log.warning("[auth] Failed login for %r from %s", user, self.transport.peer)
            await self.transport.send(Frame(MsgType.AUTH_FAIL, {"message": "Invalid credentials"}))

    # ── Ping ───────────────────────────────────────────────────────────────────
    async def _handle_ping(self, frame: Frame) -> None:
        await self.transport.send(Frame(MsgType.PONG, {"ts": frame.meta.get("ts")}))

    # ── Upload ─────────────────────────────────────────────────────────────────
    async def _handle_upload_start(self, frame: Frame) -> None:
        rel       = frame.meta["path"]
        upload_id = str(uuid.uuid4())
        dest      = self._resolve(rel)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        self._uploads[upload_id] = PendingUpload(tmp, frame.meta["size"], frame.meta["sha256"])
        log.info("[upload] START  %s  size=%d  by=%s", rel, frame.meta["size"], self.username)
        await self.transport.send(ack(upload_id=upload_id, chunk_size=MAX_CHUNK))

    async def _handle_upload_chunk(self, frame: Frame) -> None:
        upload_id = frame.meta["upload_id"]
        up = self._uploads.get(upload_id)
        if up is None:
            await self.transport.send(error(404, f"Unknown upload_id: {upload_id}"))
            return
        up.write_chunk(frame.payload)
        await self.transport.send(ack(upload_id=upload_id, received=up.received))

    async def _handle_upload_done(self, frame: Frame) -> None:
        upload_id = frame.meta["upload_id"]
        rel       = frame.meta["path"]
        up = self._uploads.pop(upload_id, None)
        if up is None:
            await self.transport.send(error(404, f"Unknown upload_id: {upload_id}"))
            return

        dest = self._resolve(rel)
        if up.finalize():
            up.path.replace(dest)
            checksum = _file_hash(dest)
            log.info("[upload] DONE  %s  ✓  by=%s", rel, self.username)
            await self.transport.send(ack(upload_id=upload_id, sha256_verified=True, path=rel))
            # ── Push change to all other connected clients ─────────────────────
            await self.registry.notify_change(self, {
                "op":     "upsert",
                "path":   rel,
                "size":   dest.stat().st_size,
                "sha256": checksum,
                "by":     self.username,
            })
        else:
            up.path.unlink(missing_ok=True)
            log.warning("[upload] CHECKSUM MISMATCH  %s", rel)
            await self.transport.send(error(422, "Checksum mismatch — upload rejected"))

    # ── Download ───────────────────────────────────────────────────────────────
    async def _handle_download(self, frame: Frame) -> None:
        rel  = frame.meta["path"]
        path = self._resolve(rel)
        if not path.exists() or not path.is_file():
            await self.transport.send(error(404, f"Not found: {rel}"))
            return

        file_size = path.stat().st_size
        checksum  = _file_hash(path)

        await self.transport.send(Frame(
            MsgType.DOWNLOAD_START,
            {"path": rel, "size": file_size, "sha256": checksum, "chunk_size": MAX_CHUNK},
        ))
        with open(path, "rb") as f:
            offset = 0
            while chunk := f.read(MAX_CHUNK):
                await self.transport.send(Frame(
                    MsgType.DOWNLOAD_CHUNK,
                    {"offset": offset, "chunk_size": len(chunk)},
                    chunk,
                ))
                offset += len(chunk)
        await self.transport.send(Frame(MsgType.DOWNLOAD_DONE, {"path": rel, "sha256": checksum}))
        log.info("[download] DONE  %s  %dB  by=%s", rel, file_size, self.username)

    # ── Filesystem ops ─────────────────────────────────────────────────────────
    async def _handle_list(self, frame: Frame) -> None:
        rel  = frame.meta.get("path", ".")
        path = self._resolve(rel)
        if not path.is_dir():
            await self.transport.send(error(404, f"Not a directory: {rel}"))
            return
        entries = []
        for item in sorted(path.iterdir()):
            if item.name.endswith(".part"):
                continue   # hide in-progress uploads
            st = item.stat()
            entries.append({
                "name":     item.name,
                "type":     "dir" if item.is_dir() else "file",
                "size":     st.st_size,
                "modified": st.st_mtime,
            })
        await self.transport.send(Frame(MsgType.LIST_RESULT, {"path": rel, "entries": entries}))

    async def _handle_mkdir(self, frame: Frame) -> None:
        rel  = frame.meta["path"]
        path = self._resolve(rel)
        path.mkdir(parents=True, exist_ok=True)
        log.info("[mkdir] %s  by=%s", rel, self.username)
        await self.transport.send(ack(path=rel))
        await self.registry.notify_change(self, {
            "op":  "mkdir",
            "path": rel,
            "by":  self.username,
        })

    async def _handle_delete(self, frame: Frame) -> None:
        rel  = frame.meta["path"]
        path = self._resolve(rel)
        if not path.exists():
            await self.transport.send(error(404, f"Not found: {rel}"))
            return
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        log.info("[delete] %s  by=%s", rel, self.username)
        await self.transport.send(ack(path=rel))
        await self.registry.notify_change(self, {
            "op":  "delete",
            "path": rel,
            "by":  self.username,
        })

    async def _handle_move(self, frame: Frame) -> None:
        src = self._resolve(frame.meta["src"])
        dst = self._resolve(frame.meta["dst"])
        if not src.exists():
            await self.transport.send(error(404, f"Not found: {frame.meta['src']}"))
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        log.info("[move] %s → %s  by=%s", frame.meta["src"], frame.meta["dst"], self.username)
        await self.transport.send(ack())
        await self.registry.notify_change(self, {
            "op":  "move",
            "src": frame.meta["src"],
            "dst": frame.meta["dst"],
            "by":  self.username,
        })

    async def _handle_stat(self, frame: Frame) -> None:
        rel  = frame.meta["path"]
        path = self._resolve(rel)
        if not path.exists():
            await self.transport.send(error(404, f"Not found: {rel}"))
            return
        st = path.stat()
        await self.transport.send(Frame(MsgType.STAT_RESULT, {
            "path":     rel,
            "type":     "dir" if path.is_dir() else "file",
            "size":     st.st_size,
            "modified": st.st_mtime,
            "sha256":   _file_hash(path) if path.is_file() else None,
        }))

    async def _handle_sync_request(self, frame: Frame) -> None:
        """Return full {path: sha256} map of shared storage."""
        state = {}
        for path in self.root.rglob("*"):
            if path.is_file() and not path.name.endswith(".part"):
                rel = path.relative_to(self.root).as_posix()
                state[rel] = _file_hash(path)
        await self.transport.send(Frame(MsgType.SYNC_DELTA, {"state": state}))
        log.info("[sync] Sent state to %s  (%d files)", self.username, len(state))

    async def _handle_disconnect(self, frame: Frame) -> None:
        log.info("[-] %s disconnected gracefully", self.username)
        await self.transport.send(ack())
        raise asyncio.IncompleteReadError(b"", 0)


# ── Server entry point ─────────────────────────────────────────────────────────
class NoPointServer:
    def __init__(
        self,
        host:         str = "127.0.0.1",
        port:         int = 9876,
        storage_root: str = "./storage",
        users:        Optional[dict[str, str]] = None,
    ) -> None:
        self.host     = host
        self.port     = port
        self.root     = Path(storage_root) / "shared"   # ← one dir, all users
        self.users    = users or DEFAULT_USERS
        self.registry = SessionRegistry()
        self.root.mkdir(parents=True, exist_ok=True)

    async def _handle(self, reader, writer) -> None:
        transport = Transport(reader, writer)
        session   = ClientSession(transport, self.root, self.users, self.registry)
        await session.run()

    async def serve_forever(self) -> None:
        server = await asyncio.start_server(self._handle, self.host, self.port)
        addr   = server.sockets[0].getsockname()
        log.info("NoPoint Drive  listening on  %s:%d", *addr)
        log.info("Shared storage:             %s", self.root.resolve())
        log.info("Registered users:           %s", ", ".join(self.users))
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="NoPoint Drive Server")
    p.add_argument("--host",    default="127.0.0.1")
    p.add_argument("--port",    default=9876, type=int)
    p.add_argument("--storage", default="./storage", metavar="DIR")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s  %(name)s  %(message)s",
    )
    srv = NoPointServer(args.host, args.port, args.storage)
    asyncio.run(srv.serve_forever())