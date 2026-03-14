import asyncio
import hashlib
import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Dict, Optional

from protocol.message import (
    Frame, MsgType, ProtocolError, AuthError, ChecksumError,
    ack, error, sha256, MAX_CHUNK
)
from protocol.transport import Transport

log = logging.getLogger("nopoint.server")

DEFAULT_USERS={
    "tudors":"tudors",
    "rzv":'rzv',
    'ts':'ts',
    'rog':'rog'
}

# Functie ajutatoare pentru a calcula hash-ul fara sa umplem RAM-ul
def calculate_file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(MAX_CHUNK):
            h.update(chunk)
    return h.hexdigest()


# ── In-progress uploads ────────────────────────────────────────────────────────
class PendingUpload:
    def __init__(self, path: Path, total_size: int, expected_sha256: str):
        self.path = path
        self.total_size = total_size
        self.expected_sha = expected_sha256
        self.received = 0
        self.fh = open(path, "wb")

    def write_chunk(self, data: bytes):
        self.fh.write(data)
        self.received += len(data)

    def finalize(self) -> bool:
        self.fh.close()
        # FIX: Folosim functia cu chunk-uri, nu self.path.read_bytes()
        actual = calculate_file_hash(self.path)
        return actual == self.expected_sha

    def abort(self):
        self.fh.close()
        self.path.unlink(missing_ok=True)


# ── Client session ─────────────────────────────────────────────────────────────
class ClientSession:
    def __init__(self, transport: Transport, storage_root: Path, users: Dict[str, str]):
        self.transport = transport
        self.root = storage_root
        self.users = users
        self.authenticated = False
        self.username: Optional[str] = None
        self._uploads: Dict[str, PendingUpload] = {}

    def _user_root(self) -> Path:
        p = self.root / self.username
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _resolve(self, rel: str) -> Path:
        target = (self._user_root() / rel).resolve()
        if not str(target).startswith(str(self._user_root().resolve())):
            raise ProtocolError("Path traversal detected")
        return target

    async def run(self):
        peer = self.transport.peer
        log.info("[+] Client connected: %s", peer)
        try:
            while True:
                frame = await self.transport.recv()
                await self._dispatch(frame)
        except asyncio.IncompleteReadError:
            log.info("[-] Client disconnected: %s", peer)
        except AuthError as e:
            await self.transport.send(error(403, str(e)))
        except ProtocolError as e:
            log.warning("[!] Protocol error from %s: %s", peer, e)
            await self.transport.send(error(400, str(e)))
        except Exception as e:
            log.exception("[!] Unexpected error from %s: %s", peer, e)
        finally:
            await self.transport.close()

    async def _dispatch(self, frame: Frame):
        if not self.authenticated and frame.msg_type not in (MsgType.AUTH_REQ, MsgType.PING):
            await self.transport.send(error(401, "Not authenticated"))
            return

        handlers = {
            MsgType.AUTH_REQ: self._handle_auth,
            MsgType.PING: self._handle_ping,
            MsgType.UPLOAD_START: self._handle_upload_start,
            MsgType.UPLOAD_CHUNK: self._handle_upload_chunk,
            MsgType.UPLOAD_DONE: self._handle_upload_done,
            MsgType.DOWNLOAD_REQ: self._handle_download,
            MsgType.LIST_DIR: self._handle_list,
            MsgType.MKDIR: self._handle_mkdir,
            MsgType.DELETE: self._handle_delete,
            MsgType.MOVE: self._handle_move,
            MsgType.STAT: self._handle_stat,
            MsgType.SYNC_REQUEST: self._handle_sync_request,  # Adaugat!
            MsgType.DISCONNECT: self._handle_disconnect,
        }
        handler = handlers.get(frame.msg_type)
        if handler is None:
            await self.transport.send(error(400, f"Unknown message type: {frame.msg_type}"))
            return
        await handler(frame)

    async def _handle_auth(self, frame: Frame):
        user = frame.meta.get("username", "")
        pw = frame.meta.get("password", "")
        if self.users.get(user) == pw:
            self.authenticated = True
            self.username = user
            log.info("[auth] %s authenticated", user)
            await self.transport.send(Frame(MsgType.AUTH_OK, {"username": user}))
        else:
            log.warning("[auth] Failed login for %r", user)
            await self.transport.send(Frame(MsgType.AUTH_FAIL, {"message": "Invalid credentials"}))

    async def _handle_ping(self, frame: Frame):
        await self.transport.send(Frame(MsgType.PONG, {"ts": frame.meta.get("ts")}))

    async def _handle_upload_start(self, frame: Frame):
        rel = frame.meta["path"]
        total_size = frame.meta["size"]
        checksum = frame.meta["sha256"]
        upload_id = str(uuid.uuid4())

        dest = self._resolve(rel)
        dest.parent.mkdir(parents=True, exist_ok=True)

        tmp = dest.with_suffix(dest.suffix + ".part")
        self._uploads[upload_id] = PendingUpload(tmp, total_size, checksum)

        log.info("[upload] START %s  size=%d  id=%s", rel, total_size, upload_id)
        await self.transport.send(ack(upload_id=upload_id, chunk_size=MAX_CHUNK))

    async def _handle_upload_chunk(self, frame: Frame):
        upload_id = frame.meta["upload_id"]
        offset = frame.meta.get("offset", 0)
        up = self._uploads.get(upload_id)
        if up is None:
            await self.transport.send(error(404, f"Unknown upload_id: {upload_id}"))
            return

        up.write_chunk(frame.payload)
        await self.transport.send(ack(upload_id=upload_id, received=up.received))

    async def _handle_upload_done(self, frame: Frame):
        upload_id = frame.meta["upload_id"]
        rel = frame.meta["path"]
        up = self._uploads.pop(upload_id, None)
        if up is None:
            await self.transport.send(error(404, f"Unknown upload_id: {upload_id}"))
            return

        dest = self._resolve(rel)
        if up.finalize():
            up.path.replace(dest)
            log.info("[upload] DONE %s  verified ✓", rel)
            await self.transport.send(ack(upload_id=upload_id, sha256_verified=True, path=rel))
        else:
            up.path.unlink(missing_ok=True)
            log.warning("[upload] CHECKSUM MISMATCH %s", rel)
            await self.transport.send(error(422, "Checksum mismatch — upload rejected"))

    async def _handle_download(self, frame: Frame):
        rel = frame.meta["path"]
        path = self._resolve(rel)

        if not path.exists() or not path.is_file():
            await self.transport.send(error(404, f"Not found or not a file: {rel}"))
            return

        file_size = path.stat().st_size
        # FIX: Folosim functia optimizata
        checksum = calculate_file_hash(path)

        await self.transport.send(Frame(
            MsgType.DOWNLOAD_START,
            {"path": rel, "size": file_size, "sha256": checksum, "chunk_size": MAX_CHUNK}
        ))

        # FIX: Citim de pe disc in chunks si trimitem pe retea fara sa umplem RAM-ul
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
        log.info("[download] DONE %s  %dB", rel, file_size)

    async def _handle_list(self, frame: Frame):
        rel = frame.meta.get("path", ".")
        path = self._resolve(rel)
        if not path.is_dir():
            await self.transport.send(error(404, f"Not a directory: {rel}"))
            return

        entries = []
        for item in sorted(path.iterdir()):
            st = item.stat()
            entries.append({
                "name": item.name,
                "type": "dir" if item.is_dir() else "file",
                "size": st.st_size,
                "modified": st.st_mtime,
            })
        await self.transport.send(Frame(MsgType.LIST_RESULT, {"path": rel, "entries": entries}))

    async def _handle_sync_request(self, frame: Frame):
        """ Returneaza o harta completa (path: hash) a tuturor fisierelor userului. """
        state = {}
        user_root = self._user_root()

        # Parcurgem tot directorul recursiv
        for path in user_root.rglob('*'):
            if path.is_file():
                # Calculam calea relativa fata de radacina userului (ex: "poze/vacanta.jpg")
                rel_path = path.relative_to(user_root).as_posix()
                state[rel_path] = calculate_file_hash(path)

        await self.transport.send(Frame(MsgType.SYNC_DELTA, {"state": state}))
        log.info("[sync] Sent state to %s (found %d files)", self.username, len(state))

    async def _handle_mkdir(self, frame: Frame):
        rel = frame.meta["path"]
        path = self._resolve(rel)
        path.mkdir(parents=True, exist_ok=True)
        log.info("[mkdir] %s", rel)
        await self.transport.send(ack(path=rel))

    async def _handle_delete(self, frame: Frame):
        rel = frame.meta["path"]
        path = self._resolve(rel)
        if not path.exists():
            await self.transport.send(error(404, f"Not found: {rel}"))
            return
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        log.info("[delete] %s", rel)
        await self.transport.send(ack(path=rel))

    async def _handle_move(self, frame: Frame):
        src = self._resolve(frame.meta["src"])
        dst = self._resolve(frame.meta["dst"])
        if not src.exists():
            await self.transport.send(error(404, f"Not found: {frame.meta['src']}"))
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        log.info("[move] %s → %s", frame.meta["src"], frame.meta["dst"])
        await self.transport.send(ack())

    async def _handle_stat(self, frame: Frame):
        rel = frame.meta["path"]
        path = self._resolve(rel)
        if not path.exists():
            await self.transport.send(error(404, f"Not found: {rel}"))
            return
        st = path.stat()
        await self.transport.send(Frame(MsgType.STAT_RESULT, {
            "path": rel,
            "type": "dir" if path.is_dir() else "file",
            "size": st.st_size,
            "modified": st.st_mtime,
            "sha256": calculate_file_hash(path) if path.is_file() else None,
        }))

    async def _handle_disconnect(self, frame: Frame):
        log.info("[-] %s requested disconnect", self.username)
        await self.transport.send(ack())
        raise asyncio.IncompleteReadError(b"", 0)


class NoPointServer:
    def __init__(
            self,
            host: str = "127.0.0.1",
            port: int = 9876,
            storage_root: str = "./storage",
            users: Optional[Dict[str, str]] = None,
    ):
        self.host = host
        self.port = port
        self.root = Path(storage_root)
        self.users = users or DEFAULT_USERS
        self.root.mkdir(parents=True, exist_ok=True)

    async def _handle(self, reader, writer):
        transport = Transport(reader, writer)
        session = ClientSession(transport, self.root, self.users)
        await session.run()

    async def serve_forever(self):
        server = await asyncio.start_server(self._handle, self.host, self.port)
        addr = server.sockets[0].getsockname()
        log.info("ClouDrive server listening on %s:%s", *addr)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    srv = NoPointServer()
    asyncio.run(srv.serve_forever())