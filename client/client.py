"""
nopoint.client
===============
High-level async API that speaks the NoPoint Drive protocol.

Usage
-----
    async with NoPointClient("127.0.0.1", 9876) as c:
        await c.connect("alice", "s3cr3t")

        await c.upload("local/report.pdf", "docs/report.pdf",
                       on_progress=lambda s, t: print(f"{s}/{t}"))

        entries = await c.list_dir("docs")
        info    = await c.stat("docs/report.pdf")

        await c.download("docs/report.pdf", "/tmp/report.pdf")
        await c.move("docs/report.pdf", "archive/report.pdf")
        await c.delete("archive/report.pdf")
"""

import asyncio
import logging
import time
from collections.abc import Callable, AsyncIterator
from pathlib import Path

from protocol.message import (
    Frame,
    MsgType,
    ProtocolError,
    AuthError,
    ChecksumError,
    sha256,
    MAX_CHUNK,
)
from protocol.transport import Transport, open_connection

log = logging.getLogger("nopoint.client")


# ── Types ──────────────────────────────────────────────────────────────────────
type ProgressCallback = Callable[[int, int], None]   # (bytes_done, total_bytes)
type EntryList        = list[dict]


# ── Client ─────────────────────────────────────────────────────────────────────
class NoPointClient:
    """
    Async client for the NoPoint Drive protocol.

    All methods are coroutines and must be awaited.
    The client holds a single persistent TCP connection per instance;
    create one client per logical session (or reconnect after disconnect).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9876) -> None:
        self.host = host
        self.port = port
        self._transport: Transport | None = None

    # ══════════════════════════════════════════════════════════════════════════
    #  Connection management
    # ══════════════════════════════════════════════════════════════════════════

    @property
    def connected(self) -> bool:
        return self._transport is not None and not self._transport._closed

    async def connect(self, username: str, password: str) -> None:
        """
        Open a TCP connection and authenticate.
        Raises AuthError on bad credentials.
        Raises ProtocolError on unexpected server response.
        """
        if self.connected:
            raise RuntimeError("Already connected — call disconnect() first")

        log.debug("Connecting to %s:%d as %s", self.host, self.port, username)
        self._transport = await open_connection(self.host, self.port)

        resp = await self._transport.send_recv(
            Frame(MsgType.AUTH_REQ, {"username": username, "password": password})
        )

        match resp.msg_type:
            case MsgType.AUTH_OK:
                log.info("[auth] Connected as %s @ %s:%d", username, self.host, self.port)
            case MsgType.AUTH_FAIL:
                await self._transport.close()
                self._transport = None
                raise AuthError(resp.meta.get("message", "Invalid credentials"))
            case _:
                await self._transport.close()
                self._transport = None
                raise ProtocolError(f"Unexpected AUTH response: {resp.msg_type.name}")

    async def disconnect(self) -> None:
        """Gracefully tell the server we're leaving, then close the socket."""
        if not self.connected:
            return
        try:
            await self._transport.send(Frame(MsgType.DISCONNECT))
        except Exception:
            pass   # best-effort
        finally:
            await self._transport.close()
            self._transport = None
            log.info("[auth] Disconnected from %s:%d", self.host, self.port)

    # ══════════════════════════════════════════════════════════════════════════
    #  Internal helpers
    # ══════════════════════════════════════════════════════════════════════════

    @property
    def _t(self) -> Transport:
        if not self.connected:
            raise RuntimeError("Not connected — call connect() first")
        return self._transport

    async def _expect(self, *types: MsgType) -> Frame:
        """
        Receive one frame and assert its type is in *types*.
        Raises ProtocolError on ERROR frames or type mismatch.
        """
        frame = await self._t.recv()
        if frame.msg_type is MsgType.ERROR:
            code = frame.meta.get("code", "?")
            msg  = frame.meta.get("message", "unknown error")
            raise ProtocolError(f"Server error {code}: {msg}")
        if frame.msg_type not in types:
            expected = ", ".join(t.name for t in types)
            raise ProtocolError(
                f"Expected [{expected}], got {frame.msg_type.name}"
            )
        return frame

    # ══════════════════════════════════════════════════════════════════════════
    #  Ping
    # ══════════════════════════════════════════════════════════════════════════

    async def ping(self) -> float:
        """Return round-trip time in seconds."""
        t0 = time.monotonic()
        await self._t.send(Frame(MsgType.PING, {"ts": t0}))
        await self._expect(MsgType.PONG)
        return time.monotonic() - t0

    # ══════════════════════════════════════════════════════════════════════════
    #  Upload
    # ══════════════════════════════════════════════════════════════════════════

    async def upload(
        self,
        local_path:  str | Path,
        remote_path: str,
        *,
        on_progress: ProgressCallback | None = None,
        chunk_size:  int | None = None,
    ) -> None:
        """
        Upload a local file to *remote_path* on the server.

        The file is read into memory once, SHA-256 verified by the server,
        and streamed in chunks.  on_progress(bytes_sent, total_bytes) is
        called after each chunk ACK.

        Raises ChecksumError if the server rejects the final checksum.
        """
        data      = Path(local_path).read_bytes()
        total     = len(data)
        checksum  = sha256(data)

        log.debug("[upload] START  %s  →  %s  (%d bytes)", local_path, remote_path, total)

        # ── 1. Announce ────────────────────────────────────────────────────────
        await self._t.send(Frame(MsgType.UPLOAD_START, {
            "path":   remote_path,
            "size":   total,
            "sha256": checksum,
        }))
        ack       = await self._expect(MsgType.ACK)
        upload_id = ack.meta["upload_id"]
        cs        = chunk_size or ack.meta.get("chunk_size", MAX_CHUNK)

        # ── 2. Stream chunks ───────────────────────────────────────────────────
        sent = 0
        for offset in range(0, total, cs):
            chunk = data[offset : offset + cs]
            await self._t.send(Frame(
                MsgType.UPLOAD_CHUNK,
                {"upload_id": upload_id, "offset": offset},
                chunk,
            ))
            await self._expect(MsgType.ACK)
            sent += len(chunk)
            log.debug("[upload] chunk  offset=%d  +%dB  total=%d", offset, len(chunk), sent)
            if on_progress:
                on_progress(sent, total)

        # ── 3. Finalise ────────────────────────────────────────────────────────
        await self._t.send(Frame(MsgType.UPLOAD_DONE, {
            "upload_id": upload_id,
            "path":      remote_path,
        }))
        final = await self._expect(MsgType.ACK)
        if not final.meta.get("sha256_verified"):
            raise ChecksumError(
                f"Server rejected upload of {remote_path!r}: checksum mismatch"
            )
        log.info("[upload] ✓  %s  (%d bytes)", remote_path, total)

    # ══════════════════════════════════════════════════════════════════════════
    #  Download
    # ══════════════════════════════════════════════════════════════════════════

    async def download(
        self,
        remote_path: str,
        local_path:  str | Path,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        """
        Download *remote_path* from the server and write it to *local_path*.

        Parent directories are created automatically.
        Raises ChecksumError if the reassembled file doesn't match the
        server-supplied SHA-256.
        """
        await self._t.send(Frame(MsgType.DOWNLOAD_REQ, {"path": remote_path}))

        start      = await self._expect(MsgType.DOWNLOAD_START)
        total      = start.meta["size"]
        server_sha = start.meta["sha256"]

        log.debug("[download] START  %s  (%d bytes)", remote_path, total)

        chunks: list[bytes] = []
        received = 0

        while True:
            frame = await self._expect(MsgType.DOWNLOAD_CHUNK, MsgType.DOWNLOAD_DONE)
            if frame.msg_type is MsgType.DOWNLOAD_DONE:
                break
            chunks.append(frame.payload)
            received += len(frame.payload)
            log.debug("[download] chunk  +%dB  total=%d", len(frame.payload), received)
            if on_progress:
                on_progress(received, total)

        data = b"".join(chunks)

        if sha256(data) != server_sha:
            raise ChecksumError(
                f"Download of {remote_path!r} failed integrity check"
            )

        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        log.info("[download] ✓  %s  →  %s  (%d bytes)", remote_path, local_path, len(data))

    # ══════════════════════════════════════════════════════════════════════════
    #  Streaming download (large files / memory-constrained)
    # ══════════════════════════════════════════════════════════════════════════

    async def download_stream(
        self,
        remote_path: str,
        local_path:  str | Path,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        """
        Like download(), but writes each chunk to disk immediately instead of
        buffering the whole file in memory.  Trades memory for a two-pass
        SHA-256 verify at the end (reads the file back once).

        Prefer this for files larger than a few hundred MB.
        """
        await self._t.send(Frame(MsgType.DOWNLOAD_REQ, {"path": remote_path}))

        start      = await self._expect(MsgType.DOWNLOAD_START)
        total      = start.meta["size"]
        server_sha = start.meta["sha256"]

        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        received = 0
        with dest.open("wb") as fh:
            while True:
                frame = await self._expect(MsgType.DOWNLOAD_CHUNK, MsgType.DOWNLOAD_DONE)
                if frame.msg_type is MsgType.DOWNLOAD_DONE:
                    break
                fh.write(frame.payload)
                received += len(frame.payload)
                if on_progress:
                    on_progress(received, total)

        if sha256(dest.read_bytes()) != server_sha:
            dest.unlink(missing_ok=True)
            raise ChecksumError(
                f"Streamed download of {remote_path!r} failed integrity check — file removed"
            )

        log.info("[download_stream] ✓  %s  →  %s  (%d bytes)", remote_path, local_path, received)

    # ══════════════════════════════════════════════════════════════════════════
    #  Filesystem operations
    # ══════════════════════════════════════════════════════════════════════════

    async def list_dir(self, path: str = ".") -> EntryList:
        """
        Return a list of entry dicts for *path*:
            [{"name": str, "type": "file"|"dir", "size": int, "modified": float}, …]
        """
        await self._t.send(Frame(MsgType.LIST_DIR, {"path": path}))
        resp = await self._expect(MsgType.LIST_RESULT)
        return resp.meta["entries"]

    async def mkdir(self, path: str) -> None:
        """Create a directory (and all parents) at *path*."""
        await self._t.send(Frame(MsgType.MKDIR, {"path": path}))
        await self._expect(MsgType.ACK)
        log.debug("[mkdir] %s", path)

    async def delete(self, path: str) -> None:
        """
        Delete a file or directory at *path*.
        Directories are removed recursively on the server side.
        """
        await self._t.send(Frame(MsgType.DELETE, {"path": path}))
        await self._expect(MsgType.ACK)
        log.debug("[delete] %s", path)

    async def move(self, src: str, dst: str) -> None:
        """Move or rename *src* to *dst*."""
        await self._t.send(Frame(MsgType.MOVE, {"src": src, "dst": dst}))
        await self._expect(MsgType.ACK)
        log.debug("[move] %s  →  %s", src, dst)

    async def stat(self, path: str) -> dict:
        """
        Return metadata for *path*:
            {"path": str, "type": str, "size": int,
             "modified": float, "sha256": str | None}
        """
        await self._t.send(Frame(MsgType.STAT, {"path": path}))
        resp = await self._expect(MsgType.STAT_RESULT)
        return resp.meta

    # ══════════════════════════════════════════════════════════════════════════
    #  Context manager
    # ══════════════════════════════════════════════════════════════════════════

    async def __aenter__(self) -> "NoPointClient":
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()