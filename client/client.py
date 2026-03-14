"""
nopoint.client
===============
High-level async API that speaks the NoPoint Drive protocol.

Demultiplexing
--------------
A single background "reader pump" task owns the TCP recv loop.
Every incoming frame is routed to one of two queues:

  _reply_queue  — responses to commands the client sent
                  (AUTH_OK, ACK, LIST_RESULT, DOWNLOAD_CHUNK, …)

  _push_queue   — unsolicited server pushes
                  (SYNC_DELTA notifications from other users)

Commands call _expect() which reads from _reply_queue.
sync_listener reads from _push_queue via iter_pushes().

This guarantees only one coroutine ever reads from the socket.
"""

import asyncio
import logging
import time
from collections.abc import Callable, AsyncGenerator
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

# Frame types that are server-pushed (never a direct reply to a command)
_PUSH_TYPES = {MsgType.SYNC_DELTA}

type ProgressCallback = Callable[[int, int], None]
type EntryList        = list[dict]


class NoPointClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 9876) -> None:
        self.host = host
        self.port = port
        self._transport:   Transport | None        = None
        self._pump_task:   asyncio.Task | None     = None
        self._reply_queue: asyncio.Queue[Frame]    = asyncio.Queue()
        self._push_queue:  asyncio.Queue[Frame]    = asyncio.Queue()

    # ══════════════════════════════════════════════════════════════════════════
    #  Connection
    # ══════════════════════════════════════════════════════════════════════════

    @property
    def connected(self) -> bool:
        return self._transport is not None and not self._transport._closed

    async def connect(self, username: str, password: str) -> None:
        if self.connected:
            raise RuntimeError("Already connected")

        self._transport = await open_connection(self.host, self.port)

        # Start the reader pump before sending anything
        self._reply_queue = asyncio.Queue()
        self._push_queue  = asyncio.Queue()
        self._pump_task   = asyncio.create_task(
            self._reader_pump(), name="reader-pump"
        )

        # Auth handshake — use _send + _expect directly (pump already running)
        await self._transport.send(
            Frame(MsgType.AUTH_REQ, {"username": username, "password": password})
        )
        resp = await self._expect(MsgType.AUTH_OK, MsgType.AUTH_FAIL)

        match resp.msg_type:
            case MsgType.AUTH_OK:
                log.info("[auth] Connected as %s @ %s:%d", username, self.host, self.port)
            case MsgType.AUTH_FAIL:
                await self._shutdown_pump()
                await self._transport.close()
                self._transport = None
                raise AuthError(resp.meta.get("message", "Invalid credentials"))

    async def disconnect(self) -> None:
        if not self.connected:
            return
        try:
            await self._transport.send(Frame(MsgType.DISCONNECT))
        except Exception:
            pass
        finally:
            await self._shutdown_pump()
            await self._transport.close()
            self._transport = None

    # ══════════════════════════════════════════════════════════════════════════
    #  Reader pump — the ONLY coroutine that calls transport.recv()
    # ══════════════════════════════════════════════════════════════════════════

    async def _reader_pump(self) -> None:
        """
        Read frames off the wire forever and route them to the right queue.
        Exits when the connection closes or is cancelled.
        """
        try:
            while True:
                frame = await self._transport.recv()
                if frame.msg_type in _PUSH_TYPES:
                    await self._push_queue.put(frame)
                else:
                    await self._reply_queue.put(frame)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug("[pump] stopped: %s", e)
            # Unblock anyone waiting on _expect() so they get an error
            sentinel = Frame(MsgType.ERROR, {"code": 0, "message": f"Connection lost: {e}"})
            await self._reply_queue.put(sentinel)
            await self._push_queue.put(sentinel)

    async def _shutdown_pump(self) -> None:
        if self._pump_task and not self._pump_task.done():
            self._pump_task.cancel()
            try:
                await self._pump_task
            except asyncio.CancelledError:
                pass
        self._pump_task = None

    # ══════════════════════════════════════════════════════════════════════════
    #  Internal helpers
    # ══════════════════════════════════════════════════════════════════════════

    @property
    def _t(self) -> Transport:
        if not self.connected:
            raise RuntimeError("Not connected — call connect() first")
        return self._transport

    async def _expect(self, *types: MsgType) -> Frame:
        """Pull the next reply frame and assert its type."""
        frame = await self._reply_queue.get()
        if frame.msg_type is MsgType.ERROR:
            raise ProtocolError(
                f"Server error {frame.meta.get('code')}: {frame.meta.get('message')}"
            )
        if frame.msg_type not in types:
            expected = ", ".join(t.name for t in types)
            raise ProtocolError(f"Expected [{expected}], got {frame.msg_type.name}")
        return frame

    async def iter_pushes(self) -> AsyncGenerator[Frame, None]:
        """
        Async generator that yields server-pushed SYNC_DELTA frames.
        Used by sync_listener.  Exits when the connection drops.
        """
        while self.connected:
            frame = await self._push_queue.get()
            if frame.msg_type is MsgType.ERROR:
                break
            yield frame

    # ══════════════════════════════════════════════════════════════════════════
    #  Ping
    # ══════════════════════════════════════════════════════════════════════════

    async def ping(self) -> float:
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
        data     = Path(local_path).read_bytes()
        total    = len(data)
        checksum = sha256(data)

        await self._t.send(Frame(MsgType.UPLOAD_START, {
            "path": remote_path, "size": total, "sha256": checksum,
        }))
        ack_frame = await self._expect(MsgType.ACK)
        upload_id = ack_frame.meta["upload_id"]
        cs        = chunk_size or ack_frame.meta.get("chunk_size", MAX_CHUNK)

        sent = 0
        for offset in range(0, total, cs):
            chunk = data[offset: offset + cs]
            await self._t.send(Frame(
                MsgType.UPLOAD_CHUNK,
                {"upload_id": upload_id, "offset": offset},
                chunk,
            ))
            await self._expect(MsgType.ACK)
            sent += len(chunk)
            if on_progress:
                on_progress(sent, total)

        await self._t.send(Frame(MsgType.UPLOAD_DONE, {
            "upload_id": upload_id, "path": remote_path,
        }))
        final = await self._expect(MsgType.ACK)
        if not final.meta.get("sha256_verified"):
            raise ChecksumError(f"Server rejected {remote_path!r}: checksum mismatch")
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
        await self._t.send(Frame(MsgType.DOWNLOAD_REQ, {"path": remote_path}))

        start      = await self._expect(MsgType.DOWNLOAD_START)
        total      = start.meta["size"]
        server_sha = start.meta["sha256"]

        chunks: list[bytes] = []
        received = 0
        while True:
            frame = await self._expect(MsgType.DOWNLOAD_CHUNK, MsgType.DOWNLOAD_DONE)
            if frame.msg_type is MsgType.DOWNLOAD_DONE:
                break
            chunks.append(frame.payload)
            received += len(frame.payload)
            if on_progress:
                on_progress(received, total)

        data = b"".join(chunks)
        if sha256(data) != server_sha:
            raise ChecksumError(f"Download of {remote_path!r} failed integrity check")

        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        log.info("[download] ✓  %s  (%d bytes)", remote_path, len(data))

    async def download_stream(
        self,
        remote_path: str,
        local_path:  str | Path,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> None:
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
            raise ChecksumError(f"Streamed download of {remote_path!r} failed integrity check")

        log.info("[download_stream] ✓  %s  (%d bytes)", remote_path, received)

    # ══════════════════════════════════════════════════════════════════════════
    #  Filesystem ops
    # ══════════════════════════════════════════════════════════════════════════

    async def list_dir(self, path: str = ".") -> EntryList:
        await self._t.send(Frame(MsgType.LIST_DIR, {"path": path}))
        resp = await self._expect(MsgType.LIST_RESULT)
        return resp.meta["entries"]

    async def mkdir(self, path: str) -> None:
        await self._t.send(Frame(MsgType.MKDIR, {"path": path}))
        await self._expect(MsgType.ACK)

    async def delete(self, path: str) -> None:
        await self._t.send(Frame(MsgType.DELETE, {"path": path}))
        await self._expect(MsgType.ACK)

    async def move(self, src: str, dst: str) -> None:
        await self._t.send(Frame(MsgType.MOVE, {"src": src, "dst": dst}))
        await self._expect(MsgType.ACK)

    async def stat(self, path: str) -> dict:
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