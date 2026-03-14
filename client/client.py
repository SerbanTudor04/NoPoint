"""
ClouDrive Client
=================
High-level async API that speaks the ClouDrive protocol.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import AsyncIterator, Callable, List, Optional

from protocol.message import (
    Frame, MsgType, ProtocolError, AuthError, ChecksumError,
    sha256, MAX_CHUNK,
)
from protocol.transport import Transport, open_connection

log = logging.getLogger("cloudrive.client")


class ClouDriveClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 9876):
        self.host      = host
        self.port      = port
        self._transport: Optional[Transport] = None

    # ── Connection ─────────────────────────────────────────────────────────────
    async def connect(self, username: str, password: str) -> None:
        self._transport = await open_connection(self.host, self.port)
        frame = await self._transport.send_recv(
            Frame(MsgType.AUTH_REQ, {"username": username, "password": password})
        )
        if frame.msg_type == MsgType.AUTH_FAIL:
            await self._transport.close()
            raise AuthError(frame.meta.get("message", "Auth failed"))
        if frame.msg_type != MsgType.AUTH_OK:
            raise ProtocolError(f"Unexpected response to AUTH_REQ: {frame.msg_type}")
        log.info("[auth] Connected as %s", username)

    async def disconnect(self) -> None:
        if self._transport:
            await self._transport.send(Frame(MsgType.DISCONNECT))
            await self._transport.close()
            self._transport = None

    # ── Helpers ────────────────────────────────────────────────────────────────
    @property
    def t(self) -> Transport:
        if self._transport is None:
            raise RuntimeError("Not connected — call connect() first")
        return self._transport

    async def _expect(self, *types: MsgType) -> Frame:
        frame = await self.t.recv()
        if frame.msg_type == MsgType.ERROR:
            raise ProtocolError(f"Server error {frame.meta.get('code')}: {frame.meta.get('message')}")
        if frame.msg_type not in types:
            raise ProtocolError(f"Expected {types}, got {frame.msg_type}")
        return frame

    # ── Ping ───────────────────────────────────────────────────────────────────
    async def ping(self) -> float:
        import time
        ts = time.monotonic()
        await self.t.send(Frame(MsgType.PING, {"ts": ts}))
        await self._expect(MsgType.PONG)
        return time.monotonic() - ts

    # ── Upload ─────────────────────────────────────────────────────────────────
    async def upload(
        self,
        local_path:   str,
        remote_path:  str,
        on_progress:  Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """Upload a local file to the server."""
        data      = Path(local_path).read_bytes()
        file_size = len(data)
        checksum  = sha256(data)

        # 1. Start upload
        await self.t.send(Frame(MsgType.UPLOAD_START, {
            "path":   remote_path,
            "size":   file_size,
            "sha256": checksum,
        }))
        resp      = await self._expect(MsgType.ACK)
        upload_id = resp.meta["upload_id"]
        chunk_sz  = resp.meta.get("chunk_size", MAX_CHUNK)

        # 2. Stream chunks
        sent = 0
        for offset in range(0, file_size, chunk_sz):
            chunk = data[offset: offset + chunk_sz]
            await self.t.send(Frame(
                MsgType.UPLOAD_CHUNK,
                {"upload_id": upload_id, "offset": offset},
                chunk,
            ))
            await self._expect(MsgType.ACK)
            sent += len(chunk)
            if on_progress:
                on_progress(sent, file_size)

        # 3. Finalise
        await self.t.send(Frame(MsgType.UPLOAD_DONE, {
            "upload_id": upload_id,
            "path":      remote_path,
        }))
        resp = await self._expect(MsgType.ACK)
        if not resp.meta.get("sha256_verified"):
            raise ChecksumError("Server reported checksum mismatch")
        log.info("[upload] ✓ %s → %s", local_path, remote_path)

    # ── Download ───────────────────────────────────────────────────────────────
    async def download(
        self,
        remote_path:  str,
        local_path:   str,
        on_progress:  Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """Download a file from the server to a local path."""
        await self.t.send(Frame(MsgType.DOWNLOAD_REQ, {"path": remote_path}))

        start  = await self._expect(MsgType.DOWNLOAD_START)
        total  = start.meta["size"]
        remote_sha = start.meta["sha256"]

        chunks   = []
        received = 0
        while True:
            frame = await self._expect(MsgType.DOWNLOAD_CHUNK, MsgType.DOWNLOAD_DONE)
            if frame.msg_type == MsgType.DOWNLOAD_DONE:
                break
            chunks.append(frame.payload)
            received += len(frame.payload)
            if on_progress:
                on_progress(received, total)

        data = b"".join(chunks)
        if sha256(data) != remote_sha:
            raise ChecksumError("Downloaded file checksum mismatch")

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        Path(local_path).write_bytes(data)
        log.info("[download] ✓ %s → %s  (%dB)", remote_path, local_path, len(data))

    # ── Filesystem ops ─────────────────────────────────────────────────────────
    async def list_dir(self, path: str = ".") -> List[dict]:
        await self.t.send(Frame(MsgType.LIST_DIR, {"path": path}))
        resp = await self._expect(MsgType.LIST_RESULT)
        return resp.meta["entries"]

    async def mkdir(self, path: str) -> None:
        await self.t.send(Frame(MsgType.MKDIR, {"path": path}))
        await self._expect(MsgType.ACK)

    async def delete(self, path: str) -> None:
        await self.t.send(Frame(MsgType.DELETE, {"path": path}))
        await self._expect(MsgType.ACK)

    async def move(self, src: str, dst: str) -> None:
        await self.t.send(Frame(MsgType.MOVE, {"src": src, "dst": dst}))
        await self._expect(MsgType.ACK)

    async def stat(self, path: str) -> dict:
        await self.t.send(Frame(MsgType.STAT, {"path": path}))
        resp = await self._expect(MsgType.STAT_RESULT)
        return resp.meta

    # ── Context manager ────────────────────────────────────────────────────────
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.disconnect()

