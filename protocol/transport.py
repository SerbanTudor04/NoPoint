"""
NoPoint Protocol - Async Transport
=====================================
Wraps asyncio StreamReader/StreamWriter with frame-level read/write.
"""
import asyncio
import logging
import json
from protocol.message import Frame, HEADER_SIZE

log = logging.getLogger("nopoint.transport")


class Transport:
    """
    Async frame transport over a TCP connection.
    All I/O is non-blocking via asyncio streams.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def send(self, frame: Frame) -> None:
        if self._closed:
            raise ConnectionError("Transport is closed")
        data = frame.encode()
        self._writer.write(data)
        await self._writer.drain()
        log.debug("→ %s  (%d bytes)", frame.msg_type.name, len(data))

    async def recv(self) -> Frame:
        if self._closed:
            raise ConnectionError("Transport is closed")

        # 1. Read fixed header
        raw_header = await self._reader.readexactly(HEADER_SIZE)
        _, msg_type, meta_len, payload_len = Frame.decode_header(raw_header)

        # 2. Read metadata JSON
        meta_bytes = await self._reader.readexactly(meta_len) if meta_len else b"{}"

        # 3. Read binary payload
        payload = await self._reader.readexactly(payload_len) if payload_len else b""


        meta = json.loads(meta_bytes)
        frame = Frame(msg_type, meta, payload)
        log.debug("← %s  (meta=%dB payload=%dB)", msg_type.name, meta_len, payload_len)
        return frame

    # ── Helpers ────────────────────────────────────────────────────────────────
    async def send_recv(self, frame: Frame) -> Frame:
        """Send a frame and wait for the immediate response."""
        await self.send(frame)
        return await self.recv()

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    @property
    def peer(self) -> str:
        try:
            addr = self._writer.get_extra_info("peername")
            return f"{addr[0]}:{addr[1]}" if addr else "unknown"
        except Exception:
            return "unknown"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


async def open_connection(host: str, port: int) -> "Transport":
    reader, writer = await asyncio.open_connection(host, port)
    return Transport(reader, writer)
