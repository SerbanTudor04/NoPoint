"""
NoPoint Protocol - Wire format
=================================
Frame layout:
  [4B magic][1B version][1B msg_type][2B meta_len][4B payload_len][metaJSON][binary payload]

Total fixed header = 12 bytes
"""
import hashlib
import json
import struct
from dataclasses import dataclass, field
from enum import IntEnum

MAGIC        = b"CLDR"
VERSION      = 1
HEADER_FMT   = "!4sBBHI"          # network byte order
HEADER_SIZE  = struct.calcsize(HEADER_FMT)   # 12 bytes
MAX_CHUNK    = 1 * 1024 * 1024    # 1 MB default chunk size


class MsgType(IntEnum):
    # Auth
    AUTH_REQ = 0x01
    AUTH_OK = 0x02
    AUTH_FAIL = 0x03

    # File upload  (client → server)
    UPLOAD_START = 0x10
    UPLOAD_CHUNK = 0x11
    UPLOAD_DONE = 0x12

    # File download (server → client)
    DOWNLOAD_REQ = 0x20
    DOWNLOAD_START = 0x21
    DOWNLOAD_CHUNK = 0x22
    DOWNLOAD_DONE = 0x23

    # Filesystem ops
    LIST_DIR = 0x30
    LIST_RESULT = 0x31
    MKDIR = 0x32
    DELETE = 0x33
    MOVE = 0x34
    STAT = 0x35
    STAT_RESULT = 0x36

    # Sync
    SYNC_REQUEST = 0x40
    SYNC_DELTA = 0x41

    # Control
    ACK = 0xF0
    ERROR = 0xFE
    PING = 0xFC
    PONG = 0xFD
    DISCONNECT = 0xFF


@dataclass
class Frame:
    """
    A single protocol frame.
    meta    : dict   — JSON metadata (commands, file info, errors…)
    payload : bytes  — raw binary (file chunk, or empty)
    """
    msg_type: MsgType
    meta: dict = field(default_factory=dict)
    payload: bytes = b""

    # ── Encode ─────────────────────────────────────────────────────────────────
    def encode(self) -> bytes:
        meta_bytes = json.dumps(self.meta, separators=(",", ":")).encode()
        header = struct.pack(
            HEADER_FMT,
            MAGIC,
            VERSION,
            int(self.msg_type),
            len(meta_bytes),
            len(self.payload),
        )
        return header + meta_bytes + self.payload

    # ── Decode header (static) ─────────────────────────────────────────────────
    @staticmethod
    def decode_header(raw: bytes) -> tuple[int, MsgType, int, int]:
        if len(raw) != HEADER_SIZE:
            raise ValueError(f"Header must be {HEADER_SIZE} bytes, got {len(raw)}")
        magic, version, msg_type, meta_len, payload_len = struct.unpack(HEADER_FMT, raw)
        if magic != MAGIC:
            raise ProtocolError(f"Bad magic: {magic!r}, expected {MAGIC!r}")
        if version != VERSION:
            raise ProtocolError(f"Unsupported version: {version}")
        return version, MsgType(msg_type), meta_len, payload_len

    # ── Pretty print ───────────────────────────────────────────────────────────
    def __repr__(self) -> str:
        return (
            f"Frame({self.msg_type.name} "
            f"meta={self.meta} "
            f"payload={len(self.payload)}B)"
        )


def ack(upload_id: str = "", **extra) -> Frame:
    return Frame(MsgType.ACK, {"upload_id": upload_id, **extra})


def error(code: int, message: str) -> Frame:
    return Frame(MsgType.ERROR, {"code": code, "message": message})


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ProtocolError(Exception):
    pass


class AuthError(ProtocolError):
    pass


class ChecksumError(ProtocolError):
    pass