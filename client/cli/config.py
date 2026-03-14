"""
nopoint.cli.config
===================
Persists connection profile and sync-root to a JSON file at
~/.config/nopoint/config.json

Loaded once at startup; saved whenever the user changes a setting.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

_CONFIG_PATH = Path.home() / ".config" / "nopoint" / "config.json"


@dataclass
class Config:
    host:      str  = "127.0.0.1"
    port:      int  = 9876
    username:  str  = ""
    sync_root: str  = str(Path.home() / "NoPointDrive")

    # ── Persistence ────────────────────────────────────────────────────────────
    @classmethod
    def load(cls) -> "Config":
        if _CONFIG_PATH.exists():
            try:
                data = json.loads(_CONFIG_PATH.read_text())
                return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
            except Exception:
                pass   # corrupt config → fall back to defaults
        return cls()

    def save(self) -> None:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(asdict(self), indent=2))

    # ── Helpers ────────────────────────────────────────────────────────────────
    @property
    def sync_path(self) -> Path:
        p = Path(self.sync_root).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    def local_path_for(self, remote: str) -> Path:
        """Map a remote path to an absolute local path inside sync_root."""
        return self.sync_path / remote.lstrip("/")