"""
stats.py — Thread-safe statistics collector for TelegramFS.

Every module imports the module-level STATS singleton and calls its
recording methods.  The TUI reads a lock-protected snapshot so it
always sees a consistent view without blocking FUSE ops for long.
"""

import threading
from collections import deque
from dataclasses import dataclass, field
from time import time
from typing import Optional

MAX_LOG_ENTRIES = 300


# ---------------------------------------------------------------------------
# Data classes used in snapshots
# ---------------------------------------------------------------------------

@dataclass
class HandleInfo:
    fh: int
    name: str
    dirty: bool
    buffer_bytes: int
    opened_at: float = field(default_factory=time)


@dataclass
class LogEntry:
    timestamp: float
    level: str       # "INFO" | "SUCCESS" | "WARNING" | "ERROR"
    operation: str   # short op name, e.g. "UPLOAD"
    detail: str      # human-readable detail


# ---------------------------------------------------------------------------
# Stats collector
# ---------------------------------------------------------------------------

class FsStats:
    """
    All public methods are safe to call from any thread.
    Call snapshot() to get a point-in-time dict for display.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.started_at: float = time()

        # ── Cache ──────────────────────────────────────────────────────
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.cache_evictions: int = 0
        self.cache_current_bytes: int = 0
        self.cache_max_bytes: int = 0

        # ── Telegram transfers ─────────────────────────────────────────
        self.uploads_total: int = 0
        self.uploads_bytes: int = 0
        self.uploads_failed: int = 0
        self.downloads_total: int = 0
        self.downloads_bytes: int = 0
        self.downloads_failed: int = 0
        self.active_uploads: int = 0
        self.active_downloads: int = 0

        # ── FUSE operations ────────────────────────────────────────────
        self.ops_read: int = 0
        self.ops_write: int = 0
        self.ops_create: int = 0
        self.ops_delete: int = 0
        self.ops_rename: int = 0
        self.ops_lookup: int = 0
        self.bytes_read: int = 0
        self.bytes_written: int = 0

        # ── Configuration ──────────────────────────────────────────────
        self.encryption_enabled: bool = False
        self.mountpoint: str = ""

        # ── Open file handles ──────────────────────────────────────────
        self._handles: dict[int, HandleInfo] = {}

        # ── Activity log ───────────────────────────────────────────────
        self._log: deque[LogEntry] = deque(maxlen=MAX_LOG_ENTRIES)

    # ── Cache ──────────────────────────────────────────────────────────

    def record_cache_hit(self) -> None:
        with self._lock:
            self.cache_hits += 1

    def record_cache_miss(self) -> None:
        with self._lock:
            self.cache_misses += 1

    def record_cache_eviction(self) -> None:
        with self._lock:
            self.cache_evictions += 1

    def set_cache_size(self, current_bytes: int, max_bytes: int) -> None:
        with self._lock:
            self.cache_current_bytes = current_bytes
            self.cache_max_bytes = max_bytes

    # ── Telegram transfers ─────────────────────────────────────────────

    def begin_upload(self) -> None:
        with self._lock:
            self.active_uploads += 1

    def end_upload(self, n_bytes: int, *, success: bool) -> None:
        with self._lock:
            self.active_uploads = max(0, self.active_uploads - 1)
            if success:
                self.uploads_total += 1
                self.uploads_bytes += n_bytes
            else:
                self.uploads_failed += 1

    def begin_download(self) -> None:
        with self._lock:
            self.active_downloads += 1

    def end_download(self, n_bytes: int, *, success: bool) -> None:
        with self._lock:
            self.active_downloads = max(0, self.active_downloads - 1)
            if success:
                self.downloads_total += 1
                self.downloads_bytes += n_bytes
            else:
                self.downloads_failed += 1

    # ── FUSE operations ────────────────────────────────────────────────

    def record_read(self, n_bytes: int) -> None:
        with self._lock:
            self.ops_read += 1
            self.bytes_read += n_bytes

    def record_write(self, n_bytes: int) -> None:
        with self._lock:
            self.ops_write += 1
            self.bytes_written += n_bytes

    def record_create(self, name: str = "") -> None:
        with self._lock:
            self.ops_create += 1
        self.log("SUCCESS", "CREATE", name)

    def record_delete(self, name: str = "") -> None:
        with self._lock:
            self.ops_delete += 1
        self.log("WARNING", "DELETE", name)

    def record_rename(self, old: str = "", new: str = "") -> None:
        with self._lock:
            self.ops_rename += 1
        self.log("INFO", "RENAME", f"{old} → {new}" if old else "")

    def record_lookup(self) -> None:
        with self._lock:
            self.ops_lookup += 1

    # ── Open file handles ──────────────────────────────────────────────

    def open_handle(self, fh: int, name: str) -> None:
        with self._lock:
            self._handles[fh] = HandleInfo(fh=fh, name=name, dirty=False, buffer_bytes=0)

    def update_handle(self, fh: int, *, dirty: bool, buffer_bytes: int) -> None:
        with self._lock:
            if fh in self._handles:
                self._handles[fh].dirty = dirty
                self._handles[fh].buffer_bytes = buffer_bytes

    def close_handle(self, fh: int) -> None:
        with self._lock:
            self._handles.pop(fh, None)

    # ── Activity log ───────────────────────────────────────────────────

    def log(self, level: str, operation: str, detail: str = "") -> None:
        with self._lock:
            self._log.append(LogEntry(time(), level, operation, detail))

    # ── Snapshot ───────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Return a consistent copy of all stats for the TUI to render."""
        with self._lock:
            total_reqs = self.cache_hits + self.cache_misses
            return {
                "uptime": time() - self.started_at,
                "mountpoint": self.mountpoint,
                "encryption": self.encryption_enabled,
                "cache": {
                    "hits": self.cache_hits,
                    "misses": self.cache_misses,
                    "hit_rate": self.cache_hits / total_reqs if total_reqs else 0.0,
                    "current_bytes": self.cache_current_bytes,
                    "max_bytes": self.cache_max_bytes,
                    "evictions": self.cache_evictions,
                },
                "transfers": {
                    "uploads_total": self.uploads_total,
                    "uploads_bytes": self.uploads_bytes,
                    "uploads_failed": self.uploads_failed,
                    "downloads_total": self.downloads_total,
                    "downloads_bytes": self.downloads_bytes,
                    "downloads_failed": self.downloads_failed,
                    "active_uploads": self.active_uploads,
                    "active_downloads": self.active_downloads,
                },
                "ops": {
                    "read": self.ops_read,
                    "write": self.ops_write,
                    "create": self.ops_create,
                    "delete": self.ops_delete,
                    "rename": self.ops_rename,
                    "lookup": self.ops_lookup,
                    "bytes_read": self.bytes_read,
                    "bytes_written": self.bytes_written,
                },
                "handles": list(self._handles.values()),
                "log": list(self._log),
            }


# Module-level singleton — import this everywhere.
STATS = FsStats()
