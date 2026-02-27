# ver 1.0
import threading
from collections import deque
from dataclasses import dataclass, field
from time import time
from typing import Optional

MAX_LOG_ENTRIES = 300


@dataclass
class HandleInfo:
    fh:           int
    name:         str
    dirty:        bool
    buffer_bytes: int
    opened_at:    float = field(default_factory=time)


@dataclass
class LogEntry:
    timestamp: float
    level:     str
    operation: str
    detail:    str


class FsStats:
    def __init__(self) -> None:
        self._lock      = threading.RLock()
        self.started_at = time()

        self.cache_hits:          int = 0
        self.cache_misses:        int = 0
        self.cache_evictions:     int = 0
        self.cache_current_bytes: int = 0
        self.cache_max_bytes:     int = 0

        self.uploads_total:    int = 0
        self.uploads_bytes:    int = 0
        self.uploads_failed:   int = 0
        self.downloads_total:  int = 0
        self.downloads_bytes:  int = 0
        self.downloads_failed: int = 0
        self.active_uploads:   int = 0
        self.active_downloads: int = 0

        # Verification counters
        self.verify_passes:         int = 0   # blocks that passed full content check
        self.verify_content_fails:  int = 0   # downloaded but hash didn't match
        self.verify_missing:        int = 0   # msg ID didn't exist on Telegram
        self.verify_reuploads:      int = 0   # re-uploads triggered by verify
        self.verify_total_checks:   int = 0   # total blocks submitted for verification
        self.verify_hard_failures:  int = 0   # flushes aborted after all retries exhausted
        self.active_verifications:  int = 0   # currently running verify operations
        self.dl_hash_fails:         int = 0   # blocks whose hash mismatched on download

        self.ops_read:      int = 0
        self.ops_write:     int = 0
        self.ops_create:    int = 0
        self.ops_delete:    int = 0
        self.ops_rename:    int = 0
        self.ops_lookup:    int = 0
        self.bytes_read:    int = 0
        self.bytes_written: int = 0

        self.encryption_enabled: bool = False
        self.mountpoint:         str  = ""

        # ── network connectivity ───────────────────────────────────────────
        self.network_online:         bool            = True   # optimistic until first probe
        self._network_went_offline:  Optional[float] = None   # monotonic timestamp

        self._handles: dict[int, HandleInfo] = {}
        self._log: deque[LogEntry] = deque(maxlen=MAX_LOG_ENTRIES)

        # ── direct upload progress ────────────────────────────────────────────────
        self.upload_active:           bool  = False
        self.upload_stop_requested:   bool  = False
        self.upload_finished:          bool  = False
        self.upload_files_total:   int   = 0
        self.upload_files_done:    int   = 0
        self.upload_files_skipped: int   = 0
        self.upload_files_errors:  int   = 0
        self.upload_bytes_total:   int   = 0
        self.upload_bytes_done:    int   = 0
        self.upload_current_file:  str   = ""
        self.upload_current_size:  int   = 0
        self.upload_current_block: int   = 0
        self.upload_current_nblocks: int = 0
        self.upload_current_block_bytes_done:  int = 0
        self.upload_current_block_bytes_total: int = 0

    # ── cache ──────────────────────────────────────────────────────────────

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
            self.cache_max_bytes     = max_bytes

    # ── transfers ──────────────────────────────────────────────────────────

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

    # ── verification ───────────────────────────────────────────────────────

    def begin_verify(self) -> None:
        with self._lock:
            self.active_verifications += 1

    def end_verify(self) -> None:
        with self._lock:
            self.active_verifications = max(0, self.active_verifications - 1)

    def record_verify_check(self, n_blocks: int) -> None:
        with self._lock:
            self.verify_total_checks += n_blocks

    def record_verify_pass(self, n_blocks: int = 1) -> None:
        with self._lock:
            self.verify_passes += n_blocks

    def record_verify_content_fail(self) -> None:
        with self._lock:
            self.verify_content_fails += 1

    def record_verify_missing(self) -> None:
        with self._lock:
            self.verify_missing += 1

    def record_verify_reupload(self, n: int = 1) -> None:
        with self._lock:
            self.verify_reuploads += n

    def record_verify_hard_failure(self) -> None:
        with self._lock:
            self.verify_hard_failures += 1

    def record_dl_hash_fail(self) -> None:
        """Increment counter when a downloaded block's SHA-256 does not match the stored hash."""
        with self._lock:
            self.dl_hash_fails += 1

    # ── fs ops ─────────────────────────────────────────────────────────────

    def record_read(self, n_bytes: int) -> None:
        with self._lock:
            self.ops_read   += 1
            self.bytes_read += n_bytes

    def record_write(self, n_bytes: int) -> None:
        with self._lock:
            self.ops_write     += 1
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

    # ── handles ────────────────────────────────────────────────────────────

    def open_handle(self, fh: int, name: str) -> None:
        with self._lock:
            self._handles[fh] = HandleInfo(fh=fh, name=name, dirty=False, buffer_bytes=0)

    def update_handle(self, fh: int, *, dirty: bool, buffer_bytes: int) -> None:
        with self._lock:
            if fh in self._handles:
                self._handles[fh].dirty        = dirty
                self._handles[fh].buffer_bytes = buffer_bytes

    def close_handle(self, fh: int) -> None:
        with self._lock:
            self._handles.pop(fh, None)

    # ── logging ────────────────────────────────────────────────────────────

    def log(self, level: str, operation: str, detail: str = "") -> None:
        with self._lock:
            self._log.append(LogEntry(time(), level, operation, detail))

    # ── direct upload progress ───────────────────────────────────────────────────

    def upload_set_totals(self, n_files: int, n_bytes: int) -> None:
        with self._lock:
            self.upload_active          = True
            self.upload_stop_requested  = False
            self.upload_finished        = False
            self.upload_files_total     = n_files
            self.upload_bytes_total     = n_bytes

    def upload_file_start(self, path: str, size: int) -> None:
        with self._lock:
            self.upload_current_file   = path
            self.upload_current_size   = size
            self.upload_current_block  = 0
            self.upload_current_nblocks = 0

    def upload_block_start(self, block_idx: int, n_blocks: int, path: str) -> None:
        with self._lock:
            self.upload_current_block              = block_idx
            self.upload_current_nblocks            = n_blocks
            self.upload_current_block_bytes_done   = 0
            self.upload_current_block_bytes_total  = 0

    def upload_block_done(self, n_bytes: int) -> None:
        with self._lock:
            self.upload_bytes_done += n_bytes

    def upload_block_progress(self, sent: int, total: int) -> None:
        """Update intra-block upload progress (called from Telegram progress callback)."""
        with self._lock:
            self.upload_current_block_bytes_done  = sent
            self.upload_current_block_bytes_total = total

    def upload_file_done(self) -> None:
        with self._lock:
            self.upload_files_done    += 1
            self.upload_current_file   = ""

    def upload_file_skipped(self, size: int) -> None:
        with self._lock:
            self.upload_files_skipped += 1
            self.upload_bytes_done    += size

    def upload_file_error(self) -> None:
        with self._lock:
            self.upload_files_errors += 1
            self.upload_current_file  = ""

    def upload_finish(self) -> None:
        with self._lock:
            self.upload_active       = False
            self.upload_finished     = True
            self.upload_current_file = ""

    # ── network ───────────────────────────────────────────────────────────

    def set_network_state(self, online: bool) -> None:
        import time as _time
        with self._lock:
            if online and not self.network_online:
                self._network_went_offline = None
            elif not online and self.network_online:
                self._network_went_offline = _time.monotonic()
            self.network_online = online

    def network_offline_seconds(self) -> float:
        """Seconds since the network went offline (0 if currently online)."""
        import time as _time
        with self._lock:
            if self._network_went_offline is None:
                return 0.0
            return _time.monotonic() - self._network_went_offline

    # ── snapshot ───────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        with self._lock:
            cache_total = self.cache_hits + self.cache_misses
            return {
                "uptime":     time() - self.started_at,
                "mountpoint": self.mountpoint,
                "encryption": self.encryption_enabled,
                "network": {
                    "online": self.network_online,
                },
                "cache": {
                    "hits":          self.cache_hits,
                    "misses":        self.cache_misses,
                    "hit_rate":      self.cache_hits / cache_total if cache_total else 0.0,
                    "current_bytes": self.cache_current_bytes,
                    "max_bytes":     self.cache_max_bytes,
                    "evictions":     self.cache_evictions,
                },
                "transfers": {
                    "uploads_total":    self.uploads_total,
                    "uploads_bytes":    self.uploads_bytes,
                    "uploads_failed":   self.uploads_failed,
                    "downloads_total":  self.downloads_total,
                    "downloads_bytes":  self.downloads_bytes,
                    "downloads_failed": self.downloads_failed,
                    "active_uploads":   self.active_uploads,
                    "active_downloads": self.active_downloads,
                },
                "verify": {
                    "total_checks":  self.verify_total_checks,
                    "passes":        self.verify_passes,
                    "missing":       self.verify_missing,
                    "content_fails": self.verify_content_fails,
                    "reuploads":     self.verify_reuploads,
                    "hard_failures": self.verify_hard_failures,
                    "dl_hash_fails": self.dl_hash_fails,
                    "active":        self.active_verifications,
                    "pass_rate": (
                        self.verify_passes / self.verify_total_checks
                        if self.verify_total_checks else 1.0
                    ),
                },
                "ops": {
                    "read":          self.ops_read,
                    "write":         self.ops_write,
                    "create":        self.ops_create,
                    "delete":        self.ops_delete,
                    "rename":        self.ops_rename,
                    "lookup":        self.ops_lookup,
                    "bytes_read":    self.bytes_read,
                    "bytes_written": self.bytes_written,
                },
                "handles": list(self._handles.values()),
                "log":     list(self._log),
                "direct_upload": {
                    "active":               self.upload_active,
                    "stop_requested":       self.upload_stop_requested,
                    "finished":             self.upload_finished,
                    "files_total":     self.upload_files_total,
                    "files_done":      self.upload_files_done,
                    "files_skipped":   self.upload_files_skipped,
                    "files_errors":    self.upload_files_errors,
                    "bytes_total":     self.upload_bytes_total,
                    "bytes_done":      self.upload_bytes_done,
                    "current_file":    self.upload_current_file,
                    "current_block":   self.upload_current_block,
                    "current_nblocks": self.upload_current_nblocks,
                    "current_block_bytes_done":  self.upload_current_block_bytes_done,
                    "current_block_bytes_total": self.upload_current_block_bytes_total,
                },
            }


STATS = FsStats()
