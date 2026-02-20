import logging
import math
import time as _time

logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
from telethon import TelegramClient, sync
from dotenv import load_dotenv
import os
from io import BytesIO
from cryptography.fernet import Fernet
from cachetools import LRUCache
import gc

from stats import STATS

load_dotenv()

FILE_MAX_SIZE_BYTES = int(2 * 1e9)  # 2 GB per Telegram chunk

CACHE_MAXSIZE = int(5e9)  # 5 GB
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def _sizeof(val) -> int:
    try:
        return len(val)
    except Exception:
        return 1


def _progress_cb(sent_bytes: int, total: int) -> None:
    pct = int(sent_bytes / total * 100)
    if pct % 10 == 0:
        print(f"Progress: {pct}%...")


def _with_retry(fn, *args, retries: int = MAX_RETRIES, delay: int = RETRY_DELAY, **kwargs):
    """Call *fn* with exponential back-off retries on exception."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            logging.warning("Attempt %d/%d failed for %s: %s", attempt, retries, fn.__name__, exc)
            if attempt < retries:
                _time.sleep(delay * attempt)
    raise last_exc  # type: ignore[misc]


class TelegramFileClient:
    def __init__(self, session_name: str, api_id: str, api_hash: str, channel_link: str) -> None:
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.client.start()
        self.channel_entity = self.client.get_entity(channel_link)

        # Treat a blank / whitespace-only key the same as "no key".
        raw_key = os.getenv("ENCRYPTION_KEY", "").strip()
        self.encryption_key: bytes | None = raw_key.encode() if raw_key else None

        self.cached_files: LRUCache = LRUCache(CACHE_MAXSIZE, getsizeof=_sizeof)

        # Publish config to stats
        STATS.encryption_enabled = self.encryption_key is not None
        STATS.set_cache_size(0, CACHE_MAXSIZE)

        print("USING ENCRYPTION:", self.encryption_key is not None)

    # ── Internal helpers ───────────────────────────────────────────────

    def _encrypt(self, data: bytes) -> bytes:
        if self.encryption_key is None:
            return data
        return Fernet(self.encryption_key).encrypt(data)

    def _decrypt(self, data: bytes) -> bytes:
        if self.encryption_key is None:
            return data
        return Fernet(self.encryption_key).decrypt(data)

    def _split_into_chunks(self, data: bytes) -> list[bytes]:
        """Split *data* into ≤ FILE_MAX_SIZE_BYTES chunks."""
        if len(data) <= FILE_MAX_SIZE_BYTES:
            return [data]
        n = math.ceil(len(data) / FILE_MAX_SIZE_BYTES)
        return [data[i * FILE_MAX_SIZE_BYTES:(i + 1) * FILE_MAX_SIZE_BYTES] for i in range(n)]

    def _update_cache_stats(self) -> None:
        STATS.set_cache_size(self.cached_files.currsize, self.cached_files.maxsize)

    # ── Public API ─────────────────────────────────────────────────────

    def upload_file(self, bytesio: BytesIO, fh: int, file_name: str = "") -> list:
        """Upload a file (possibly in chunks) to Telegram and return the sent messages."""
        # Invalidate stale cache entry before upload.
        if fh in self.cached_files:
            self.cached_files.pop(fh)
            gc.collect()

        raw = bytesio.read()
        n_bytes = len(raw)
        payload = self._encrypt(raw)
        chunks = self._split_into_chunks(payload)

        STATS.begin_upload()
        STATS.log("INFO", "UPLOAD", f"{file_name} ({_fmt_bytes(n_bytes)}, {len(chunks)} chunk(s))")

        upload_results = []
        success = False
        try:
            for i, chunk in enumerate(chunks):
                part_name = f"{file_name}_part{i}.txt"
                tg_file = _with_retry(
                    self.client.upload_file,
                    chunk,
                    file_name=part_name,
                    part_size_kb=512,
                    progress_callback=_progress_cb,
                )
                msg = _with_retry(self.client.send_file, self.channel_entity, tg_file)
                upload_results.append(msg)

            # Cache immediately so the next read avoids a round-trip.
            self.cached_files[fh] = bytearray(raw)
            self._update_cache_stats()
            success = True
            STATS.log("SUCCESS", "UPLOAD", f"{file_name} done ({_fmt_bytes(n_bytes)})")
        except Exception as exc:
            STATS.log("ERROR", "UPLOAD", f"{file_name} FAILED: {exc}")
            raise
        finally:
            STATS.end_upload(n_bytes, success=success)

        return upload_results

    def get_cached_file(self, fh: int) -> bytearray | None:
        entry = self.cached_files.get(fh)
        if entry:
            STATS.record_cache_hit()
            return entry
        STATS.record_cache_miss()
        return None

    def download_file(self, fh: int, msg_ids: list[int]) -> bytearray:
        """Download and reassemble a (possibly chunked) file from Telegram."""
        cached = self.cached_files.get(fh)
        if cached:
            STATS.record_cache_hit()
            return cached
        STATS.record_cache_miss()

        STATS.begin_download()
        STATS.log("INFO", "DOWNLOAD", f"fh={fh} msgs={msg_ids}")
        success = False
        n_bytes = 0
        try:
            msgs = _with_retry(self.client.get_messages, self.channel_entity, ids=msg_ids)
            buf = BytesIO()
            for m in msgs:
                data = _with_retry(m.download_media, bytes)
                if data is None:
                    raise RuntimeError(f"Failed to download media for message {m.id}")
                buf.write(data)

            buf.seek(0)
            raw = buf.read()
            decrypted = self._decrypt(raw)
            result = bytearray(decrypted)
            n_bytes = len(result)

            self.cached_files[fh] = result
            self._update_cache_stats()
            success = True
            STATS.log("SUCCESS", "DOWNLOAD", f"fh={fh}  {_fmt_bytes(n_bytes)}")
            return result
        except Exception as exc:
            STATS.log("ERROR", "DOWNLOAD", f"fh={fh} FAILED: {exc}")
            raise
        finally:
            STATS.end_download(n_bytes, success=success)

    def delete_messages(self, ids: list[int]) -> None:
        if ids:
            STATS.log("INFO", "DELETE_MSGS", f"ids={ids}")
            _with_retry(self.client.delete_messages, self.channel_entity, message_ids=ids)


def _fmt_bytes(n: int | float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"
