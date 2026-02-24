from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Optional

import trio
from cachetools import LRUCache

from crypto import BlockCipher
from stats import STATS

log = logging.getLogger(__name__)

BLOCK_SIZE         = 4 * 1024 * 1024
_DEFAULT_CACHE_MB  = 1 * 1024
CACHE_MAX_BLOCKS   = int(os.getenv("CACHE_MAX_BLOCKS",
                         str(_DEFAULT_CACHE_MB * 1024 * 1024 // BLOCK_SIZE)))
MAX_CONCURRENT_UL  = int(os.getenv("MAX_CONCURRENT_UPLOADS",   "4"))
MAX_CONCURRENT_DL  = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "4"))
MAX_RETRIES        = 5
RETRY_BASE_DELAY   = 1.5
DELETE_BATCH_DELAY = float(os.getenv("DELETE_BATCH_DELAY", "30"))

# Plaintext prefix on every manifest — lets scanners skip non-TelegramFS messages cheaply.
MANIFEST_MAGIC = b"TGFS_META_V1\n"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def blocks_for_range(offset: int, length: int) -> list[int]:
    if length <= 0:
        return []
    return list(range(offset // BLOCK_SIZE, (offset + length - 1) // BLOCK_SIZE + 1))


def _random_tag(n: int = 8) -> str:
    return os.urandom(n // 2).hex()


class DeferredDeleter:
    """Batch-delete old Telegram messages in the background to keep flush latency low."""

    _MAX_DELETE_FAILURES = 5

    def __init__(self, tg_client) -> None:
        self._tg           = tg_client
        self._queue:       list[int] = []
        self._fail_streak: int       = 0

    def enqueue(self, ids: list[int]) -> None:
        if ids:
            self._queue.extend(ids)

    async def flush(self) -> None:
        if not self._queue:
            return
        ids = list(self._queue)
        self._queue.clear()
        STATS.log("INFO", "DEL_BATCH", f"deleting {len(ids)} old message(s)")
        try:
            await trio.to_thread.run_sync(self._tg.delete_messages, ids)
            self._fail_streak = 0
        except Exception as exc:
            self._fail_streak += 1
            if self._fail_streak <= self._MAX_DELETE_FAILURES:
                log.warning("DeferredDeleter: batch delete failed (streak=%d/%d): %s",
                            self._fail_streak, self._MAX_DELETE_FAILURES, exc)
                self._queue.extend(ids)
            else:
                log.error("DeferredDeleter: giving up on %d message(s) after %d consecutive "
                          "failures — they will not be deleted.  Last error: %s",
                          len(ids), self._fail_streak, exc)
                STATS.log("ERROR", "DEL_ABANDON",
                          f"{len(ids)} msg(s) abandoned after {self._fail_streak} failures")

    async def run_background(self) -> None:
        log.info("DeferredDeleter: started (interval=%.0fs)", DELETE_BATCH_DELAY)
        try:
            while True:
                await trio.sleep(DELETE_BATCH_DELAY)
                await self.flush()
        except trio.Cancelled:
            pass
        finally:
            await self.flush()


class BlockStore:
    def __init__(self, tg_client, cipher: Optional[BlockCipher]) -> None:
        self._tg     = tg_client
        self._cipher = cipher
        self.deleter = DeferredDeleter(tg_client)
        self._cache: LRUCache = LRUCache(maxsize=CACHE_MAX_BLOCKS)
        # Maps (inode, block_idx) → trio.Event for in-progress downloads.
        # Concurrent reads for the same block wait on the event instead of
        # issuing duplicate Telegram requests.
        self._in_flight: dict[tuple[int, int], trio.Event] = {}

        STATS.encryption_enabled = cipher is not None
        STATS.set_cache_size(0, CACHE_MAX_BLOCKS * BLOCK_SIZE)
        log.info("BlockStore ready — block=%d MiB, cache=%d blocks, enc=%s",
                 BLOCK_SIZE // (1024 * 1024), CACHE_MAX_BLOCKS,
                 "AES-256-GCM" if cipher else "off")

    # ── cache ──────────────────────────────────────────────────────────────

    def _cache_get(self, inode: int, idx: int) -> Optional[bytes]:
        val = self._cache.get((inode, idx))
        if val is not None:
            STATS.record_cache_hit()
            return val
        STATS.record_cache_miss()
        return None

    def _cache_put(self, inode: int, idx: int, data: bytes) -> None:
        self._cache[(inode, idx)] = data
        STATS.set_cache_size(
            self._cache.currsize * BLOCK_SIZE,
            self._cache.maxsize  * BLOCK_SIZE,
        )

    def evict_inode(self, inode: int) -> None:
        for k in [k for k in self._cache.keys() if k[0] == inode]:
            self._cache.pop(k, None)

    def _encrypt(self, data: bytes) -> bytes:
        return self._cipher.encrypt(data) if self._cipher else data

    def _decrypt(self, data: bytes) -> bytes:
        return self._cipher.decrypt(data) if self._cipher else data

    # ── manifest encoding ──────────────────────────────────────────────────

    def _encode_manifest(
        self, inode, path, size, mtime_ns, block_msg_ids,
        block_hashes: Optional[dict] = None,
    ) -> bytes:
        payload = json.dumps(
            {
                "v":        3,
                "path":     path,
                "filename": path.rsplit("/", 1)[-1],
                "inode":    inode,
                "size":     size,
                "mtime_ns": mtime_ns,
                "blocks":   {str(k): v for k, v in block_msg_ids.items()},
                "hashes":   {str(k): v for k, v in (block_hashes or {}).items()},
            },
            separators=(",", ":"),
        ).encode()
        return MANIFEST_MAGIC + self._encrypt(payload)

    def _decode_manifest(self, raw: bytes) -> Optional[dict]:
        if not raw.startswith(MANIFEST_MAGIC):
            return None
        try:
            data = json.loads(self._decrypt(raw[len(MANIFEST_MAGIC):]))
            data["blocks"] = {int(k): v for k, v in data.get("blocks", {}).items()}
            data["hashes"] = {int(k): v for k, v in data.get("hashes", {}).items()}
            if "path" not in data and "filename" in data:
                data["path"] = data["filename"]  # v1 back-compat
            return data
        except Exception as exc:
            log.warning("Manifest decode failed: %s", exc)
            return None

    # ── retry helper ───────────────────────────────────────────────────────

    async def _retry(self, fn, log_key: str):
        """Run *fn* (a no-arg async callable) with exponential back-off retries."""
        last: Exception = RuntimeError("never ran")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return await fn()
            except Exception as exc:
                last = exc
                STATS.log("WARNING", f"{log_key}_RETRY",
                          f"attempt {attempt}/{MAX_RETRIES}: {exc}")
                if attempt < MAX_RETRIES:
                    await trio.sleep(RETRY_BASE_DELAY * attempt)
        STATS.log("ERROR", f"{log_key}_FAIL", "gave up")
        raise last

    # ── low-level transfer ─────────────────────────────────────────────────

    async def _download_raw(self, msg_id: int) -> bytes:
        """Download raw (possibly encrypted) bytes from Telegram."""
        return await self._retry(
            lambda: trio.to_thread.run_sync(self._tg.download_block, msg_id),
            f"DL msg={msg_id}",
        )

    async def _download_one(self, msg_id: int) -> bytes:
        """Download and decrypt a single block."""
        return self._decrypt(await self._download_raw(msg_id))

    async def _upload_raw(self, data: bytes, tg_name: str) -> int:
        """Upload pre-built bytes to Telegram under *tg_name*."""
        return await self._retry(
            lambda: trio.to_thread.run_sync(self._tg.upload_block, data, tg_name),
            f"UL name={tg_name}",
        )

    async def _upload_one(self, data: bytes) -> int:
        """Encrypt *data* and upload it as a new Telegram message."""
        tg_name = f"data_{_random_tag()}"
        enc     = self._encrypt(data)
        return await self._retry(
            lambda: trio.to_thread.run_sync(self._tg.upload_block, enc, tg_name),
            f"UL name={tg_name}",
        )

    # ── public API ─────────────────────────────────────────────────────────

    async def read_range(
        self, inode, offset, length, msg_id_map,
        hash_map: Optional[dict] = None,
    ) -> bytes:
        """
        Read bytes from [offset, offset+length).

        hash_map: optional {block_idx: sha256_hex} of expected plaintext hashes.
        When provided, every freshly-downloaded block is verified against its stored
        hash; a mismatch raises RuntimeError so the caller can surface EIO.
        Cache hits are not re-hashed (they were verified on first download).
        """
        if length <= 0:
            return b""

        indices    = blocks_for_range(offset, length)
        block_data: dict[int, bytes] = {}
        dl_limiter = trio.CapacityLimiter(MAX_CONCURRENT_DL)

        async def fetch(idx: int) -> None:
            key = (inode, idx)

            if key in self._in_flight:
                await self._in_flight[key].wait()
                block_data[idx] = self._cache_get(inode, idx) or b""
                return

            cached = self._cache_get(inode, idx)
            if cached is not None:
                block_data[idx] = cached
                return

            msg_id = msg_id_map.get(idx)
            if msg_id is None:
                block_data[idx] = b""
                return

            ev = trio.Event()
            self._in_flight[key] = ev
            STATS.begin_download()
            try:
                data = await self._download_one(msg_id)
            except Exception:
                STATS.end_download(0, success=False)
                raise
            finally:
                del self._in_flight[key]
                ev.set()

            if hash_map:
                expected_hex = hash_map.get(idx)
                if expected_hex is not None:
                    actual_hex = sha256_hex(data)
                    if actual_hex != expected_hex:
                        STATS.log(
                            "ERROR", "DL_HASH_FAIL",
                            f"inode={inode} block={idx} msg={msg_id} "
                            f"expected={expected_hex[:12]}… got={actual_hex[:12]}…",
                        )
                        STATS.record_dl_hash_fail()
                        STATS.end_download(len(data), success=False)
                        raise RuntimeError(
                            f"Block integrity check FAILED: inode={inode} block={idx} "
                            f"msg={msg_id} — data is corrupt or was tampered with."
                        )
                    log.debug("dl-hash OK  inode=%d block=%d msg=%d sha256=%.12s…",
                              inode, idx, msg_id, actual_hex)

            self._cache_put(inode, idx, data)
            block_data[idx] = data
            STATS.end_download(len(data), success=True)

        async def fetch_limited(idx: int) -> None:
            async with dl_limiter:
                await fetch(idx)

        async with trio.open_nursery() as nursery:
            for idx in indices:
                nursery.start_soon(fetch_limited, idx)

        parts: list[bytes] = []
        for idx in indices:
            data    = block_data.get(idx, b"")
            b_start = idx * BLOCK_SIZE
            sl_s    = max(offset - b_start, 0)
            sl_e    = min(offset + length - b_start, BLOCK_SIZE)
            parts.append(data[sl_s:sl_e])
        return b"".join(parts)

    async def upload_blocks(
        self, inode, dirty, file_name
    ) -> tuple[dict[int, int], dict[int, str]]:
        """
        Upload dirty blocks to Telegram.

        Returns (msg_ids, hashes):
          msg_ids: {block_idx: telegram_msg_id}
          hashes:  {block_idx: sha256_hex_of_plaintext}
        """
        results: dict[int, int] = {}
        hashes:  dict[int, str] = {}
        ul_limiter = trio.CapacityLimiter(MAX_CONCURRENT_UL)

        async def upload_one(idx: int, data: bytes) -> None:
            async with ul_limiter:
                STATS.begin_upload()
                try:
                    h            = sha256_hex(data)
                    msg_id       = await self._upload_one(data)
                    results[idx] = msg_id
                    hashes[idx]  = h
                    self._cache_put(inode, idx, data)
                    STATS.end_upload(len(data), success=True)
                    STATS.log("SUCCESS", "BLOCK_UL",
                              f"{file_name}[{idx}] → msg={msg_id}  ({len(data):,} B)")
                except Exception:
                    STATS.end_upload(0, success=False)
                    raise

        async with trio.open_nursery() as nursery:
            for idx, data in dirty.items():
                nursery.start_soon(upload_one, idx,
                                   data if isinstance(data, bytes) else bytes(data))
        return results, hashes

    async def upload_manifest(
        self, inode, path, size, mtime_ns, block_msg_ids,
        block_hashes: Optional[dict] = None,
    ) -> int:
        """Upload a manifest; block_hashes maps block_idx → sha256_hex (plaintext)."""
        tg_name = f"meta_{_random_tag()}"
        blob    = await trio.to_thread.run_sync(
            lambda: self._encode_manifest(
                inode, path, size, mtime_ns, block_msg_ids, block_hashes
            )
        )
        msg_id = await self._upload_raw(blob, tg_name)
        STATS.log("SUCCESS", "META_UL", f"inode={inode} path=<encrypted> → msg={msg_id}")
        return msg_id

    async def download_manifest(self, msg_id: int) -> Optional[dict]:
        try:
            raw = await self._download_raw(msg_id)
        except Exception as exc:
            log.warning("download_manifest: failed to fetch msg %d: %s", msg_id, exc)
            return None
        return await trio.to_thread.run_sync(self._decode_manifest, raw)

    async def reconstruct_from_telegram(self, message_iter) -> list[dict]:
        results: list[dict] = []
        for msg_id, raw in message_iter:
            manifest = await trio.to_thread.run_sync(self._decode_manifest, raw)
            if manifest is not None:
                manifest["meta_msg_id"] = msg_id
                results.append(manifest)
        return results

    def delete_messages(self, ids: list[int]) -> None:
        if ids:
            STATS.log("INFO", "DELETE_MSGS", f"ids={ids}")
            self._tg.delete_messages(ids)
