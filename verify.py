"""
verify.py — Block-level integrity verification for TelegramFS.

Phase 1 — Existence check (fast, batch): asks Telegram which message IDs are
present and re-uploads any that are missing from the in-memory dirty_bytes copy.

Phase 2 — Content verification (per-block download): downloads every uploaded
block, decrypts it, and compares its SHA-256 against the pre-upload plaintext
digest.  Catches silent corruption: wrong bytes, truncation, key mismatch, etc.
Skip phase 2 by setting VERIFY_CONTENT=0 in the environment.

Both phases retry up to MAX_VERIFY_RETRIES times before raising RuntimeError,
which causes the flush to abort with EIO (dirty data is preserved for retry).
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import TYPE_CHECKING

import trio

from stats import STATS

if TYPE_CHECKING:
    from block_store import BlockStore

log = logging.getLogger(__name__)

MAX_VERIFY_RETRIES = int(os.getenv("MAX_VERIFY_RETRIES", "3"))
VERIFY_CONTENT     = os.getenv("VERIFY_CONTENT", "1").strip() not in ("0", "false", "no")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _download_and_check(
    bs:                 "BlockStore",
    msg_id:             int,
    expected_plaintext: bytes,
    expected_hash:      "str | None" = None,
) -> bool:
    """
    Download one block and verify its integrity.

    Uses expected_hash (pre-upload SHA-256) when available for a cheap hash-only
    comparison; falls back to byte-for-byte comparison against expected_plaintext.
    """
    try:
        actual = await bs._download_one(msg_id)
    except Exception as exc:
        log.warning("verify: content-check download of msg=%d failed: %s", msg_id, exc)
        return False

    actual_hash = _sha256(actual)

    if expected_hash is not None:
        if actual_hash == expected_hash:
            return True
        log.error(
            "verify: CONTENT MISMATCH (hash)  msg=%d  "
            "expected_sha256=%.12s…  actual_sha256=%.12s…  actual_len=%d",
            msg_id, expected_hash, actual_hash, len(actual),
        )
        return False

    if actual == expected_plaintext:
        return True
    log.error(
        "verify: CONTENT MISMATCH (bytes)  msg=%d  "
        "expected_sha256=%.12s…  actual_sha256=%.12s…  "
        "expected_len=%d  actual_len=%d",
        msg_id, _sha256(expected_plaintext), actual_hash,
        len(expected_plaintext), len(actual),
    )
    return False


async def verify_and_fix_blocks(
    bs:          "BlockStore",
    inode:       int,
    dirty_bytes: dict[int, bytes],
    new_ids:     dict[int, int],
    fname:       str = "",
    pre_hashes:  "dict[int, str] | None" = None,
) -> tuple[dict[int, int], dict[int, str]]:
    """
    Verify every block in new_ids is present on Telegram with correct content.
    Re-uploads any that fail.  Raises RuntimeError after MAX_VERIFY_RETRIES attempts.

    pre_hashes: optional {block_idx: sha256_hex} computed before upload.
    When provided it is used for content comparison to avoid re-hashing dirty_bytes.

    Returns the final confirmed {block_idx: msg_id} mapping (may differ from
    new_ids if re-uploads were necessary).
    """
    confirmed = dict(new_ids)
    n_blocks  = len(confirmed)

    STATS.begin_verify()
    STATS.record_verify_check(n_blocks)
    log.debug("verify: starting  inode=%d  blocks=%d  content=%s",
              inode, n_blocks, VERIFY_CONTENT)

    try:
        for attempt in range(1, MAX_VERIFY_RETRIES + 1):

            # ── Phase 1: existence check ───────────────────────────────────
            missing_msg_ids: list[int] = await trio.to_thread.run_sync(
                bs._tg.check_messages_exist, list(confirmed.values())
            )

            if missing_msg_ids:
                mid_to_idx   = {mid: idx for idx, mid in confirmed.items()}
                missing_idxs = [mid_to_idx[mid] for mid in missing_msg_ids if mid in mid_to_idx]
                log.warning(
                    "verify: phase-1 (existence)  attempt=%d/%d  missing=%d/%d  indices=%s",
                    attempt, MAX_VERIFY_RETRIES, len(missing_idxs), n_blocks, missing_idxs,
                )
                for mid in missing_msg_ids:
                    STATS.record_verify_missing()
                    STATS.log("WARNING", "VERIFY_MISS",
                              f"msg={mid} inode={inode} attempt={attempt}")

                no_data = [idx for idx in missing_idxs if idx not in dirty_bytes]
                if no_data:
                    raise RuntimeError(
                        f"verify: block(s) {no_data} missing on Telegram and not in "
                        f"memory — cannot re-upload  (inode={inode})"
                    )
                STATS.record_verify_reupload(len(missing_idxs))
                reup, reup_hashes = await bs.upload_blocks(
                    inode,
                    {idx: dirty_bytes[idx] for idx in missing_idxs},
                    fname or f"verify_retry_inode{inode}",
                )
                confirmed.update(reup)
                if pre_hashes is not None:
                    pre_hashes.update(reup_hashes)
                continue  # re-check existence of freshly uploaded blocks

            # ── Phase 2: content verification ─────────────────────────────
            if not VERIFY_CONTENT:
                STATS.record_verify_pass(n_blocks)
                log.debug("verify: phase-2 skipped (VERIFY_CONTENT=0)  inode=%d", inode)
                return confirmed, (pre_hashes or {})

            results: dict[int, bool] = {}
            dl_limiter = trio.CapacityLimiter(
                max(1, getattr(bs, "_max_concurrent_dl", 4))
            )

            async def check_one(idx: int, msg_id: int) -> None:
                async with dl_limiter:
                    ok = await _download_and_check(
                        bs, msg_id, dirty_bytes[idx],
                        expected_hash=(pre_hashes or {}).get(idx),
                    )
                    results[idx] = ok

            async with trio.open_nursery() as nursery:
                for idx, msg_id in confirmed.items():
                    if idx in dirty_bytes:
                        nursery.start_soon(check_one, idx, msg_id)

            bad_idxs = [idx for idx, ok in results.items() if not ok]

            if not bad_idxs:
                STATS.record_verify_pass(n_blocks)
                if attempt > 1:
                    log.info(
                        "verify: all %d block(s) confirmed OK  inode=%d  after %d attempt(s)",
                        n_blocks, inode, attempt,
                    )
                else:
                    log.debug("verify: all %d block(s) OK  inode=%d", n_blocks, inode)
                return confirmed, (pre_hashes or {})

            for idx in bad_idxs:
                STATS.record_verify_content_fail()
                STATS.log("ERROR", "VERIFY_CORRUPT",
                          f"inode={inode} block={idx} msg={confirmed[idx]} attempt={attempt}")
            log.error(
                "verify: phase-2 (content) FAIL  attempt=%d/%d  corrupt=%d/%d  indices=%s",
                attempt, MAX_VERIFY_RETRIES, len(bad_idxs), n_blocks, bad_idxs,
            )

            no_data = [idx for idx in bad_idxs if idx not in dirty_bytes]
            if no_data:
                raise RuntimeError(
                    f"verify: block(s) {no_data} have wrong content on Telegram and not in "
                    f"memory — cannot re-upload  (inode={inode})"
                )
            STATS.record_verify_reupload(len(bad_idxs))
            reup, reup_hashes = await bs.upload_blocks(
                inode,
                {idx: dirty_bytes[idx] for idx in bad_idxs},
                fname or f"verify_reup_inode{inode}",
            )
            confirmed.update(reup)
            if pre_hashes is not None:
                pre_hashes.update(reup_hashes)

        # ── All retries exhausted ─────────────────────────────────────────
        final_missing = await trio.to_thread.run_sync(
            bs._tg.check_messages_exist, list(confirmed.values())
        )
        mid_to_idx = {mid: idx for idx, mid in confirmed.items()}
        problems: list[str] = []
        if final_missing:
            missing_idxs = [mid_to_idx.get(m, "?") for m in final_missing]
            problems.append(f"{len(final_missing)} block(s) still missing: indices={missing_idxs}")

        STATS.record_verify_hard_failure()
        raise RuntimeError(
            f"verify: inode={inode} exhausted {MAX_VERIFY_RETRIES} attempt(s).  "
            + ("  ".join(problems) or "persistent content mismatch")
            + "  — flush ABORTED, dirty data preserved for retry."
        )

    finally:
        STATS.end_verify()
