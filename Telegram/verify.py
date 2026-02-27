# ver 1.0
"""
verify.py — Block-level integrity verification for TelegramFS.

Two-phase approach per flush:
  Phase 1 — Existence check (fast, batch)
      Asks Telegram which message IDs are actually present.  Any that are
      missing get re-uploaded immediately from the in-memory dirty_bytes copy.

  Phase 2 — Content verification (slower, per-block download)
      Downloads every uploaded block, decrypts it, and compares its SHA-256
      digest against the digest of the plaintext we intended to upload.
      This catches silent corruption: wrong bytes, truncation, encryption
      key mismatch, Telegram-side data mangling, etc.

      Content verification can be skipped by setting:
          VERIFY_CONTENT=0   in .env / the environment

Both phases re-upload any bad blocks and retry up to MAX_VERIFY_RETRIES
times before raising RuntimeError (which causes the flush to be aborted
with EIO — the dirty data is preserved in memory for the next attempt).
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
    expected_plaintext: "bytes | None",
    expected_hash:      "str | None" = None,
) -> bool:
    """
    Download one block from Telegram, decrypt it, and verify its integrity.

    Comparison strategy (fastest first):
      1. If expected_hash is supplied (pre-upload SHA-256), compare hashes only.
         This is cheap and avoids keeping two copies of the plaintext in RAM.
         Pass expected_plaintext=None in this case to avoid holding the big buffer.
      2. Otherwise fall back to a byte-for-byte comparison against expected_plaintext
         (backwards-compatible for callers that don't supply pre_hashes).
    """
    try:
        actual = await bs._download_one(msg_id)
    except Exception as exc:
        log.warning("verify: content-check download of msg=%d failed: %s", msg_id, exc)
        return False

    actual_hash = _sha256(actual)

    if expected_hash is not None:
        # Fast path: compare pre-computed hash against downloaded hash.
        if actual_hash == expected_hash:
            return True
        log.error(
            "verify: CONTENT MISMATCH (hash)  msg=%d  "
            "expected_sha256=%.12s…  actual_sha256=%.12s…  actual_len=%d",
            msg_id, expected_hash, actual_hash, len(actual),
        )
        return False

    # Slow path: byte comparison (expected_hash not available, e.g. old code path).
    if expected_plaintext is None:
        # No reference data available — cannot verify content.
        log.error("verify: CONTENT MISMATCH impossible — no expected_plaintext and no expected_hash  msg=%d", msg_id)
        return False
    if actual == expected_plaintext:
        return True
    expected_digest = _sha256(expected_plaintext)
    log.error(
        "verify: CONTENT MISMATCH (bytes)  msg=%d  "
        "expected_sha256=%.12s…  actual_sha256=%.12s…  "
        "expected_len=%d  actual_len=%d",
        msg_id, expected_digest, actual_hash,
        len(expected_plaintext), len(actual),
    )
    return False


async def verify_and_fix_blocks(
    bs:          "BlockStore",
    inode:       int,
    dirty_bytes: dict[int, bytes],   # block_idx → plaintext bytes (immutable snapshot)
    new_ids:     dict[int, int],     # block_idx → telegram msg_id
    fname:       str = "",
    pre_hashes:  "dict[int, str] | None" = None,
) -> tuple[dict[int, int], dict[int, str]]:
    """
    Verify that every block in new_ids is present on Telegram AND has the
    correct content.  Re-upload any that fail.  Raises RuntimeError if
    blocks cannot be confirmed after MAX_VERIFY_RETRIES attempts.

    pre_hashes: optional {block_idx: sha256_hex} computed BEFORE the upload
    (i.e. sha256 of the plaintext, not the ciphertext).  When provided it is
    used directly for content comparison so we never re-hash dirty_bytes on
    every retry.  When absent the hash is derived from dirty_bytes on the fly
    (backwards-compatible path).

    Returns (confirmed_ids, pre_hashes) — confirmed may differ from new_ids
    if re-uploads were necessary; pre_hashes is updated with any re-upload hashes.
    """
    confirmed = dict(new_ids)
    n_blocks  = len(confirmed)

    STATS.begin_verify()
    STATS.record_verify_check(n_blocks)
    log.debug("verify: starting  inode=%d  blocks=%d  content=%s",
              inode, n_blocks, VERIFY_CONTENT)

    try:
        for attempt in range(1, MAX_VERIFY_RETRIES + 1):

            # ── Phase 1: existence check (fast batch API call) ─────────────
            missing_msg_ids: list[int] = await trio.to_thread.run_sync(
                bs._tg.check_messages_exist, list(confirmed.values())
            )

            if missing_msg_ids:
                mid_to_idx  = {mid: idx for idx, mid in confirmed.items()}
                missing_idxs = [mid_to_idx[mid] for mid in missing_msg_ids if mid in mid_to_idx]
                log.warning(
                    "verify: phase-1 (existence)  attempt=%d/%d  "
                    "missing=%d/%d  indices=%s",
                    attempt, MAX_VERIFY_RETRIES,
                    len(missing_idxs), n_blocks, missing_idxs,
                )
                for mid in missing_msg_ids:
                    STATS.record_verify_missing()
                    STATS.log("WARNING", "VERIFY_MISS",
                              f"msg={mid} inode={inode} attempt={attempt}")

                # Re-upload each missing block.
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
                # Loop again to re-check existence of the freshly uploaded blocks.
                continue

            # Phase 1 clean — all messages exist.

            # ── Phase 2: content verification (per-block download) ─────────
            if not VERIFY_CONTENT:
                STATS.record_verify_pass(n_blocks)
                log.debug("verify: phase-2 skipped (VERIFY_CONTENT=0)  inode=%d", inode)
                return confirmed, (pre_hashes or {})

            bad_idxs: list[int] = []
            dl_limiter = trio.CapacityLimiter(
                max(1, getattr(bs, "_max_concurrent_dl", 4))
            )

            results: dict[int, bool] = {}

            async def check_one(idx: int, msg_id: int) -> None:
                async with dl_limiter:
                    exp_hash = (pre_hashes or {}).get(idx)
                    # When a pre-upload hash is available, pass None for the
                    # plaintext so the closure does not keep a 4 MiB reference
                    # alive for the duration of the download + hash comparison.
                    # Only fall back to the full bytes if no hash is known.
                    exp_plain = None if exp_hash is not None else dirty_bytes.get(idx)
                    ok = await _download_and_check(
                        bs, msg_id, exp_plain,
                        expected_hash=exp_hash,
                    )
                    results[idx] = ok

            try:
                async with trio.open_nursery() as nursery:
                    for idx, msg_id in confirmed.items():
                        if idx in dirty_bytes:          # only check blocks we have a copy of
                            nursery.start_soon(check_one, idx, msg_id)
            except BaseExceptionGroup as eg:
                causes = eg.exceptions
                if len(causes) == 1:
                    raise causes[0] from causes[0].__cause__
                raise

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

            # Content mismatches found.
            for idx in bad_idxs:
                STATS.record_verify_content_fail()
                STATS.log("ERROR", "VERIFY_CORRUPT",
                          f"inode={inode} block={idx} msg={confirmed[idx]} attempt={attempt}")
            log.error(
                "verify: phase-2 (content) FAIL  attempt=%d/%d  "
                "corrupt=%d/%d  indices=%s",
                attempt, MAX_VERIFY_RETRIES,
                len(bad_idxs), n_blocks, bad_idxs,
            )

            # Re-upload corrupt blocks.
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
            # Loop to re-verify.

        # ── All retries exhausted ─────────────────────────────────────────
        # Final check so the error message is accurate.
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
