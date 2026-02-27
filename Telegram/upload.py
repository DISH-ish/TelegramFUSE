# ver 1.1
"""
upload.py — recursive directory direct-upload to TelegramFS.

Walks a source directory and uploads every file directly into BlockStore,
writing inode/block/manifest DB records so the files appear in the mounted
filesystem.  Only one block (4 MiB) is held in RAM at a time.

Usage (via main.py):
    python main.py --upload /mnt/raid0 [--upload-dest /subdir]
"""
from __future__ import annotations

import logging
import os
import sqlite3
import stat
from time import time
from typing import Optional

import trio

from block_store import BlockStore, BLOCK_SIZE, sha256_hex
from schema import ROOT_INODE, ensure_schema
from stats import STATS
from verify import verify_and_fix_blocks

log = logging.getLogger(__name__)

# Maximum number of blocks being uploaded to Telegram at the same time.
# E.g. 4 means: 4×1-block files, or 2×2-block files, or 1×4-block file, …
# File-level concurrency: how many files upload simultaneously.
# Block-level concurrency is controlled by MAX_CONCURRENT_UPLOADS
# via the shared BlockStore._ul_limiter (same limit as FUSE flushes).
UPLOAD_FILE_CONCURRENCY = int(os.getenv("MAX_CONCURRENT_UPLOADS", "4"))


# ── helpers ────────────────────────────────────────────────────────────────

def _now_ns() -> int:
    return int(time() * 1e9)


def _file_mode() -> int:
    return (stat.S_IFREG
            | stat.S_IRUSR | stat.S_IWUSR
            | stat.S_IRGRP
            | stat.S_IROTH)


def _dir_mode() -> int:
    return (stat.S_IFDIR
            | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH)


def _lookup(cur: sqlite3.Cursor, name: bytes, parent: int) -> Optional[sqlite3.Row]:
    cur.execute(
        "SELECT c.inode, i.size, i.mode FROM contents c "
        "JOIN inodes i ON i.id = c.inode "
        "WHERE c.name=? AND c.parent_inode=?",
        (name, parent),
    )
    return cur.fetchone()


def _ensure_dir(db: sqlite3.Connection, cur: sqlite3.Cursor, name: bytes, parent: int) -> int:
    """Return the inode id for a directory, creating it if needed."""
    row = _lookup(cur, name, parent)
    if row is not None:
        return row["inode"]
    now = _now_ns()
    cur.execute(
        "INSERT INTO inodes (mode, uid, gid, mtime_ns, atime_ns, ctime_ns, size) "
        "VALUES (?,?,?,?,?,?,0)",
        (_dir_mode(), os.getuid(), os.getgid(), now, now, now),
    )
    inode = cur.lastrowid
    cur.execute(
        "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
        (name, inode, parent),
    )
    cur.execute(
        "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
        (b"..", inode, inode),
    )
    db.commit()
    return inode


def _ensure_dest_path(db: sqlite3.Connection, cur: sqlite3.Cursor, dest: str) -> int:
    """Walk/create every component of dest (relative path) under ROOT_INODE."""
    parts = [p for p in dest.strip("/").split("/") if p]
    parent = ROOT_INODE
    for part in parts:
        parent = _ensure_dir(db, cur, part.encode(), parent)
    return parent


# ── core upload ────────────────────────────────────────────────────────────

async def _upload_file(
    src_path:    str,
    inode:       int,
    block_store: BlockStore,
    db:          sqlite3.Connection,
    fname:       str,
) -> None:
    """Upload src_path block by block into block_store, updating the DB as we go.

    Block-level concurrency is controlled by block_store._ul_limiter
    (MAX_CONCURRENT_UPLOADS env var), shared with FUSE flushes.
    """
    cur        = db.cursor()
    file_size  = os.path.getsize(src_path)
    n_blocks   = max(1, (file_size + BLOCK_SIZE - 1) // BLOCK_SIZE) if file_size else 0
    now_ts     = time()

    # Collect old msg_ids so we can delete them after the new upload completes.
    cur.execute("SELECT msg_id FROM blocks WHERE inode=?", (inode,))
    old_block_ids = [r["msg_id"] for r in cur.fetchall()]
    cur.execute("SELECT meta_msg_id FROM file_meta WHERE inode=?", (inode,))
    old_meta_row = cur.fetchone()
    old_meta_id  = old_meta_row["meta_msg_id"] if old_meta_row else None

    # Clear old block records — we're overwriting.
    cur.execute("DELETE FROM blocks WHERE inode=?", (inode,))
    db.commit()

    all_msg_ids:    dict[int, int] = {}
    all_pre_hashes: dict[int, str] = {}
    bytes_done = 0

    with open(src_path, "rb") as fobj:
        for block_idx in range(n_blocks):
            data = fobj.read(BLOCK_SIZE)
            if not data:
                break

            STATS.upload_block_start(block_idx, n_blocks, src_path)

            # Record in-flight (crash safety).
            cur.execute(
                "INSERT OR REPLACE INTO pending_blocks "
                "(msg_id, inode, block_idx, started_at) VALUES (0,?,?,?)",
                (inode, block_idx, now_ts),
            )
            db.commit()

            # Upload + verify this single block.
            # Concurrency is capped by block_store._ul_limiter (shared with
            # FUSE flushes) — no separate limiter needed here.
            try:
                new_ids, pre_hashes = await block_store.upload_blocks(
                    inode, {block_idx: data}, fname
                )
            except Exception as exc:
                STATS.log("ERROR", "DU_UL_FAIL",
                          f"{fname}[{block_idx}] failed after retries: {exc}")
                raise

            try:
                new_ids, pre_hashes = await verify_and_fix_blocks(
                    block_store, inode, {block_idx: data}, new_ids, fname,
                    pre_hashes=pre_hashes,
                )
            except RuntimeError as exc:
                STATS.log("ERROR", "DU_VERIFY_FAIL",
                          f"{fname}[{block_idx}]: {exc}")
                raise

            msg_id = new_ids[block_idx]

            # Replace placeholder with real msg_id.
            cur.execute(
                "INSERT OR REPLACE INTO pending_blocks "
                "(msg_id, inode, block_idx, started_at) VALUES (?,?,?,?)",
                (msg_id, inode, block_idx, now_ts),
            )
            cur.execute("DELETE FROM pending_blocks WHERE inode=? AND msg_id=0", (inode,))
            cur.execute(
                "INSERT OR REPLACE INTO blocks (inode, block_idx, msg_id, sha256) "
                "VALUES (?,?,?,?)",
                (inode, block_idx, msg_id, pre_hashes.get(block_idx)),
            )
            db.commit()

            all_msg_ids.update(new_ids)
            all_pre_hashes.update(pre_hashes)
            bytes_done += len(data)
            STATS.upload_block_done(len(data))

            # data goes out of scope here — GC can reclaim it before next block.

    # Update inode size + timestamps.
    now = _now_ns()
    cur.execute(
        "UPDATE inodes SET size=?, mtime_ns=?, ctime_ns=? WHERE id=?",
        (file_size, now, now, inode),
    )
    db.commit()

    # Upload manifest.
    try:
        meta_id = await block_store.upload_manifest(
            inode=inode, path=fname, size=file_size,
            mtime_ns=now, block_msg_ids=all_msg_ids, block_hashes=all_pre_hashes,
        )
        cur.execute(
            "INSERT OR REPLACE INTO file_meta (inode, meta_msg_id) VALUES (?,?)",
            (inode, meta_id),
        )
        db.commit()
    except Exception as exc:
        STATS.log("WARNING", "DU_META_FAIL",
                  f"{fname}: manifest upload failed (data is safe): {exc}")

    # Clear pending_blocks and schedule old messages for deletion.
    cur.execute("DELETE FROM pending_blocks WHERE inode=?", (inode,))
    db.commit()

    stale = old_block_ids[:]
    if old_meta_id is not None:
        stale.append(old_meta_id)
    if stale:
        block_store.deleter.enqueue(stale)


# ── directory walker ────────────────────────────────────────────────────────

async def run_upload(
    src_root:    str,
    dest_subdir: str,
    block_store: BlockStore,
    db:          sqlite3.Connection,
) -> None:
    """
    Recursively direct-upload src_root into the TelegramFS under dest_subdir.

    Files are skipped if they already exist with the same size.
    Files are overwritten if sizes differ.
    """
    cur = db.cursor()
    db.text_factory = str
    db.row_factory  = sqlite3.Row

    ensure_schema(db)
    dest_root_inode = _ensure_dest_path(db, cur, dest_subdir)

    # Count totals for progress display.
    log.info("Scanning %s …", src_root)
    STATS.log("INFO", "DU_SCAN", f"scanning {src_root} …")
    total_files = 0
    total_bytes = 0
    for dirpath, _dirnames, filenames in os.walk(src_root):
        for fname in filenames:
            fp = os.path.join(dirpath, fname)
            try:
                total_bytes += os.path.getsize(fp)
                total_files += 1
            except OSError:
                pass
    STATS.upload_set_totals(total_files, total_bytes)
    STATS.log("INFO", "DU_START",
              f"{total_files} files  {total_bytes // 1024**2} MiB  src={src_root!r}")

    # Counters mutated from inside nursery tasks — wrapped in list for closure.
    _done    = [0]
    _skipped = [0]
    _errors  = [0]

    # file_sem bounds how many files upload concurrently.  Block-level
    # concurrency (and the overall upload cap) is the shared
    # block_store._ul_limiter (MAX_CONCURRENT_UPLOADS env var).
    # Using the same value keeps semantics simple: e.g. MAX_CONCURRENT_UPLOADS=4
    # means at most 4 blocks in flight total, spread across ≤4 concurrent files.
    file_sem = trio.Semaphore(UPLOAD_FILE_CONCURRENCY)

    async def _upload_one(src_path: str, inode: int, filename: str, src_size: int) -> None:
        """Task: upload one file. The file_sem slot is held by the caller."""
        STATS.upload_file_start(src_path, src_size)
        STATS.log("INFO", "DU_FILE", f"→ {src_path!r}  ({src_size:,} B)")
        try:
            await _upload_file(src_path, inode, block_store, db, filename)
            _done[0] += 1
            STATS.upload_file_done()
            STATS.log("SUCCESS", "DU_FILE_OK", f"{src_path!r}")
        except Exception as exc:
            _errors[0] += 1
            STATS.upload_file_error(src_path)
            STATS.log("ERROR", "DU_FILE_FAIL", f"{src_path!r}: {exc}")
            log.error("direct-upload: failed to upload %s: %s", src_path, exc, exc_info=True)

    async def _run_and_release(src_path: str, inode: int,
                              filename: str, src_size: int) -> None:
        try:
            await _upload_one(src_path, inode, filename, src_size)
        finally:
            file_sem.release()

    async with trio.open_nursery() as nursery:
        for dirpath, dirnames, filenames in os.walk(src_root, followlinks=False):
            if STATS.upload_stop_requested:
                STATS.log("WARNING", "DU_STOPPED",
                          "stop requested — finishing in-flight files then exiting")
                break

            # Mirror the directory structure under dest_root_inode.
            rel_dir = os.path.relpath(dirpath, src_root)
            if rel_dir == ".":
                cur_dir_inode = dest_root_inode
            else:
                parts = rel_dir.split(os.sep)
                cur_dir_inode = dest_root_inode
                for part in parts:
                    cur_dir_inode = _ensure_dir(db, cur, part.encode(), cur_dir_inode)

            for filename in sorted(filenames):
                if STATS.upload_stop_requested:
                    break

                src_path  = os.path.join(dirpath, filename)
                fname_enc = filename.encode()

                try:
                    src_size = os.path.getsize(src_path)
                except OSError as exc:
                    STATS.log("WARNING", "DU_STAT_FAIL", f"{src_path}: {exc}")
                    _errors[0] += 1
                    continue

                # Check for existing entry.
                existing = _lookup(cur, fname_enc, cur_dir_inode)

                if existing is not None:
                    existing_inode = existing["inode"]
                    existing_size  = existing["size"]
                    if existing_size == src_size:
                        STATS.log("INFO", "DU_SKIP",
                                  f"{src_path!r} — same size ({src_size:,} B), skipping")
                        _skipped[0] += 1
                        STATS.upload_file_skipped(src_size)
                        continue
                    else:
                        STATS.log("INFO", "DU_OVERWRITE",
                                  f"{src_path!r} — size changed "
                                  f"({existing_size:,} → {src_size:,} B), overwriting")
                        inode = existing_inode
                else:
                    # Create a new inode for this file.
                    now = _now_ns()
                    cur.execute(
                        "INSERT INTO inodes (mode, uid, gid, mtime_ns, atime_ns, ctime_ns, size) "
                        "VALUES (?,?,?,?,?,?,0)",
                        (_file_mode(), os.getuid(), os.getgid(), now, now, now),
                    )
                    inode = cur.lastrowid
                    cur.execute(
                        "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
                        (fname_enc, inode, cur_dir_inode),
                    )
                    db.commit()

                # Acquire one slot before spawning so the walk loop blocks
                # when UPLOAD_FILE_CONCURRENCY files are already active.
                # The slot is released inside _upload_one when the file finishes.
                await file_sem.acquire()
                nursery.start_soon(_run_and_release, src_path, inode, filename, src_size)

    files_done    = _done[0]
    files_skipped = _skipped[0]
    errors        = _errors[0]

    STATS.upload_finish()
    STATS.log(
        "SUCCESS" if errors == 0 else "WARNING",
        "DU_DONE",
        f"done — {files_done} uploaded  {files_skipped} skipped  {errors} errors",
    )
    log.info("Direct upload complete: %d uploaded, %d skipped, %d errors",
             files_done, files_skipped, errors)
