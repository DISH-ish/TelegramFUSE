"""
backup.py — Recursive directory backup to TelegramFS.

Walks a source directory and uploads every file directly into BlockStore,
writing inode/block/manifest DB records so files appear in the mounted
filesystem.  Only one block (4 MiB) is held in RAM at a time.

Usage (via main.py):
    python main.py --backup /mnt/raid0 [--backup-dest /subdir]
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
from stats import STATS
from verify import verify_and_fix_blocks

log = logging.getLogger(__name__)

ROOT_INODE: int = 1   # matches pyfuse3.ROOT_INODE


def _ensure_schema(db: sqlite3.Connection) -> None:
    """Create the TelegramFS schema if it doesn't already exist."""
    cur = db.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inodes'")
    if cur.fetchone() is not None:
        cur.execute("PRAGMA table_info(blocks)")
        cols = {row[1] for row in cur.fetchall()}
        if "sha256" not in cols:
            cur.execute("ALTER TABLE blocks ADD COLUMN sha256 TEXT")
        db.commit()
        return

    cur.executescript("""
        CREATE TABLE inodes (
            id INTEGER PRIMARY KEY, uid INT NOT NULL, gid INT NOT NULL,
            mode INT NOT NULL, mtime_ns INT NOT NULL, atime_ns INT NOT NULL,
            ctime_ns INT NOT NULL, target BLOB(256),
            size INT NOT NULL DEFAULT 0, rdev INT NOT NULL DEFAULT 0
        );
        CREATE TABLE blocks (
            inode INT NOT NULL REFERENCES inodes(id),
            block_idx INT NOT NULL, msg_id INT NOT NULL,
            sha256 TEXT,
            PRIMARY KEY (inode, block_idx)
        );
        CREATE INDEX idx_blocks_inode ON blocks(inode);
        CREATE TABLE file_meta (
            inode INT NOT NULL PRIMARY KEY REFERENCES inodes(id),
            meta_msg_id INT NOT NULL
        );
        CREATE TABLE pending_blocks (
            msg_id INT PRIMARY KEY, inode INT NOT NULL,
            block_idx INT NOT NULL, started_at REAL NOT NULL
        );
        CREATE TABLE contents (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            name BLOB(256) NOT NULL, inode INT NOT NULL REFERENCES inodes(id),
            parent_inode INT NOT NULL REFERENCES inodes(id),
            UNIQUE (name, parent_inode)
        );
    """)

    now_ns = int(time() * 1e9)
    mode   = _dir_mode()
    cur.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns) VALUES (?,?,?,?,?,?,?)",
        (ROOT_INODE, mode, os.getuid(), os.getgid(), now_ns, now_ns, now_ns),
    )
    cur.execute(
        "INSERT INTO contents (name, parent_inode, inode) VALUES (?,?,?)",
        (b"..", ROOT_INODE, ROOT_INODE),
    )
    db.commit()
    log.info("backup: initialized fresh TelegramFS schema")


def _now_ns() -> int:
    return int(time() * 1e9)


def _file_mode() -> int:
    return stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH


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
    cur.execute("INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
                (name, inode, parent))
    cur.execute("INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
                (b"..", inode, inode))
    db.commit()
    return inode


def _ensure_dest_path(db: sqlite3.Connection, cur: sqlite3.Cursor, dest: str) -> int:
    """Walk/create every component of dest (relative path) under ROOT_INODE."""
    parent = ROOT_INODE
    for part in [p for p in dest.strip("/").split("/") if p]:
        parent = _ensure_dir(db, cur, part.encode(), parent)
    return parent


async def _upload_file(
    src_path: str,
    inode: int,
    block_store: BlockStore,
    db: sqlite3.Connection,
    cur: sqlite3.Cursor,
    fname: str,
) -> None:
    """Upload src_path block by block into block_store, updating the DB as we go."""
    file_size = os.path.getsize(src_path)
    n_blocks  = max(1, (file_size + BLOCK_SIZE - 1) // BLOCK_SIZE) if file_size else 0
    now_ts    = time()

    cur.execute("SELECT msg_id FROM blocks WHERE inode=?", (inode,))
    old_block_ids = [r["msg_id"] for r in cur.fetchall()]
    cur.execute("SELECT meta_msg_id FROM file_meta WHERE inode=?", (inode,))
    old_meta_row = cur.fetchone()
    old_meta_id  = old_meta_row["meta_msg_id"] if old_meta_row else None

    cur.execute("DELETE FROM blocks WHERE inode=?", (inode,))
    db.commit()

    all_msg_ids:    dict[int, int] = {}
    all_pre_hashes: dict[int, str] = {}

    with open(src_path, "rb") as fobj:
        for block_idx in range(n_blocks):
            data = fobj.read(BLOCK_SIZE)
            if not data:
                break

            STATS.backup_block_start(block_idx, n_blocks, src_path)

            cur.execute(
                "INSERT OR REPLACE INTO pending_blocks "
                "(msg_id, inode, block_idx, started_at) VALUES (0,?,?,?)",
                (inode, block_idx, now_ts),
            )
            db.commit()

            try:
                new_ids, pre_hashes = await block_store.upload_blocks(
                    inode, {block_idx: data}, fname
                )
            except Exception as exc:
                STATS.log("ERROR", "BACKUP_UL_FAIL",
                          f"{fname}[{block_idx}] failed after retries: {exc}")
                raise

            try:
                new_ids, pre_hashes = await verify_and_fix_blocks(
                    block_store, inode, {block_idx: data}, new_ids, fname,
                    pre_hashes=pre_hashes,
                )
            except RuntimeError as exc:
                STATS.log("ERROR", "BACKUP_VERIFY_FAIL", f"{fname}[{block_idx}]: {exc}")
                raise

            msg_id = new_ids[block_idx]

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
            STATS.backup_block_done(len(data))

    now = _now_ns()
    cur.execute(
        "UPDATE inodes SET size=?, mtime_ns=?, ctime_ns=? WHERE id=?",
        (file_size, now, now, inode),
    )
    db.commit()

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
        STATS.log("WARNING", "BACKUP_META_FAIL",
                  f"{fname}: manifest upload failed (data is safe): {exc}")

    cur.execute("DELETE FROM pending_blocks WHERE inode=?", (inode,))
    db.commit()

    stale = old_block_ids[:]
    if old_meta_id is not None:
        stale.append(old_meta_id)
    if stale:
        block_store.deleter.enqueue(stale)


async def run_backup(
    src_root:    str,
    dest_subdir: str,
    block_store: BlockStore,
    db:          sqlite3.Connection,
) -> None:
    """
    Recursively backup src_root into the TelegramFS under dest_subdir.

    Files are skipped if they already exist with the same size.
    Files are overwritten if sizes differ.
    """
    cur = db.cursor()
    db.text_factory = str
    db.row_factory  = sqlite3.Row

    _ensure_schema(db)
    dest_root_inode = _ensure_dest_path(db, cur, dest_subdir)

    log.info("Scanning %s …", src_root)
    STATS.log("INFO", "BACKUP_SCAN", f"scanning {src_root} …")
    total_files = total_bytes = 0
    for dirpath, _dirnames, filenames in os.walk(src_root):
        for fname in filenames:
            try:
                total_bytes += os.path.getsize(os.path.join(dirpath, fname))
                total_files += 1
            except OSError:
                pass
    STATS.backup_set_totals(total_files, total_bytes)
    STATS.log("INFO", "BACKUP_START",
              f"{total_files} files  {total_bytes // 1024**2} MiB  src={src_root!r}")

    files_done = files_skipped = errors = 0

    for dirpath, dirnames, filenames in os.walk(src_root, followlinks=False):
        rel_dir = os.path.relpath(dirpath, src_root)
        if rel_dir == ".":
            cur_dir_inode = dest_root_inode
        else:
            cur_dir_inode = dest_root_inode
            for part in rel_dir.split(os.sep):
                cur_dir_inode = _ensure_dir(db, cur, part.encode(), cur_dir_inode)

        for filename in sorted(filenames):
            src_path  = os.path.join(dirpath, filename)
            fname_enc = filename.encode()

            try:
                src_size = os.path.getsize(src_path)
            except OSError as exc:
                STATS.log("WARNING", "BACKUP_STAT_FAIL", f"{src_path}: {exc}")
                errors += 1
                continue

            existing = _lookup(cur, fname_enc, cur_dir_inode)

            if existing is not None:
                existing_inode = existing["inode"]
                existing_size  = existing["size"]
                if existing_size == src_size:
                    STATS.log("INFO", "BACKUP_SKIP",
                              f"{src_path!r} — same size ({src_size:,} B), skipping")
                    files_skipped += 1
                    STATS.backup_file_skipped(src_size)
                    continue
                STATS.log("INFO", "BACKUP_OVERWRITE",
                          f"{src_path!r} — size changed "
                          f"({existing_size:,} → {src_size:,} B), overwriting")
                inode = existing_inode
            else:
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

            STATS.backup_file_start(src_path, src_size)
            STATS.log("INFO", "BACKUP_FILE", f"→ {src_path!r}  ({src_size:,} B)")

            try:
                await _upload_file(src_path, inode, block_store, db, cur, filename)
                files_done += 1
                STATS.backup_file_done()
                STATS.log("SUCCESS", "BACKUP_FILE_OK", f"{src_path!r}")
            except Exception as exc:
                errors += 1
                STATS.backup_file_error()
                STATS.log("ERROR", "BACKUP_FILE_FAIL", f"{src_path!r}: {exc}")
                log.error("backup: failed to upload %s: %s", src_path, exc, exc_info=True)

    STATS.backup_finish()
    STATS.log(
        "SUCCESS" if errors == 0 else "WARNING",
        "BACKUP_DONE",
        f"done — {files_done} uploaded  {files_skipped} skipped  {errors} errors",
    )
    log.info("Backup complete: %d uploaded, %d skipped, %d errors",
             files_done, files_skipped, errors)
