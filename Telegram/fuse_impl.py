#!/usr/bin/env python3
# ver 1.1
from __future__ import annotations

import atexit
import errno
import logging
import os
import stat
import sqlite3
import sys
import threading
from argparse import ArgumentParser
from collections import defaultdict
from time import time

import pyfuse3
import trio
from pyfuse3 import FUSEError

from block_store import BlockStore, BLOCK_SIZE, blocks_for_range, sha256_hex
from stats import STATS
from verify import verify_and_fix_blocks
from schema import ensure_schema

try:
    import faulthandler
    faulthandler.enable()
except ImportError:
    pass

log = logging.getLogger(__name__)

_FREE_BLOCKS     = (100 * 1024 ** 4) // 512  # report ~100 TiB free to the OS
MAX_DIRTY_BYTES  = int(os.getenv("MAX_DIRTY_BYTES", str(64 * 1024 ** 2)))  # default 64 MiB writeback pressure threshold


class Operations(pyfuse3.Operations):
    enable_writeback_cache = True

    def __init__(self, block_store: BlockStore) -> None:
        super().__init__()
        self.bs = block_store

        self.db = sqlite3.connect("telegram.db", check_same_thread=False)
        self.db.text_factory = str
        self.db.row_factory  = sqlite3.Row
        self.cursor          = self.db.cursor()
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")

        self.inode_open_count: defaultdict[int, int] = defaultdict(int)
        self._dirty_blocks: dict[int, dict[int, bytearray]] = {}
        self._flushing_fhs: set[int] = set()
        # Per-inode read cache: avoids two SQLite queries on every read() call.
        # { inode: {"size": int, "msg_id_map": dict[int, int]} }
        self._inode_cache: dict[int, dict] = {}

        ensure_schema(self.db)
        self._cleanup_pending_uploads()

    def _cleanup_pending_uploads(self) -> None:
        """
        On startup, reconcile any pending_blocks rows left by a previous crash or unclean shutdown.

        A row with msg_id == 0 means the upload was still in-flight when we died — there is no
        real Telegram message to act on, so we just discard the placeholder.

        A row with msg_id != 0 means the upload completed but the DB commit that would have moved
        the msg_id into the `blocks` table never happened.  We RESTORE those blocks rather than
        deleting them; throwing away successfully-uploaded data was the original bug.

        A row whose (inode, block_idx) is already present in `blocks` is a true duplicate —
        the block was re-uploaded on a retry after the crash.  The pending msg_id is orphaned and
        should be deleted from Telegram.
        """
        self.cursor.execute("SELECT msg_id, inode, block_idx FROM pending_blocks")
        pending = self.cursor.fetchall()
        if not pending:
            return

        log.warning("Found %d pending_blocks row(s) from a previous unclean shutdown", len(pending))
        orphan_ids:   list[int] = []
        restored:     int       = 0

        for row in pending:
            if row["msg_id"] == 0:
                # Upload was still in-flight — no real message exists.
                continue

            self.cursor.execute(
                "SELECT msg_id FROM blocks WHERE inode=? AND block_idx=?",
                (row["inode"], row["block_idx"]),
            )
            existing = self.cursor.fetchone()

            if existing is None:
                # Upload finished but the DB commit never happened — restore it.
                self.cursor.execute(
                    "INSERT OR REPLACE INTO blocks (inode, block_idx, msg_id) VALUES (?,?,?)",
                    (row["inode"], row["block_idx"], row["msg_id"]),
                )
                log.info("Restored pending block: inode=%d block_idx=%d msg_id=%d",
                         row["inode"], row["block_idx"], row["msg_id"])
                restored += 1
            elif existing["msg_id"] != row["msg_id"]:
                # A different msg_id is already committed for this slot — ours is a stale orphan.
                orphan_ids.append(row["msg_id"])

        if orphan_ids:
            try:
                self.bs.delete_messages(orphan_ids)
            except Exception as exc:
                log.error("Failed to delete orphan messages from Telegram: %s", exc)

        self.cursor.execute("DELETE FROM pending_blocks")
        self.db.commit()
        log.info("Pending-block cleanup done: %d restored, %d orphan(s) removed",
                 restored, len(orphan_ids))

    def _one(self, sql: str, params: tuple = ()) -> sqlite3.Row:
        self.cursor.execute(sql, params)
        row = self.cursor.fetchone()
        if row is None:
            raise NoSuchRowError()
        return row

    def _all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        self.cursor.execute(sql, params)
        rows = self.cursor.fetchall()
        if not rows:
            raise NoSuchRowError()
        return rows

    def _msg_id_map(self, inode: int) -> dict[int, int]:
        self.cursor.execute("SELECT block_idx, msg_id FROM blocks WHERE inode=?", (inode,))
        return {r["block_idx"]: r["msg_id"] for r in self.cursor.fetchall()}

    def _hash_map(self, inode: int) -> dict[int, str]:
        """Return {block_idx: sha256_hex} for every block whose hash is stored in the DB."""
        self.cursor.execute("SELECT block_idx, sha256 FROM blocks WHERE inode=? AND sha256 IS NOT NULL", (inode,))
        return {r["block_idx"]: r["sha256"] for r in self.cursor.fetchall()}

    def _inode_path(self, inode: int) -> str:
        """Walk contents table upward to build the full relative path."""
        parts, current, seen = [], inode, set()
        while current not in seen and current != pyfuse3.ROOT_INODE:
            seen.add(current)
            self.cursor.execute(
                "SELECT name, parent_inode FROM contents WHERE inode=? AND name != '..' LIMIT 1",
                (current,),
            )
            row = self.cursor.fetchone()
            if row is None:
                break
            parts.append(
                row["name"].decode(errors="replace") if isinstance(row["name"], bytes) else row["name"]
            )
            parent = row["parent_inode"]
            if parent == pyfuse3.ROOT_INODE:
                break
            current = parent
        parts.reverse()
        return "/".join(parts)

    def _inode_name(self, inode: int) -> str:
        path = self._inode_path(inode)
        return path.rsplit("/", 1)[-1] if path else ""

    async def lookup(self, inode_p: int, name: bytes, ctx=None):
        STATS.record_lookup()
        if name == b".":
            return await self.getattr(inode_p, ctx)
        if name == b"..":
            try:
                row = self._one("SELECT parent_inode FROM contents WHERE inode=?", (inode_p,))
                return await self.getattr(row["parent_inode"], ctx)
            except NoSuchRowError:
                raise FUSEError(errno.ENOENT)
        try:
            row = self._one(
                "SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                (name, inode_p),
            )
            return await self.getattr(row["inode"], ctx)
        except NoSuchRowError:
            raise FUSEError(errno.ENOENT)

    async def getattr(self, inode: int, ctx=None):
        try:
            row = self._one("SELECT * FROM inodes WHERE id=?", (inode,))
        except NoSuchRowError:
            raise FUSEError(errno.ENOENT)

        self.cursor.execute("SELECT COUNT(*) FROM contents WHERE inode=?", (inode,))
        nlink = self.cursor.fetchone()[0]

        e               = pyfuse3.EntryAttributes()
        e.st_ino        = inode
        e.generation    = 0
        e.entry_timeout = 300
        e.attr_timeout  = 300
        e.st_mode       = row["mode"]
        e.st_nlink      = nlink
        e.st_uid        = row["uid"]
        e.st_gid        = row["gid"]
        e.st_rdev       = row["rdev"]
        e.st_size       = row["size"]
        e.st_blksize    = 512
        e.st_blocks     = (row["size"] + 511) // 512 or 1
        e.st_atime_ns   = row["atime_ns"]
        e.st_mtime_ns   = row["mtime_ns"]
        e.st_ctime_ns   = row["ctime_ns"]
        return e

    async def setattr(self, inode: int, attr, fields, fh, ctx):
        if fields.update_size:
            await self._handle_truncate(inode, attr.st_size, fh)
        if fields.update_mode:
            self.cursor.execute("UPDATE inodes SET mode=?     WHERE id=?", (attr.st_mode,     inode))
        if fields.update_uid:
            self.cursor.execute("UPDATE inodes SET uid=?      WHERE id=?", (attr.st_uid,      inode))
        if fields.update_gid:
            self.cursor.execute("UPDATE inodes SET gid=?      WHERE id=?", (attr.st_gid,      inode))
        if fields.update_atime:
            self.cursor.execute("UPDATE inodes SET atime_ns=? WHERE id=?", (attr.st_atime_ns, inode))
        if fields.update_mtime:
            self.cursor.execute("UPDATE inodes SET mtime_ns=? WHERE id=?", (attr.st_mtime_ns, inode))
        ctime = attr.st_ctime_ns if fields.update_ctime else int(time() * 1e9)
        self.cursor.execute("UPDATE inodes SET ctime_ns=? WHERE id=?", (ctime, inode))
        self.db.commit()
        return await self.getattr(inode)

    async def _handle_truncate(self, inode: int, new_size: int, fh) -> None:
        fh_key = fh if fh is not None else inode
        self.cursor.execute("SELECT size FROM inodes WHERE id=?", (inode,))
        row          = self.cursor.fetchone()
        current_size = row["size"] if row else 0

        if new_size == 0:
            self.cursor.execute("SELECT msg_id FROM blocks WHERE inode=?", (inode,))
            old_ids = [r[0] for r in self.cursor.fetchall()]
            self.cursor.execute("SELECT meta_msg_id FROM file_meta WHERE inode=?", (inode,))
            meta_row = self.cursor.fetchone()
            if meta_row:
                old_ids.append(meta_row["meta_msg_id"])
            if old_ids:
                await trio.to_thread.run_sync(self.bs.delete_messages, old_ids)
            self.cursor.execute("DELETE FROM blocks    WHERE inode=?", (inode,))
            self.cursor.execute("DELETE FROM file_meta WHERE inode=?", (inode,))
            self._dirty_blocks.pop(fh_key, None)
            self.bs.evict_inode(inode)
            if inode in self._inode_cache:
                self._inode_cache[inode]["msg_id_map"] = {}
                self._inode_cache[inode]["hash_map"]   = {}
                self._inode_cache[inode]["size"] = 0

        elif new_size < current_size:
            last_idx = (new_size - 1) // BLOCK_SIZE
            last_end = (last_idx + 1) * BLOCK_SIZE

            self.cursor.execute(
                "SELECT msg_id FROM blocks WHERE inode=? AND block_idx>?", (inode, last_idx)
            )
            old_ids = [r[0] for r in self.cursor.fetchall()]
            if old_ids:
                await trio.to_thread.run_sync(self.bs.delete_messages, old_ids)
            self.cursor.execute(
                "DELETE FROM blocks WHERE inode=? AND block_idx>?", (inode, last_idx)
            )
            if inode in self._inode_cache:
                self._inode_cache[inode]["msg_id_map"] = {
                    k: v for k, v in self._inode_cache[inode]["msg_id_map"].items()
                    if k <= last_idx
                }

            if new_size < last_end:
                msg_id_map = self._inode_cache[inode]["msg_id_map"] if inode in self._inode_cache else self._msg_id_map(inode)
                b_start    = last_idx * BLOCK_SIZE
                block_data = await self.bs.read_range(inode, b_start, BLOCK_SIZE, msg_id_map)
                trimmed    = bytearray(block_data[:new_size - b_start])
                self._dirty_blocks.setdefault(fh_key, {})[last_idx] = trimmed
                self.bs._cache.pop((inode, last_idx), None)

        self.cursor.execute("UPDATE inodes SET size=? WHERE id=?", (new_size, inode))
        if inode in self._inode_cache:
            self._inode_cache[inode]["size"] = new_size

    async def readlink(self, inode: int, ctx):
        return self._one("SELECT target FROM inodes WHERE id=?", (inode,))["target"]

    async def opendir(self, inode: int, ctx):
        return inode

    async def readdir(self, inode: int, off: int, token):
        if off == 0:
            off = -1
        cur = self.db.cursor()
        cur.execute(
            "SELECT * FROM contents WHERE parent_inode=? AND rowid>? ORDER BY rowid",
            (inode, off),
        )
        for row in cur:
            pyfuse3.readdir_reply(token, row["name"], await self.getattr(row["inode"]), row["rowid"])

    async def statfs(self, ctx):
        s = pyfuse3.StatvfsData()
        s.f_bsize = s.f_frsize = BLOCK_SIZE
        self.cursor.execute("SELECT COALESCE(SUM(size),0) FROM inodes")
        used_blocks = (self.cursor.fetchone()[0] + BLOCK_SIZE - 1) // BLOCK_SIZE
        s.f_blocks = used_blocks + _FREE_BLOCKS
        s.f_bfree  = s.f_bavail = _FREE_BLOCKS
        self.cursor.execute("SELECT COUNT(id) FROM inodes")
        inodes    = self.cursor.fetchone()[0]
        s.f_files = inodes
        s.f_ffree = s.f_favail = max(inodes, 100)
        return s

    async def access(self, inode: int, mode: int, ctx):
        return True

    async def mkdir(self, inode_p, name, mode, ctx):
        return await self._create(inode_p, name, mode, ctx)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        return await self._create(inode_p, name, mode, ctx, rdev=rdev)

    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK
                | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
                | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return await self._create(inode_p, name, mode, ctx, target=target)

    async def link(self, inode, new_inode_p, new_name, ctx):
        if (await self.getattr(new_inode_p)).st_nlink == 0:
            raise FUSEError(errno.EINVAL)
        self.cursor.execute(
            "INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
            (new_name, inode, new_inode_p),
        )
        self.db.commit()
        return await self.getattr(inode)

    async def unlink(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)
        if stat.S_ISDIR(entry.st_mode):
            raise FUSEError(errno.EISDIR)
        self._remove(inode_p, name, entry)

    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)
        if not stat.S_ISDIR(entry.st_mode):
            raise FUSEError(errno.ENOTDIR)
        self._remove(inode_p, name, entry)

    def _remove(self, inode_p, name, entry) -> None:
        inode = entry.st_ino
        self.cursor.execute("SELECT COUNT(*) FROM contents WHERE parent_inode=?", (inode,))
        if self.cursor.fetchone()[0] > 0:
            raise FUSEError(errno.ENOTEMPTY)

        STATS.record_delete(name.decode(errors="replace") if isinstance(name, bytes) else name)
        self.cursor.execute(
            "DELETE FROM contents WHERE name=? AND parent_inode=?", (name, inode_p)
        )
        if entry.st_nlink == 1 and inode not in self.inode_open_count:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (inode,))
            self._delete_blocks_sync(inode)
        self.db.commit()

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new, flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        entry_old = await self.lookup(inode_p_old, name_old)
        try:
            entry_new    = await self.lookup(inode_p_new, name_new)
            target_exists = True
        except FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            target_exists = False

        if target_exists:
            self._replace(inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new)
        else:
            self.cursor.execute(
                "UPDATE contents SET name=?, parent_inode=? WHERE name=? AND parent_inode=?",
                (name_new, inode_p_new, name_old, inode_p_old),
            )
            self.db.commit()

        STATS.record_rename(
            name_old.decode(errors="replace"),
            name_new.decode(errors="replace"),
        )

    def _replace(self, inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new) -> None:
        self.cursor.execute(
            "SELECT COUNT(*) FROM contents WHERE parent_inode=?", (entry_new.st_ino,)
        )
        if self.cursor.fetchone()[0] > 0:
            raise FUSEError(errno.ENOTEMPTY)
        self.cursor.execute(
            "UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
            (entry_old.st_ino, name_new, inode_p_new),
        )
        self.cursor.execute(
            "DELETE FROM contents WHERE name=? AND parent_inode=?", (name_old, inode_p_old)
        )
        if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (entry_new.st_ino,))
            self._delete_blocks_sync(entry_new.st_ino)
        self.db.commit()

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if (await self.getattr(inode_p)).st_nlink == 0:
            raise FUSEError(errno.EINVAL)
        now_ns = int(time() * 1e9)
        self.cursor.execute(
            "INSERT INTO inodes (uid,gid,mode,mtime_ns,atime_ns,ctime_ns,target,rdev) VALUES(?,?,?,?,?,?,?,?)",
            (ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev),
        )
        inode = self.cursor.lastrowid
        self.cursor.execute(
            "INSERT INTO contents(name,inode,parent_inode) VALUES(?,?,?)", (name, inode, inode_p)
        )
        self.db.commit()
        STATS.record_create(name.decode(errors="replace") if isinstance(name, bytes) else name)
        return await self.getattr(inode)

    async def open(self, inode, flags, ctx):
        self.inode_open_count[inode] += 1
        STATS.open_handle(inode, self._inode_name(inode))
        if inode not in self._inode_cache:
            self.cursor.execute("SELECT size FROM inodes WHERE id=?", (inode,))
            row = self.cursor.fetchone()
            self._inode_cache[inode] = {
                "size":       row["size"] if row else 0,
                "msg_id_map": self._msg_id_map(inode),
                "hash_map":   self._hash_map(inode),
            }
        return pyfuse3.FileInfo(fh=inode, keep_cache=True)

    async def create(self, inode_parent, name, mode, flags, ctx):
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        STATS.open_handle(entry.st_ino, name.decode(errors="replace") if isinstance(name, bytes) else name)
        self._inode_cache[entry.st_ino] = {"size": 0, "msg_id_map": {}, "hash_map": {}}
        return pyfuse3.FileInfo(fh=entry.st_ino, keep_cache=True), entry

    async def read(self, fh: int, offset: int, length: int) -> bytes:
        ic = self._inode_cache.get(fh)
        if ic is None:
            # Fallback for handles opened before this change (e.g. directories).
            self.cursor.execute("SELECT size FROM inodes WHERE id=?", (fh,))
            row = self.cursor.fetchone()
            if row is None:
                return b""
            file_size  = row["size"]
            msg_id_map = self._msg_id_map(fh)
            hash_map   = self._hash_map(fh)
        else:
            file_size  = ic["size"]
            msg_id_map = ic["msg_id_map"]
            hash_map   = ic.get("hash_map", {})

        if offset >= file_size:
            return b""
        length = min(length, file_size - offset)

        dirty = self._dirty_blocks.get(fh, {})

        # Inject dirty blocks into the LRU cache so read_range sees latest data
        # (dirty blocks are already verified pre-upload; skip hash check for them).
        for idx, buf in dirty.items():
            self.bs._cache[(fh, idx)] = bytes(buf)

        # Pass hash_map so read_range can verify any freshly-downloaded block
        # against its stored SHA-256.  Dirty-block cache hits bypass this because
        # they are already the authoritative in-memory data.
        try:
            data = await self.bs.read_range(fh, offset, length, msg_id_map, hash_map)
        except Exception as exc:
            # Any download failure (hash mismatch, Telegram error, timeout, …)
            # surfaces as EIO so Nemo/the kernel shows a proper transfer error.
            log.error("read: download failed fh=%d offset=%d: %s", fh, offset, exc)
            STATS.log("ERROR", "READ_FAIL", f"fh={fh} offset={offset}: {exc}")
            raise FUSEError(errno.EIO) from exc
        STATS.record_read(len(data))
        return data

    async def write(self, fh: int, offset: int, buf: bytes) -> int:
        if fh not in self._dirty_blocks:
            self._dirty_blocks[fh] = {}

        dirty      = self._dirty_blocks[fh]
        ic         = self._inode_cache.get(fh)
        msg_id_map = ic["msg_id_map"] if ic else self._msg_id_map(fh)
        end        = offset + len(buf)

        for idx in blocks_for_range(offset, len(buf)):
            if idx not in dirty:
                b_start = idx * BLOCK_SIZE
                # Re-read msg_id_map from _inode_cache each time we await so we
                # always use the freshest mapping (a concurrent flush could have
                # updated it while we were downloading a previous block).
                _ic = self._inode_cache.get(fh)
                _map = _ic["msg_id_map"] if _ic else self._msg_id_map(fh)
                block_data = await self.bs.read_range(fh, b_start, BLOCK_SIZE, _map)
                # ── TOCTOU guard ─────────────────────────────────────────────
                # Re-check AFTER the await.  Trio is cooperative, so another
                # concurrent write() call can run during read_range() and may
                # have already initialised dirty[idx] with its own data.
                # If we overwrote that bytearray here we would silently discard
                # the other write's changes — producing the alternating-zeros
                # corruption pattern observed in practice.
                # Only initialise the slot if no other coroutine beat us to it.
                if idx not in dirty:
                    dirty[idx] = bytearray(block_data)
                # else: another write() already owns this bytearray; our data
                # will be patched into it in the apply-writes loop below.

        for idx in blocks_for_range(offset, len(buf)):
            block   = dirty[idx]
            b_start = idx * BLOCK_SIZE
            w_start = max(offset, b_start)
            w_end   = min(end, b_start + BLOCK_SIZE)
            l_start = w_start - b_start
            l_end   = w_end   - b_start
            b_off   = w_start - offset
            b_lim   = w_end   - offset
            if l_end > len(block):
                block += b"\0" * (l_end - len(block))
                dirty[idx] = block
            dirty[idx][l_start:l_end] = buf[b_off:b_lim]

        self.cursor.execute("SELECT size FROM inodes WHERE id=?", (fh,))
        row = self.cursor.fetchone()
        if row and end > row["size"]:
            self.cursor.execute("UPDATE inodes SET size=? WHERE id=?", (end, fh))
            self.db.commit()
            if fh in self._inode_cache:
                self._inode_cache[fh]["size"] = end

        STATS.record_write(len(buf))
        STATS.update_handle(fh, dirty=True, buffer_bytes=sum(len(b) for b in dirty.values()))
        return len(buf)

    async def flush(self, fh: int, lock_owner: int = 0) -> None:
        """
        Called by the kernel when an application closes its file descriptor.
        This is the correct hook for uploading dirty data: the kernel guarantees
        that the calling process is blocked until flush() returns, so `cp` or any
        other writer will not exit until the upload has finished.  This eliminates
        the race where the process exits before release() has had time to run.
        """
        if fh in self._dirty_blocks:
            STATS.log("INFO", "FLUSH", f"fh={fh}")
            await self._flush_fh(fh)

    async def fsync(self, fh: int, datasync: bool):
        if fh in self._dirty_blocks:
            STATS.log("INFO", "FSYNC", f"fh={fh}")
            await self._flush_fh(fh)

    async def release(self, fh: int):
        """
        Called asynchronously after all file descriptors for an inode are closed.
        By this point flush() has already run and uploaded everything, so there
        should be no dirty blocks left.  We only do housekeeping here.
        """
        self.inode_open_count[fh] -= 1
        # Defensively clear any remaining dirty state (e.g. if flush was skipped
        # due to an error path), but do NOT attempt another upload here — release()
        # is not waited on by the calling process, so an upload here can be killed
        # mid-transfer when the user quits.
        self._dirty_blocks.pop(fh, None)

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            self._inode_cache.pop(fh, None)
            STATS.close_handle(fh)
            try:
                if (await self.getattr(fh)).st_nlink == 0:
                    self.cursor.execute("DELETE FROM inodes WHERE id=?", (fh,))
                    self.db.commit()
            except FUSEError:
                pass

    async def _flush_fh(self, fh: int) -> None:
        """
        Upload dirty blocks, update DB, upload manifest.
        Crash-safe: pending_blocks records survive a crash so orphaned uploads
        can be detected and cleaned on next startup.
        """
        if not self._dirty_blocks.get(fh):
            return
        self._flushing_fhs.add(fh)

        # Take an immutable snapshot of the blocks we intend to flush.
        # We do NOT pop from _dirty_blocks here — see detailed comment below.
        #
        # Why snapshot instead of pop?
        # (a) write() holds references to the same bytearray objects that live
        #     in _dirty_blocks[fh].  If we pop and then upload, a concurrent
        #     write() (trio permits this at any await point) can mutate the
        #     bytearray we are mid-upload, silently corrupting the data sent to
        #     Telegram.  bytes() creates a separate immutable copy.
        # (b) On verification failure we previously did
        #         self._dirty_blocks[fh] = dirty
        #     which blindly overwrites any new writes that arrived during the
        #     upload — data loss.  Leaving the dict intact avoids this entirely.
        flushed_indices = list(self._dirty_blocks[fh].keys())

        fpath = self._inode_path(fh)
        fname = fpath.rsplit("/", 1)[-1] if fpath else ""
        STATS.log("INFO", "FLUSH", f"fh={fh}  path={fpath!r}  {len(flushed_indices)} dirty block(s)")

        # Collect old msg_ids that will be superseded so we can delete them later.
        old_msg_ids: list[int] = []
        for idx in flushed_indices:
            self.cursor.execute(
                "SELECT msg_id FROM blocks WHERE inode=? AND block_idx=?", (fh, idx)
            )
            row = self.cursor.fetchone()
            if row:
                old_msg_ids.append(row["msg_id"])

        self.cursor.execute("SELECT meta_msg_id FROM file_meta WHERE inode=?", (fh,))
        old_meta_row = self.cursor.fetchone()
        old_meta_id  = old_meta_row["meta_msg_id"] if old_meta_row else None

        # Upload and verify in batches to cap peak RAM usage.
        # Each batch snapshots only FLUSH_BATCH_SIZE blocks (default 4 × 4 MiB = 16 MiB),
        # uploads, verifies, commits to DB, then frees the snapshot before moving on.
        # _dirty_blocks[fh] is left intact throughout so concurrent write() calls
        # landing during the flush are never lost.
        FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "4"))
        now_ts  = time()
        all_new_ids:    dict[int, int] = {}
        all_pre_hashes: dict[int, str] = {}

        for batch_start in range(0, len(flushed_indices), FLUSH_BATCH_SIZE):
            batch_indices = flushed_indices[batch_start:batch_start + FLUSH_BATCH_SIZE]

            # Snapshot only this batch — O(batch_size × block_size) extra RAM.
            batch_dirty: dict[int, bytes] = {
                idx: bytes(self._dirty_blocks[fh][idx])
                for idx in batch_indices
                if idx in self._dirty_blocks[fh]
            }
            if not batch_dirty:
                continue

            # Step 1: record in-flight uploads for crash recovery.
            for idx in batch_dirty:
                self.cursor.execute(
                    "INSERT OR REPLACE INTO pending_blocks (msg_id, inode, block_idx, started_at) VALUES (0,?,?,?)",
                    (fh, idx, now_ts),
                )
            self.db.commit()

            # Step 2: upload + verify this batch.
            # Any Telegram / network failure after all retries raises here → EIO.
            try:
                batch_new_ids, batch_pre_hashes = await self.bs.upload_blocks(fh, batch_dirty, fname)
            except Exception as exc:
                STATS.log("ERROR", "UL_FAIL_FATAL",
                          f"fh={fh} batch={batch_indices}: {exc}")
                log.error("_flush_fh: upload_blocks failed inode=%d batch=%s: %s",
                          fh, batch_indices, exc)
                raise FUSEError(errno.EIO) from exc
            log.debug("pre-upload hashes: inode=%d batch=%s  %s",
                      fh, batch_indices,
                      {idx: h[:12]+"…" for idx, h in batch_pre_hashes.items()})

            try:
                batch_new_ids, batch_pre_hashes = await verify_and_fix_blocks(
                    self.bs, fh, batch_dirty, batch_new_ids, fname,
                    pre_hashes=batch_pre_hashes,
                )
            except RuntimeError as exc:
                STATS.log("ERROR", "VERIFY_FAIL", str(exc))
                log.error("_flush_fh: verification failed inode=%d batch=%s — aborting: %s",
                          fh, batch_indices, exc)
                # Inject confirmed-good blocks from previous batches + this batch's
                # snapshot into the cache so reads see correct data until next flush.
                for idx, data in batch_dirty.items():
                    self.bs._cache[(fh, idx)] = data
                raise FUSEError(errno.EIO) from exc

            # Step 3: replace placeholder pending rows with real msg_ids.
            for idx, msg_id in batch_new_ids.items():
                self.cursor.execute(
                    "INSERT OR REPLACE INTO pending_blocks (msg_id, inode, block_idx, started_at) VALUES (?,?,?,?)",
                    (msg_id, fh, idx, now_ts),
                )
            self.cursor.execute("DELETE FROM pending_blocks WHERE inode=? AND msg_id=0", (fh,))
            self.db.commit()

            # Step 4: commit this batch to the blocks table.
            for idx, msg_id in batch_new_ids.items():
                self.cursor.execute(
                    "INSERT OR REPLACE INTO blocks (inode, block_idx, msg_id, sha256) VALUES (?,?,?,?)",
                    (fh, idx, msg_id, batch_pre_hashes.get(idx)),
                )
            self.db.commit()

            # Accumulate results and update inode cache incrementally.
            all_new_ids.update(batch_new_ids)
            all_pre_hashes.update(batch_pre_hashes)
            if fh in self._inode_cache:
                self._inode_cache[fh]["msg_id_map"].update(batch_new_ids)
                self._inode_cache[fh].setdefault("hash_map", {}).update(batch_pre_hashes)

            # Free the batch snapshot — its RAM is now reclaimed before the next batch.
            del batch_dirty

            # Remove successfully flushed indices from dirty_blocks so the next
            # flush() call doesn't re-upload them.  New writes to the same indices
            # during this flush will have replaced the bytearray in _dirty_blocks[fh],
            # so we only pop indices that were in *this* batch.
            if fh in self._dirty_blocks:
                for idx in batch_new_ids:
                    self._dirty_blocks[fh].pop(idx, None)
                if not self._dirty_blocks[fh]:
                    del self._dirty_blocks[fh]

        # Unify naming for the post-batch steps.
        new_ids    = all_new_ids
        pre_hashes = all_pre_hashes

        # Finalise mtime / size.
        now_ns = int(time() * 1e9)
        self.cursor.execute("UPDATE inodes SET mtime_ns=?, ctime_ns=? WHERE id=?", (now_ns, now_ns, fh))
        self.cursor.execute("SELECT size FROM inodes WHERE id=?", (fh,))
        size_row  = self.cursor.fetchone()
        file_size = size_row["size"] if size_row else 0
        self.db.commit()
        if fh in self._inode_cache:
            self._inode_cache[fh]["size"] = file_size

        # Step 5: upload new manifest (includes hashes for repair recovery).
        # A manifest failure does NOT raise EIO — the actual data blocks are
        # already committed to the DB and safely stored on Telegram.  The manifest
        # is only used for repair/rebuild; losing it is non-fatal for the file.
        try:
            new_meta_id = await self.bs.upload_manifest(
                inode=fh, path=fpath, size=file_size,
                mtime_ns=now_ns, block_msg_ids=self._msg_id_map(fh),
                block_hashes=self._hash_map(fh),
            )
        except Exception as exc:
            STATS.log("WARNING", "META_UL_FAIL",
                      f"fh={fh} manifest upload failed (data is safe): {exc}")
            log.warning("_flush_fh: manifest upload failed inode=%d (continuing): %s", fh, exc)
            new_meta_id = None
        if new_meta_id is not None:
            self.cursor.execute(
                "INSERT OR REPLACE INTO file_meta (inode, meta_msg_id) VALUES (?,?)", (fh, new_meta_id)
            )
            self.db.commit()

        # Step 6: clear pending_blocks.
        self.cursor.execute("DELETE FROM pending_blocks WHERE inode=?", (fh,))
        self.db.commit()

        # Step 7: enqueue stale messages for background deletion.
        stale_ids = old_msg_ids[:]
        if old_meta_id is not None:
            stale_ids.append(old_meta_id)
        self.bs.deleter.enqueue(stale_ids)

        remaining = self._dirty_blocks.get(fh, {})
        STATS.update_handle(
            fh,
            dirty=bool(remaining),
            buffer_bytes=sum(len(b) for b in remaining.values()),
        )
        STATS.log("SUCCESS", "FLUSH", f"fh={fh}  {len(new_ids)} block(s) committed  meta={new_meta_id}"
                  + (f"  ({len(remaining)} new-dirty remain)" if remaining else ""))
        self._flushing_fhs.discard(fh)

    async def _flush_fh_guarded(self, fh: int) -> None:
        """Flush fh and always remove it from _flushing_fhs when done."""
        try:
            await self._flush_fh(fh)
        except FUSEError as exc:
            log.warning("pressure flush failed fh=%d: %s", fh, exc)
        except Exception as exc:
            log.error("pressure flush unexpected error fh=%d: %s", fh, exc, exc_info=True)
        finally:
            self._flushing_fhs.discard(fh)

    async def _writeback_pressure_loop(self) -> None:
        """Background task: flush dirty file handles when total in-memory dirty
        data exceeds MAX_DIRTY_BYTES.  Multiple handles are flushed concurrently;
        the shared BlockStore._ul_limiter caps total concurrent block uploads.

        Runs in the same trio thread as pyfuse3.main — no locking needed.
        Only one flush per fh runs at a time (enforced by _flushing_fhs).
        """
        log.debug("writeback pressure loop started  threshold=%d MiB",
                  MAX_DIRTY_BYTES // 1024 ** 2)
        async with trio.open_nursery() as nursery:
            while True:
                await trio.sleep(0.5)
                fh_sizes = {
                    fh: sum(len(b) for b in blocks.values())
                    for fh, blocks in self._dirty_blocks.items()
                }
                total = sum(fh_sizes.values())
                if total < MAX_DIRTY_BYTES:
                    continue

                # Launch a flush task for every dirty fh not already flushing.
                # The shared _ul_limiter in BlockStore caps how many blocks are
                # actually in-flight across all these concurrent flushes.
                candidates = [
                    (sz, fh) for fh, sz in fh_sizes.items()
                    if fh not in self._flushing_fhs and sz > 0
                ]
                if not candidates:
                    continue

                STATS.log(
                    "INFO", "WRITEBACK_PRESSURE",
                    f"total dirty={total // 1024**2} MiB >= threshold="
                    f"{MAX_DIRTY_BYTES // 1024**2} MiB — "
                    f"launching {len(candidates)} flush task(s)",
                )
                for _, fh in sorted(candidates, reverse=True):
                    self._flushing_fhs.add(fh)
                    nursery.start_soon(self._flush_fh_guarded, fh)

    async def _drain_dirty_blocks(self) -> None:
        """Flush all remaining dirty blocks to Telegram after unmount,
        concurrently. Called once pyfuse3.main() has returned (kernel detached)
        so no new writes can arrive."""
        fh_list = [fh for fh, blocks in self._dirty_blocks.items() if blocks]
        if not fh_list:
            return
        total = sum(
            sum(len(b) for b in self._dirty_blocks[fh].values())
            for fh in fh_list
        )
        STATS.log(
            "WARNING", "SHUTDOWN_DRAIN",
            f"flushing {len(fh_list)} dirty handle(s)  "
            f"({total // 1024**2} MiB) before exit …",
        )
        async with trio.open_nursery() as nursery:
            for fh in fh_list:
                if fh in self._flushing_fhs:
                    continue  # already in progress — will finish naturally
                self._flushing_fhs.add(fh)
                nursery.start_soon(self._flush_fh_guarded, fh)
        STATS.log("INFO", "SHUTDOWN_DRAIN", "drain complete")

    def _delete_blocks_sync(self, inode: int) -> None:
        """
        Remove all DB records for *inode* and schedule the corresponding Telegram
        messages for background deletion.

        IMPORTANT: we intentionally do NOT call self.bs.delete_messages() here.
        That method is synchronous and makes a blocking Telegram API call, which
        would freeze the entire FUSE worker thread (and therefore the whole
        filesystem) for the duration of the round-trip.  This is what caused file
        managers to hang on delete — the kernel holds the VFS lock on the parent
        directory until unlink() returns, so every other filesystem operation
        (including ls) blocks too.

        Instead we hand the message IDs to DeferredDeleter, which flushes them in
        a background loop every DELETE_BATCH_DELAY seconds.  The inode and contents
        rows are already removed from the DB above, so the file is instantly gone
        from the user's perspective; the Telegram-side cleanup just happens shortly
        after.
        """
        self.cursor.execute("SELECT msg_id FROM blocks WHERE inode=?", (inode,))
        ids = [r[0] for r in self.cursor.fetchall()]
        self.cursor.execute("SELECT meta_msg_id FROM file_meta WHERE inode=?", (inode,))
        meta_row = self.cursor.fetchone()
        if meta_row:
            ids.append(meta_row["meta_msg_id"])
        # Enqueue for background deletion — never block the FUSE thread here.
        self.bs.deleter.enqueue(ids)
        self.cursor.execute("DELETE FROM blocks    WHERE inode=?", (inode,))
        self.cursor.execute("DELETE FROM file_meta WHERE inode=?", (inode,))
        self.bs.evict_inode(inode)
        if inode in self._inode_cache:
            del self._inode_cache[inode]


class NoSuchRowError(Exception):
    pass

class NoUniqueValueError(Exception):
    pass


def init_logging(debug: bool = False) -> None:
    fmt     = logging.Formatter("%(asctime)s.%(msecs)03d %(threadName)s [%(name)s] %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    root.addHandler(handler)


def parse_args():
    p = ArgumentParser()
    p.add_argument("mountpoint",   type=str)
    p.add_argument("--debug",      action="store_true")
    p.add_argument("--debug-fuse", action="store_true")
    p.add_argument("--no-monitor", action="store_true")
    return p.parse_args()


def runFs(block_store: BlockStore) -> None:
    options = parse_args()
    init_logging(options.debug)

    STATS.mountpoint = options.mountpoint
    STATS.log("INFO", "STARTUP", f"Mounting at {options.mountpoint}")

    operations   = Operations(block_store)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("fsname=telegram_fuse")
    fuse_options.add("allow_other")
    fuse_options.discard("default_permissions")
    if options.debug_fuse:
        fuse_options.add("debug")

    pyfuse3.init(operations, options.mountpoint, fuse_options)

    def cleanup():
        STATS.log("WARNING", "SHUTDOWN", "Unmounting filesystem")
        operations.cursor.close()
        operations.db.close()
        pyfuse3.close(unmount=True)

    atexit.register(cleanup)

    async def _run_with_deleter():
        async with trio.open_nursery() as nursery:
            nursery.start_soon(pyfuse3.main)
            nursery.start_soon(block_store.deleter.run_background)
            nursery.start_soon(operations._writeback_pressure_loop)
            # pyfuse3.main() exits when the filesystem is unmounted.
            # That cancels the nursery scope, which sends Cancelled to the
            # other tasks.  If any task is blocked inside a Telegram awaitable
            # with no nearby checkpoint the cancel may take a while, so we
            # give the nursery a hard 8-second deadline from the moment
            # pyfuse3.main() returns.
            #
            # We can't set the deadline here (before the tasks start), so
            # instead we wrap pyfuse3.main in a helper that arms a cancel
            # scope on the outer nursery once it finishes.
            pass  # (structure comment only — deadline set via wrapper below)

    async def _run_with_deleter():  # noqa: F811 (intentional re-definition)
        # Wrapper that arms a hard deadline once pyfuse3.main exits.
        TEARDOWN_GRACE = 8  # seconds

        async def _pyfuse3_main_then_deadline():
            await pyfuse3.main()
            # pyfuse3.main() returned → kernel has detached.  No new writes
            # can arrive, but dirty blocks in RAM haven't been uploaded yet.
            # Flush them now before allowing teardown.
            await operations._drain_dirty_blocks()
            # Give the deleter and other nursery tasks TEARDOWN_GRACE seconds
            # to finish, then hard-cancel them.
            nursery.cancel_scope.deadline = trio.current_time() + TEARDOWN_GRACE

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_pyfuse3_main_then_deadline)
            nursery.start_soon(block_store.deleter.run_background)
            nursery.start_soon(operations._writeback_pressure_loop)

    if not options.no_monitor:
        fuse_thread = threading.Thread(
            target=lambda: trio.run(_run_with_deleter),
            name="fuse-worker",
            daemon=True,
        )
        fuse_thread.start()
        from monitor import launch_monitor_blocking

        def fuse_shutdown(app):
            import os, signal, threading
            STATS.log("WARNING", "SHUTDOWN", "Unmounting filesystem …")

            def _do_unmount():
                # Step 1: ask pyfuse3 to unmount.  This should cause pyfuse3.main()
                # to return, which lets trio cancel the nursery (deleter, pressure
                # loop), and the fuse_thread exits.  BUT pyfuse3.close() can itself
                # block if the kernel has in-flight FUSE requests, so we run it in
                # yet another thread with its own timeout.
                close_done = threading.Event()

                def _close():
                    try:
                        pyfuse3.close(unmount=True)
                    except Exception:
                        pass
                    finally:
                        close_done.set()

                threading.Thread(target=_close, daemon=True, name="pyfuse3-close").start()

                # Wait up to 3 s for the close to finish, then proceed regardless.
                close_done.wait(timeout=3)
                if not close_done.is_set():
                    STATS.log("WARNING", "SHUTDOWN",
                              "pyfuse3.close() blocked >3 s — forcing unmount via fusermount")
                    try:
                        import subprocess, shutil
                        mnt = getattr(options, "mountpoint", None)
                        if mnt and shutil.which("fusermount3"):
                            subprocess.run(["fusermount3", "-uz", str(mnt)],
                                           timeout=3, check=False)
                        elif mnt and shutil.which("fusermount"):
                            subprocess.run(["fusermount", "-uz", str(mnt)],
                                           timeout=3, check=False)
                    except Exception:
                        pass

                # Step 2: wait up to 5 s for the fuse_thread to finish cleanly.
                fuse_thread.join(timeout=5)

                if fuse_thread.is_alive():
                    STATS.log("WARNING", "SHUTDOWN",
                              "fuse thread still alive — SIGTERM")
                    os.kill(os.getpid(), signal.SIGTERM)
                    fuse_thread.join(timeout=3)

                if fuse_thread.is_alive():
                    STATS.log("WARNING", "SHUTDOWN", "still alive — SIGKILL")
                    os.kill(os.getpid(), signal.SIGKILL)

                # Step 3: exit the TUI (runs on Textual's main thread).
                app.call_from_thread(app.exit)

            threading.Thread(target=_do_unmount, daemon=True, name="shutdown").start()

        launch_monitor_blocking(shutdown_callback=fuse_shutdown)
        fuse_thread.join()
    else:
        trio.run(_run_with_deleter)
