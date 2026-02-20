#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Based on the tmpfs.py example from pyfuse3: https://github.com/libfuse/pyfuse3/blob/master/examples/tmpfs.py

A mountable filesystem that stores data on Telegram. Maintains a sqlite db on-disk to keep track of stuff
like filename, which Telegram message IDs are associated with a file, etc.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import os
import sys

basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
        os.path.exists(os.path.join(basedir, 'src', 'pyfuse3', '__init__.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

import pyfuse3
import errno
import stat
from time import time
import sqlite3
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio
from io import BytesIO
import gc
import atexit

from stats import STATS

try:
    import faulthandler
    faulthandler.enable()
except ImportError:
    pass

log = logging.getLogger()

# Large but realistic free-space value (100 TB in 512-byte blocks).
_FREE_BLOCKS = (100 * 1024 ** 4) // 512


class Operations(pyfuse3.Operations):
    enable_writeback_cache = True

    def __init__(self, client):
        super().__init__()
        self.db = sqlite3.connect('telegram.db')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.inode_open_count: defaultdict[int, int] = defaultdict(int)
        self.client = client

        # Per-file-handle write buffers  {fh: bytearray}
        self._write_buffers: dict[int, bytearray] = {}
        # Track which file handles have unsaved changes
        self._dirty: set[int] = set()

        try:
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                ("inodes",),
            )
            if self.cursor.fetchone() is None:
                print("Creating tables…")
                self._init_tables()
        except Exception as exc:
            log.error("Error initialising tables: %s", exc)
            raise

    # ── Debug ──────────────────────────────────────────────────────────

    def load_tables(self):
        self.cursor.execute("SELECT * FROM inodes;")
        rows = self.cursor.fetchall()
        print(f"GOT {len(rows)} inodes")
        for row in rows:
            print("Row:", "    ".join(str(r) for r in row))

    # ── Schema bootstrap ───────────────────────────────────────────────

    def _init_tables(self):
        self.cursor.execute("""
            CREATE TABLE inodes (
                id        INTEGER PRIMARY KEY,
                uid       INT NOT NULL,
                gid       INT NOT NULL,
                mode      INT NOT NULL,
                mtime_ns  INT NOT NULL,
                atime_ns  INT NOT NULL,
                ctime_ns  INT NOT NULL,
                target    BLOB(256),
                size      INT NOT NULL DEFAULT 0,
                rdev      INT NOT NULL DEFAULT 0,
                data      BLOB
            )
        """)
        self.cursor.execute("""
            CREATE TABLE telegram_messages (
                id    INTEGER PRIMARY KEY,
                inode INT NOT NULL REFERENCES inodes(id)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE contents (
                rowid        INTEGER PRIMARY KEY AUTOINCREMENT,
                name         BLOB(256) NOT NULL,
                inode        INT NOT NULL REFERENCES inodes(id),
                parent_inode INT NOT NULL REFERENCES inodes(id),
                UNIQUE (name, parent_inode)
            )
        """)
        now_ns = int(time() * 1e9)
        mode = (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        self.cursor.execute(
            "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns) VALUES (?,?,?,?,?,?,?)",
            (pyfuse3.ROOT_INODE, mode, os.getuid(), os.getgid(), now_ns, now_ns, now_ns),
        )
        self.cursor.execute(
            "INSERT INTO contents (name, parent_inode, inode) VALUES (?,?,?)",
            (b'..', pyfuse3.ROOT_INODE, pyfuse3.ROOT_INODE),
        )
        self.db.commit()

    # ── DB helpers ─────────────────────────────────────────────────────

    def _query_one(self, sql, params=()):
        """Return exactly one row or raise NoSuchRowError / NoUniqueValueError."""
        self.cursor.execute(sql, params)
        row = self.cursor.fetchone()
        if row is None:
            raise NoSuchRowError()
        extra = self.cursor.fetchone()
        if extra is not None:
            raise NoUniqueValueError()
        return row

    def _query_all(self, sql, params=()):
        """Return all rows or raise NoSuchRowError."""
        self.cursor.execute(sql, params)
        rows = self.cursor.fetchall()
        if not rows:
            raise NoSuchRowError()
        return rows

    # Compatibility aliases
    def get_row(self, *a, **kw):
        return self._query_one(*a, **kw)

    def get_rows(self, *a, **kw):
        return self._query_all(*a, **kw)

    # ── Name helper ────────────────────────────────────────────────────

    def _inode_name(self, fh: int) -> str:
        try:
            row = self._query_one("SELECT name FROM contents WHERE inode=?", (fh,))
            return row["name"].decode(errors="replace")
        except NoSuchRowError:
            return ""

    # ── FUSE operations ────────────────────────────────────────────────

    async def lookup(self, inode_p, name, ctx=None):
        STATS.record_lookup()
        if name == b'.':
            inode = inode_p
        elif name == b'..':
            row = self._query_one(
                "SELECT parent_inode FROM contents WHERE inode=?", (inode_p,)
            )
            inode = row['parent_inode']
        else:
            try:
                row = self._query_one(
                    "SELECT inode FROM contents WHERE name=? AND parent_inode=?",
                    (name, inode_p),
                )
                inode = row['inode']
            except NoSuchRowError:
                raise pyfuse3.FUSEError(errno.ENOENT)
        return await self.getattr(inode, ctx)

    async def getattr(self, inode, ctx=None):
        try:
            row = self._query_one("SELECT * FROM inodes WHERE id=?", (inode,))
        except NoSuchRowError:
            raise pyfuse3.FUSEError(errno.ENOENT)

        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        entry.st_mode = row['mode']
        entry.st_nlink = self._query_one(
            "SELECT COUNT(inode) FROM contents WHERE inode=?", (inode,)
        )[0]
        entry.st_uid = row['uid']
        entry.st_gid = row['gid']
        entry.st_rdev = row['rdev']
        entry.st_size = row['size']
        entry.st_blksize = 512
        entry.st_blocks = 1
        entry.st_atime_ns = row['atime_ns']
        entry.st_mtime_ns = row['mtime_ns']
        entry.st_ctime_ns = row['ctime_ns']
        return entry

    async def readlink(self, inode, ctx):
        return self._query_one('SELECT target FROM inodes WHERE id=?', (inode,))['target']

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        if off == 0:
            off = -1
        cursor2 = self.db.cursor()
        cursor2.execute(
            "SELECT * FROM contents WHERE parent_inode=? AND rowid > ? ORDER BY rowid",
            (inode, off),
        )
        for row in cursor2:
            pyfuse3.readdir_reply(token, row['name'], await self.getattr(row['inode']), row['rowid'])

    async def unlink(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)
        if stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.EISDIR)
        self._remove(inode_p, name, entry)

    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)
        if not stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        self._remove(inode_p, name, entry)

    def _remove(self, inode_p, name, entry):
        inode = entry.st_ino

        child_count = self._query_one(
            "SELECT COUNT(inode) FROM contents WHERE parent_inode=?", (inode,)
        )[0]
        if child_count > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        decoded_name = name.decode(errors='replace') if isinstance(name, bytes) else name
        STATS.record_delete(decoded_name)

        # Always remove the directory entry.
        self.cursor.execute(
            "DELETE FROM contents WHERE name=? AND parent_inode=?", (name, inode_p)
        )

        is_open = inode in self.inode_open_count
        if entry.st_nlink == 1 and not is_open:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (inode,))
            self._delete_telegram_msgs(inode)
            self.cursor.execute("DELETE FROM telegram_messages WHERE inode=?", (inode,))

        self.db.commit()

    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
                | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return await self._create(inode_p, name, mode, ctx, target=target)

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new, flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        entry_old = await self.lookup(inode_p_old, name_old)

        old_str = name_old.decode(errors='replace') if isinstance(name_old, bytes) else name_old
        new_str = name_new.decode(errors='replace') if isinstance(name_new, bytes) else name_new

        try:
            entry_new = await self.lookup(inode_p_new, name_new)
        except pyfuse3.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new)
        else:
            self.cursor.execute(
                "UPDATE contents SET name=?, parent_inode=? WHERE name=? AND parent_inode=?",
                (name_new, inode_p_new, name_old, inode_p_old),
            )
            self.db.commit()

        STATS.record_rename(old_str, new_str)

    def _delete_telegram_msgs(self, inode: int) -> None:
        """Delete Telegram messages for *inode* using a fresh cursor to avoid conflicts."""
        cur = self.db.cursor()
        cur.execute("SELECT id FROM telegram_messages WHERE inode=?", (inode,))
        ids = [r[0] for r in cur.fetchall()]
        if ids:
            self.client.delete_messages(ids)

    def _replace(self, inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new):
        child_count = self._query_one(
            "SELECT COUNT(inode) FROM contents WHERE parent_inode=?", (entry_new.st_ino,)
        )[0]
        if child_count > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        self.cursor.execute(
            "UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
            (entry_old.st_ino, name_new, inode_p_new),
        )
        self.db.execute(
            "DELETE FROM contents WHERE name=? AND parent_inode=?", (name_old, inode_p_old)
        )

        if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (entry_new.st_ino,))
            self._delete_telegram_msgs(entry_new.st_ino)
            self.cursor.execute(
                "DELETE FROM telegram_messages WHERE inode=?", (entry_new.st_ino,)
            )

        self.db.commit()

    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warning("Attempted to create entry %s with unlinked parent %d", new_name, new_inode_p)
            raise FUSEError(errno.EINVAL)
        self.cursor.execute(
            "INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
            (new_name, inode, new_inode_p),
        )
        self.db.commit()
        return await self.getattr(inode)

    async def setattr(self, inode, attr, fields, fh, ctx):
        if fields.update_size:
            data = await self._get_telegram_data(fh if fh is not None else inode)
            if data is None:
                data = b''
            if len(data) < attr.st_size:
                data = data + b'\0' * (attr.st_size - len(data))
            else:
                data = data[:attr.st_size]
            self.cursor.execute('UPDATE inodes SET size=? WHERE id=?', (attr.st_size, inode))

        if fields.update_mode:
            self.cursor.execute('UPDATE inodes SET mode=? WHERE id=?', (attr.st_mode, inode))
        if fields.update_uid:
            self.cursor.execute('UPDATE inodes SET uid=? WHERE id=?', (attr.st_uid, inode))
        if fields.update_gid:
            self.cursor.execute('UPDATE inodes SET gid=? WHERE id=?', (attr.st_gid, inode))
        if fields.update_atime:
            self.cursor.execute('UPDATE inodes SET atime_ns=? WHERE id=?', (attr.st_atime_ns, inode))
        if fields.update_mtime:
            self.cursor.execute('UPDATE inodes SET mtime_ns=? WHERE id=?', (attr.st_mtime_ns, inode))

        ctime = attr.st_ctime_ns if fields.update_ctime else int(time() * 1e9)
        self.cursor.execute('UPDATE inodes SET ctime_ns=? WHERE id=?', (ctime, inode))

        self.db.commit()
        return await self.getattr(inode)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        return await self._create(inode_p, name, mode, ctx, rdev=rdev)

    async def mkdir(self, inode_p, name, mode, ctx):
        return await self._create(inode_p, name, mode, ctx)

    async def statfs(self, ctx):
        stat_ = pyfuse3.StatvfsData()
        stat_.f_bsize = 512
        stat_.f_frsize = 512

        used_size = self._query_one('SELECT SUM(size) FROM inodes')[0] or 0
        used_blocks = used_size // stat_.f_frsize

        stat_.f_blocks = used_blocks + _FREE_BLOCKS
        stat_.f_bfree = _FREE_BLOCKS
        stat_.f_bavail = _FREE_BLOCKS

        inodes = self._query_one('SELECT COUNT(id) FROM inodes')[0]
        stat_.f_files = inodes
        stat_.f_ffree = max(inodes, 100)
        stat_.f_favail = stat_.f_ffree
        return stat_

    async def open(self, inode, flags, ctx):
        self.inode_open_count[inode] += 1
        name = self._inode_name(inode)
        STATS.open_handle(inode, name)
        return pyfuse3.FileInfo(fh=inode)

    async def access(self, inode, mode, ctx):
        return True

    async def create(self, inode_parent, name, mode, flags, ctx):
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        decoded = name.decode(errors='replace') if isinstance(name, bytes) else name
        STATS.open_handle(entry.st_ino, decoded)
        return pyfuse3.FileInfo(fh=entry.st_ino), entry

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning("Attempted to create entry %s with unlinked parent %d", name, inode_p)
            raise FUSEError(errno.EINVAL)

        now_ns = int(time() * 1e9)
        self.cursor.execute(
            'INSERT INTO inodes (uid, gid, mode, mtime_ns, atime_ns, ctime_ns, target, rdev) '
            'VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
            (ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev),
        )
        inode = self.cursor.lastrowid
        self.db.execute(
            "INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
            (name, inode, inode_p),
        )
        self.db.commit()

        decoded = name.decode(errors='replace') if isinstance(name, bytes) else name
        STATS.record_create(decoded)
        return await self.getattr(inode)

    # ── Telegram data helpers ──────────────────────────────────────────

    async def _get_telegram_data(self, fh: int) -> bytearray:
        """Fetch file contents from cache or Telegram. Never returns None."""
        cached = self.client.get_cached_file(fh)
        if cached is not None:
            return cached
        try:
            rows = self._query_all('SELECT id FROM telegram_messages WHERE inode=?', (fh,))
            ids = [r['id'] for r in rows]
            return self.client.download_file(fh, ids)
        except NoSuchRowError:
            return bytearray(b'')
        except Exception as exc:
            log.error("Failed to fetch Telegram data for fh=%d: %s", fh, exc)
            raise pyfuse3.FUSEError(errno.EIO)

    # ── Read / write ───────────────────────────────────────────────────

    async def read(self, fh, offset, length):
        try:
            self._query_one('SELECT id FROM inodes WHERE id=?', (fh,))
        except NoSuchRowError:
            return b''
        data = await self._get_telegram_data(fh)
        result = bytes(data[offset:offset + length])
        STATS.record_read(len(result))
        return result

    async def write(self, fh, offset, buf):
        # Initialise the per-fh write buffer on first write.
        if fh not in self._write_buffers:
            existing = await self._get_telegram_data(fh)
            self._write_buffers[fh] = bytearray(existing)

        buf_data = self._write_buffers[fh]
        end = offset + len(buf)
        if end > len(buf_data):
            buf_data += b'\0' * (end - len(buf_data))

        buf_data[offset:end] = buf
        self._dirty.add(fh)

        STATS.record_write(len(buf))
        STATS.update_handle(fh, dirty=True, buffer_bytes=len(self._write_buffers[fh]))
        return len(buf)

    async def close(self, fh):
        pass

    async def fsync(self, fh, datasync):
        """Flush dirty data to Telegram immediately."""
        if fh in self._dirty:
            STATS.log("INFO", "FSYNC", f"fh={fh}")
            await self._flush_fh(fh)

    async def _flush_fh(self, fh: int) -> None:
        """Upload buffered data for *fh* to Telegram and update the DB."""
        if fh not in self._write_buffers:
            return

        data = self._write_buffers.pop(fh)
        self._dirty.discard(fh)
        gc.collect()

        fname = self._inode_name(fh)

        telegram_msgs = self.client.upload_file(BytesIO(data), fh, fname)

        self._delete_telegram_msgs(fh)
        self.cursor.execute("DELETE FROM telegram_messages WHERE inode=?", (fh,))
        for msg in telegram_msgs:
            self.cursor.execute(
                "INSERT INTO telegram_messages (id, inode) VALUES (?, ?)", (msg.id, fh)
            )

        self.cursor.execute('UPDATE inodes SET size=? WHERE id=?', (len(data), fh))
        self.db.commit()
        STATS.update_handle(fh, dirty=False, buffer_bytes=0)

    async def release(self, fh):
        self.inode_open_count[fh] -= 1

        if fh in self._dirty:
            await self._flush_fh(fh)
        else:
            self._write_buffers.pop(fh, None)

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            STATS.close_handle(fh)
            try:
                if (await self.getattr(fh)).st_nlink == 0:
                    self.cursor.execute("DELETE FROM inodes WHERE id=?", (fh,))
                    self.db.commit()
            except pyfuse3.FUSEError:
                pass


# ── Exception types ────────────────────────────────────────────────────────

class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query returned more than one row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query returned zero rows'


# ── Logging / CLI / entry point ────────────────────────────────────────────

def init_logging(debug=False):
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d %(threadName)s: [%(name)s] %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    level = logging.DEBUG if debug else logging.INFO
    handler.setLevel(level)
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('mountpoint', type=str, default="./telegramfs",
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('--no-monitor', action='store_true', default=False,
                        help='Disable the TUI monitor')
    return parser.parse_args()


def runFs(client):
    options = parse_args()
    init_logging(options.debug)

    STATS.mountpoint = options.mountpoint
    STATS.log("INFO", "STARTUP", f"Mounting at {options.mountpoint}")

    operations = Operations(client)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=telegram_fuse')
    fuse_options.add('allow_other')
    fuse_options.discard('default_permissions')
    if options.debug_fuse:
        fuse_options.add('debug')

    pyfuse3.init(operations, options.mountpoint, fuse_options)

    if not options.no_monitor:
        from monitor import launch_monitor_thread
        launch_monitor_thread()

    def cleanup():
        print("RUNNING CLEANUP")
        STATS.log("WARNING", "SHUTDOWN", "Unmounting filesystem")
        operations.cursor.close()
        operations.db.close()
        pyfuse3.close(unmount=True)

    atexit.register(cleanup)
    trio.run(pyfuse3.main)
