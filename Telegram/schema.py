# ver 1.0
"""
schema.py — TelegramFS SQLite schema, migrations, and root-inode helpers.

Single source of truth for all DDL.  No pyfuse3 dependency — ROOT_INODE is
the FUSE-protocol constant 1, not derived from the library.

Public API
----------
ROOT_INODE          int   — inode number reserved for the filesystem root
init_schema(db)           — create tables + root inode (fresh database)
migrate_schema(db)        — idempotent: add missing tables / columns
ensure_schema(db)         — init_schema on a blank DB, migrate_schema otherwise
"""
from __future__ import annotations

import logging
import os
import sqlite3
import stat
from time import time

log = logging.getLogger(__name__)

# The FUSE protocol reserves inode 1 for the filesystem root.
# pyfuse3.ROOT_INODE == 1; defining it here avoids importing pyfuse3
# in modules that don't need the library for anything else.
ROOT_INODE: int = 1

# ── DDL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLES = """\
CREATE TABLE inodes (
    id       INTEGER PRIMARY KEY,
    uid      INT NOT NULL,
    gid      INT NOT NULL,
    mode     INT NOT NULL,
    mtime_ns INT NOT NULL,
    atime_ns INT NOT NULL,
    ctime_ns INT NOT NULL,
    target   BLOB(256),
    size     INT NOT NULL DEFAULT 0,
    rdev     INT NOT NULL DEFAULT 0
);
CREATE TABLE blocks (
    inode     INT NOT NULL REFERENCES inodes(id),
    block_idx INT NOT NULL,
    msg_id    INT NOT NULL,
    sha256    TEXT,
    PRIMARY KEY (inode, block_idx)
);
CREATE INDEX idx_blocks_inode ON blocks(inode);
CREATE TABLE file_meta (
    inode       INT NOT NULL PRIMARY KEY REFERENCES inodes(id),
    meta_msg_id INT NOT NULL
);
CREATE TABLE pending_blocks (
    msg_id    INT  PRIMARY KEY,
    inode     INT  NOT NULL,
    block_idx INT  NOT NULL,
    started_at REAL NOT NULL
);
CREATE TABLE contents (
    rowid        INTEGER PRIMARY KEY AUTOINCREMENT,
    name         BLOB(256) NOT NULL,
    inode        INT NOT NULL REFERENCES inodes(id),
    parent_inode INT NOT NULL REFERENCES inodes(id),
    UNIQUE (name, parent_inode)
);
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _root_dir_mode() -> int:
    return (stat.S_IFDIR
            | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH)


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    )
    return cur.fetchone() is not None


def _column_exists(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


# ── Public API ────────────────────────────────────────────────────────────────

def init_schema(db: sqlite3.Connection) -> None:
    """
    Create all tables and insert the root inode.
    Call this only on a blank database (no existing tables).
    """
    db.executescript(_CREATE_TABLES)

    now_ns = int(time() * 1e9)
    db.execute(
        "INSERT INTO inodes (id, mode, uid, gid, mtime_ns, atime_ns, ctime_ns)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ROOT_INODE, _root_dir_mode(), os.getuid(), os.getgid(),
         now_ns, now_ns, now_ns),
    )
    db.execute(
        "INSERT INTO contents (name, parent_inode, inode) VALUES (?, ?, ?)",
        (b"..", ROOT_INODE, ROOT_INODE),
    )
    db.commit()
    log.info("schema: initialised fresh TelegramFS database")


def migrate_schema(db: sqlite3.Connection) -> None:
    """
    Idempotent migration: add any tables or columns introduced after the
    initial release.  Safe to call on an already up-to-date database.
    """
    cur     = db.cursor()
    changed = False

    if not _table_exists(cur, "file_meta"):
        cur.execute("""\
            CREATE TABLE file_meta (
                inode       INT NOT NULL PRIMARY KEY REFERENCES inodes(id),
                meta_msg_id INT NOT NULL
            )""")
        log.info("schema: migration — created file_meta table")
        changed = True

    if not _table_exists(cur, "pending_blocks"):
        cur.execute("""\
            CREATE TABLE pending_blocks (
                msg_id     INT  PRIMARY KEY,
                inode      INT  NOT NULL,
                block_idx  INT  NOT NULL,
                started_at REAL NOT NULL
            )""")
        log.info("schema: migration — created pending_blocks table")
        changed = True

    if not _column_exists(cur, "blocks", "sha256"):
        cur.execute("ALTER TABLE blocks ADD COLUMN sha256 TEXT")
        log.info("schema: migration — added sha256 column to blocks")
        changed = True

    if changed:
        db.commit()


def ensure_schema(db: sqlite3.Connection) -> None:
    """
    Initialise or migrate the schema as needed.

    - Blank database  → init_schema (create tables + root inode)
    - Existing schema → migrate_schema (add any missing tables / columns)
    """
    cur = db.cursor()
    if _table_exists(cur, "inodes"):
        migrate_schema(db)
    else:
        init_schema(db)
