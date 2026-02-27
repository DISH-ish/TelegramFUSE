# ver 1.1
from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import stat
import sys
from time import time

from schema import ROOT_INODE, init_schema

from block_store import MANIFEST_MAGIC

log     = logging.getLogger(__name__)
DB_PATH = "telegram.db"



def _scan_manifests(tg_client, block_store) -> dict[str, dict]:
    """Return {path: newest_manifest} by scanning all channel messages."""
    best: dict[str, dict] = {}

    def _progress(sc, dl):
        print(f"\r  Scanned {sc:,} messages, {dl:,} with media …", end="", flush=True)

    for msg_id, raw in tg_client.iter_all_messages_raw(progress_cb=_progress):
        if not raw.startswith(MANIFEST_MAGIC):
            continue
        manifest = block_store._decode_manifest(raw)
        if manifest is None:
            continue
        manifest["meta_msg_id"] = msg_id
        path = manifest.get("path") or manifest.get("filename", "")
        if not path:
            log.warning("Manifest msg=%d has no path — skipping", msg_id)
            continue
        existing = best.get(path)
        if existing is None or msg_id > existing["meta_msg_id"]:
            best[path] = manifest

    print()
    log.info("Scan: %d unique paths found", len(best))
    return best


def _make_dir(db, cur, name, parent_inode, uid, gid, now_ns, dir_cache) -> int:
    key = (name, parent_inode)
    if key in dir_cache:
        return dir_cache[key]
    cur.execute(
        "SELECT inode FROM contents WHERE name=? AND parent_inode=?",
        (name.encode(), parent_inode),
    )
    row = cur.fetchone()
    if row:
        dir_cache[key] = row[0]
        return row[0]

    mode = (stat.S_IFDIR
            | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
            | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH)
    cur.execute(
        "INSERT INTO inodes (uid,gid,mode,mtime_ns,atime_ns,ctime_ns) VALUES (?,?,?,?,?,?)",
        (uid, gid, mode, now_ns, now_ns, now_ns),
    )
    inode = cur.lastrowid
    cur.execute(
        "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
        (name.encode(), inode, parent_inode),
    )
    dir_cache[key] = inode
    return inode


def _write_db(db: sqlite3.Connection, manifests: dict[str, dict]) -> int:
    now_ns    = int(time() * 1e9)
    file_mode = stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    uid, gid  = os.getuid(), os.getgid()
    cur       = db.cursor()
    dir_cache: dict[tuple[str, int], int] = {}
    written   = 0

    for path, manifest in manifests.items():
        parts        = path.split("/")
        leaf, dirs   = parts[-1], parts[:-1]
        parent_inode = ROOT_INODE

        for dir_name in dirs:
            if dir_name:
                parent_inode = _make_dir(db, cur, dir_name, parent_inode, uid, gid, now_ns, dir_cache)

        cur.execute(
            "INSERT INTO inodes (uid,gid,mode,mtime_ns,atime_ns,ctime_ns,size) VALUES (?,?,?,?,?,?,?)",
            (uid, gid, file_mode, manifest.get("mtime_ns", now_ns), now_ns, now_ns, manifest.get("size", 0)),
        )
        inode      = cur.lastrowid
        name_bytes = leaf.encode(errors="replace")

        try:
            cur.execute(
                "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
                (name_bytes, inode, parent_inode),
            )
        except sqlite3.IntegrityError:
            alt = f"{leaf}.recovered_{inode}".encode(errors="replace")
            log.warning("Filename collision for %r — inserting as %r", leaf, alt.decode())
            cur.execute(
                "INSERT INTO contents (name, inode, parent_inode) VALUES (?,?,?)",
                (alt, inode, parent_inode),
            )

        hashes = manifest.get("hashes", {})  # {block_idx: sha256_hex} — present in v3+ manifests
        for block_idx, msg_id in manifest.get("blocks", {}).items():
            sha256 = hashes.get(int(block_idx)) or hashes.get(str(block_idx))
            cur.execute(
                "INSERT INTO blocks (inode, block_idx, msg_id, sha256) VALUES (?,?,?,?)",
                (inode, int(block_idx), msg_id, sha256),
            )

        cur.execute(
            "INSERT INTO file_meta (inode, meta_msg_id) VALUES (?,?)",
            (inode, manifest["meta_msg_id"]),
        )
        log.info("  Recovered %-50s  size=%-10d  blocks=%d",
                 path, manifest.get("size", 0), len(manifest.get("blocks", {})))
        written += 1

    db.commit()
    return written


def run_repair(tg_client, block_store, db_path: str = DB_PATH) -> int:
    print("\n━━━  TelegramFS Database Repair  ━━━\n")

    backup_path = db_path + ".bak"
    had_backup  = os.path.exists(db_path)
    if had_backup:
        shutil.copy2(db_path, backup_path)
        print(f"  ⚑  Existing database backed up → {backup_path}")
    else:
        print("  ℹ  No existing database found — will create a fresh one.")

    if not block_store._cipher:
        print(
            "\n  WARNING: Encryption is OFF.\n"
            "           If you previously ran with encryption enabled, set\n"
            "           ENCRYPTION_KEY in .env and re-run.\n"
        )

    print("\n[1/3] Scanning Telegram channel for manifest blocks …")
    manifests = _scan_manifests(tg_client, block_store)

    if not manifests:
        print(
            "\n  ✗  No manifests found.\n"
            "     Possible causes: wrong ENCRYPTION_KEY, empty channel, or purged messages.\n"
        )
        if had_backup:
            print(f"  Your original database is intact at: {backup_path}")
        return 0

    print(f"  ✓  {len(manifests)} file(s) recovered.")
    print(f"\n[2/3] Reconstructing directory tree and writing {db_path} …")

    if os.path.exists(db_path):
        os.remove(db_path)

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        init_schema(db)
        written = _write_db(db, manifests)
    except Exception as exc:
        db.close()
        if had_backup:
            shutil.copy2(backup_path, db_path)
            print(f"\n  ✗  Repair failed ({exc}). Original database restored.")
        else:
            print(f"\n  ✗  Repair failed: {exc}")
        raise
    finally:
        db.close()

    print(f"\n[3/3] Done.\n")
    print(f"  ✓  {written} file(s) recovered with full directory structure.")
    if had_backup:
        print(f"  ℹ  Original database preserved at: {backup_path}")
    print("\n  NOTE: Deleted files whose Telegram messages still exist may reappear.\n")
    return written


def run_check(tg_client, block_store, db_path: str = DB_PATH) -> bool:
    print("\n━━━  TelegramFS Integrity Check  ━━━\n")

    if not os.path.exists(db_path):
        print(f"  ✗  Database not found: {db_path}")
        print("     Run --repair to rebuild it from Telegram.")
        return False

    db  = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    id_to_owners: dict[int, list[str]] = {}

    cur.execute("SELECT inode, block_idx, msg_id FROM blocks")
    for row in cur.fetchall():
        id_to_owners.setdefault(row["msg_id"], []).append(
            f"inode={row['inode']} block={row['block_idx']}"
        )

    cur.execute("""
        SELECT fm.inode, fm.meta_msg_id, c.name
        FROM file_meta fm
        LEFT JOIN contents c ON c.inode = fm.inode AND c.name != '..'
    """)
    for row in cur.fetchall():
        fname = row["name"]
        if isinstance(fname, bytes):
            fname = fname.decode(errors="replace")
        id_to_owners.setdefault(row["meta_msg_id"], []).append(
            f"manifest for inode={row['inode']} ({fname})"
        )

    db.close()

    if not id_to_owners:
        print("  ℹ  Database contains no files — nothing to check.")
        return True

    all_ids = list(id_to_owners.keys())
    print(f"  Checking {len(all_ids):,} Telegram message(s) …\n")
    missing = tg_client.check_messages_exist(all_ids)
    print(f"  ✓  {len(all_ids) - len(missing):,} message(s) present.")

    if missing:
        print(f"  ✗  {len(missing):,} message(s) MISSING:\n")
        for msg_id in missing:
            for desc in id_to_owners.get(msg_id, []):
                print(f"       msg_id={msg_id}  →  {desc}")
        print(
            "\n  Reading affected files will fail.\n"
            "  Options: --repair to rebuild DB, or re-upload affected files.\n"
        )
        return False

    print("\n  ✓  All messages verified — filesystem is healthy.")
    return True


def run_sweep(tg_client, block_store, db_path: str = DB_PATH) -> int:
    print("\n━━━  TelegramFS Orphan Sweep  ━━━\n")

    live_ids: set[int] = set()
    if os.path.exists(db_path):
        db  = sqlite3.connect(db_path)
        cur = db.cursor()
        cur.execute("SELECT msg_id FROM blocks")
        live_ids.update(r[0] for r in cur.fetchall())
        cur.execute("SELECT meta_msg_id FROM file_meta")
        live_ids.update(r[0] for r in cur.fetchall())
        db.close()
        print(f"  Live database references {len(live_ids):,} Telegram message(s).")
    else:
        print("  WARNING: No local database found. Run --repair first.\n")
        return 0

    print("\n  Scanning channel for TelegramFS messages …")
    print("  (data blocks are decrypted to verify they belong to TelegramFS)\n")
    all_tgfs_ids: set[int]   = set()
    naked_block_ids: set[int] = set()  # confirmed data blocks with no manifest
    manifests_seen: list[dict] = []
    skipped_foreign = 0

    def _progress(sc, dl):
        print(f"\r  Scanned {sc:,} messages, {dl:,} with media …", end="", flush=True)

    for msg_id, raw in tg_client.iter_all_messages_raw(progress_cb=_progress):
        if raw.startswith(MANIFEST_MAGIC):
            # It's a manifest — decode and collect referenced block IDs.
            manifest = block_store._decode_manifest(raw)
            if manifest is None:
                continue
            all_tgfs_ids.add(msg_id)
            manifest["meta_msg_id"] = msg_id
            manifests_seen.append(manifest)
            all_tgfs_ids.update(manifest.get("blocks", {}).values())
        else:
            # Might be a data block (naked — no manifest, e.g. from a crashed upload).
            # Try to decrypt: AES-GCM auth tag will reject foreign/corrupt data.
            # Without encryption we accept anything <= BLOCK_SIZE as a data block.
            try:
                from block_store import BLOCK_SIZE
                decrypted = block_store._decrypt(raw)
                if len(decrypted) <= BLOCK_SIZE:
                    all_tgfs_ids.add(msg_id)
                    naked_block_ids.add(msg_id)
                else:
                    skipped_foreign += 1
            except Exception:
                # Decryption failed — not a TelegramFS block.
                skipped_foreign += 1

    print()
    if skipped_foreign:
        print(f"  Skipped {skipped_foreign:,} non-TelegramFS message(s) (foreign media).")

    orphan_ids = sorted(all_tgfs_ids - live_ids)

    print(f"\n  TelegramFS messages on channel : {len(all_tgfs_ids):,}")
    print(f"    of which naked data blocks   : {len(naked_block_ids):,}")
    print(f"  Referenced by live database   : {len(live_ids):,}")
    print(f"  Orphaned                      : {len(orphan_ids):,}")

    if not orphan_ids:
        print("\n  ✓  Channel is clean — no orphans found.")
        return 0

    # Classify orphans: manifests, their referenced blocks, naked data blocks.
    data_block_owners: dict[int, str] = {}
    for m in manifests_seen:
        path = m.get("path") or m.get("filename", "?")
        for block_msg_id in m.get("blocks", {}).values():
            if block_msg_id in orphan_ids:
                data_block_owners[block_msg_id] = path

    manifest_orphans     = [mid for mid in orphan_ids if mid not in data_block_owners and mid not in naked_block_ids]
    linked_block_orphans = [mid for mid in orphan_ids if mid in data_block_owners]
    naked_block_orphans  = [mid for mid in orphan_ids if mid in naked_block_ids]
    print(f"\n  Orphan breakdown:")
    print(f"    Orphaned manifests            : {len(manifest_orphans):,}")
    print(f"    Orphaned blocks (has manifest) : {len(linked_block_orphans):,}")
    print(f"    Naked blocks (crashed upload)  : {len(naked_block_orphans):,}")

    if len(orphan_ids) <= 30:
        print("\n  Orphan message IDs:")
        for mid in orphan_ids:
            if mid in data_block_owners:
                print(f"    msg={mid}  data block for: {data_block_owners[mid]!r}")
            elif mid in naked_block_ids:
                print(f"    msg={mid}  naked data block (crashed upload)")
            else:
                print(f"    msg={mid}  orphaned manifest")

    print()
    try:
        answer = input(f"  Delete {len(orphan_ids):,} orphaned message(s)? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n  Aborted.")
        return 0

    if answer not in ("y", "yes"):
        print("  Skipped — nothing deleted.")
        return 0

    print(f"  Deleting {len(orphan_ids):,} messages …", end="", flush=True)
    try:
        block_store.delete_messages(orphan_ids)
        print(" done.")
        print(f"\n  ✓  {len(orphan_ids):,} orphaned message(s) removed.")
    except Exception as exc:
        print(f"\n  ✗  Deletion failed: {exc}")
        return 0

    return len(orphan_ids)
