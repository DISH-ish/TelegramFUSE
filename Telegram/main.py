import os
import sqlite3
import sys
from dotenv import load_dotenv

from crypto import BlockCipher
from block_store import BlockStore
from TelegramFUSE import TelegramFileClient
from fuse_impl import runFs
from stats import STATS


_REQUIRED_VARS = {
    "APP_ID":       "Telegram API ID      — get from https://my.telegram.org/myapp",
    "APP_HASH":     "Telegram API hash    — get from https://my.telegram.org/myapp",
    "CHANNEL_LINK": "Telegram channel URL — e.g. https://t.me/yourchannel",
    "SESSION_NAME": "Session file name    — any string, e.g. 'myfs'",
}


def _validate_env() -> bool:
    missing = [(v, d) for v, d in _REQUIRED_VARS.items() if not os.getenv(v, "").strip()]
    if not missing:
        return True
    print("\n  ✗  Missing required environment variable(s):\n")
    for var, desc in missing:
        print(f"       {var:<15}  {desc}")
    print("\n  Copy .env.sample to .env and fill in the missing values.\n")
    return False


def _parse_top_level() -> tuple[bool, bool, bool, bool, str | None, str]:
    argv        = sys.argv
    do_repair   = "--repair" in argv or "--repair-only" in argv
    repair_only = "--repair-only" in argv
    do_check    = "--check" in argv
    do_sweep    = "--sweep" in argv

    backup_src  = None
    backup_dest = "/"
    if "--backup" in argv:
        idx = argv.index("--backup")
        if idx + 1 < len(argv):
            backup_src = argv[idx + 1]
        else:
            print("  ✗  --backup requires a source path argument.")
            sys.exit(1)
    if "--backup-dest" in argv:
        idx = argv.index("--backup-dest")
        if idx + 1 < len(argv):
            backup_dest = argv[idx + 1]
        else:
            print("  ✗  --backup-dest requires a destination path argument.")
            sys.exit(1)

    return do_repair, repair_only, do_check, do_sweep, backup_src, backup_dest


def _setup_cipher(enc_key_hex: str) -> BlockCipher | None:
    if enc_key_hex:
        return BlockCipher.from_hex(enc_key_hex)
    print("ENCRYPTION_KEY is not set.")
    if input("Generate a new AES-256 key and save to .env? (y/n): ").strip().lower() in ("y", "yes"):
        enc_key_hex = BlockCipher.generate_key_hex()
        _write_env_key(enc_key_hex)
        os.environ["ENCRYPTION_KEY"] = enc_key_hex
        print(f"✓ Key generated and saved.\n  Key (hex): {enc_key_hex}")
        print("  Keep this key — you need it to access your files from another machine.")
        return BlockCipher.from_hex(enc_key_hex)
    print("Running without encryption.")
    return None


def _write_env_key(hex_key: str) -> None:
    try:
        with open(".env") as f:
            lines = f.readlines()
        with open(".env", "w") as f:
            replaced = False
            for line in lines:
                if line.strip().startswith("ENCRYPTION_KEY="):
                    f.write(f"ENCRYPTION_KEY={hex_key}\n")
                    replaced = True
                else:
                    f.write(line)
            if not replaced:
                f.write(f"ENCRYPTION_KEY={hex_key}\n")
    except FileNotFoundError:
        with open(".env", "w") as f:
            f.write(f"ENCRYPTION_KEY={hex_key}\n")


def _db_is_corrupt(db_path: str) -> bool:
    if not os.path.exists(db_path):
        return False
    try:
        con    = sqlite3.connect(db_path)
        result = con.execute("PRAGMA integrity_check").fetchone()
        con.close()
        return result is None or result[0] != "ok"
    except sqlite3.DatabaseError:
        return True


def init() -> None:
    load_dotenv()
    do_repair, repair_only, do_check, do_sweep, backup_src, backup_dest = _parse_top_level()

    if not _validate_env():
        sys.exit(1)

    enc_key_hex  = os.getenv("ENCRYPTION_KEY", "").strip()
    cipher       = _setup_cipher(enc_key_hex)
    tg_client    = TelegramFileClient(
        os.getenv("SESSION_NAME"), os.getenv("APP_ID"),
        os.getenv("APP_HASH"), os.getenv("CHANNEL_LINK"),
    )
    block_store  = BlockStore(tg_client, cipher)

    print(f"Encryption : {'AES-256-GCM' if cipher else 'OFF'}")
    print(f"Block size : 4 MiB")

    if not do_repair and _db_is_corrupt("telegram.db"):
        print("\n  ⚠  telegram.db appears corrupted. Run --repair to reconstruct it.\n")

    if do_check:
        from repair import run_check
        sys.exit(0 if run_check(tg_client, block_store) else 1)

    if do_sweep:
        from repair import run_sweep
        run_sweep(tg_client, block_store)
        sys.exit(0)

    if do_repair:
        from repair import run_repair
        if run_repair(tg_client, block_store) == 0:
            print("  Repair found nothing to recover. Exiting.")
            sys.exit(1)
        if repair_only:
            print("  --repair-only: not mounting. Exiting.")
            sys.exit(0)
        print("  Proceeding to mount …\n")

    if backup_src is not None:
        import threading
        import trio
        from backup import run_backup
        if not os.path.isdir(backup_src):
            print(f"  ✗  Backup source does not exist or is not a directory: {backup_src!r}")
            sys.exit(1)
        print(f"\n  Backing up {backup_src!r} → TelegramFS:{backup_dest!r}")
        print("  (DB must not be in use by a mounted filesystem)\n")
        db = sqlite3.connect("telegram.db", check_same_thread=False)
        db.text_factory = str
        db.row_factory  = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")

        no_monitor = "--no-monitor" in sys.argv

        async def _run_backup_with_deleter():
            async with trio.open_nursery() as nursery:
                nursery.start_soon(block_store.deleter.run_background)
                # done_callback=None: BackupPanel polls STATS.backup_finished
                # and calls app.exit() from within the Textual event loop.
                await run_backup(backup_src, backup_dest, block_store, db)
                nursery.cancel_scope.cancel()

        if no_monitor:
            trio.run(_run_backup_with_deleter)
            db.close()
        else:
            def backup_shutdown(app):  # noqa: ARG001
                """Q pressed: stop after current file, do not exit immediately.
                BackupPanel polls STATS.backup_finished and exits the app once done."""
                STATS.backup_stop_requested = True
                STATS.log("WARNING", "BACKUP_STOP_REQ",
                          "finishing current file then stopping …")

            backup_thread = threading.Thread(
                target=lambda: trio.run(_run_backup_with_deleter),
                name="backup-worker",
                daemon=False,  # never kill mid-upload
            )
            backup_thread.start()
            from monitor import launch_monitor_blocking
            # Q → backup_shutdown sets stop flag; BackupPanel exits app when done.
            # Natural finish → BackupPanel polls backup_finished, exits app.
            launch_monitor_blocking(shutdown_callback=backup_shutdown)
            backup_thread.join()
            db.close()
        return

    runFs(block_store)


if __name__ == "__main__":
    init()
