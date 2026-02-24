#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from datetime import datetime
from time import time

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Label, RichLog, Static

from stats import STATS


def _fmt_bytes(n: int | float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _fmt_uptime(secs: float) -> str:
    h, rem = divmod(int(secs), 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _pct_bar(ratio: float, width: int = 20) -> str:
    filled = int(ratio * width)
    return f"{'█' * filled}{'░' * (width - filled)} {ratio * 100:.1f}%"


def _level_color(level: str) -> str:
    return {"SUCCESS": "green", "INFO": "cyan", "WARNING": "yellow", "ERROR": "red"}.get(level, "white")


def _health_color(ratio: float) -> str:
    if ratio >= 0.99:
        return "green"
    if ratio >= 0.95:
        return "yellow"
    return "red"


def _read_db_stats() -> dict:
    try:
        db  = sqlite3.connect("file:telegram.db?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM inodes")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM inodes WHERE mode & 61440=16384")
        dirs = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(SUM(size),0) FROM inodes")
        total_bytes = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM blocks")
        tg_blocks = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pending_blocks")
        pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM file_meta")
        manifests = cur.fetchone()[0]
        # Count how many blocks have a stored SHA-256 hash (v3+ manifests/flushes).
        try:
            cur.execute("SELECT COUNT(*) FROM blocks WHERE sha256 IS NOT NULL")
            hashed_blocks = cur.fetchone()[0]
        except Exception:
            hashed_blocks = None
        files = total - dirs
        db.close()
        return {
            "inodes": total, "files": files, "dirs": dirs,
            "total_bytes": total_bytes, "tg_blocks": tg_blocks,
            "avg_blocks": tg_blocks / files if files else 0.0,
            "pending": pending, "manifests": manifests,
            "hashed_blocks": hashed_blocks,
            "error": None,
        }
    except Exception as exc:
        return {
            "error": str(exc), "inodes": 0, "files": 0, "dirs": 0,
            "total_bytes": 0, "tg_blocks": 0, "avg_blocks": 0.0,
            "pending": 0, "manifests": 0, "hashed_blocks": None,
        }

# Panels
class CachePanel(Static):
    DEFAULT_CSS = "CachePanel { border: round $success; padding: 0 1; width: 1fr; height: 100%; }"

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def on_resize(self) -> None:
        self._refresh()

    def _refresh(self) -> None:
        s   = STATS.snapshot()["cache"]
        hr  = s["hit_rate"]
        cur = s["current_bytes"]
        mx  = s["max_bytes"]
        col = "green" if hr >= 0.8 else ("yellow" if hr >= 0.5 else "red")
        bw  = max(4, self.size.width - 7)
        self.update("\n".join([
            f"[bold cyan]CACHE[/]",
            f"[{col}]{_pct_bar(hr, bw)}[/]",
            f"Hit [green]{s['hits']:,}[/]  Miss [red]{s['misses']:,}[/]  Evict [yellow]{s['evictions']:,}[/]",
            f"{_pct_bar(cur / mx if mx else 0, bw)}",
            f"[cyan]{_fmt_bytes(cur)}[/] / {_fmt_bytes(mx)}",
        ]))



class VerifyPanel(Static):
    DEFAULT_CSS = "VerifyPanel { border: round $error; padding: 0 1; width: 1fr; height: 100%; }"

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def on_resize(self) -> None:
        self._refresh()

    def _refresh(self) -> None:
        v   = STATS.snapshot()["verify"]
        pr  = v["pass_rate"]
        col = _health_color(pr)
        av  = v["active"]
        hf  = v["hard_failures"]
        cf  = v["content_fails"]
        ms  = v["missing"]
        dl  = v.get("dl_hash_fails", 0)
        bw  = max(4, self.size.width - 9)
        self.update("\n".join([
            f"[bold cyan]VERIFY[/]  [dim]active={av}[/]",
            f"[{col}]{_pct_bar(pr, bw)}[/]",
            f"[dim]UL:[/] Chk [cyan]{v['total_checks']:,}[/]  Ok [green]{v['passes']:,}[/]  Re-up [yellow]{v['reuploads']:,}[/]",
            f"Miss [{'red' if ms else 'dim'}]{ms:,}[/]  Corrupt [{'red bold' if cf else 'dim'}]{cf:,}[/]  Hard-Fail [{'red bold' if hf else 'dim'}]{hf:,}[/]",
            f"[dim]DL:[/] Hash-Fail [{'red bold' if dl else 'dim'}]{dl:,}[/]",
        ]))

class OpsPanel(Static):
    DEFAULT_CSS = "OpsPanel { border: round $primary; padding: 0 1; width: 0.7fr; height: 100%; }"

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        t   = STATS.snapshot()["transfers"]
        au  = t["active_uploads"]
        ad  = t["active_downloads"]
        err_col = "red" if (t["uploads_failed"] + t["downloads_failed"]) > 0 else "green"
        o = STATS.snapshot()["ops"]
        self.update("\n".join([
            "[bold cyan]FUSE OPS[/]",
            f"Read [green]{o['read']:,}[/] ({_fmt_bytes(o['bytes_read'])}) Write [yellow]{o['write']:,}[/] ({_fmt_bytes(o['bytes_written'])})",
            f"Lookup [cyan]{o['lookup']:,}[/] Create [green]{o['create']:,}[/] Del [red]{o['delete']:,}[/] Ren [magenta]{o['rename']:,}[/]",
            f"",
            f"[bold cyan]TRANSFERS[/] [dim]↑{au} ↓{ad}[/]",
            f"[dim]↑[/][green]{t['uploads_total']:,}[/] blocks [cyan]{_fmt_bytes(t['uploads_bytes'])}[/] fail [{err_col}]{t['uploads_failed']}[/]",
            f"[dim]↓[/][green]{t['downloads_total']:,}[/] blocks [cyan]{_fmt_bytes(t['downloads_bytes'])}[/] fail [{err_col}]{t['downloads_failed']}[/]",
        ]))


class DbPanel(Static):
    DEFAULT_CSS = "DbPanel { border: round $surface; padding: 0 1; height: 4; }"

    def on_mount(self) -> None:
        self.set_interval(3.0, self._refresh)

    def _refresh(self) -> None:
        d   = _read_db_stats()
        snap = STATS.snapshot()
        enc = snap["encryption"]
        mp  = snap["mountpoint"] or "[dim]unknown[/]"
        vhf = snap["verify"]["hard_failures"]

        if d["error"]:
            self.update(f"[bold cyan]  DATABASE[/]   [red]Error: {d['error']}[/]")
            return

        pending_txt = (
            f"  [bold red]Pending:{d['pending']}[/]" if d["pending"] > 0 else ""
        )
        health_txt = (
            f"  [bold red]{vhf}hard verify failure(s)[/]" if vhf > 0 else
            "  [green]healthy[/]"
        )

        # Hash coverage: how many blocks have a stored SHA-256 (v3+ only).
        hb = d.get("hashed_blocks")
        tb = d["tg_blocks"]
        if hb is None:
            hash_cov_txt = "[dim]n/a (old schema)[/]"
        elif tb == 0:
            hash_cov_txt = "[dim]—[/]"
        else:
            pct = hb / tb * 100
            col = "green" if pct >= 100 else ("yellow" if pct >= 50 else "red")
            hash_cov_txt = f"[{col}]{hb:,}/{tb:,} ({pct:.0f}%)[/]"

        self.update(
            f"[bold cyan]  DATABASE[/]   "
            f"Inodes [cyan]{d['inodes']:,}[/]  Files [green]{d['files']:,}[/]  "
            f"Dirs [blue]{d['dirs']:,}[/]  TG blocks [yellow]{d['tg_blocks']:,}[/]  "
            f"(avg [dim]{d['avg_blocks']:.1f}[/] blk/file)  "
            f"Manifests [cyan]{d['manifests']:,}[/]  "
            f"Stored [cyan]{_fmt_bytes(d['total_bytes'])}[/]"
            f"{pending_txt}\n"
            f"  Encryption {'[green]AES-256-GCM[/]' if enc else '[red]off[/]'}  │  "
            f"Content verify {'[green]on[/]' if _content_verify_on() else '[yellow]off[/]'}  │  "
            f"Hash coverage {hash_cov_txt}  │  "
            f"Mount [bold]{mp}[/]{health_txt}"
        )


def _content_verify_on() -> bool:
    import os
    return os.getenv("VERIFY_CONTENT", "1").strip() not in ("0", "false", "no")


class HandlesPanel(Static):
    DEFAULT_CSS = "HandlesPanel { border: round $warning; padding: 0 1; height: 8; }"

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        handles = STATS.snapshot()["handles"]
        if not handles:
            self.update("[bold cyan]  OPEN HANDLES[/]   [dim]none[/]")
            return
        lines = ["[bold cyan]  OPEN HANDLES[/]"]
        for h in handles[:6]:
            buf = f"  buf=[yellow]{_fmt_bytes(h.buffer_bytes)}[/]" if h.buffer_bytes else ""
            lines.append(
                f"  {'[red]●[/]' if h.dirty else '[dim]○[/]'} fh=[cyan]{h.fh:<4}[/] "
                f"[bold]{h.name or '(unnamed)'}[/]{buf}  [dim]{time() - h.opened_at:.0f}s[/]"
            )
        if len(handles) > 6:
            lines.append(f"  [dim]… and {len(handles) - 6} more[/]")
        self.update("\n".join(lines))


class BackupPanel(Static):
    DEFAULT_CSS = "BackupPanel { border: round $accent; padding: 0 1; height: 7; display: none; }"

    def on_mount(self) -> None:
        self.set_interval(0.5, self._refresh)

    def _refresh(self) -> None:
        b = STATS.snapshot()["backup"]
        if b["finished"]:
            # Backup completed (naturally or via stop) — close the TUI.
            self.app.exit()
            return
        if not b["active"]:
            self.display = False
            return
        self.display = True

        bt  = b["bytes_total"]
        bd  = b["bytes_done"]
        ft  = b["files_total"]
        fd  = b["files_done"]
        fs  = b["files_skipped"]
        fe  = b["files_errors"]
        bw  = max(4, self.size.width - 12)

        pct_bytes = bd / bt if bt else 0.0
        pct_files = (fd + fs) / ft if ft else 0.0
        cur  = b["current_file"]
        cblk = b["current_block"]
        cnbl = b["current_nblocks"]

        cur_display = ("…" + cur[-(bw - 2):]) if len(cur) > bw else cur

        blk_str = (f"  block {cblk + 1}/{cnbl}"
                   if cnbl else "")

        lines = [
            f"[bold cyan]  BACKUP[/]"
            f"  [dim]files {fd + fs}/{ft}[/]"
            f"  [yellow]err {fe}[/]" if fe else
            f"[bold cyan]  BACKUP[/]  [dim]files {fd + fs}/{ft}[/]",
            f"  files  [{_pct_bar(pct_files, bw)}] {pct_files * 100:.0f}%",
            f"  bytes  [{_pct_bar(pct_bytes, bw)}] "
            f"{_fmt_bytes(bd)} / {_fmt_bytes(bt)}",
            f"  [dim]{cur_display}[/][cyan]{blk_str}[/]" if cur_display else "  [dim]scanning…[/]",
        ]
        self.update("\n".join(lines))

    def on_resize(self) -> None:
        self._refresh()


class ActivityLog(RichLog):
    DEFAULT_CSS = "ActivityLog { border: round $surface; padding: 0 1; height: 1fr; }"
    _last_ts: float = 0.0

    def on_mount(self) -> None:
        self.set_interval(0.5, self._refresh)

    def _refresh(self) -> None:
        entries = STATS.snapshot()["log"]
        for e in entries:
            if e.timestamp <= self._last_ts:
                continue
            ts    = datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S")
            color = _level_color(e.level)
            self.write(
                f"[dim]{ts}[/] [{color}]{e.level:<7}[/] [bold]{e.operation}[/]"
                f"{f'  [dim]{e.detail}[/]' if e.detail else ''}"
            )
        if entries:
            self._last_ts = max(self._last_ts, entries[-1].timestamp)


_SPINNER = ["=---", "-=--", "--=-", "---=", "----", "---=", "--=-", "-=--", "=---", "----"]


class StatusBar(Static):
    DEFAULT_CSS = "StatusBar { dock: top; background: $panel; color: $text; padding: 0 2; height: 1; }"
    _tick: int = 0

    def on_mount(self) -> None:
        self.set_interval(0.2, self._refresh)

    def _refresh(self) -> None:
        snap = STATS.snapshot()
        au   = snap["transfers"]["active_uploads"]
        ad   = snap["transfers"]["active_downloads"]
        av   = snap["verify"]["active"]
        hf   = snap["verify"]["hard_failures"]
        dl   = snap["verify"].get("dl_hash_fails", 0)

        spinner = ""
        parts   = []
        if au:
            parts.append(f"↑{au}")
        if ad:
            parts.append(f"↓{ad}")
        if av:
            parts.append(f"✓{av}")
        if parts:
            self._tick = (self._tick + 1) % len(_SPINNER)
            spinner = f" [yellow]{_SPINNER[self._tick]} {' '.join(parts)}[/]"

        alerts = []
        if hf > 0:
            alerts.append(f"{hf} hard failure(s)")
        if dl > 0:
            alerts.append(f"{dl} DL hash fail(s)")
        health = f"  [bold red]{'  '.join(alerts)}[/]" if alerts else ""

        self.update(
            f" [bold cyan]/\ TelegramFS Monitor[/]"
            f"  uptime [green]{_fmt_uptime(snap['uptime'])}[/]{spinner}{health}"
        )


class TelegramFsMonitor(App):
    CSS = """
    Screen { background: $background; layout: vertical; }
    #top-row { layout: horizontal; height: 9; }
    #log-label { color: $text-muted; padding: 0 2; height: 1; }
    """
    BINDINGS = [
        Binding("q",       "quit",        "Quit"),
        Binding("Q",       "force_quit",  "Force quit"),
        Binding("c",       "clear_log",   "Clear log"),
    ]

    def __init__(self, shutdown_callback=None, **kwargs):
        super().__init__(**kwargs)
        self._shutdown_callback = shutdown_callback

    def compose(self) -> ComposeResult:
        yield StatusBar()
        with Horizontal(id="top-row"):
            yield CachePanel()
            yield VerifyPanel()
            yield OpsPanel()
        yield DbPanel()
        yield BackupPanel()
        yield HandlesPanel()
        yield Label(" ACTIVITY LOG", id="log-label")
        yield ActivityLog(highlight=True, markup=True, wrap=False)
        yield Footer()

    def action_quit(self) -> None:
        if self._shutdown_callback is not None:
            self._shutdown_callback(self)
        else:
            self.exit()

    def action_force_quit(self) -> None:
        import os, signal
        STATS.log("WARNING", "FORCE_QUIT", "killed by user")
        os.kill(os.getpid(), signal.SIGKILL)

    def action_clear_log(self) -> None:
        log_widget = self.query_one(ActivityLog)
        log_widget.clear()
        log_widget._last_ts = 0.0


def launch_monitor_blocking(shutdown_callback=None) -> None:
    TelegramFsMonitor(shutdown_callback=shutdown_callback).run()


if __name__ == "__main__":
    STATS.log("INFO", "STANDALONE", "Reading from telegram.db — live ops not available")
    TelegramFsMonitor().run()
