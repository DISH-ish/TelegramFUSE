#!/usr/bin/env python3
"""
monitor.py — Live TUI dashboard for TelegramFS.

Runs in a background daemon thread alongside the FUSE filesystem.
Can also be launched standalone: python monitor.py <mountpoint>
(in standalone mode it reads directly from telegram.db for DB stats).

Usage (embedded – called by main.py):
    from monitor import launch_monitor_thread
    launch_monitor_thread()

Usage (standalone):
    python monitor.py
"""

from __future__ import annotations

import threading
import sqlite3
import sys
from datetime import datetime
from time import time
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Footer, Header, Static, Label, RichLog

from stats import STATS


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_bytes(n: int | float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _fmt_uptime(secs: float) -> str:
    h = int(secs // 3600)
    m = int((secs % 3600) // 60)
    s = int(secs % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _pct_bar(ratio: float, width: int = 20) -> str:
    """Render a simple ASCII progress bar."""
    filled = int(ratio * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {ratio * 100:.1f}%"


def _level_color(level: str) -> str:
    return {
        "SUCCESS": "green",
        "INFO": "cyan",
        "WARNING": "yellow",
        "ERROR": "red",
    }.get(level, "white")


def _read_db_stats() -> dict:
    """Read inode / file stats directly from the sqlite DB."""
    try:
        db = sqlite3.connect("file:telegram.db?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        cur = db.cursor()

        cur.execute("SELECT COUNT(*) FROM inodes")
        total_inodes = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM inodes WHERE mode & 0o170000 = 0o040000")
        dirs = cur.fetchone()[0]

        files = total_inodes - dirs

        cur.execute("SELECT COALESCE(SUM(size), 0) FROM inodes")
        total_bytes = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM telegram_messages")
        tg_messages = cur.fetchone()[0]

        db.close()
        return {
            "inodes": total_inodes,
            "files": files,
            "dirs": dirs,
            "total_bytes": total_bytes,
            "tg_messages": tg_messages,
            "error": None,
        }
    except Exception as exc:
        return {"error": str(exc), "inodes": 0, "files": 0, "dirs": 0,
                "total_bytes": 0, "tg_messages": 0}


# ─────────────────────────────────────────────────────────────────────────────
# Panels (each a Static that refreshes its own markup)
# ─────────────────────────────────────────────────────────────────────────────

class CachePanel(Static):
    """LRU cache statistics."""

    DEFAULT_CSS = """
    CachePanel {
        border: round $success;
        padding: 0 1;
        width: 1fr;
        height: 100%;
    }
    """

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        snap = STATS.snapshot()["cache"]
        hr = snap["hit_rate"]
        cur = snap["current_bytes"]
        mx = snap["max_bytes"]
        usage = cur / mx if mx else 0.0

        color = "green" if hr >= 0.8 else ("yellow" if hr >= 0.5 else "red")

        lines = [
            "[bold cyan]  CACHE[/]",
            "",
            f"  Hits       [green]{snap['hits']:>8,}[/]",
            f"  Misses     [red]{snap['misses']:>8,}[/]",
            f"  Evictions  [yellow]{snap['evictions']:>8,}[/]",
            "",
            f"  Hit rate   [{color}]{_pct_bar(hr, 14)}[/]",
            f"  Used       {_pct_bar(usage, 14)}",
            f"  Size       [cyan]{_fmt_bytes(cur)}[/] / {_fmt_bytes(mx)}",
        ]
        self.update("\n".join(lines))


class TransferPanel(Static):
    """Upload / download statistics."""

    DEFAULT_CSS = """
    TransferPanel {
        border: round $warning;
        padding: 0 1;
        width: 1fr;
        height: 100%;
    }
    """

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        t = STATS.snapshot()["transfers"]
        au = t["active_uploads"]
        ad = t["active_downloads"]

        up_color   = "yellow bold" if au > 0 else "green"
        down_color = "yellow bold" if ad > 0 else "green"
        err_color  = "red" if (t["uploads_failed"] + t["downloads_failed"]) > 0 else "green"

        lines = [
            "[bold cyan]  TRANSFERS[/]",
            "",
            f"  [bold]Uploads[/]",
            f"    Total    [green]{t['uploads_total']:>7,}[/]",
            f"    Data     [cyan]{_fmt_bytes(t['uploads_bytes']):>10}[/]",
            f"    Failed   [{err_color}]{t['uploads_failed']:>7,}[/]",
            f"    Active   [{up_color}]{au:>7,}[/]",
            "",
            f"  [bold]Downloads[/]",
            f"    Total    [green]{t['downloads_total']:>7,}[/]",
            f"    Data     [cyan]{_fmt_bytes(t['downloads_bytes']):>10}[/]",
            f"    Failed   [{err_color}]{t['downloads_failed']:>7,}[/]",
            f"    Active   [{down_color}]{ad:>7,}[/]",
        ]
        self.update("\n".join(lines))


class OpsPanel(Static):
    """FUSE operation counters."""

    DEFAULT_CSS = """
    OpsPanel {
        border: round $primary;
        padding: 0 1;
        width: 1fr;
        height: 100%;
    }
    """

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        o = STATS.snapshot()["ops"]
        lines = [
            "[bold cyan]  FUSE OPS[/]",
            "",
            f"  Reads      [green]{o['read']:>8,}[/]   ({_fmt_bytes(o['bytes_read'])})",
            f"  Writes     [yellow]{o['write']:>8,}[/]   ({_fmt_bytes(o['bytes_written'])})",
            f"  Lookups    [cyan]{o['lookup']:>8,}[/]",
            "",
            f"  Creates    [green]{o['create']:>8,}[/]",
            f"  Deletes    [red]{o['delete']:>8,}[/]",
            f"  Renames    [magenta]{o['rename']:>8,}[/]",
        ]
        self.update("\n".join(lines))


class DbPanel(Static):
    """SQLite / filesystem metadata stats."""

    DEFAULT_CSS = """
    DbPanel {
        border: round $surface;
        padding: 0 1;
        height: 5;
    }
    """

    def on_mount(self) -> None:
        self.set_interval(3.0, self._refresh)   # DB reads are heavier → slower poll

    def _refresh(self) -> None:
        d = _read_db_stats()
        enc = STATS.snapshot()["encryption"]
        enc_str = "[green]✓ AES-128[/]" if enc else "[red]✗ off[/]"
        mp = STATS.snapshot()["mountpoint"] or "[dim]unknown[/]"

        if d["error"]:
            self.update(f"[bold cyan]  DATABASE[/]   [red]Error: {d['error']}[/]")
            return

        self.update(
            f"[bold cyan]  DATABASE[/]   "
            f"Inodes [cyan]{d['inodes']:,}[/]  "
            f"Files [green]{d['files']:,}[/]  "
            f"Dirs [blue]{d['dirs']:,}[/]  "
            f"TG msgs [yellow]{d['tg_messages']:,}[/]  "
            f"Stored [cyan]{_fmt_bytes(d['total_bytes'])}[/]  "
            f"│  Encryption {enc_str}  "
            f"│  Mount [bold]{mp}[/]"
        )


class HandlesPanel(Static):
    """Open file-handle table."""

    DEFAULT_CSS = """
    HandlesPanel {
        border: round $warning;
        padding: 0 1;
        height: 8;
    }
    """

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh)

    def _refresh(self) -> None:
        handles = STATS.snapshot()["handles"]
        if not handles:
            self.update("[bold cyan]  OPEN HANDLES[/]   [dim]none[/]")
            return

        lines = ["[bold cyan]  OPEN HANDLES[/]"]
        for h in handles[:6]:  # cap display at 6 rows
            dirty_icon = "[red]●[/]" if h.dirty else "[dim]○[/]"
            buf = f"  buf=[yellow]{_fmt_bytes(h.buffer_bytes)}[/]" if h.buffer_bytes else ""
            age = time() - h.opened_at
            lines.append(
                f"  {dirty_icon} fh=[cyan]{h.fh:<4}[/] "
                f"[bold]{h.name or '(unnamed)'}[/]{buf}  "
                f"[dim]{age:.0f}s[/]"
            )
        if len(handles) > 6:
            lines.append(f"  [dim]… and {len(handles) - 6} more[/]")
        self.update("\n".join(lines))


class ActivityLog(RichLog):
    """Scrolling activity log sourced from STATS._log."""

    DEFAULT_CSS = """
    ActivityLog {
        border: round $surface;
        padding: 0 1;
        height: 1fr;
    }
    """

    _seen_count: int = 0

    def on_mount(self) -> None:
        self.set_interval(0.5, self._refresh)

    def _refresh(self) -> None:
        entries = STATS.snapshot()["log"]
        # Only write new entries since the last poll.
        new = entries[self._seen_count:]
        self._seen_count = len(entries)

        for e in new:
            ts = datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S")
            color = _level_color(e.level)
            level_str = f"[{color}]{e.level:<7}[/]"
            detail = f" [dim]{e.detail}[/]" if e.detail else ""
            self.write(f"[dim]{ts}[/] {level_str} [bold]{e.operation}[/]{detail}")


# ─────────────────────────────────────────────────────────────────────────────
# Header status bar (uptime + active-transfer spinner)
# ─────────────────────────────────────────────────────────────────────────────

_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

class StatusBar(Static):
    DEFAULT_CSS = """
    StatusBar {
        dock: top;
        background: $panel;
        color: $text;
        padding: 0 2;
        height: 1;
    }
    """

    _tick: int = 0

    def on_mount(self) -> None:
        self.set_interval(0.2, self._refresh)

    def _refresh(self) -> None:
        snap = STATS.snapshot()
        uptime = _fmt_uptime(snap["uptime"])
        au = snap["transfers"]["active_uploads"]
        ad = snap["transfers"]["active_downloads"]

        spinner = ""
        if au or ad:
            self._tick = (self._tick + 1) % len(_SPINNER)
            parts = []
            if au:
                parts.append(f"↑{au}")
            if ad:
                parts.append(f"↓{ad}")
            spinner = f" [yellow]{_SPINNER[self._tick]} {' '.join(parts)}[/]"

        self.update(
            f" [bold cyan]📡 TelegramFS Monitor[/]"
            f"  uptime [green]{uptime}[/]{spinner}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

class TelegramFsMonitor(App):
    """Live dashboard for TelegramFS."""

    CSS = """
    Screen {
        background: $background;
        layout: vertical;
    }

    #top-row {
        layout: horizontal;
        height: 12;
        margin: 0 0 0 0;
    }

    #mid-row {
        layout: vertical;
        height: auto;
    }

    #log-label {
        color: $text-muted;
        padding: 0 2;
        height: 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("c", "clear_log", "Clear log"),
    ]

    def compose(self) -> ComposeResult:
        yield StatusBar()
        with Horizontal(id="top-row"):
            yield CachePanel()
            yield TransferPanel()
            yield OpsPanel()
        yield DbPanel()
        yield HandlesPanel()
        yield Label(" ACTIVITY LOG", id="log-label")
        yield ActivityLog(highlight=True, markup=True, wrap=False)
        yield Footer()

    def action_clear_log(self) -> None:
        log_widget = self.query_one(ActivityLog)
        log_widget.clear()
        log_widget._seen_count = 0


# ─────────────────────────────────────────────────────────────────────────────
# Thread launcher (called by main.py)
# ─────────────────────────────────────────────────────────────────────────────

def launch_monitor_thread() -> threading.Thread:
    """
    Start the TUI in a background daemon thread so it doesn't block
    the trio event loop that drives FUSE.
    """
    def _run():
        app = TelegramFsMonitor()
        app.run()

    t = threading.Thread(target=_run, name="tui-monitor", daemon=True)
    t.start()
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # When run standalone, we don't have a live FUSE process writing to STATS,
    # but we can still watch the DB for changes.  A polling loop writes fake
    # log entries so the log panel isn't empty.
    import time as _time

    STATS.log("INFO", "STANDALONE", "Reading from telegram.db — live ops not available")

    app = TelegramFsMonitor()
    app.run()
