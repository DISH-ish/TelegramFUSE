# ver 1.0
"""
connectivity.py — Background network connectivity monitor for TelegramFS.

Probes PING_HOST:PING_PORT via TCP (no ICMP, no root required) at configurable
intervals.  When connectivity is restored after an outage, fires an optional
reconnect callback so Telethon can re-establish its MTProto session immediately
rather than waiting for the next retry cycle.

Environment variables:
    PING_HOST              Host to probe          (default: 1.1.1.1)
    PING_PORT              Port to probe          (default: 443)
    PING_TIMEOUT           TCP connect timeout, s (default: 3)
    PING_INTERVAL_ONLINE   Probe interval online, s  (default: 60)
    PING_INTERVAL_OFFLINE  Probe interval offline, s (default: 10)
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Callable, Optional

from stats import STATS

log = logging.getLogger(__name__)

PING_HOST             = os.getenv("PING_HOST",            "1.1.1.1")
PING_PORT             = int(os.getenv("PING_PORT",        "443"))
PING_TIMEOUT          = float(os.getenv("PING_TIMEOUT",   "3"))
PING_INTERVAL_ONLINE  = float(os.getenv("PING_INTERVAL_ONLINE",  "60"))
PING_INTERVAL_OFFLINE = float(os.getenv("PING_INTERVAL_OFFLINE", "10"))


def _tcp_ping(host: str, port: int, timeout: float) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class ConnectivityMonitor:
    """
    Daemon thread that probes PING_HOST:PING_PORT periodically.

    Transitions:
      ONLINE  -> OFFLINE : logs warning, updates STATS
      OFFLINE -> ONLINE  : logs info, updates STATS, calls on_reconnect()

    The is_online property is readable from any thread or trio task at any time.
    Call notify_failure() from block_store when all retries exhaust to
    pre-emptively mark offline without waiting for the next ping cycle.
    """

    def __init__(self, on_reconnect: Optional[Callable[[], None]] = None) -> None:
        self._on_reconnect = on_reconnect
        self._stop         = threading.Event()
        self._thread       = threading.Thread(
            target=self._run, name="connectivity-monitor", daemon=True,
        )

    @property
    def is_online(self) -> bool:
        return STATS.network_online

    def notify_failure(self) -> None:
        """Pre-emptively mark offline when an operation exhausts all retries."""
        if STATS.network_online:
            STATS.set_network_state(False)
            STATS.log("WARNING", "NET_DOWN",
                      f"operations failed — marking offline until "
                      f"{PING_HOST}:{PING_PORT} responds")
            log.warning("ConnectivityMonitor: pre-emptive offline (retries exhausted)")

    def start(self) -> "ConnectivityMonitor":
        self._thread.start()
        log.info(
            "ConnectivityMonitor started  host=%s:%d  "
            "online_interval=%.0fs  offline_interval=%.0fs",
            PING_HOST, PING_PORT, PING_INTERVAL_ONLINE, PING_INTERVAL_OFFLINE,
        )
        return self

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        online = _tcp_ping(PING_HOST, PING_PORT, PING_TIMEOUT)
        STATS.set_network_state(online)
        log.info("ConnectivityMonitor: initial state %s", "ONLINE" if online else "OFFLINE")

        while not self._stop.is_set():
            interval = PING_INTERVAL_ONLINE if online else PING_INTERVAL_OFFLINE
            deadline = time.monotonic() + interval
            while time.monotonic() < deadline:
                if self._stop.is_set():
                    return
                time.sleep(min(1.0, deadline - time.monotonic()))

            now_online = _tcp_ping(PING_HOST, PING_PORT, PING_TIMEOUT)

            if online and not now_online:
                online = False
                STATS.set_network_state(False)
                STATS.log("WARNING", "NET_DOWN",
                          f"connectivity lost ({PING_HOST}:{PING_PORT} unreachable)")
                log.warning("ConnectivityMonitor: network DOWN — %s:%d unreachable",
                            PING_HOST, PING_PORT)

            elif not online and now_online:
                online = True
                offline_s = STATS.network_offline_seconds()
                STATS.set_network_state(True)
                STATS.log("SUCCESS", "NET_UP",
                          f"connectivity restored after {offline_s:.0f}s — reconnecting")
                log.info("ConnectivityMonitor: network UP after %.0fs", offline_s)
                if self._on_reconnect is not None:
                    try:
                        self._on_reconnect()
                    except Exception as exc:
                        log.error("ConnectivityMonitor: reconnect failed: %s", exc)
