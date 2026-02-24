from __future__ import annotations

import asyncio
import logging
import threading
from io import BytesIO

from telethon import TelegramClient
from dotenv import load_dotenv
import os

log = logging.getLogger(__name__)
load_dotenv()

# Single asyncio loop in a daemon thread — Telethon requires asyncio while
# pyfuse3 runs on trio; this bridge lets both coexist.
_tg_loop:      asyncio.AbstractEventLoop | None = None
_tg_loop_lock: threading.Lock                   = threading.Lock()


def _get_tg_loop() -> asyncio.AbstractEventLoop:
    global _tg_loop
    with _tg_loop_lock:
        if _tg_loop is None or not _tg_loop.is_running():
            ready = threading.Event()

            def _run(loop, evt):
                asyncio.set_event_loop(loop)
                loop.call_soon(evt.set)
                loop.run_forever()

            loop = asyncio.new_event_loop()
            threading.Thread(target=_run, args=(loop, ready),
                             name="telethon-loop", daemon=True).start()
            ready.wait()
            _tg_loop = loop
        return _tg_loop


def _run_sync(coro):
    return asyncio.run_coroutine_threadsafe(coro, _get_tg_loop()).result()


def _progress_cb(sent: int, total: int) -> None:
    pct = int(sent / total * 100) if total else 0
    if pct % 10 == 0:
        log.debug("Upload progress: %d%%", pct)


class TelegramFileClient:
    def __init__(self, session_name, api_id, api_hash, channel_link) -> None:
        loop             = _get_tg_loop()
        self._local_addr = os.getenv("LOCAL_ADDR", "").strip() or None

        async def _init():
            client = TelegramClient(
                session_name, api_id, api_hash,
                loop=loop,
                local_addr=self._local_addr,
            )
            await client.start()

            # Patch secondary DC connections so LOCAL_ADDR is respected for
            # file-transfer connections, not just the main MTProto session.
            if self._local_addr:
                _orig_borrow = client._borrow_exported_sender

                async def _patched_borrow(dc_id):
                    old = getattr(client, '_local_addr', None)
                    client._local_addr = self._local_addr
                    try:
                        return await _orig_borrow(dc_id)
                    finally:
                        client._local_addr = old

                client._borrow_exported_sender = _patched_borrow
                log.info("Secondary DC connections patched to bind to %s", self._local_addr)

            entity = await client.get_entity(channel_link)
            return client, entity

        self._tg, self._channel = _run_sync(_init())
        log.info("Telegram client ready (local_addr=%s)", self._local_addr or "default")

    def upload_block(self, data: bytes, name: str) -> int:
        async def _do():
            tg_file = await self._tg.upload_file(
                BytesIO(data), file_name=f"{name}.bin",
                part_size_kb=512, progress_callback=_progress_cb,
            )
            return (await self._tg.send_file(self._channel, tg_file)).id
        return _run_sync(_do())

    def download_block(self, msg_id: int) -> bytes:
        async def _do():
            msg = await self._tg.get_messages(self._channel, ids=msg_id)
            if msg is None or msg.media is None:
                raise RuntimeError(f"No media for message {msg_id}")
            data = await msg.download_media(bytes)
            if data is None:
                raise RuntimeError(f"download_media returned None for message {msg_id}")
            return data
        return _run_sync(_do())

    def delete_messages(self, ids: list[int]) -> None:
        if not ids:
            return
        _run_sync(self._tg.delete_messages(self._channel, message_ids=ids))

    def check_messages_exist(self, ids: list[int]) -> list[int]:
        """Return the subset of *ids* that are missing or have no media."""
        missing    = []
        batch_size = 200
        for i in range(0, len(ids), batch_size):
            batch   = ids[i:i + batch_size]
            msgs    = _run_sync(self._tg.get_messages(self._channel, ids=batch))
            msg_map = {m.id: m for m in msgs if m is not None}
            for msg_id in batch:
                m = msg_map.get(msg_id)
                if m is None or m.media is None:
                    missing.append(msg_id)
        return missing

    def iter_all_messages_raw(self, batch_size=100, progress_cb=None):
        """Yield (msg_id, raw_bytes) for every media message in the channel."""
        scanned = collected = max_id = 0

        while True:
            batch = _run_sync(self._tg.get_messages(
                self._channel,
                **{"limit": batch_size, **({"max_id": max_id} if max_id else {})}
            ))
            if not batch:
                break

            for msg in batch:
                scanned += 1
                if msg.media is None:
                    continue
                raw = _run_sync(msg.download_media(bytes))
                if raw:
                    collected += 1
                    yield msg.id, raw

            if progress_cb:
                progress_cb(scanned, collected)

            if len(batch) < batch_size:
                break

            max_id = min(m.id for m in batch)
