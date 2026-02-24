from __future__ import annotations

import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_SIZE = 12
KEY_SIZE   = 32


class BlockCipher:
    """AES-256-GCM per-block encryption. Wire format: nonce(12B) || ciphertext || tag(16B)."""

    def __init__(self, key: bytes) -> None:
        if len(key) != KEY_SIZE:
            raise ValueError(f"Key must be {KEY_SIZE} bytes, got {len(key)}")
        self._aesgcm = AESGCM(key)

    def encrypt(self, plaintext: bytes) -> bytes:
        nonce = os.urandom(NONCE_SIZE)
        return nonce + self._aesgcm.encrypt(nonce, plaintext, None)

    def decrypt(self, data: bytes) -> bytes:
        if len(data) < NONCE_SIZE + 16:
            raise ValueError(f"Ciphertext too short ({len(data)} B)")
        return bytes(self._aesgcm.decrypt(data[:NONCE_SIZE], data[NONCE_SIZE:], None))

    @staticmethod
    def generate_key_hex() -> str:
        return os.urandom(KEY_SIZE).hex()

    @classmethod
    def from_hex(cls, hex_str: str) -> "BlockCipher":
        return cls(bytes.fromhex(hex_str.strip()))
