"""MAC pseudonymisation shared by capture, bronze ingest, and the hashing script."""

from __future__ import annotations

import hashlib
import hmac
import re

_RAW_MAC = re.compile(r"^[0-9a-f]{2}([:-]?[0-9a-f]{2}){5}$")
HASH_LENGTH = 16


def normalize_mac(mac: str) -> str:
    return mac.strip().lower().replace("-", "").replace(":", "")


def is_raw_mac(value: str | None) -> bool:
    return bool(value) and _RAW_MAC.match(value.strip().lower()) is not None


def is_locally_administered(mac: str) -> bool:
    """True for randomized / software-assigned MACs (U/L bit set in the first octet)."""
    normalized = normalize_mac(mac)
    return bool(int(normalized[0:2], 16) & 0x02)


def oui(mac: str) -> str | None:
    """Vendor prefix for globally administered MACs; None for randomized ones."""
    if is_locally_administered(mac):
        return None
    return normalize_mac(mac)[:6].upper()


def keyed_hash(value: str, salt: bytes, length: int = HASH_LENGTH) -> str:
    return hmac.new(salt, value.encode("utf-8"), hashlib.sha256).hexdigest()[:length]


def hash_mac(mac: str, salt: bytes) -> str:
    return keyed_hash(normalize_mac(mac), salt)


def require_salt(salt: str | bytes | None) -> bytes:
    if not salt:
        raise ValueError("MAC_HASH_SALT is not set; refusing to process raw MAC addresses")
    return salt.encode("utf-8") if isinstance(salt, str) else salt
