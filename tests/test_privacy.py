from __future__ import annotations

import pytest

from etlwifi.privacy import (
    hash_mac,
    is_locally_administered,
    is_raw_mac,
    keyed_hash,
    normalize_mac,
    oui,
    require_salt,
)


def test_normalize_mac_accepts_common_formats() -> None:
    assert normalize_mac("AA:BB:CC:DD:EE:FF") == "aabbccddeeff"
    assert normalize_mac(" aa-bb-cc-dd-ee-ff ") == "aabbccddeeff"


def test_is_raw_mac_distinguishes_pseudonyms() -> None:
    assert is_raw_mac("00:1c:f0:9e:26:ce")
    assert not is_raw_mac("3f2a9c0b1d4e5f60")


def test_locally_administered_bit_marks_randomized_macs() -> None:
    assert is_locally_administered("da:a1:00:00:00:01")
    assert is_locally_administered("02:00:00:00:00:00")
    assert not is_locally_administered("00:1c:f0:9e:26:ce")


def test_oui_is_only_exposed_for_universal_addresses() -> None:
    assert oui("00:1c:f0:9e:26:ce") == "001CF0"
    assert oui("da:a1:00:00:00:01") is None


def test_hash_mac_is_keyed_and_format_insensitive() -> None:
    salt = b"salt-a"
    first = hash_mac("00:1C:F0:9E:26:CE", salt)
    assert first == hash_mac("00-1c-f0-9e-26-ce", salt)
    assert first != hash_mac("00:1c:f0:9e:26:ce", b"salt-b")
    assert len(first) == 16
    assert keyed_hash("x", salt, length=8) == keyed_hash("x", salt)[:8]


@pytest.mark.parametrize("salt", [None, ""])
def test_require_salt_fails_closed(salt: str | None) -> None:
    with pytest.raises(ValueError, match="MAC_HASH_SALT"):
        require_salt(salt)
