"""Extract the per-frame facts we aggregate from scapy packets."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

FRAME_MGMT = 0
FRAME_CTRL = 1
FRAME_DATA = 2

SUBTYPE_PROBE_REQ = 4
SUBTYPE_PROBE_RESP = 5
SUBTYPE_BEACON = 8

RETRY_FLAG = 0x08

IE_SSID = 0
IE_RATES = 1
IE_DS_PARAMS = 3
IE_HT_CAPS = 45
IE_EXT_RATES = 50
IE_EXT_CAPS = 127
IE_VENDOR = 221

# Elements whose contents are stable per chipset/driver; SSID and channel vary per probe.
_FINGERPRINT_CONTENT_IES = {IE_RATES, IE_HT_CAPS, IE_EXT_RATES, IE_EXT_CAPS}


@dataclass(frozen=True)
class FrameInfo:
    sender: str
    size: int
    frame_type: int
    subtype: int
    rssi: int | None
    retry: bool
    ap_channel: int | None = None
    ssid: bytes | None = None
    probe_signature: str | None = None

    @property
    def is_beacon(self) -> bool:
        return self.frame_type == FRAME_MGMT and self.subtype == SUBTYPE_BEACON

    @property
    def is_probe_request(self) -> bool:
        return self.frame_type == FRAME_MGMT and self.subtype == SUBTYPE_PROBE_REQ

    @property
    def is_probe_response(self) -> bool:
        return self.frame_type == FRAME_MGMT and self.subtype == SUBTYPE_PROBE_RESP


def _iter_elements(pkt: Any):
    from scapy.layers.dot11 import Dot11Elt

    element = pkt.getlayer(Dot11Elt)
    while element is not None:
        yield element
        element = element.payload.getlayer(Dot11Elt)


def probe_signature(elements: list[tuple[int, bytes]]) -> str:
    """Order of IEs plus capability contents: a coarse device-model fingerprint."""
    digest = hashlib.sha256()
    for element_id, info in elements:
        if element_id in (IE_SSID, IE_DS_PARAMS):
            continue
        digest.update(element_id.to_bytes(1, "big"))
        if element_id in _FINGERPRINT_CONTENT_IES:
            digest.update(info)
        elif element_id == IE_VENDOR:
            digest.update(info[:4])
    return digest.hexdigest()[:16]


def extract_frame(pkt: Any) -> FrameInfo | None:
    from scapy.layers.dot11 import Dot11

    if not pkt.haslayer(Dot11):
        return None
    dot11 = pkt[Dot11]
    sender = dot11.addr2
    if not sender:
        return None

    rssi = getattr(pkt, "dBm_AntSignal", None)
    frame_type = int(dot11.type)
    subtype = int(dot11.subtype)
    flags = int(dot11.FCfield)

    ap_channel: int | None = None
    ssid: bytes | None = None
    signature: str | None = None

    if frame_type == FRAME_MGMT and subtype in (
        SUBTYPE_BEACON,
        SUBTYPE_PROBE_REQ,
        SUBTYPE_PROBE_RESP,
    ):
        elements = [(int(e.ID), bytes(e.info or b"")) for e in _iter_elements(pkt)]
        for element_id, info in elements:
            if element_id == IE_SSID and ssid is None:
                ssid = info
            elif element_id == IE_DS_PARAMS and info:
                ap_channel = info[0]
        if subtype == SUBTYPE_PROBE_REQ:
            signature = probe_signature(elements)

    return FrameInfo(
        sender=sender,
        size=len(pkt),
        frame_type=frame_type,
        subtype=subtype,
        rssi=int(rssi) if rssi is not None else None,
        retry=bool(flags & RETRY_FLAG),
        ap_channel=ap_channel,
        ssid=ssid,
        probe_signature=signature,
    )
