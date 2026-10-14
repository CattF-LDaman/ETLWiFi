"""Push serving documents to the portfolio over HMAC-signed HTTP."""

from __future__ import annotations

import gzip
import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-ETLWiFi-Signature"
TIMESTAMP_HEADER = "X-ETLWiFi-Timestamp"
INGEST_PATH = "/api/ingest/wifi/{kind}"
USER_AGENT = "etlwifi-publisher/1.0"


def sign(secret: str, timestamp: str, body: bytes) -> str:
    message = timestamp.encode("ascii") + b"." + body
    return "sha256=" + hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


class Publisher:
    def __init__(
        self,
        base_url: str,
        secret: str,
        *,
        timeout: float = 20.0,
        retries: int = 3,
        backoff: float = 2.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.secret = secret
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

    def _request(self, kind: str, body: bytes) -> urllib.request.Request:
        timestamp = str(int(time.time()))
        return urllib.request.Request(
            self.base_url + INGEST_PATH.format(kind=kind),
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
                "User-Agent": USER_AGENT,
                TIMESTAMP_HEADER: timestamp,
                SIGNATURE_HEADER: sign(self.secret, timestamp, body),
            },
        )

    def publish(self, kind: str, payload: dict) -> bool:
        body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"), compresslevel=6)
        for attempt in range(1, self.retries + 1):
            try:
                with urllib.request.urlopen(self._request(kind, body), timeout=self.timeout) as response:
                    if 200 <= response.status < 300:
                        log.debug("published", extra={"kind": kind, "bytes": len(body)})
                        return True
            except urllib.error.HTTPError as exc:
                if 400 <= exc.code < 500 and exc.code not in (408, 429):
                    log.error("publish rejected", extra={"kind": kind, "status": exc.code})
                    return False
                log.warning("publish failed", extra={"kind": kind, "status": exc.code, "attempt": attempt})
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                log.warning("publish failed", extra={"kind": kind, "error": str(exc), "attempt": attempt})
            if attempt < self.retries:
                time.sleep(self.backoff**attempt)
        return False
