from __future__ import annotations

import os
from urllib.parse import quote, urlsplit, urlunsplit

from arq.connections import RedisSettings


def redis_dsn() -> str:
    """Return a Redis DSN with password characters safely quoted."""
    raw = os.environ.get("HLTHPRT_REDIS_ADDRESS") or os.environ.get("REDIS_URL") or "redis://127.0.0.1:6379/0"
    parsed = urlsplit(raw)
    if parsed.scheme != "redis" or not parsed.username or parsed.password is None:
        return raw
    host = parsed.hostname or "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f":{quote(parsed.password, safe='')}@{host}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path or "/0", parsed.query, parsed.fragment))


def redis_settings() -> RedisSettings:
    """Build ARQ Redis settings from the normalized Redis DSN."""
    return RedisSettings.from_dsn(redis_dsn())
