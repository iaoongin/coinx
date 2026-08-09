"""Helpers for deterministic, read-only API replay checks."""

from flask import request


def request_as_of_ms():
    """Return an optional millisecond timestamp supplied by the caller.

    ``as_of_ms`` is intentionally opt-in so normal production requests keep
    their existing wall-clock behavior. Invalid values are rejected by the
    route as a client error instead of silently changing the query window.
    """
    raw = request.args.get("as_of_ms")
    if raw is None or not str(raw).strip():
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("as_of_ms must be an integer Unix timestamp in milliseconds") from exc
    if value <= 0:
        raise ValueError("as_of_ms must be greater than zero")
    return value
