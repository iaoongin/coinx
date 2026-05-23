from __future__ import annotations

import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from threading import Lock, local


_rate_limit_wait_state = local()


def record_rate_limit_wait_seconds(seconds):
    seconds = max(0.0, float(seconds or 0.0))
    if seconds <= 0:
        return
    _rate_limit_wait_state.seconds = getattr(_rate_limit_wait_state, 'seconds', 0.0) + seconds


def consume_rate_limit_wait_seconds():
    seconds = getattr(_rate_limit_wait_state, 'seconds', 0.0)
    _rate_limit_wait_state.seconds = 0.0
    return seconds


class RateLimitUnavailable(RuntimeError):
    """Raised when a rate limit group is cooling down or budget is unavailable."""

    def __init__(self, exchange, group, wait_seconds, reason='budget_unavailable'):
        super().__init__(f'{exchange} {group} unavailable for {wait_seconds:.2f}s: {reason}')
        self.exchange = exchange
        self.group = group
        self.wait_seconds = float(wait_seconds)
        self.reason = reason


@dataclass
class RateLimitState:
    next_allowed_at: float = 0.0
    cooldown_until: float = 0.0
    remain: int | None = None
    limit: int | None = None
    reset_at: float | None = None
    budget_initialized: bool = False
    budget_unavailable_until: float = 0.0
    last_headers: dict | None = None


class RateLimitRegistry:
    def __init__(self):
        self._lock = Lock()
        self._states = {}

    def clear(self):
        with self._lock:
            self._states.clear()

    def reset_group(self, exchange, group, proxy_id='direct'):
        with self._lock:
            self._states[(exchange, group, proxy_id)] = RateLimitState(last_headers={})

    def get_state_snapshot(self, exchange, group, proxy_id='direct'):
        with self._lock:
            state = self._states.setdefault((exchange, group, proxy_id), RateLimitState(last_headers={}))
            return RateLimitState(
                next_allowed_at=state.next_allowed_at,
                cooldown_until=state.cooldown_until,
                remain=state.remain,
                limit=state.limit,
                reset_at=state.reset_at,
                budget_initialized=state.budget_initialized,
                budget_unavailable_until=state.budget_unavailable_until,
                last_headers=dict(state.last_headers or {}),
            )

    def unavailable_remaining_seconds(self, exchange, group, proxy_id='direct'):
        with self._lock:
            state = self._states.setdefault((exchange, group, proxy_id), RateLimitState(last_headers={}))
            now = time.time()
            waits = [
                max(0.0, state.cooldown_until - now),
                max(0.0, state.budget_unavailable_until - now),
            ]
            if state.remain is not None and state.remain <= 0 and state.reset_at is not None:
                waits.append(max(0.0, state.reset_at - now))
            return max(waits)

    def wait_for_slot(self, exchange, group, proxy_id='direct', min_interval_ms=0, consume_budget=False):
        min_interval_seconds = max(0.0, float(min_interval_ms) / 1000.0)
        total_wait_seconds = 0.0
        while True:
            with self._lock:
                state = self._states.setdefault((exchange, group, proxy_id), RateLimitState(last_headers={}))
                now = time.time()

                if state.reset_at is not None and now >= state.reset_at:
                    state.remain = None
                    state.limit = None
                    state.reset_at = None
                    state.budget_initialized = False

                wait_seconds = max(
                    0.0,
                    state.cooldown_until - now,
                    state.budget_unavailable_until - now,
                    state.next_allowed_at - now,
                )
                if state.remain is not None and state.remain <= 0 and state.reset_at is not None:
                    wait_seconds = max(wait_seconds, state.reset_at - now)

                if wait_seconds <= 0:
                    state.next_allowed_at = max(state.next_allowed_at, now + min_interval_seconds)
                    if consume_budget and state.remain is not None and state.remain > 0:
                        state.remain -= 1
                    return total_wait_seconds
            total_wait_seconds += wait_seconds
            record_rate_limit_wait_seconds(wait_seconds)
            time.sleep(wait_seconds)

    def mark_cooldown(self, exchange, group, wait_seconds, proxy_id='direct', headers=None, budget_unavailable=False):
        now = time.time()
        wait_seconds = max(0.0, float(wait_seconds))
        with self._lock:
            state = self._states.setdefault((exchange, group, proxy_id), RateLimitState(last_headers={}))
            state.cooldown_until = max(state.cooldown_until, now + wait_seconds)
            state.next_allowed_at = max(state.next_allowed_at, now + wait_seconds)
            if headers is not None:
                state.last_headers = dict(headers)
            if budget_unavailable:
                state.budget_initialized = False
                state.remain = None
                state.limit = None
                state.reset_at = None
                state.budget_unavailable_until = max(state.budget_unavailable_until, now + wait_seconds)

    def update_budget(
        self,
        exchange,
        group,
        proxy_id='direct',
        *,
        limit=None,
        remain=None,
        reset_at=None,
        next_allowed_at=None,
        headers=None,
    ):
        with self._lock:
            state = self._states.setdefault((exchange, group, proxy_id), RateLimitState(last_headers={}))
            if headers is not None:
                state.last_headers = dict(headers)
            if limit is not None:
                state.limit = limit
            if remain is not None:
                state.remain = remain
            if reset_at is not None:
                state.reset_at = float(reset_at)
            if limit is not None or remain is not None or reset_at is not None:
                state.budget_initialized = True
                state.budget_unavailable_until = 0.0
            if next_allowed_at is not None:
                state.next_allowed_at = max(state.next_allowed_at, float(next_allowed_at))


def parse_retry_after_seconds(value):
    if value in (None, ''):
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        return max(0.0, retry_at.timestamp() - time.time())
    except (TypeError, ValueError, OverflowError):
        return None
