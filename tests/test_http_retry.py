import pytest
import requests

from coinx.collector.binance.client import (
    BinanceRateLimitUnavailable,
    clear_binance_rate_limit_state,
    request_with_binance_retry,
    request_with_retry,
)


class _FakeResponse:
    def __init__(self, status_code, reason='Error'):
        self.status_code = status_code
        self.reason = reason

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.exceptions.HTTPError(f'{self.status_code} {self.reason}')
            error.response = self
            raise error


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, params=None, timeout=10):
        self.calls += 1
        return self._responses.pop(0)


def test_request_with_retry_does_not_retry_non_retryable_http_status(monkeypatch):
    session = _FakeSession([_FakeResponse(402, 'Payment Required')])
    sleep_calls = []

    monkeypatch.setattr('coinx.collector.binance.client.time.sleep', lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(requests.exceptions.HTTPError):
        request_with_retry(session, 'https://example.com')

    assert session.calls == 1
    assert sleep_calls == []


def test_request_with_binance_retry_marks_binance_cooldown_after_429(monkeypatch):
    clear_binance_rate_limit_state()
    session = _FakeSession([_FakeResponse(429, 'Too Many Requests')])
    session._responses[0].headers = {}
    sleep_calls = []

    monkeypatch.setattr('coinx.collector.binance.client.time.sleep', lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(BinanceRateLimitUnavailable):
        request_with_binance_retry(session, 'https://example.com', max_retries=0)

    with pytest.raises(BinanceRateLimitUnavailable):
        request_with_binance_retry(session, 'https://example.com', max_retries=0)

    assert session.calls == 1
    assert sleep_calls == []
