import pytest
import requests

from coinx.collector.binance.client import request_with_retry


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
