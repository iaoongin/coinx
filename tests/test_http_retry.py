import logging

import pytest
import requests

from coinx.collector import proxy_pool
from coinx.collector.binance.client import (
    BinanceRateLimitUnavailable,
    clear_binance_rate_limit_state,
    request_with_binance_retry,
    request_with_retry,
)
from coinx.collector.proxy_pool import ProxyPool, parse_proxy_pool_urls


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


def test_request_with_retry_logs_proxy_context_on_retry(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    session = _FakeSession([requests.exceptions.ConnectionError('boom'), _FakeResponse(200, 'OK')])
    session.proxies = {'https': 'http://proxy.example.com:2261'}
    sleep_calls = []

    def fake_get(url, params=None, timeout=10):
        session.calls += 1
        response = session._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    session.get = fake_get
    monkeypatch.setattr('coinx.collector.binance.client.time.sleep', lambda seconds: sleep_calls.append(seconds))

    response = request_with_retry(session, 'https://example.com')

    assert response.status_code == 200
    assert session.calls == 2
    assert sleep_calls == [0.5]
    assert 'proxy.example.com:2261' in caplog.text


def test_parse_proxy_pool_urls_keeps_socks5_scheme():
    proxies = parse_proxy_pool_urls('DE=socks5://DE:token@proxy.example.com:2261')

    assert proxies == [{'id': 'DE', 'url': 'socks5://DE:token@proxy.example.com:2261'}]


def test_proxy_pool_filters_unavailable_proxies_during_initialization(monkeypatch):
    pool = ProxyPool(
        proxies=[
            {'id': 'good', 'url': 'http://good.example.com:2261'},
        ]
    )

    assert pool.enabled() is True
    assert pool.all_proxy_ids() == ['good']


def test_build_okx_proxy_pool_falls_back_to_direct_when_all_proxies_unavailable(monkeypatch):
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_URLS', 'bad-a=http://bad-a.example.com:2261;bad-b=http://bad-b.example.com:2261')
    monkeypatch.setattr(proxy_pool, 'USE_PROXY', False)
    monkeypatch.setattr(proxy_pool, 'USE_PROXY_POOL', True)
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_STRATEGY', 'round_robin')
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_FAIL_COOLDOWN_SECONDS', 30)
    monkeypatch.setattr(
        ProxyPool,
        'check_proxies_concurrently',
        staticmethod(
            lambda proxies: {
                'available': [],
                'unavailable': [
                    {'id': 'bad-a', 'latency_ms': 5000, 'reason': 'timed out'},
                    {'id': 'bad-b', 'latency_ms': 5000, 'reason': 'timed out'},
                ],
            }
        ),
    )

    pool = proxy_pool.build_okx_proxy_pool()

    assert pool.enabled() is False
    assert pool.choose_proxy() == 'direct'


def test_proxy_pool_concurrent_health_check_collects_latency_and_status(monkeypatch):
    class _Session:
        def __init__(self, proxy_url):
            self.proxy_url = proxy_url

        def get(self, url, timeout=10):
            assert url == proxy_pool.PROXY_CHECK_URL
            assert timeout == 5
            if self.proxy_url.endswith('bad.example.com:2261'):
                raise requests.exceptions.ConnectTimeout('timed out')
            return _FakeResponse(200, 'OK')

    time_points = iter([10.0, 10.12, 20.0, 25.0])
    monkeypatch.setattr(proxy_pool, '_build_session', lambda proxy_url=None: _Session(proxy_url))
    monkeypatch.setattr(proxy_pool.time, 'perf_counter', lambda: next(time_points))

    results = ProxyPool.check_proxies_concurrently(
        [
            {'id': 'good', 'url': 'http://good.example.com:2261'},
            {'id': 'bad', 'url': 'http://bad.example.com:2261'},
        ]
    )

    assert [item['id'] for item in results['available']] == ['good']
    assert [item['id'] for item in results['unavailable']] == ['bad']
    assert results['available'][0]['latency_ms'] in (119, 120)
    assert results['unavailable'][0]['latency_ms'] == 5000
    assert 'timed out' in results['unavailable'][0]['reason']


def test_build_okx_proxy_pool_logs_health_check_summary(monkeypatch, caplog):
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_URLS', 'good=http://good.example.com:2261;bad=http://bad.example.com:2261')
    monkeypatch.setattr(proxy_pool, 'USE_PROXY', False)
    monkeypatch.setattr(proxy_pool, 'USE_PROXY_POOL', True)
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_STRATEGY', 'round_robin')
    monkeypatch.setattr(proxy_pool, 'PROXY_POOL_FAIL_COOLDOWN_SECONDS', 30)
    monkeypatch.setattr(
        ProxyPool,
        'check_proxies_concurrently',
        staticmethod(
            lambda proxies: {
                'available': [{'id': 'good', 'url': 'http://good.example.com:2261', 'latency_ms': 123}],
                'unavailable': [{'id': 'bad', 'url': 'http://bad.example.com:2261', 'latency_ms': 5000, 'reason': 'timed out'}],
            }
        ),
    )

    pool = proxy_pool.build_okx_proxy_pool()

    assert pool.all_proxy_ids() == ['good']
    assert 'available=good(123ms)' in caplog.text
    assert 'unavailable=bad(5000ms, timed out)' in caplog.text
