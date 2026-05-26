import time

import requests

from coinx.collector.rate_limit import (
    RateLimitRegistry,
    RateLimitUnavailable,
    parse_retry_after_seconds,
    record_rate_limit_wait_seconds,
)
from coinx.config import HTTPS_PROXY_URL, PROXY_URL, USE_PROXY
from coinx.utils import logger


_global_session = None
RETRYABLE_HTTP_STATUS_CODES = {403, 408, 409, 425, 429, 500, 502, 503, 504}
BINANCE_REPAIR_COOLDOWN_SECONDS = 2.0
_binance_rate_limits = RateLimitRegistry()


class BinanceRateLimitUnavailable(RateLimitUnavailable):
    """Raised when Binance repair traffic is cooling down."""

    def __init__(self, group, wait_seconds, reason='budget_unavailable'):
        super().__init__('binance', group, wait_seconds, reason=reason)


def clear_binance_rate_limit_state():
    _binance_rate_limits.clear()


def is_binance_budget_unavailable(group='default'):
    return _binance_rate_limits.unavailable_remaining_seconds('binance', group) > 0


def get_session():
    """Create a shared requests session with optional proxy configuration."""
    global _global_session
    if _global_session is None:
        _global_session = requests.Session()
        _global_session.headers.update(
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Connection': 'keep-alive',
            }
        )

        if USE_PROXY:
            proxies = {
                'http': PROXY_URL,
                'https': HTTPS_PROXY_URL,
            }
            _global_session.proxies.update(proxies)
            logger.info("使用代理: %s", PROXY_URL)

    return _global_session


def _merge_request_headers(session, headers):
    if not headers:
        return None
    merged_headers = dict(session.headers)
    for key, value in headers.items():
        if value is None:
            merged_headers.pop(key, None)
        else:
            merged_headers[key] = value
    return merged_headers


def _session_proxy_summary(session):
    proxies = getattr(session, 'proxies', None) or {}
    if not isinstance(proxies, dict):
        return 'direct'
    https_proxy = proxies.get('https')
    http_proxy = proxies.get('http')
    return https_proxy or http_proxy or 'direct'


def request_with_retry(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5, headers=None):
    """Perform GET requests with bounded retry only."""
    attempt = 0
    request_headers = _merge_request_headers(session, headers)

    while True:
        try:
            request_kwargs = {'params': params, 'timeout': timeout}
            if request_headers is not None:
                request_kwargs['headers'] = request_headers
            response = session.get(url, **request_kwargs)
            if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                error = requests.exceptions.HTTPError(f"{response.status_code} {response.reason}")
                error.response = response
                raise error
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as exc:
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code not in RETRYABLE_HTTP_STATUS_CODES:
                raise exc

            attempt += 1
            if attempt > max_retries:
                raise exc

            delay = base_delay * (2 ** (attempt - 1))
            if delay > 1.5:
                delay = 1.5
            logger.debug(
                "请求失败，将在 %.2fs 后重试（第%d/%d次）: %s, proxy=%s, 错误: %s",
                delay,
                attempt,
                max_retries,
                url,
                _session_proxy_summary(session),
                exc,
            )
            record_rate_limit_wait_seconds(delay)
            time.sleep(delay)


def request_with_binance_retry(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5, headers=None):
    """Perform GET requests with bounded retry and a lightweight Binance cooldown."""
    attempt = 0
    request_headers = _merge_request_headers(session, headers)
    rate_limit_group = 'default'

    while True:
        wait_seconds = _binance_rate_limits.unavailable_remaining_seconds('binance', rate_limit_group)
        if wait_seconds > 0:
            raise BinanceRateLimitUnavailable(rate_limit_group, wait_seconds)

        try:
            return request_with_retry(
                session,
                url,
                params=params,
                timeout=timeout,
                max_retries=max_retries,
                base_delay=base_delay,
                headers=headers,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as exc:
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code not in RETRYABLE_HTTP_STATUS_CODES:
                raise exc

            if response is not None and response.status_code in (403, 429):
                retry_after_seconds = parse_retry_after_seconds(response.headers.get('Retry-After'))
                cooldown_seconds = retry_after_seconds if retry_after_seconds is not None else BINANCE_REPAIR_COOLDOWN_SECONDS
                _binance_rate_limits.mark_cooldown('binance', rate_limit_group, cooldown_seconds)
                logger.warning(
                    "Binance request cooldown: url=%s status=%s wait=%.2fs headers=%s",
                    url,
                    response.status_code,
                    cooldown_seconds,
                    {
                        key: value
                        for key, value in response.headers.items()
                        if 'retry' in key.lower() or 'weight' in key.lower()
                    },
                )
            attempt += 1
            if attempt > 1:
                raise exc
