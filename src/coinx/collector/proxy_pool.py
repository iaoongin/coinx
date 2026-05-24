from __future__ import annotations

import threading
import time

import requests

from coinx.config import (
    HTTPS_PROXY_URL,
    PROXY_POOL_FAIL_COOLDOWN_SECONDS,
    PROXY_POOL_STRATEGY,
    PROXY_POOL_URLS,
    PROXY_URL,
    USE_PROXY,
    USE_PROXY_POOL,
)
from coinx.utils import logger


DEFAULT_PROXY_ID = 'direct'
PROXY_CHECK_URL = 'https://www.okx.com/api/v5/public/time'
PROXY_CHECK_TIMEOUT_SECONDS = 5


def _build_session(proxy_url=None):
    session = requests.Session()
    session.headers.update(
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
        }
    )
    if proxy_url:
        session.proxies.update(
            {
                'http': proxy_url,
                'https': proxy_url,
            }
        )
    return session


def parse_proxy_pool_urls(value):
    entries = []
    raw_value = (value or '').strip()
    if not raw_value:
        return entries
    for chunk in raw_value.split(';'):
        chunk = chunk.strip()
        if not chunk:
            continue
        if '=' not in chunk:
            raise ValueError(f'invalid proxy pool entry: {chunk}')
        proxy_id, proxy_url = chunk.split('=', 1)
        proxy_id = proxy_id.strip()
        proxy_url = proxy_url.strip()
        if not proxy_id:
            raise ValueError(f'invalid proxy pool entry: {chunk}')
        entries.append(
            {
                'id': proxy_id,
                'url': None if proxy_url in ('', 'direct') else proxy_url,
            }
        )
    return entries


class ProxyPool:
    def __init__(self, proxies=None, strategy='round_robin', fail_cooldown_seconds=30):
        self._lock = threading.Lock()
        self._strategy = strategy or 'round_robin'
        self._fail_cooldown_seconds = max(1, int(fail_cooldown_seconds or 30))
        self._cursor = 0
        self._proxies = []

        for proxy in proxies or []:
            proxy_id = proxy['id']
            proxy_url = proxy.get('url')
            if not self._is_proxy_available(proxy):
                logger.warning('Proxy unavailable during initialization, skipped: %s', proxy_id)
                continue
            self._proxies.append(
                {
                    'id': proxy_id,
                    'url': proxy_url,
                    'session': _build_session(proxy_url),
                    'cooldown_until': 0.0,
                    'last_used_at': 0.0,
                }
            )

    def enabled(self):
        return len(self._proxies) > 0

    def all_proxy_ids(self):
        with self._lock:
            return [proxy['id'] for proxy in self._proxies]

    def get_session(self, proxy_id=None):
        resolved_id = proxy_id or DEFAULT_PROXY_ID
        with self._lock:
            proxy = self._find_proxy_unlocked(resolved_id)
            if proxy is None:
                raise KeyError(f'unknown proxy id: {resolved_id}')
            proxy['last_used_at'] = time.time()
            return proxy['session']

    def choose_proxy(self):
        with self._lock:
            if not self._proxies:
                return DEFAULT_PROXY_ID
            available = self._available_proxies_unlocked()
            if not available:
                proxy = min(self._proxies, key=lambda item: item['cooldown_until'])
                return proxy['id']
            if self._strategy == 'least_recently_used':
                proxy = min(available, key=lambda item: item['last_used_at'])
                proxy['last_used_at'] = time.time()
                return proxy['id']

            start = self._cursor
            total = len(self._proxies)
            for offset in range(total):
                proxy = self._proxies[(start + offset) % total]
                if proxy in available:
                    self._cursor = (start + offset + 1) % total
                    proxy['last_used_at'] = time.time()
                    return proxy['id']
            proxy = available[0]
            proxy['last_used_at'] = time.time()
            return proxy['id']

    def mark_failure(self, proxy_id, cooldown_seconds=None):
        with self._lock:
            proxy = self._find_proxy_unlocked(proxy_id)
            if proxy is None:
                return
            wait_seconds = max(0.0, float(cooldown_seconds or self._fail_cooldown_seconds))
            proxy['cooldown_until'] = max(proxy['cooldown_until'], time.time() + wait_seconds)

    def remaining_cooldown(self, proxy_id):
        with self._lock:
            proxy = self._find_proxy_unlocked(proxy_id)
            if proxy is None:
                return 0.0
            return max(0.0, proxy['cooldown_until'] - time.time())

    def _find_proxy_unlocked(self, proxy_id):
        for proxy in self._proxies:
            if proxy['id'] == proxy_id:
                return proxy
        return None

    def _available_proxies_unlocked(self):
        now = time.time()
        return [proxy for proxy in self._proxies if proxy['cooldown_until'] <= now]

    def _is_proxy_available(self, proxy):
        proxy_url = proxy.get('url')
        if not proxy_url:
            return True
        session = _build_session(proxy_url)
        try:
            response = session.get(PROXY_CHECK_URL, timeout=PROXY_CHECK_TIMEOUT_SECONDS)
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            logger.warning('Proxy health check failed for %s: %s', proxy['id'], exc)
            return False


def build_okx_proxy_pool():
    proxies = parse_proxy_pool_urls(PROXY_POOL_URLS)
    if not proxies:
        if USE_PROXY:
            proxies = [{'id': DEFAULT_PROXY_ID, 'url': HTTPS_PROXY_URL or PROXY_URL}]
        else:
            proxies = [{'id': DEFAULT_PROXY_ID, 'url': None}]
    pool = ProxyPool(
        proxies=proxies,
        strategy=PROXY_POOL_STRATEGY,
        fail_cooldown_seconds=PROXY_POOL_FAIL_COOLDOWN_SECONDS,
    )
    if proxies and not pool.enabled():
        logger.warning('All configured proxies are unavailable during initialization, fallback to direct connection.')
    return pool


_okx_proxy_pool = build_okx_proxy_pool()


def okx_proxy_pool_enabled():
    return bool(USE_PROXY_POOL and _okx_proxy_pool.enabled())


def choose_okx_proxy_id():
    if not okx_proxy_pool_enabled():
        return DEFAULT_PROXY_ID
    proxy_id = _okx_proxy_pool.choose_proxy()
    logger.debug('OKX proxy selected: %s', proxy_id)
    return proxy_id


def get_okx_session(proxy_id=None):
    if okx_proxy_pool_enabled():
        try:
            return _okx_proxy_pool.get_session(proxy_id=proxy_id)
        except KeyError:
            logger.warning('OKX proxy not found in pool, fallback to direct session: %s', proxy_id)
    if USE_PROXY:
        return _build_session(HTTPS_PROXY_URL or PROXY_URL)
    return _build_session(None)


def mark_okx_proxy_failure(proxy_id, cooldown_seconds=None):
    if not okx_proxy_pool_enabled():
        return
    _okx_proxy_pool.mark_failure(proxy_id, cooldown_seconds=cooldown_seconds)
