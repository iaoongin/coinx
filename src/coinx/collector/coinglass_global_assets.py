"""Live CoinGlass global-assets collection and response decoding."""

from __future__ import annotations

import base64
import json
import time
import zlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


DEFAULT_PAGE_URL = "https://www.coinglass.com/zh?category=crypto"
DEFAULT_API_URL = "https://capi.coinglass.com/api/home/v2/coinMarkets"
DEFAULT_CATEGORY = "crypto"
DEFAULT_PAGE_SIZE = 100
DEFAULT_PAGES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

RESPONSE_KEY_SEEDS = {
    "55": "170b070da9654622",
    "66": "d6537d845a964081",
    "77": "863f08689c97435b",
}

REQUESTED_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("资产", "symbol"),
    ("持仓/市值", "oiMarketCapRadio"),
    ("持仓变化(1h)", "h1OiChangePercent"),
    ("持仓变化(4h)", "h4OiChangePercent"),
    ("持仓变化(24h)", "h24OiChangePercent"),
    ("市值", "marketCap"),
    ("持仓", "openInterest"),
    ("价格", "price"),
    ("价格变化(24h)", "h24PriceChangePercent"),
    ("资金费率", "avgFundingRateByOi"),
    ("24小时成交额", "volUsd"),
    ("成交额变化(24h)", "h24VolChangePercent"),
    ("24小时爆仓", "h24LiquidationUsd"),
)

MODEL_FIELD_MAP = {
    "oi_market_cap_ratio": "oiMarketCapRadio",
    "h1_oi_change": "h1OiChangePercent",
    "h4_oi_change": "h4OiChangePercent",
    "h24_oi_change": "h24OiChangePercent",
    "market_cap": "marketCap",
    "open_interest": "openInterest",
    "price": "price",
    "h24_price_change": "h24PriceChangePercent",
    "avg_funding_rate": "avgFundingRateByOi",
    "h24_volume": "volUsd",
    "h24_volume_change": "h24VolChangePercent",
    "h24_liquidation": "h24LiquidationUsd",
}

class CoinGlassError(RuntimeError):
    """Raised when CoinGlass returns an invalid or incomplete response."""


def create_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Origin": "https://www.coinglass.com",
            "Referer": DEFAULT_PAGE_URL,
        }
    )
    return session


def _request_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "encryption": "true",
        "cache-ts-v2": str(int(time.time() * 1000)),
        "language": "zh",
    }


def _api_path(url: str) -> str:
    path = urlsplit(url).path
    api_marker = path.find("/api/")
    if api_marker < 0:
        raise CoinGlassError(f"cannot derive CoinGlass API path from {url}")
    return path[api_marker:]


def _response_key_seed(
    version: str,
    response_headers: Mapping[str, str],
    request_headers: Mapping[str, str],
    request_url: str,
) -> str:
    if version in RESPONSE_KEY_SEEDS:
        return RESPONSE_KEY_SEEDS[version]
    if version == "0":
        return request_headers["cache-ts-v2"]
    if version == "2":
        response_time = response_headers.get("time")
        if not response_time:
            raise CoinGlassError("CoinGlass response v=2 is missing the time header")
        return response_time
    if version == "1":
        return _api_path(request_url)
    raise CoinGlassError(f"unsupported CoinGlass response encryption version: {version}")


def _aes_key(key: str) -> bytes:
    encoded = key.encode("utf-8")
    if len(encoded) != 16:
        raise CoinGlassError("CoinGlass response key is not a valid AES-128 key")
    return encoded


def _decrypt_and_inflate(value: str, key: str) -> str:
    try:
        ciphertext = base64.b64decode(value, validate=True)
        decryptor = Cipher(algorithms.AES(_aes_key(key)), modes.ECB()).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
    except (ValueError, TypeError) as exc:
        raise CoinGlassError("invalid CoinGlass encrypted payload") from exc

    if not padded:
        raise CoinGlassError("empty CoinGlass encrypted payload")
    padding_size = padded[-1]
    if padding_size < 1 or padding_size > 16 or padded[-padding_size:] != bytes([padding_size]) * padding_size:
        raise CoinGlassError("invalid CoinGlass AES padding")
    compressed = padded[:-padding_size]
    for window_bits in (31, 15, -15):
        try:
            decompressed = zlib.decompress(compressed, window_bits)
            return decompressed.decode("utf-8")
        except (zlib.error, UnicodeDecodeError):
            continue
    raise CoinGlassError("CoinGlass payload is not valid compressed UTF-8 data")


def decode_response(
    response: requests.Response,
    request_headers: Mapping[str, str],
    request_url: str,
) -> Dict[str, Any]:
    try:
        envelope = response.json()
    except ValueError as exc:
        raise CoinGlassError("CoinGlass returned non-JSON data") from exc
    if not isinstance(envelope, dict):
        raise CoinGlassError("CoinGlass response envelope is not an object")
    if str(envelope.get("code")) != "0":
        raise CoinGlassError(
            f"CoinGlass returned code={envelope.get('code')!r}, msg={envelope.get('msg')!r}"
        )
    encrypted_data = envelope.get("data")
    if not isinstance(encrypted_data, str):
        raise CoinGlassError("CoinGlass response does not contain encrypted data")

    version = response.headers.get("v")
    user_header = response.headers.get("user")
    if not version or not user_header:
        raise CoinGlassError("CoinGlass response is missing encryption headers")
    seed = _response_key_seed(version, response.headers, request_headers, request_url)
    base_key = base64.b64encode(seed.encode("utf-8")).decode("ascii")[:16]
    response_key = _decrypt_and_inflate(user_header, base_key)
    try:
        decoded = json.loads(_decrypt_and_inflate(encrypted_data, response_key))
    except json.JSONDecodeError as exc:
        raise CoinGlassError("CoinGlass decrypted data is not JSON") from exc
    if not isinstance(decoded, dict):
        raise CoinGlassError("CoinGlass decrypted data is not an object")
    return decoded


def _fetch_page(
    session: requests.Session,
    api_url: str,
    page: int,
    page_size: int,
    category: str,
    timeout: float,
) -> Dict[str, Any]:
    headers = _request_headers()
    params = {
        "sort": "",
        "order": "",
        "keyword": "",
        "pageNum": page,
        "pageSize": page_size,
        "tags": category,
        "ex": "all",
    }
    response = session.get(api_url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = decode_response(response, headers, response.url)
    rows = payload.get("list")
    if not isinstance(rows, list):
        raise CoinGlassError(f"CoinGlass page {page} does not contain a list")
    return {
        "page": page,
        "page_size": page_size,
        "returned": len(rows),
        "total": payload.get("total"),
        "rows": rows,
    }


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def normalize_row(row: Mapping[str, Any], snapshot_time: int) -> Optional[Dict[str, Any]]:
    symbol = str(row.get("symbol") or "").strip()
    if not symbol:
        return None
    normalized = {"symbol": symbol, "snapshot_time": int(snapshot_time)}
    for target, source in MODEL_FIELD_MAP.items():
        normalized[target] = _number(row.get(source))
    return normalized


def fetch_pages(
    *,
    pages: int = DEFAULT_PAGES,
    page_size: int = DEFAULT_PAGE_SIZE,
    category: str = DEFAULT_CATEGORY,
    api_url: str = DEFAULT_API_URL,
    timeout: float = 30.0,
    session: Optional[requests.Session] = None,
    snapshot_time: Optional[int] = None,
) -> Dict[str, Any]:
    if pages < 1:
        raise ValueError("pages must be at least 1")
    if page_size < 1:
        raise ValueError("page_size must be at least 1")

    own_session = session is None
    http = session or create_session()
    try:
        page_results = [
            _fetch_page(http, api_url, page, page_size, category, timeout)
            for page in range(1, pages + 1)
        ]
    finally:
        if own_session:
            http.close()

    captured_at = int(snapshot_time if snapshot_time is not None else time.time() * 1000)
    rows: List[Dict[str, Any]] = []
    for page_result in page_results:
        for row in page_result["rows"]:
            normalized = normalize_row(row, captured_at)
            if normalized is not None:
                rows.append(normalized)
    total = page_results[0].get("total") if page_results else None
    return {
        "source": {
            "page_url": DEFAULT_PAGE_URL,
            "api_url": api_url,
            "category": category,
            "pages": pages,
            "page_size": page_size,
        },
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_time": captured_at,
        "total": total,
        "returned": len(rows),
        "pages": page_results,
        "rows": rows,
    }
