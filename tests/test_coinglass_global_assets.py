import base64
import json
import zlib
from pathlib import Path

import requests
import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from coinx.collector.coinglass_global_assets import normalize_row


def _load_script_module():
    import importlib.util

    path = Path("scripts/fetch_coinglass_global_assets.py")
    spec = importlib.util.spec_from_file_location("fetch_coinglass_global_assets", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _encrypt(value, key):
    if isinstance(key, str):
        key = key.encode("utf-8")
    compressor = zlib.compressobj(wbits=31)
    compressed = compressor.compress(value.encode("utf-8")) + compressor.flush()
    padding_size = 16 - len(compressed) % 16
    padded = compressed + bytes([padding_size]) * padding_size
    encryptor = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode("ascii")


def test_decode_response_handles_coinglass_v66_encryption():
    module = _load_script_module()
    seed = module.RESPONSE_KEY_SEEDS["66"]
    base_key = base64.b64encode(seed.encode("utf-8"))[:16]
    response_key = "a68397f69c0441d8"
    payload = {"total": 1, "list": [{"symbol": "BTC", "price": 65000}]}

    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({
        "code": "0",
        "data": _encrypt(json.dumps(payload), response_key),
    }).encode("utf-8")
    response.headers["v"] = "66"
    response.headers["user"] = _encrypt(response_key, base_key)

    decoded = module.decode_response(response, {"cache-ts-v2": "123"}, module.DEFAULT_API_URL)

    assert decoded == payload


def test_select_requested_fields_keeps_missing_values_as_none():
    module = _load_script_module()

    selected = module.select_requested_fields({"symbol": "BTC", "price": 65000})

    assert selected["资产"] == "BTC"
    assert selected["价格"] == 65000
    assert selected["市值"] is None
    assert selected["持仓变化(4h)"] is None


def test_normalize_row_preserves_percentage_point_fields():
    normalized = normalize_row({
        'symbol': 'BTC',
        'oiMarketCapRadio': 0.0342,
        'h1OiChangePercent': -0.8,
        'h4OiChangePercent': 2.33,
        'h24OiChangePercent': -5.63,
        'h24PriceChangePercent': -4.25,
        'avgFundingRateByOi': 0.0044,
        'h24VolChangePercent': -25.86,
    }, 1000)

    assert normalized['oi_market_cap_ratio'] == 0.0342
    assert normalized['h1_oi_change'] == pytest.approx(-0.8)
    assert normalized['h4_oi_change'] == pytest.approx(2.33)
    assert normalized['h24_oi_change'] == pytest.approx(-5.63)
    assert normalized['h24_price_change'] == pytest.approx(-4.25)
    assert normalized['avg_funding_rate'] == pytest.approx(0.0044)
    assert normalized['h24_volume_change'] == pytest.approx(-25.86)
