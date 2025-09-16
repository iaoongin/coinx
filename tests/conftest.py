import sys
import os
import pytest

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web'))

@pytest.fixture
def sample_coin_data():
    """提供示例币种数据用于测试"""
    return {
        "symbol": "BTCUSDT",
        "current": {
            "timestamp": 1757922414738,
            "symbol": "BTCUSDT",
            "openInterest": 130440.0003647555,
            "time": 1757922414738
        },
        "intervals": [
            {
                "timestamp": 1757922424745,
                "symbol": "BTCUSDT",
                "interval": "5m",
                "openInterest": 183807.75562624887,
                "time": 1757922424745
            },
            {
                "timestamp": 1757922434761,
                "symbol": "BTCUSDT",
                "interval": "15m",
                "openInterest": 181677.94152889028,
                "time": 1757922434761
            },
            {
                "timestamp": 1757922444776,
                "symbol": "BTCUSDT",
                "interval": "30m",
                "openInterest": 114286.05048403078,
                "time": 1757922444776
            },
            {
                "timestamp": 1757922454784,
                "symbol": "BTCUSDT",
                "interval": "1h",
                "openInterest": 100002.63953167282,
                "time": 1757922454784
            }
        ],
        "update_time": 1757922486244
    }

@pytest.fixture
def sample_coins_data():
    """提供示例多币种数据用于测试"""
    return [
        {
            "symbol": "BTCUSDT",
            "current": {
                "timestamp": 1757922414738,
                "symbol": "BTCUSDT",
                "openInterest": 130440.0003647555,
                "time": 1757922414738
            },
            "intervals": [
                {
                    "timestamp": 1757922424745,
                    "symbol": "BTCUSDT",
                    "interval": "5m",
                    "openInterest": 183807.75562624887,
                    "time": 1757922424745
                },
                {
                    "timestamp": 1757922434761,
                    "symbol": "BTCUSDT",
                    "interval": "15m",
                    "openInterest": 181677.94152889028,
                    "time": 1757922434761
                }
            ],
            "update_time": 1757922486244
        },
        {
            "symbol": "ETHUSDT",
            "current": {
                "timestamp": 1757922414738,
                "symbol": "ETHUSDT",
                "openInterest": 80440.0003647555,
                "time": 1757922414738
            },
            "intervals": [
                {
                    "timestamp": 1757922424745,
                    "symbol": "ETHUSDT",
                    "interval": "5m",
                    "openInterest": 93807.75562624887,
                    "time": 1757922424745
                }
            ],
            "update_time": 1757922486244
        }
    ]