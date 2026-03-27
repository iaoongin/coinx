from coinx.collector.binance.series import (
    parse_top_long_short_position_ratio,
    parse_open_interest_hist,
    parse_klines,
    parse_global_long_short_account_ratio,
)
from coinx.models import BinanceKline, BinanceOpenInterestHist


def test_parse_top_long_short_position_ratio_maps_fields():
    payload = [
        {
            "symbol": "BTCUSDT",
            "longShortRatio": "1.9600",
            "longAccount": "0.6622",
            "shortAccount": "0.3378",
            "timestamp": 1711526400000,
        }
    ]

    records = parse_top_long_short_position_ratio(payload, symbol="BTCUSDT", period="5m")

    assert records == [
        {
            "symbol": "BTCUSDT",
            "period": "5m",
            "event_time": 1711526400000,
            "long_short_ratio": 1.96,
            "long_account": 0.6622,
            "short_account": 0.3378,
            "raw_json": payload[0],
        }
    ]


def test_parse_open_interest_hist_keeps_cmc_supply():
    payload = [
        {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "12345.67800000",
            "sumOpenInterestValue": "987654321.12000000",
            "CMCCirculatingSupply": "19650000.12000000",
            "timestamp": 1711526400000,
        }
    ]

    records = parse_open_interest_hist(payload, symbol="BTCUSDT", period="15m")

    assert records[0]["sum_open_interest"] == 12345.678
    assert records[0]["sum_open_interest_value"] == 987654321.12
    assert records[0]["cmc_circulating_supply"] == 19650000.12
    assert records[0]["event_time"] == 1711526400000


def test_parse_klines_maps_array_positions():
    payload = [
        [
            1711526400000,
            "68000.10",
            "68100.20",
            "67950.30",
            "68020.40",
            "123.45",
            1711526699999,
            "8398765.43",
            9876,
            "60.70",
            "4123456.78",
            "0",
        ]
    ]

    records = parse_klines(payload, symbol="BTCUSDT", period="5m")

    assert records[0]["open_time"] == 1711526400000
    assert records[0]["close_time"] == 1711526699999
    assert records[0]["open_price"] == 68000.10
    assert records[0]["trade_count"] == 9876
    assert records[0]["taker_buy_quote_volume"] == 4123456.78


def test_model_comments_are_defined_for_new_tables():
    assert BinanceOpenInterestHist.__table__.comment == "Binance 合约持仓量历史数据表"
    assert BinanceOpenInterestHist.__table__.c.cmc_circulating_supply.comment == "CMC 流通供应量"
    assert BinanceKline.__table__.comment == "Binance K线历史数据表"
    assert BinanceKline.__table__.c.open_price.comment == "开盘价"


def test_parse_global_long_short_account_ratio_uses_period_argument():
    payload = [
        {
            "symbol": "ETHUSDT",
            "longShortRatio": "0.8800",
            "longAccount": "0.4681",
            "shortAccount": "0.5319",
            "timestamp": 1711526400000,
        }
    ]

    records = parse_global_long_short_account_ratio(payload, symbol="ETHUSDT", period="1h")

    assert records[0]["symbol"] == "ETHUSDT"
    assert records[0]["period"] == "1h"
    assert records[0]["long_short_ratio"] == 0.88
