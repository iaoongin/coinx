from coinx.models import BinanceKline, BinanceOpenInterestHist, BinanceTakerBuySellVol
from coinx.repositories.homepage_series import FIVE_MINUTES_MS


EXPECTED_INTERVALS = ['5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h']
FULL_HISTORY_POINTS = 2017
START_TIME_MS = 1_700_000_000_000


def seed_complete_homepage_series(session, symbol='BTCUSDT', start_time_ms=START_TIME_MS, periods=FULL_HISTORY_POINTS):
    for index in range(periods):
        event_time = start_time_ms + index * FIVE_MINUTES_MS
        close_price = 100.0 + index
        open_price = close_price - 0.5
        open_interest = 1000.0 + index * 10.0
        open_interest_value = open_interest * close_price
        taker_base = 2000.0 + index * 5.0

        session.add(
            BinanceOpenInterestHist(
                symbol=symbol,
                period='5m',
                event_time=event_time,
                sum_open_interest=open_interest,
                sum_open_interest_value=open_interest_value,
                cmc_circulating_supply=None,
                raw_json={},
            )
        )
        session.add(
            BinanceKline(
                symbol=symbol,
                period='5m',
                open_time=event_time,
                close_time=event_time + FIVE_MINUTES_MS - 1,
                open_price=open_price,
                high_price=close_price + 1,
                low_price=open_price - 1,
                close_price=close_price,
                volume=100 + index,
                quote_volume=1000.0 + index,
                trade_count=10 + index,
                taker_buy_base_volume=50 + index,
                taker_buy_quote_volume=600.0 + index,
                raw_json=[],
            )
        )
        session.add(
            BinanceTakerBuySellVol(
                symbol=symbol,
                period='5m',
                event_time=event_time,
                buy_sell_ratio=1.2 + index * 0.001,
                buy_vol=taker_base,
                sell_vol=taker_base * 0.8,
                raw_json={},
            )
        )

    session.commit()


def normalize_changes(changes):
    if isinstance(changes, list):
        return {item['interval']: item for item in changes}
    return changes


def assert_complete_interval_contract(coin):
    assert coin['current_open_interest'] is not None
    assert coin['current_open_interest_value'] is not None
    assert coin['current_price'] is not None
    assert coin['current_open_interest_formatted'] != 'N/A'
    assert coin['current_open_interest_value_formatted'] != 'N/A'
    assert coin['current_price_formatted'] != 'N/A'

    changes = normalize_changes(coin['changes'])

    assert set(changes.keys()) == set(EXPECTED_INTERVALS)
    assert set(coin['net_inflow'].keys()) == set(EXPECTED_INTERVALS)

    for interval in EXPECTED_INTERVALS:
        change = changes[interval]
        assert change['current_price'] is not None, f'{interval}: current_price'
        assert change['open_interest'] is not None, f'{interval}: open_interest'
        assert change['open_interest_value'] is not None, f'{interval}: open_interest_value'
        assert change['price_change'] is not None, f'{interval}: price_change'
        assert change['price_change_percent'] is not None, f'{interval}: price_change_percent'
        assert change['current_price_formatted'] != 'N/A', f'{interval}: current_price_formatted'
        assert change['open_interest_formatted'] != 'N/A', f'{interval}: open_interest_formatted'
        assert change['open_interest_value_formatted'] != 'N/A', f'{interval}: open_interest_value_formatted'
        assert isinstance(coin['net_inflow'][interval], (int, float)), f'{interval}: net_inflow'
