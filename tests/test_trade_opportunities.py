from flask import Flask
import werkzeug
from types import SimpleNamespace

from coinx.models import MarketKline
from coinx.repositories.market_structure_series import load_market_structure_aggregated_kline_maps
from coinx.repositories import trade_opportunities as trade_opportunities_repository
from coinx.repositories.trade_opportunities import (
    FOUR_HOURS_MS,
    ONE_HOUR_MS,
    _aggregate_closed_5m_klines,
    _build_trade_plan,
    _entry_score,
    _entry_state,
    _higher_timeframe_price_trend_score,
    _trend_state,
)
from coinx.web.routes.api_data import api_data_bp


def _client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_risk_score_reduces_both_directions_without_reversing_them():
    assert _entry_score(50, 30, -20) == 60
    assert _entry_score(-50, -30, -20) == -60
    assert _entry_score(10, 0, -20) == 0
    assert _entry_score(-10, 0, -20) == 0


def test_score_states_use_documented_thresholds():
    assert _trend_state(30) == '强多趋势'
    assert _trend_state(-30) == '强空趋势'
    assert _entry_state(65) == '可做多'
    assert _entry_state(20) == '等待回踩'
    assert _entry_state(-20) == '等待反弹'
    assert _entry_state(-65) == '可做空'


def _points(highs, lows):
    return [SimpleNamespace(high_price=high, low_price=low) for high, low in zip(highs, lows)]


def _closed_points(count, start=0, close_start=100):
    return [
        SimpleNamespace(
            open_time=start + index * 5 * 60 * 1000,
            high_price=close_start + index + 1,
            low_price=close_start + index - 1,
            close_price=close_start + index,
            quote_volume=10,
        )
        for index in range(count)
    ]


def test_aggregate_5m_klines_requires_a_complete_utc_bucket():
    points = _closed_points(24)
    points.pop(4)

    one_hour = _aggregate_closed_5m_klines(points, ONE_HOUR_MS)
    four_hour = _aggregate_closed_5m_klines(points, FOUR_HOURS_MS)

    assert len(one_hour) == 1
    assert one_hour[0].open_time == ONE_HOUR_MS
    assert one_hour[0].close_price == 123
    assert one_hour[0].quote_volume == 120
    assert four_hour == []


def test_sql_aggregation_returns_only_complete_5m_buckets(db_session):
    for index in range(24):
        if index == 16:
            continue
        open_time = index * 5 * 60 * 1000
        db_session.add(MarketKline(
            exchange='binance',
            symbol='BTCUSDT',
            period='5m',
            open_time=open_time,
            close_time=open_time + 5 * 60 * 1000 - 1,
            open_price=100 + index,
            high_price=101 + index,
            low_price=99 + index,
            close_price=100 + index,
            quote_volume=10,
        ))
    db_session.commit()

    aggregated = load_market_structure_aggregated_kline_maps(
        db_session,
        'binance',
        ['BTCUSDT'],
        upper_bound=23 * 5 * 60 * 1000,
        intervals={'1h': ONE_HOUR_MS},
        lookback_points=2,
    )

    points = list(aggregated['1h']['BTCUSDT'].values())
    assert len(points) == 1
    assert points[0].open_time == 0
    assert points[0].close_price == 111
    assert points[0].high_price == 112
    assert points[0].low_price == 99


def test_higher_timeframe_trend_requires_60_bars_and_scores_alignment_and_slope():
    points = _closed_points(72)

    score, state = _higher_timeframe_price_trend_score(points, alignment_weight=3, slope_weight=1)

    assert (score, state) == (4, '震荡')
    assert _higher_timeframe_price_trend_score(points[:59], alignment_weight=3, slope_weight=1) == (0, '数据不足')


def test_trade_plan_prefers_higher_timeframe_targets_over_5m_targets():
    higher_points = _closed_points(7)
    higher_points[2] = SimpleNamespace(
        open_time=higher_points[2].open_time,
        high_price=130,
        low_price=101,
        close_price=110,
        quote_volume=10,
    )
    plan = _build_trade_plan(
        '可做多',
        {
            'current_price': 100,
            'ema20': 99,
            'atr': 4,
            '_plan_points': _points(
                [99, 101, 102, 110, 103, 104, 120, 105, 106, 107],
                [98, 97, 90, 95, 96, 94, 97, 98, 99, 100],
            ),
            '_higher_timeframe_points': {'1h': higher_points, '4h': []},
        },
    )

    assert plan['target_source'] == 'higher_timeframe'
    assert plan['tp1'] == 130


def test_trade_plan_prefers_higher_timeframe_structure_for_stop_loss():
    higher_points = _closed_points(7)
    higher_points[4] = SimpleNamespace(
        open_time=higher_points[4].open_time,
        high_price=110,
        low_price=90,
        close_price=100,
        quote_volume=10,
    )
    plan = _build_trade_plan(
        '可做多',
        {
            'current_price': 100,
            'ema20': 99,
            'atr': 4,
            '_plan_points': _points(
                [99, 101, 102, 110, 103, 104, 120, 105, 106, 107],
                [98, 97, 90, 95, 96, 94, 97, 98, 99, 100],
            ),
            '_higher_timeframe_points': {'1h': higher_points, '4h': []},
        },
    )

    assert plan['stop_source'] == 'higher_timeframe'
    assert plan['stop_loss'] == 89


def test_long_trade_plan_uses_latest_structural_stop_and_targets_then_r_extension():
    plan = _build_trade_plan(
        '可做多',
        {
            'current_price': 100,
            'ema20': 99,
            'atr': 4,
            '_plan_points': _points(
                [99, 101, 102, 110, 103, 104, 120, 105, 106, 107],
                [98, 97, 90, 95, 96, 94, 97, 98, 99, 100],
            ),
        },
    )

    assert plan['entry_price'] == 100
    assert plan['stop_loss'] == 93
    assert (plan['tp1'], plan['tp2'], plan['tp3']) == (110, 120, 121)
    assert plan['stop_loss_percent'] == -7
    assert plan['tp1_percent'] == 10
    assert plan['tp1_r'] == round(10 / 7, 4)
    assert plan['space_status'] == 'adequate'


def test_short_trade_plan_uses_direction_adjusted_percentages_and_ema_entry():
    plan = _build_trade_plan(
        '等待反弹',
        {
            'current_price': 100,
            'ema20': 100,
            'atr': 4,
            '_plan_points': _points(
                [101, 102, 110, 105, 104, 106, 103, 102, 101, 100],
                [102, 101, 90, 95, 96, 94, 97, 98, 99, 100],
            ),
        },
    )

    assert plan['entry_price'] == 100
    assert plan['stop_loss'] == 107
    assert (plan['tp1'], plan['tp2'], plan['tp3']) == (94, 90, 86)
    assert plan['stop_loss_percent'] == -7
    assert plan['tp1_percent'] == 6


def test_trade_plan_uses_atr_stop_and_r_targets_without_confirmed_structure():
    plan = _build_trade_plan(
        '等待回踩',
        {
            'current_price': 100,
            'ema20': 100,
            'atr': 4,
            '_plan_points': _points(range(90, 100), range(80, 90)),
        },
    )

    assert plan['stop_loss'] == 94
    assert (plan['tp1'], plan['tp2'], plan['tp3']) == (106, 112, 118)


def test_trade_plan_marks_near_structural_target_as_insufficient_space():
    plan = _build_trade_plan(
        '可做多',
        {
            'current_price': 100,
            'ema20': 99,
            'atr': 4,
            '_plan_points': _points(
                [99, 100, 105, 101, 100, 99, 98],
                [98, 97, 90, 96, 97, 98, 99],
            ),
        },
    )

    assert plan['tp1'] == 105
    assert plan['tp1_r'] < 1
    assert plan['space_status'] == 'insufficient'
    assert plan['space_reason'] == '前方结构空间不足（TP1 < 1R）'


def test_trade_plan_marks_missing_binance_data_without_creating_prices():
    plan = _build_trade_plan('可做多', None)

    assert plan == {
        'source_exchange': 'binance',
        'status': 'unavailable',
        'reason': 'Binance 数据不足',
    }


def test_opportunity_snapshot_reuses_same_closed_5m_cache_key(monkeypatch):
    calls = []
    with trade_opportunities_repository._SNAPSHOT_CACHE_LOCK:
        trade_opportunities_repository._SNAPSHOT_CACHE.clear()
        trade_opportunities_repository._SNAPSHOT_INFLIGHT.clear()
    monkeypatch.setattr(trade_opportunities_repository, 'latest_closed_5m_open_time', lambda _: 1)
    monkeypatch.setattr(
        trade_opportunities_repository,
        '_build_trade_opportunity_snapshot',
        lambda symbols, exchanges, anchor: calls.append((symbols, exchanges, anchor)) or {'data': []},
    )

    first = trade_opportunities_repository.get_trade_opportunity_snapshot(
        symbols=['BTCUSDT'], now_ms=1, exchanges=['binance'],
    )
    second = trade_opportunities_repository.get_trade_opportunity_snapshot(
        symbols=['BTCUSDT'], now_ms=1, exchanges=['binance'],
    )

    assert first is second
    assert calls == [(('BTCUSDT',), ('binance',), 1)]


def test_opportunity_api_hides_non_candidates_and_neutral_trends_by_default(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_trade_opportunity_snapshot',
        lambda **kwargs: {
            'cache_update_time': 1,
            'summary': {'total_symbols': 3},
            'data': [
                {'symbol': 'BTCUSDT', 'entry_state': '可做多', 'entry_score': 80, 'trend_state': '强多趋势'},
                {'symbol': 'ETHUSDT', 'entry_state': '观望', 'entry_score': 0},
                {'symbol': 'SOLUSDT', 'entry_state': '等待回踩', 'entry_score': 35, 'trend_state': '震荡'},
            ],
        },
    )
    client = _client()

    response = client.get('/api/trade-opportunities')

    assert response.status_code == 200
    assert [item['symbol'] for item in response.get_json()['data']] == ['BTCUSDT']
    response = client.get('/api/trade-opportunities?scope=all')
    assert len(response.get_json()['data']) == 3
