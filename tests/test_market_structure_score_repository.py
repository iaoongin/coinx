from coinx.repositories.market_structure_score import (
    _atr,
    _build_symbol_exchange_diagnostics,
    _build_exchange_metric,
    _build_symbol_report,
    _calc_momentum_score,
    _calc_position_score,
    _calc_risk_score,
    _calc_sentiment_score,
    _calc_trend_score,
    _ema,
    _load_exchange_funding_rate_maps,
    _point_float,
    _load_binance_context_map,
    _summarize_binance_context_health,
    get_market_structure_score_symbols,
    SeriesPoint,
)
from coinx.models import (
    BinanceGlobalLongShortAccountRatio,
    BinanceTopLongShortAccountRatio,
    BinanceTopLongShortPositionRatio,
)
from coinx.repositories.homepage_series import HomepageOpenInterestPoint


def test_ema_calculates_expected_recursive_average():
    values = [1, 2, 3, 4]

    result = _ema(values, 3)

    assert round(result, 6) == 3.125


def test_atr_uses_recent_true_ranges():
    points = [
        SeriesPoint(time=1, high_price=10, low_price=8, close_price=9),
        SeriesPoint(time=2, high_price=12, low_price=9, close_price=11),
        SeriesPoint(time=3, high_price=13, low_price=10, close_price=12),
    ]

    result = _atr(points, period=2)

    assert round(result, 6) == 3.0


def test_point_float_handles_homepage_open_interest_aliases():
    point = HomepageOpenInterestPoint(symbol='BTCUSDT', event_time=1, sum_open_interest=123, sum_open_interest_value=456)

    assert _point_float(point, 'open_interest', 'sum_open_interest') == 123
    assert _point_float(point, 'open_interest_value', 'sum_open_interest_value') == 456


def test_market_structure_symbols_include_tracked_and_top_volume(monkeypatch):
    monkeypatch.setattr('coinx.repositories.market_structure_score.get_active_coins', lambda: ['BTCUSDT', 'ETHUSDT'])
    monkeypatch.setattr(
        'coinx.repositories.market_structure_score.get_market_ticker_symbols',
        lambda **kwargs: ['ETHUSDT', 'SOLUSDT', 'XRPUSDT'],
    )

    symbols = get_market_structure_score_symbols(session=object(), top_volume_limit=100)

    assert symbols == ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']


def test_load_binance_context_map_batches_latest_two_rows(db_session):
    rows = [
        BinanceTopLongShortPositionRatio(symbol='BTCUSDT', period='5m', event_time=1000, long_short_ratio=1.5, long_account=1.2, short_account=0.8),
        BinanceTopLongShortPositionRatio(symbol='BTCUSDT', period='5m', event_time=2000, long_short_ratio=1.6, long_account=1.3, short_account=0.7),
        BinanceTopLongShortAccountRatio(symbol='BTCUSDT', period='5m', event_time=1000, long_short_ratio=1.4, long_account=1.1, short_account=0.9),
        BinanceTopLongShortAccountRatio(symbol='BTCUSDT', period='5m', event_time=2000, long_short_ratio=1.5, long_account=1.2, short_account=0.8),
        BinanceGlobalLongShortAccountRatio(symbol='BTCUSDT', period='5m', event_time=1000, long_short_ratio=0.9, long_account=0.6, short_account=0.4),
        BinanceGlobalLongShortAccountRatio(symbol='BTCUSDT', period='5m', event_time=2000, long_short_ratio=1.0, long_account=0.7, short_account=0.3),
        BinanceTopLongShortPositionRatio(symbol='ETHUSDT', period='5m', event_time=1000, long_short_ratio=1.1, long_account=1.0, short_account=1.0),
        BinanceTopLongShortPositionRatio(symbol='ETHUSDT', period='5m', event_time=2000, long_short_ratio=1.2, long_account=1.1, short_account=0.9),
        BinanceTopLongShortAccountRatio(symbol='ETHUSDT', period='5m', event_time=1000, long_short_ratio=1.0, long_account=1.0, short_account=1.0),
        BinanceTopLongShortAccountRatio(symbol='ETHUSDT', period='5m', event_time=2000, long_short_ratio=1.1, long_account=1.1, short_account=0.9),
        BinanceGlobalLongShortAccountRatio(symbol='ETHUSDT', period='5m', event_time=1000, long_short_ratio=0.8, long_account=0.5, short_account=0.5),
        BinanceGlobalLongShortAccountRatio(symbol='ETHUSDT', period='5m', event_time=2000, long_short_ratio=0.85, long_account=0.55, short_account=0.45),
    ]
    for row in rows:
        db_session.add(row)
    db_session.commit()

    context_map = _load_binance_context_map(db_session, ['BTCUSDT', 'ETHUSDT'], anchor_time=2500)

    assert context_map['BTCUSDT']['top_long_short_position_ratio']['current']['long_short_ratio'] == 1.6
    assert context_map['BTCUSDT']['top_long_short_position_ratio']['previous']['long_short_ratio'] == 1.5
    assert context_map['ETHUSDT']['global_long_short_account_ratio']['current']['long_short_ratio'] == 0.85
    assert context_map['ETHUSDT']['global_long_short_account_ratio']['previous']['long_short_ratio'] == 0.8


def test_summarize_binance_context_health_reports_coverage_and_lag():
    anchor_time = 600000
    context_map = {
        'BTCUSDT': {
            'top_long_short_position_ratio': {'current': {'event_time': 600000}},
            'top_long_short_account_ratio': {'current': {'event_time': 300000}},
            'global_long_short_account_ratio': {'current': {'event_time': None}},
        },
        'ETHUSDT': {
            'top_long_short_position_ratio': {'current': {'event_time': None}},
            'top_long_short_account_ratio': {'current': {'event_time': None}},
            'global_long_short_account_ratio': {'current': {'event_time': None}},
        },
    }

    summary = _summarize_binance_context_health(context_map, anchor_time=anchor_time, symbol_count=2)

    assert round(summary['overall_coverage_percent'], 2) == 33.33
    assert summary['available_symbols'] == 1
    assert summary['ready_symbols'] == 0
    assert summary['latest_event_time'] == 600000
    assert summary['max_lag_bars'] == 1
    assert summary['lag_minutes'] == 5
    assert summary['worst_dimension']['label'] == '全市场账户比'
    assert summary['dimensions'][0]['label'] == '大户持仓比'


def test_component_scoring_rules_match_thresholds():
    assert _calc_trend_score(110, 100, 90) == (30, '多头趋势')
    assert _calc_trend_score(80, 90, 100) == (-30, '空头趋势')
    assert _calc_momentum_score(0.12, 1.2) == (25, '多')
    assert _calc_momentum_score(-0.12, 1.2) == (-25, '空')
    assert _calc_position_score(110, 100, 120, 100) == (25, '多头开仓推动')
    assert _calc_position_score(90, 100, 120, 100) == (-25, '空头开仓推动')
    assert _calc_sentiment_score(0.1, -0.1) == (10, '大户偏多，散户偏空/离场')
    assert _calc_sentiment_score(-0.1, 0.1) == (-10, '大户偏空，散户偏多/接盘')


def test_build_exchange_metric_uses_24h_quote_volume_for_volume_ratio():
    anchor_time = 1711526400000
    previous_time = anchor_time - (5 * 60 * 1000)
    kline_by_time = {
        previous_time: SeriesPoint(time=previous_time, high_price=99, low_price=95, close_price=98, quote_volume=100),
        anchor_time: SeriesPoint(time=anchor_time, high_price=102, low_price=97, close_price=100, quote_volume=200),
    }
    oi_by_time = {
        previous_time: SeriesPoint(time=previous_time, open_interest=1000, open_interest_value=98000),
        anchor_time: SeriesPoint(time=anchor_time, open_interest=1010, open_interest_value=101000),
    }

    metric = _build_exchange_metric(
        exchange='binance',
        symbol='BTCUSDT',
        oi_by_time=oi_by_time,
        kline_by_time=kline_by_time,
        taker_maps_by_period={},
        anchor_time=anchor_time,
        quote_volume_24h=28800,
    )

    assert metric is not None
    assert metric['quote_volume_24h'] == 28800
    assert metric['volume_ratio'] == 2


def test_build_symbol_exchange_diagnostics_reports_missing_anchor_reason():
    symbol = 'BTCUSDT'
    symbol_anchor_time = 2000
    exchange_maps = {
        'binance': (
            {symbol: {1000: SeriesPoint(time=1000, open_interest=100), 2000: SeriesPoint(time=2000, open_interest=120)}},
            {symbol: {1000: SeriesPoint(time=1000, close_price=100), 2000: SeriesPoint(time=2000, close_price=101)}},
            {},
            {},
        ),
        'okx': (
            {symbol: {1000: SeriesPoint(time=1000, open_interest=90)}},
            {symbol: {1000: SeriesPoint(time=1000, close_price=99), 2000: SeriesPoint(time=2000, close_price=100)}},
            {},
            {},
        ),
        'bybit': (
            {},
            {symbol: {2000: SeriesPoint(time=2000, close_price=102)}},
            {},
            {},
        ),
    }

    diagnostics = _build_symbol_exchange_diagnostics(
        exchange_maps=exchange_maps,
        symbol=symbol,
        symbol_anchor_time=symbol_anchor_time,
        included_exchanges=['binance'],
        enabled_exchanges=['binance', 'okx', 'bybit'],
    )

    assert diagnostics[0]['exchange'] == 'binance'
    assert diagnostics[0]['included'] is True
    assert diagnostics[0]['reason'] == 'included'
    assert diagnostics[1]['exchange'] == 'okx'
    assert diagnostics[1]['included'] is False
    assert diagnostics[1]['reason'] == 'anchor_missing_oi'
    assert diagnostics[1]['reason_label'] == '锚点缺少 OI'
    assert diagnostics[1]['detail']['latest_common_time'] == 1000
    assert diagnostics[2]['exchange'] == 'bybit'
    assert diagnostics[2]['reason'] == 'missing_oi'


def test_build_symbol_report_aggregates_exchange_scores():
    exchange_metrics = [
        {
            'exchange': 'binance',
            'current_time': 1711526400000,
            'current_price': 110,
            'previous_price': 100,
            'open_interest': 1200,
            'previous_open_interest': 1000,
            'open_interest_value': 132000,
            'previous_open_interest_value': 100000,
            'quote_volume': 10000,
            'taker_buy_quote_volume': 6200,
            'taker_net_pressure': 2400,
            'taker_net_pressure_ratio': 0.24,
            'volume_ratio': 1.5,
            'price_move_ratio': 0.10,
            'open_interest_change_ratio': 0.32,
            'ema20': 100,
            'ema60': 90,
            'atr': 1,
            'atr_ratio': 0.01,
            'trend_score': 30,
            'trend_direction': '多头趋势',
            'momentum_score': 25,
            'momentum_direction': '多',
            'position_score': 25,
            'position_structure': '多头开仓推动',
            'score_direction_hint': 'long',
        },
        {
            'exchange': 'okx',
            'current_time': 1711526400000,
            'current_price': 109,
            'previous_price': 99,
            'open_interest': 800,
            'previous_open_interest': 700,
            'open_interest_value': 10000,
            'previous_open_interest_value': 9000,
            'quote_volume': 7000,
            'taker_buy_quote_volume': 3800,
            'taker_net_pressure': 600,
            'taker_net_pressure_ratio': 0.0857142857,
            'volume_ratio': 1.1,
            'price_move_ratio': 0.01010101,
            'open_interest_change_ratio': 0.2457,
            'ema20': 99,
            'ema60': 91,
            'atr': 1,
            'atr_ratio': 0.0092,
            'trend_score': 30,
            'trend_direction': '多头趋势',
            'momentum_score': 0,
            'momentum_direction': '弱',
            'position_score': 10,
            'position_structure': '蓄势增仓',
            'score_direction_hint': 'long',
        },
    ]

    report = _build_symbol_report(
        symbol='BTCUSDT',
        exchange_metrics=exchange_metrics,
        binance_context={
            'top_long_short_position_ratio': {
                'current': {'long_short_ratio': 1.9},
                'previous': {'long_short_ratio': 1.7},
            },
            'top_long_short_account_ratio': {
                'current': {'long_short_ratio': 1.4},
                'previous': {'long_short_ratio': 1.3},
            },
            'global_long_short_account_ratio': {
                'current': {'long_short_ratio': 0.9},
                'previous': {'long_short_ratio': 1.0},
            },
        },
        funding_rate=0.0001,
        anchor_time=1711526400000,
        exchange_diagnostics=[
            {'exchange': 'binance', 'included': True, 'reason': 'included', 'reason_label': '已纳入', 'detail': None},
            {'exchange': 'okx', 'included': True, 'reason': 'included', 'reason_label': '已纳入', 'detail': None},
            {'exchange': 'bybit', 'included': False, 'reason': 'missing_kline', 'reason_label': '缺少 K 线', 'detail': None},
        ],
    )

    assert report['symbol'] == 'BTCUSDT'
    assert report['trade_signal'] == '强多'
    assert report['risk_level'] == '低'
    assert report['trend_direction'] == '多头趋势'
    assert report['momentum_direction'] == '多'
    assert report['position_structure'] in ('多头开仓推动', '蓄势增仓')
    assert report['sentiment_state'] == '大户偏多，散户偏空/离场'
    assert len(report['exchange_scores']) == 2
    assert report['exchange_scores'][0]['exchange'] == 'binance'
    assert report['exchange_scores'][0]['current_price'] == 110
    assert report['exchange_scores'][0]['ema20'] == 100
    assert report['exchange_scores'][0]['taker_net_pressure_ratio'] == 0.24
    assert report['funding_rate'] == 0.0001
    assert report['missing_exchanges'] == ['bybit']
    assert report['exchange_diagnostics'][2]['reason_label'] == '缺少 K 线'
    assert round(sum(item['share_percent'] for item in report['exchange_open_interest']), 6) == 100.0
    assert report['exchange_open_interest'][0]['weighted_score'] > 0


def test_build_symbol_report_uses_weighted_exchange_funding_rate_for_risk():
    exchange_metrics = [
        {
            'exchange': 'binance',
            'current_time': 1711526400000,
            'current_price': 110,
            'previous_price': 100,
            'open_interest': 1200,
            'previous_open_interest': 1000,
            'open_interest_value': 90000,
            'previous_open_interest_value': 80000,
            'quote_volume': 10000,
            'taker_buy_quote_volume': 6200,
            'taker_net_pressure': 2400,
            'taker_net_pressure_ratio': 0.24,
            'volume_ratio': 1.5,
            'price_move_ratio': 0.01,
            'open_interest_change_ratio': 0.12,
            'ema20': 100,
            'ema60': 90,
            'atr': 1,
            'atr_ratio': 0.01,
            'trend_score': 30,
            'trend_direction': '多头趋势',
            'momentum_score': 25,
            'momentum_direction': '多',
            'position_score': 25,
            'position_structure': '多头开仓推动',
            'score_direction_hint': 'long',
            'funding_rate': 0.0012,
        },
        {
            'exchange': 'okx',
            'current_time': 1711526400000,
            'current_price': 109,
            'previous_price': 99,
            'open_interest': 800,
            'previous_open_interest': 700,
            'open_interest_value': 10000,
            'previous_open_interest_value': 9000,
            'quote_volume': 7000,
            'taker_buy_quote_volume': 3800,
            'taker_net_pressure': 600,
            'taker_net_pressure_ratio': 0.0857142857,
            'volume_ratio': 1.1,
            'price_move_ratio': 0.01,
            'open_interest_change_ratio': 0.1457,
            'ema20': 99,
            'ema60': 91,
            'atr': 1,
            'atr_ratio': 0.0092,
            'trend_score': 30,
            'trend_direction': '多头趋势',
            'momentum_score': 0,
            'momentum_direction': '弱',
            'position_score': 10,
            'position_structure': '蓄势增仓',
            'score_direction_hint': 'long',
            'funding_rate': -0.0002,
        },
    ]

    report = _build_symbol_report(
        symbol='BTCUSDT',
        exchange_metrics=exchange_metrics,
        binance_context={},
        funding_rate=None,
        anchor_time=1711526400000,
    )

    assert round(report['funding_rate'], 6) == round(((0.0012 * 90000) + (-0.0002 * 10000)) / 100000, 6)
    assert report['risk_score'] == -10


def test_load_exchange_funding_rate_maps_filters_to_target_symbols(monkeypatch):
    monkeypatch.setattr(
        'coinx.repositories.market_structure_score.EXCHANGE_FUNDING_LOADERS',
        {
            'binance': lambda: {
                'BTCUSDT': {'lastFundingRate': '0.0010'},
                'ETHUSDT': {'lastFundingRate': '0.0005'},
            },
            'okx': lambda: {
                'BTCUSDT': {'fundingRate': '0.0008'},
                'SOLUSDT': {'fundingRate': '-0.0002'},
            },
        },
    )

    result = _load_exchange_funding_rate_maps(['binance', 'okx'], ['BTCUSDT'])

    assert result == {
        'binance': {'BTCUSDT': 0.001},
        'okx': {'BTCUSDT': 0.0008},
    }
