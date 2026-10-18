from decimal import Decimal

import pytest

from coinx.collector.binance.smart_money import (
    SmartMoneyApiError,
    fetch_smart_money_signal_list,
    parse_smart_money_signal_list,
)
from coinx.collector.binance import smart_money as smart_money_module
from coinx.repositories.smart_money import (
    get_smart_money_scope_symbols,
    load_latest_smart_money_for_symbol,
    load_smart_money_page,
    normalize_smart_money_row,
    save_smart_money_signals,
)
from coinx.models import Coin, MarketFuturesSmartMoneySignal, MarketFundingRate, MarketKline


def _payload(rows=None, code='000000'):
    return {
        'code': code,
        'message': None,
        'messageDetail': None,
        'data': rows if rows is not None else [{
            'timeRange': '30m',
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'netNotional': '110912348.85873988',
            'longNotional': 85661983.66624987,
            'shortNotional': 25250365.19249,
            'longTraders': 170,
            'longWhales': 89,
            'shortTraders': 167,
            'shortWhales': 59,
            'longQty': '1023.77500000',
            'shortQty': '301.68500000',
            'longAvgEntryPrice': '83672.66603135',
            'shortAvgEntryPrice': '83697.78143590',
        }],
    }


def test_parse_smart_money_signal_list_keeps_all_rows():
    rows = parse_smart_money_signal_list(_payload(), time_range='30m')

    assert len(rows) == 1
    assert rows[0]['symbol'] == 'BTCUSDT'
    assert rows[0]['timeRange'] == '30m'


def test_parse_smart_money_signal_list_rejects_business_error():
    with pytest.raises(SmartMoneyApiError, match='code=000002'):
        parse_smart_money_signal_list(_payload(rows=[], code='000002'))


def test_fetch_smart_money_signal_list_uses_web_bapi_url_and_time_range(monkeypatch):
    calls = {}

    class Response:
        def json(self):
            return _payload()

    def fake_request(_session, url, **kwargs):
        calls['url'] = url
        calls['kwargs'] = kwargs
        return Response()

    monkeypatch.setattr(smart_money_module, 'request_with_binance_retry', fake_request)

    rows = fetch_smart_money_signal_list(session=object())

    assert len(rows) == 1
    assert calls['url'].endswith('/bapi/futures/v1/public/future/smart-money/signal/list')
    assert calls['kwargs']['params'] == {'timeRange': '30m'}


def test_fetch_smart_money_signal_list_supports_all_time_range(monkeypatch):
    calls = {}

    class Response:
        def json(self):
            return _payload()

    def fake_request(_session, url, **kwargs):
        calls['kwargs'] = kwargs
        return Response()

    monkeypatch.setattr(smart_money_module, 'request_with_binance_retry', fake_request)

    fetch_smart_money_signal_list(time_range='ALL', session=object())

    assert calls['kwargs']['params'] == {'timeRange': 'ALL'}


def test_save_smart_money_signal_is_decimal_and_idempotent(db_session):
    row = normalize_smart_money_row(
        _payload()['data'][0],
        snapshot_time=1_700_000_000_000,
        period='15m',
    )
    assert row['net_notional'] == Decimal('110912348.85873988')
    assert row['long_qty'] == Decimal('1023.77500000')

    assert save_smart_money_signals([row], session=db_session) == 1
    row['side'] = 'SELL'
    assert save_smart_money_signals([row], session=db_session) == 1

    saved = db_session.query(MarketFuturesSmartMoneySignal).all()
    assert len(saved) == 1
    assert saved[0].side == 'SELL'
    assert abs(saved[0].net_notional - Decimal('110912348.85873988')) < Decimal('0.00000001')


def test_missing_numeric_fields_stay_null():
    row = normalize_smart_money_row(
        {'symbol': 'ABCUSDT', 'timeRange': '30m'},
        snapshot_time=1_700_000_000_000,
    )

    assert row['symbol'] == 'ABCUSDT'
    assert row['net_notional'] is None
    assert row['long_traders'] is None


def test_load_smart_money_page_defaults_to_latest_snapshot(db_session):
    first = normalize_smart_money_row(
        {**_payload()['data'][0], 'symbol': 'BTCUSDT', 'netNotional': '100'},
        snapshot_time=1_700_000_000_000,
    )
    latest = normalize_smart_money_row(
        {**_payload()['data'][0], 'symbol': 'ETHUSDT', 'netNotional': '200'},
        snapshot_time=1_700_000_900_000,
    )
    save_smart_money_signals([first, latest], session=db_session)
    db_session.add(MarketFundingRate(
        symbol='ETHUSDT',
        period='5m',
        event_time=1_700_000_900_000,
        mark_price=Decimal('2500.12345678'),
        exchange='binance',
    ))
    trend_start = 1_699_999_200_000
    db_session.add_all([
        MarketKline(
            exchange='binance',
            symbol='ETHUSDT',
            period='5m',
            open_time=trend_start + index * 5 * 60 * 1000,
            close_time=trend_start + index * 5 * 60 * 1000 + 5 * 60 * 1000 - 1,
            open_price=2500 + index,
            high_price=2501 + index,
            low_price=2499 + index,
            close_price=2500 + index,
        )
        for index in range(24)
    ])
    db_session.commit()

    result = load_smart_money_page(session=db_session, page_size=10, time_range='30m')

    assert result['snapshot_time'] == 1_700_000_900_000
    assert result['total_count'] == 1
    assert result['data'][0]['symbol'] == 'ETHUSDT'
    assert result['data'][0]['net_notional'] == 200.0
    assert result['data'][0]['long_short_ratio'] == pytest.approx(
        85661983.66624987 / 25250365.19249,
    )
    assert result['data'][0]['mark_price'] == pytest.approx(2500.12345678)
    assert len(result['data'][0]['price_trend']) == 2
    assert result['data'][0]['price_trend'][0]['value'] == pytest.approx(2511)
    assert result['data'][0]['price_trend'][1]['value'] == pytest.approx(2523)
    assert result['snapshots'] == [1_700_000_900_000, 1_700_000_000_000]


def test_load_latest_smart_money_for_symbol_returns_each_window(db_session):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': 'BTCUSDT', 'timeRange': time_range, 'netNotional': str(net_notional)},
            snapshot_time=snapshot_time,
            time_range=time_range,
        )
        for time_range, snapshot_time, net_notional in (
            ('30m', 1_700_000_000_000, '100'),
            ('30m', 1_700_000_900_000, '200'),
            ('ALL', 1_700_000_800_000, '300'),
        )
    ]
    save_smart_money_signals(rows, session=db_session)

    result = load_latest_smart_money_for_symbol('btcusdt', session=db_session)

    assert [row['time_range'] for row in result] == ['30m', 'ALL']
    assert [row['net_notional'] for row in result] == [200.0, 300.0]
    assert all(row['symbol'] == 'BTCUSDT' for row in result)


def test_load_smart_money_page_supports_snapshot_filter_and_paging(db_session):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol, 'netNotional': str(index)},
            snapshot_time=1_700_000_000_000,
        )
        for index, symbol in enumerate(('BTCUSDT', 'ETHUSDT', 'SOLUSDT'), start=1)
    ]
    save_smart_money_signals(rows, session=db_session)

    result = load_smart_money_page(
        session=db_session,
        snapshot_time=1_700_000_000_000,
        page=2,
        page_size=1,
        time_range='30m',
        sort_by='symbol',
        sort_order='asc',
    )

    assert result['total_count'] == 3
    assert [row['symbol'] for row in result['data']] == ['ETHUSDT']


def test_load_smart_money_page_can_sort_by_long_short_ratio(db_session):
    rows = [
        normalize_smart_money_row(
            {
                **_payload()['data'][0],
                'symbol': symbol,
                'longNotional': str(long_notional),
                'shortNotional': '100',
            },
            snapshot_time=1_700_000_000_000,
        )
        for symbol, long_notional in (('BTCUSDT', 100), ('ETHUSDT', 300))
    ]
    save_smart_money_signals(rows, session=db_session)

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        sort_by='long_short_ratio',
        sort_order='desc',
        page_size=10,
    )

    assert [row['symbol'] for row in result['data']] == ['ETHUSDT', 'BTCUSDT']


def test_load_smart_money_page_filters_by_absolute_net_notional(db_session):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol, 'netNotional': str(net_notional)},
            snapshot_time=1_700_000_000_000,
        )
        for symbol, net_notional in (
            ('BTCUSDT', 2_000_000),
            ('ETHUSDT', 500_000),
            ('SOLUSDT', -3_000_000),
        )
    ]
    save_smart_money_signals(rows, session=db_session)

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional='1000000',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert result['total_count'] == 2
    assert [row['symbol'] for row in result['data']] == ['BTCUSDT', 'SOLUSDT']


def test_load_smart_money_page_filters_by_underlying_type(db_session):
    Coin.__table__.create(bind=db_session.bind, checkfirst=True)
    db_session.add_all([
        Coin(symbol='BTCUSDT', underlying_type='COIN'),
        Coin(symbol='SNDKUSDT', underlying_type='EQUITY'),
        Coin(symbol='HKTECHUSDT', underlying_type='HK_EQUITY'),
    ])
    db_session.commit()
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol},
            snapshot_time=1_700_000_000_000,
        )
        for symbol in ('BTCUSDT', 'SNDKUSDT', 'HKTECHUSDT')
    ]
    save_smart_money_signals(rows, session=db_session)

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional=None,
        max_mark_price_spread_percent=None,
        max_price_trend_percent=None,
        underlying_type='COIN',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert result['total_count'] == 1
    assert [row['symbol'] for row in result['data']] == ['BTCUSDT']

    traditional_result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional=None,
        max_mark_price_spread_percent=None,
        max_price_trend_percent=None,
        underlying_type='TRADITIONAL',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert traditional_result['total_count'] == 2
    assert [row['symbol'] for row in traditional_result['data']] == ['HKTECHUSDT', 'SNDKUSDT']


def test_load_smart_money_page_filters_by_mark_price_distance(db_session, monkeypatch):
    rows = [
        normalize_smart_money_row(
            {
                **_payload()['data'][0],
                'symbol': symbol,
                'longAvgEntryPrice': str(long_price),
                'shortAvgEntryPrice': str(short_price),
            },
            snapshot_time=1_700_000_000_000,
        )
        for symbol, long_price, short_price in (
            ('BTCUSDT', 100, 130),
            ('ETHUSDT', 100, 120),
            ('SOLUSDT', 100, 130),
        )
    ]
    save_smart_money_signals(rows, session=db_session)
    monkeypatch.setattr(
        'coinx.repositories.smart_money.load_latest_funding_rates',
        lambda symbols, session=None: {
            'BTCUSDT': {'mark_price': 104},   # 多头均价距离 4%，命中
            'ETHUSDT': {'mark_price': 110},   # 空头均价距离约 8.33%，命中
            'SOLUSDT': {'mark_price': 115},   # 两侧都超过 10%，排除
        },
    )

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional=None,
        max_mark_price_spread_percent='10',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert result['total_count'] == 2
    assert [row['symbol'] for row in result['data']] == ['BTCUSDT', 'ETHUSDT']


def test_load_smart_money_page_filters_by_maximum_seven_day_trend(db_session, monkeypatch):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol},
            snapshot_time=1_700_000_000_000,
        )
        for symbol in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
    ]
    save_smart_money_signals(rows, session=db_session)

    trend_changes = {'BTCUSDT': -10, 'ETHUSDT': -20, 'SOLUSDT': -30}

    def fake_attach_price_trends(signal_rows, session=None):
        return [
            {
                **dict(row),
                'price_trend': [
                    {'value': 100},
                    {'value': 100 + trend_changes[row['symbol']]},
                ],
            }
            for row in signal_rows
        ]

    monkeypatch.setattr(
        'coinx.repositories.smart_money._attach_price_trends',
        fake_attach_price_trends,
    )

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional=None,
        max_mark_price_spread_percent=None,
        max_price_trend_percent='-20',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert result['total_count'] == 2
    assert [row['symbol'] for row in result['data']] == ['ETHUSDT', 'SOLUSDT']


def test_load_smart_money_page_filters_by_seven_day_peak_drawdown(db_session, monkeypatch):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol},
            snapshot_time=1_700_000_000_000,
        )
        for symbol in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
    ]
    save_smart_money_signals(rows, session=db_session)

    trend_values = {
        'BTCUSDT': [100, 95, 90],
        'ETHUSDT': [100, 90, 80],
        'SOLUSDT': [100, 105, 95],
    }

    def fake_attach_price_trends(signal_rows, session=None):
        return [
            {**dict(row), 'price_trend': [{'value': value} for value in trend_values[row['symbol']]]}
            for row in signal_rows
        ]

    monkeypatch.setattr(
        'coinx.repositories.smart_money._attach_price_trends',
        fake_attach_price_trends,
    )

    result = load_smart_money_page(
        session=db_session,
        time_range='30m',
        min_net_notional=None,
        max_mark_price_spread_percent=None,
        max_price_trend_percent=None,
        max_price_drawdown_percent='-20',
        sort_by='symbol',
        sort_order='asc',
        page_size=10,
    )

    assert result['total_count'] == 1
    assert [row['symbol'] for row in result['data']] == ['ETHUSDT']


def test_save_smart_money_signals_creates_missing_table(db_session):
    table = MarketFuturesSmartMoneySignal.__table__
    table.drop(bind=db_session.bind, checkfirst=True)
    row = normalize_smart_money_row(
        _payload()['data'][0],
        snapshot_time=1_700_000_000_000,
    )

    assert save_smart_money_signals([row], session=db_session) == 1
    assert db_session.query(MarketFuturesSmartMoneySignal).count() == 1


def test_get_smart_money_scope_symbols_uses_latest_snapshot_and_deduplicates(db_session):
    rows = [
        normalize_smart_money_row(
            {**_payload()['data'][0], 'symbol': symbol, 'netNotional': str(net_notional)},
            snapshot_time=snapshot_time,
            time_range='ALL',
        )
        for symbol, net_notional, snapshot_time in (
            ('OLDUSDT', 20_000_000, 1_700_000_000_000),
            ('BTCUSDT', 12_000_000, 1_700_000_000_000),
            ('BTCUSDT', 15_000_000, 1_700_000_000_900),
            ('ETHUSDT', -11_000_000, 1_700_000_000_900),
            ('SMALLUSDT', 9_000_000, 1_700_000_000_900),
        )
    ]
    save_smart_money_signals(rows, session=db_session)

    symbols = get_smart_money_scope_symbols(
        min_net_notional=10_000_000,
        time_range='ALL',
        period='15m',
        session=db_session,
    )

    assert set(symbols) == {'BTCUSDT', 'ETHUSDT'}
