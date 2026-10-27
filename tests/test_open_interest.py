from coinx.models import MarketKline, MarketOpenInterestHist, MarketTickers
from coinx.repositories.open_interest import get_open_interest_page


def test_open_interest_page_aggregates_exchanges_and_uses_binance_supply(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, sum_open_interest_value=100000,
            circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='okx', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=50, sum_open_interest_value=50000,
        ),
        MarketOpenInterestHist(
            exchange='bybit', symbol='ETHUSDT', period='5m', event_time=100,
            sum_open_interest=20, sum_open_interest_value=40000,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    assert result['data'][0]['symbol'] == 'BTCUSDT'
    btc = next(row for row in result['data'] if row['symbol'] == 'BTCUSDT')
    assert btc['total_open_interest'] == 150.0
    assert btc['total_open_interest_value'] == 150000.0
    assert btc['latest_price'] == 2000.0
    assert btc['circulating_market_cap'] == 2000000.0
    assert btc['circulating_supply'] == 1000.0
    assert btc['oi_circulating_ratio'] == 0.075
    assert btc['oi_circulating_percent'] == 7.5
    assert btc['exchange_count'] == 2
    assert btc['exchanges'] == ['binance', 'okx']
    assert {
        detail['exchange']: detail['open_interest_value']
        for detail in btc['exchange_details']
    } == {'binance': 100000.0, 'okx': 50000.0}

    eth = next(row for row in result['data'] if row['symbol'] == 'ETHUSDT')
    assert eth['circulating_supply'] is None
    assert eth['circulating_market_cap'] is None
    assert eth['oi_circulating_percent'] is None
    assert result['stats']['without_supply'] == 1


def test_open_interest_page_calculates_bybit_value_with_binance_price(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, sum_open_interest_value=123456,
        ),
        MarketOpenInterestHist(
            exchange='bybit', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=20, sum_open_interest_value=None,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    btc = result['data'][0]
    assert btc['total_open_interest_value'] == 163456.0
    bybit = next(detail for detail in btc['exchange_details'] if detail['exchange'] == 'bybit')
    assert bybit['open_interest_value'] == 40000.0


def test_open_interest_page_includes_24h_binance_price_trend(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=7_200_000,
            sum_open_interest=100, circulating_supply=1000,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
        MarketKline(
            exchange='binance', symbol='BTCUSDT', period='5m', open_time=1_000,
            close_time=300_999, open_price=100, high_price=101, low_price=99, close_price=100,
        ),
        MarketKline(
            exchange='binance', symbol='BTCUSDT', period='5m', open_time=3_601_000,
            close_time=3_900_999, open_price=100, high_price=111, low_price=99, close_price=110,
        ),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    assert result['data'][0]['price_trend_24h'] == [
        {'time': 0, 'value': 100.0},
        {'time': 3_600_000, 'value': 110.0},
    ]


def test_open_interest_page_applies_ratio_filter_and_keyword(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, sum_open_interest_value=120000, circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='SOLUSDT', period='5m', event_time=100,
            sum_open_interest=10, sum_open_interest_value=1000, circulating_supply=1000,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
        MarketTickers(symbol='SOLUSDT', last_price=10, close_time=100),
    ])
    db_session.commit()

    result = get_open_interest_page(
        session=db_session,
        keyword='btc',
        min_ratio_percent=5,
        only_with_supply=True,
    )

    assert result['total_count'] == 1
    assert result['data'][0]['symbol'] == 'BTCUSDT'


def test_open_interest_ratio_uses_value_not_cross_exchange_quantity(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=1000000, sum_open_interest_value=60000,
            circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='gate', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=50000000, sum_open_interest_value=40000,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    btc = result['data'][0]
    assert btc['total_open_interest'] == 51000000.0
    assert btc['total_open_interest_value'] == 100000.0
    assert btc['circulating_market_cap'] == 2000000.0
    assert btc['oi_circulating_ratio'] == 0.05
    assert btc['oi_circulating_percent'] == 5.0


def test_open_interest_page_uses_supply_from_latest_binance_row(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=90, circulating_supply=900,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=200,
            sum_open_interest=100, circulating_supply=None,
        ),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    btc = next(row for row in result['data'] if row['symbol'] == 'BTCUSDT')
    assert btc['total_open_interest'] == 100.0
    assert btc['circulating_supply'] is None
    assert btc['oi_circulating_percent'] is None


def test_open_interest_page_marks_mismatched_latest_event_times(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='okx', symbol='BTCUSDT', period='5m', event_time=95,
            sum_open_interest=50,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='ETHUSDT', period='5m', event_time=100,
            sum_open_interest=20,
        ),
    ])
    db_session.commit()

    result = get_open_interest_page(session=db_session, period='5m')

    btc = next(row for row in result['data'] if row['symbol'] == 'BTCUSDT')
    assert btc['total_open_interest'] == 150.0
    assert btc['has_time_mismatch'] is True
    assert btc['data_quality'] == 'contains_stale_data'
    assert btc['stale_exchanges'] == ['okx']
    okx = next(item for item in btc['exchange_details'] if item['exchange'] == 'okx')
    assert okx['is_stale'] is True


def test_open_interest_page_can_filter_to_time_aligned_rows(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100,
        ),
        MarketOpenInterestHist(
            exchange='okx', symbol='BTCUSDT', period='5m', event_time=95,
            sum_open_interest=50,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='ETHUSDT', period='5m', event_time=100,
            sum_open_interest=20,
        ),
        MarketOpenInterestHist(
            exchange='bybit', symbol='ETHUSDT', period='5m', event_time=100,
            sum_open_interest=10,
        ),
    ])
    db_session.commit()

    result = get_open_interest_page(
        session=db_session,
        period='5m',
        only_time_aligned=True,
    )

    assert [row['symbol'] for row in result['data']] == ['ETHUSDT']


def test_open_interest_page_can_filter_to_global_latest_time(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=200,
            sum_open_interest=100,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='ETHUSDT', period='5m', event_time=100,
            sum_open_interest=20,
        ),
    ])
    db_session.commit()

    result = get_open_interest_page(
        session=db_session,
        period='5m',
        only_latest_time=True,
    )

    assert [row['symbol'] for row in result['data']] == ['BTCUSDT']


def test_open_interest_page_uses_latest_time_after_other_filters(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=200,
            sum_open_interest=100, circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='SMALLUSDT', period='5m', event_time=300,
            sum_open_interest=10, circulating_supply=1,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=300),
        MarketTickers(symbol='SMALLUSDT', last_price=1, close_time=300),
    ])
    db_session.commit()

    result = get_open_interest_page(
        session=db_session,
        period='5m',
        min_market_cap=500000,
        only_latest_time=True,
    )

    assert [row['symbol'] for row in result['data']] == ['BTCUSDT']


def test_open_interest_page_can_filter_by_circulating_market_cap(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='SOLUSDT', period='5m', event_time=100,
            sum_open_interest=10, circulating_supply=100,
        ),
        MarketTickers(symbol='BTCUSDT', last_price=2000, close_time=100),
        MarketTickers(symbol='SOLUSDT', last_price=10, close_time=100),
    ])
    db_session.commit()

    result = get_open_interest_page(
        session=db_session,
        period='5m',
        min_market_cap=500000,
    )

    assert [row['symbol'] for row in result['data']] == ['BTCUSDT']
