from coinx.models import MarketOpenInterestHist, MarketTickers
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
    assert btc['oi_circulating_percent'] == 15.0
    assert btc['exchange_count'] == 2
    assert btc['exchanges'] == ['binance', 'okx']

    eth = next(row for row in result['data'] if row['symbol'] == 'ETHUSDT')
    assert eth['circulating_supply'] is None
    assert eth['circulating_market_cap'] is None
    assert eth['oi_circulating_percent'] is None
    assert result['stats']['without_supply'] == 1


def test_open_interest_page_applies_ratio_filter_and_keyword(db_session):
    db_session.add_all([
        MarketOpenInterestHist(
            exchange='binance', symbol='BTCUSDT', period='5m', event_time=100,
            sum_open_interest=100, circulating_supply=1000,
        ),
        MarketOpenInterestHist(
            exchange='binance', symbol='SOLUSDT', period='5m', event_time=100,
            sum_open_interest=10, circulating_supply=1000,
        ),
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
