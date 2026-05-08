import pytest
import time
from coinx.repositories.market_tickers import (
    save_market_tickers,
    get_market_tickers,
    get_market_ticker_symbols,
    get_latest_close_time,
    delete_old_records,
)
from coinx.models import MarketTickers


def seed_market_tickers(db_session, close_time, count=10):
    records = []
    for i in range(count):
        records.append({
            'symbol': f'COIN{i}USDT',
            'price_change': -10.0 + i,
            'price_change_percent': -10.0 + i,
            'weighted_avg_price': 100.0 + i,
            'last_price': 100.0 + i,
            'last_qty': 1.0 + i,
            'open_price': 90.0 + i,
            'high_price': 110.0 + i,
            'low_price': 80.0 + i,
            'volume': 1000.0 + i * 100,
            'quote_volume': 10000.0 + i * 1000,
            'open_time': close_time - 86400000,
            'close_time': close_time,
            'first_id': 1000 + i,
            'last_id': 2000 + i,
            'count': 100 + i,
        })
    
    for record in records:
        db_session.add(MarketTickers(**record))
    db_session.commit()
    return records


def test_save_market_tickers(db_session):
    close_time = int(time.time() * 1000)
    records = [
        {
            'symbol': 'BTCUSDT',
            'price_change': -100.0,
            'price_change_percent': -2.0,
            'weighted_avg_price': 50000.0,
            'last_price': 49000.0,
            'last_qty': 0.5,
            'open_price': 50000.0,
            'high_price': 51000.0,
            'low_price': 48000.0,
            'volume': 1000.0,
            'quote_volume': 50000000.0,
            'open_time': close_time - 86400000,
            'close_time': close_time,
            'first_id': 1000,
            'last_id': 2000,
            'count': 100,
        },
        {
            'symbol': 'ETHUSDT',
            'price_change': 50.0,
            'price_change_percent': 1.5,
            'weighted_avg_price': 3000.0,
            'last_price': 3050.0,
            'last_qty': 1.0,
            'open_price': 3000.0,
            'high_price': 3100.0,
            'low_price': 2900.0,
            'volume': 2000.0,
            'quote_volume': 6000000.0,
            'open_time': close_time - 86400000,
            'close_time': close_time,
            'first_id': 1001,
            'last_id': 2001,
            'count': 200,
        },
    ]
    
    count = save_market_tickers(records, collect_time=close_time, session=db_session)
    
    assert count == 2
    saved = db_session.query(MarketTickers).all()
    assert len(saved) == 2
    assert saved[0].symbol == 'BTCUSDT'
    assert saved[0].close_time == close_time


def test_save_market_tickers_empty(db_session):
    count = save_market_tickers([], session=db_session)
    assert count == 0


def test_get_market_tickers_by_price_change_down(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time)
    
    results = get_market_tickers(rank_type='price_change', direction='down', limit=5, session=db_session)
    
    assert len(results) == 5
    assert results[0].price_change_percent < results[1].price_change_percent


def test_get_market_tickers_by_price_change_up(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time)
    
    results = get_market_tickers(rank_type='price_change', direction='up', limit=5, session=db_session)
    
    assert len(results) == 5
    assert results[0].price_change_percent > results[1].price_change_percent


def test_get_market_tickers_by_quote_volume(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time)
    
    results = get_market_tickers(rank_type='quote_volume', limit=5, session=db_session)
    
    assert len(results) == 5
    assert results[0].quote_volume >= results[1].quote_volume


def test_get_market_tickers_with_limit(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time, count=20)
    
    results = get_market_tickers(rank_type='quote_volume', limit=5, session=db_session)
    
    assert len(results) == 5


def test_get_market_ticker_symbols_returns_symbols_only(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time, count=20)

    symbols = get_market_ticker_symbols(rank_type='quote_volume', limit=5, session=db_session)

    assert len(symbols) == 5
    assert symbols[0].endswith('USDT')
    assert isinstance(symbols[0], str)


def test_get_market_tickers_empty(db_session):
    results = get_market_tickers(rank_type='price_change', session=db_session)
    
    assert len(results) == 0


def test_get_latest_close_time(db_session):
    close_time = int(time.time() * 1000)
    seed_market_tickers(db_session, close_time)
    
    latest = get_latest_close_time(session=db_session)
    
    assert latest == close_time


def test_get_latest_close_time_no_data(db_session):
    latest = get_latest_close_time(session=db_session)
    
    assert latest is None


def test_delete_old_records(db_session):
    now = int(time.time() * 1000)
    old_time = now - (10 * 24 * 60 * 60 * 1000)
    new_time = now
    
    seed_market_tickers(db_session, old_time, count=2)
    seed_market_tickers(db_session, new_time, count=2)
    
    deleted = delete_old_records(days=7, session=db_session)
    
    assert deleted == 2
    remaining = db_session.query(MarketTickers).filter(MarketTickers.close_time == new_time).all()
    assert len(remaining) == 2
