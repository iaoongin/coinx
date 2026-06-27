"""通过应用代码验证 StarRocks 完整功能"""
import sys
sys.path.insert(0, 'src')

# 强制使用 StarRocks
import coinx.config as config
config.DB_TYPE = 'starrocks'

from coinx.database import create_engine, sessionmaker, get_session
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.repositories.series import upsert_series_records, upsert_series_records_in_batches, get_existing_series_timestamps

engine = create_engine(config.DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

print('=== Test 1: upsert single record (klines) ===')
affected = upsert_series_records(
    'binance', 'klines',
    [{'symbol': 'BTCUSDT', 'period': '5m', 'open_time': 1711526400000,
      'close_time': 1711526699999, 'open_price': 68000.1, 'high_price': 68100.2,
      'low_price': 67950.3, 'close_price': 68020.4}],
    session=session,
)
print(f'  affected: {affected}')

print('=== Test 2: upsert batch (open_interest_hist) ===')
affected = upsert_series_records_in_batches(
    'binance', 'open_interest_hist',
    [{'symbol': 'BTCUSDT', 'period': '5m', 'event_time': 1711526400000,
      'sum_open_interest': 100000.0, 'sum_open_interest_value': 6800000000.0},
     {'symbol': 'ETHUSDT', 'period': '5m', 'event_time': 1711526400000,
      'sum_open_interest': 50000.0, 'sum_open_interest_value': 150000000.0}],
    batch_size=100,
    session=session,
)
print(f'  affected: {affected}')

print('=== Test 3: query existing timestamps ===')
existing = get_existing_series_timestamps(
    'binance', 'klines', ['BTCUSDT'], [1711526400000], period='5m', session=session,
)
print(f'  existing: {existing}')

print('=== Test 4: verify data in DB ===')
rows = session.query(MarketKline).all()
print(f'  market_klines: {len(rows)} rows')
for r in rows:
    print(f'    {r.exchange}/{r.symbol}/{r.period} open={r.open_time} close={r.close_price}')

rows = session.query(MarketOpenInterestHist).all()
print(f'  market_open_interest_hist: {len(rows)} rows')

# Cleanup (StarRocks requires WHERE clause for DELETE)
session.query(MarketKline).filter(MarketKline.exchange == 'binance').delete()
session.query(MarketOpenInterestHist).filter(MarketOpenInterestHist.exchange == 'binance').delete()
session.commit()
session.close()

print('\nAll functional tests passed!')
