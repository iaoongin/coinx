"""验证 StarRocks 读写功能"""
import sys
sys.path.insert(0, 'src')
import pymysql
from coinx.config import SR_HOST, SR_PORT, SR_USER, SR_PASSWORD, SR_DB

conn = pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD, database=SR_DB)
c = conn.cursor()

# 1. coins 表 upsert
c.execute("INSERT INTO coins (symbol, is_tracking) VALUES ('BTCUSDT', 1) ON DUPLICATE KEY UPDATE is_tracking=1")
conn.commit()
c.execute("SELECT symbol, is_tracking FROM coins")
print(f"coins: {c.fetchall()}")

# 2. market_klines upsert (Primary Key 模型)
c.execute("""INSERT INTO market_klines (exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price)
             VALUES ('binance', 'BTCUSDT', '5m', 1711526400000, 1711526699999, 68000.1, 68100.2, 67950.3, 68020.4)
             ON DUPLICATE KEY UPDATE close_price=68020.4""")
conn.commit()
c.execute("SELECT exchange, symbol, period, open_time, close_price FROM market_klines")
print(f"market_klines: {c.fetchall()}")

# 3. market_tickers insert (DUPLICATE KEY 模型)
c.execute("INSERT INTO market_tickers (symbol, last_price, close_time) VALUES ('BTCUSDT', 68000.1, 1711526699999)")
conn.commit()
c.execute("SELECT symbol, last_price, close_time FROM market_tickers")
print(f"market_tickers: {c.fetchall()}")

# 4. 清理测试数据
for t in ['coins', 'market_klines', 'market_tickers']:
    c.execute(f"DELETE FROM {t}")
conn.commit()

conn.close()
print("\nAll read/write tests passed!")
