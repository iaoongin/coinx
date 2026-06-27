"""在 StarRocks 上执行 schema_starrocks.sql 建表"""
import sys, traceback
sys.path.insert(0, 'src')

from coinx.config import SR_HOST, SR_PORT, SR_USER, SR_PASSWORD, SR_DB
import pymysql

print(f'Connecting to StarRocks: {SR_HOST}:{SR_PORT}/{SR_DB}')
print(f'User: {SR_USER}')

conn = pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD, database=SR_DB)
cursor = conn.cursor()

# Show version
cursor.execute('SELECT VERSION()')
print(f'StarRocks version: {cursor.fetchone()[0]}')

# Drop old tables
tables = ['market_taker_buy_sell_vol', 'market_klines', 'market_open_interest_hist', 'market_tickers', 'market_snapshots', 'coins']
for t in tables:
    try:
        cursor.execute(f'DROP TABLE IF EXISTS {t}')
    except Exception as e:
        print(f'DROP {t} error: {e}')

# Execute DDL
with open('sql/schema_starrocks.sql', 'r', encoding='utf-8') as f:
    sql = f.read()

stmts = []
for s in sql.split(';'):
    s = s.strip()
    if not s:
        continue
    # 跳过纯注释语句
    lines = [l for l in s.split('\n') if l.strip() and not l.strip().startswith('--')]
    if lines:
        stmts.append(s)

print(f'Found {len(stmts)} SQL statements')

for i, stmt in enumerate(stmts):
    first_line = stmt.split('\n')[0][:80]
    try:
        cursor.execute(stmt)
        conn.commit()
        print(f'OK [{i+1}]: {first_line}')
    except Exception as e:
        print(f'FAIL [{i+1}]: {first_line}')
        print(f'  Error: {e}')
        conn.rollback()

# Verify
cursor.execute('SHOW TABLES')
rows = cursor.fetchall()
print(f'\nCreated {len(rows)} tables:')
for r in rows:
    print(f'  - {r[0]}')

conn.close()
