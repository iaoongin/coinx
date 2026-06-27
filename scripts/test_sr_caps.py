import sys; sys.path.insert(0, 'src')
from coinx.config import SR_HOST, SR_PORT, SR_USER, SR_PASSWORD, SR_DB
import pymysql
conn = pymysql.connect(host=SR_HOST, port=SR_PORT, user=SR_USER, password=SR_PASSWORD, database=SR_DB)
c = conn.cursor()

c.execute('SELECT VERSION()')
print('Version:', c.fetchone()[0])

try:
    c.execute('SHOW BACKENDS')
    backends = c.fetchall()
    print('Backends:', len(backends))
except Exception as e:
    print('SHOW BACKENDS:', e)

# Test INSERT + UPDATE as upsert alternative
try:
    c.execute("INSERT INTO coins (symbol, is_tracking) VALUES ('TESTCOIN', 1)")
    c.execute("UPDATE coins SET is_tracking=0 WHERE symbol='TESTCOIN'")
    conn.commit()
    c.execute("SELECT symbol, is_tracking FROM coins WHERE symbol='TESTCOIN'")
    print('INSERT+UPDATE:', c.fetchone())
    c.execute("DELETE FROM coins WHERE symbol='TESTCOIN'")
    conn.commit()
    print('DELETE: OK')
except Exception as e:
    print('Error:', e)

conn.close()
