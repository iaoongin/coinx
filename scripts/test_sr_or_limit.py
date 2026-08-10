#!/usr/bin/env python3
"""快速连通性测试"""
import os
import sys
sys.path.insert(0, 'src')
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

load_dotenv()


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise SystemExit(f'{name} must be set')
    return value


SR_HOST = required_env('SR_HOST')
SR_PORT = int(os.getenv('SR_PORT', '9030'))
SR_USER = required_env('SR_USER')
SR_PASSWORD = required_env('SR_PASSWORD')
SR_DB = required_env('SR_DB')

engine = create_engine(
    URL.create(
        'mysql+pymysql',
        username=SR_USER,
        password=SR_PASSWORD,
        host=SR_HOST,
        port=SR_PORT,
        database=SR_DB,
    ),
    connect_args={'connect_timeout': 10},
)

try:
    with engine.connect() as conn:
        r = conn.execute(text("SELECT COUNT(*) FROM market_open_interest_hist"))
        print(f"COUNT OK: {r.scalar()}")
except Exception as e:
    print(f"COUNT FAIL: {e}")

# 简单 OR 测试
try:
    with engine.connect() as conn:
        conds = " OR ".join([f"(exchange='v{i}' AND symbol='v{i}' AND period='5m' AND event_time={i})" for i in range(100)])
        r = conn.execute(text(f"SELECT 1 FROM market_open_interest_hist WHERE {conds} LIMIT 1"))
        print(f"100 OR: OK")
except Exception as e:
    print(f"100 OR FAIL: {e}")

# 500 OR 测试
try:
    with engine.connect() as conn:
        conds = " OR ".join([f"(exchange='v{i}' AND symbol='v{i}' AND period='5m' AND event_time={i})" for i in range(500)])
        r = conn.execute(text(f"SELECT 1 FROM market_open_interest_hist WHERE {conds} LIMIT 1"))
        print(f"500 OR: OK")
except Exception as e:
    print(f"500 OR FAIL: {e}")

# 2000 OR 测试
try:
    with engine.connect() as conn:
        conds = " OR ".join([f"(exchange='v{i}' AND symbol='v{i}' AND period='5m' AND event_time={i})" for i in range(2000)])
        r = conn.execute(text(f"SELECT 1 FROM market_open_interest_hist WHERE {conds} LIMIT 1"))
        print(f"2000 OR: OK")
except Exception as e:
    print(f"2000 OR FAIL: {e}")

print("Done")
