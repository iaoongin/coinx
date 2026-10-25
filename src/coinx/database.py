from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import scoped_session, sessionmaker, DeclarativeBase
from coinx import config

MARKET_TABLE_NAMES = frozenset({
    'market_klines',
    'market_open_interest_hist',
    'market_taker_buy_sell_vol',
    'market_funding_rate',
    'market_tickers',
    'market_snapshots',
    'market_futures_smart_money_signal',
})


def tables_for_initialization(metadata):
    """Return transactional tables, excluding CK-owned market tables."""
    tables = list(metadata.sorted_tables)
    if getattr(config, 'MARKET_WRITE_BACKEND', 'mysql') != 'clickhouse':
        return tables
    return [table for table in tables if table.name not in MARKET_TABLE_NAMES]

# 创建数据库引擎
engine = create_engine(
    config.DATABASE_URI, 
    pool_recycle=3600, 
    pool_size=10, 
    max_overflow=20,
    pool_pre_ping=True,
    echo=False  # 设置为True可以查看生成的SQL语句
)

# 创建线程安全的会话
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# 创建基类
class Base(DeclarativeBase):
    pass

#为了方便查询，可以将 query 属性绑定到 Base
Base.query = db_session.query_property()

def init_db():
    """初始化数据库，创建所有表"""
    # 在这里导入定义模型的所有模块，以便它们在元数据上正确注册。
    # 否则，您必须在调用 init_db() 之前先导入它们。
    
    from coinx import models
    
    Base.metadata.create_all(
        bind=engine,
        tables=tables_for_initialization(Base.metadata),
    )
    _ensure_market_open_interest_columns()


def _ensure_market_open_interest_columns(bind=None):
    """Apply additive MySQL columns for deployments without a migration runner."""
    bind = bind or engine
    if getattr(bind.dialect, 'name', '') != 'mysql':
        return

    inspector = inspect(bind)
    if 'market_open_interest_hist' not in inspector.get_table_names():
        return
    columns = {column['name'] for column in inspector.get_columns('market_open_interest_hist')}
    if 'circulating_supply' in columns:
        return

    with bind.begin() as connection:
        connection.execute(text(
            'ALTER TABLE market_open_interest_hist '
            'ADD COLUMN circulating_supply DECIMAL(30, 8) DEFAULT NULL '
            'COMMENT \'流通供应量（Binance/CMC）\' '
            'AFTER sum_open_interest_value'
        ))

def get_session():
    """获取一个新的会话"""
    return db_session()
