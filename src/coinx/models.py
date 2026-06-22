from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    DECIMAL,
    func,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    UniqueConstraint,
)

from coinx.database import Base


SQLITE_BIGINT_PK = BigInteger().with_variant(Integer, 'sqlite')


class Coin(Base):

    __tablename__ = 'coins'

    symbol = Column(String(50), primary_key=True)
    is_tracking = Column(Boolean, default=True)

    base_asset = Column(String(100))
    quote_asset = Column(String(100))
    margin_asset = Column(String(100))

    price_precision = Column(Integer)
    quantity_precision = Column(Integer)
    base_asset_precision = Column(Integer)
    quote_precision = Column(Integer)

    status = Column(String(50))
    onboard_date = Column(BigInteger)
    delivery_date = Column(BigInteger)

    contract_type = Column(String(50))
    underlying_type = Column(String(50))

    liquidation_fee = Column(DECIMAL(10, 6))
    maint_margin_percent = Column(DECIMAL(10, 4))
    required_margin_percent = Column(DECIMAL(10, 4))

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<Coin(symbol='{self.symbol}', is_tracking={self.is_tracking})>"


class MarketSnapshot(Base):

    __tablename__ = 'market_snapshots'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    batch_id = Column(String(50), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    price = Column(DECIMAL(24, 8))
    open_interest = Column(DECIMAL(24, 8))
    open_interest_value = Column(DECIMAL(24, 8))
    data_json = Column(JSON)
    snapshot_time = Column(BigInteger, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<MarketSnapshot(symbol='{self.symbol}', time={self.snapshot_time})>"


class MarketTickers(Base):

    __tablename__ = 'market_tickers'
    __table_args__ = (
        Index('idx_mt_symbol', 'symbol'),
        Index('idx_mt_close_time', 'close_time'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    price_change = Column(DECIMAL(24, 8))
    price_change_percent = Column(DECIMAL(20, 8))
    weighted_avg_price = Column(DECIMAL(24, 8))
    last_price = Column(DECIMAL(24, 8))
    last_qty = Column(DECIMAL(24, 8))
    open_price = Column(DECIMAL(24, 8))
    high_price = Column(DECIMAL(24, 8))
    low_price = Column(DECIMAL(24, 8))
    volume = Column(DECIMAL(30, 8))
    quote_volume = Column(DECIMAL(30, 8))
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    first_id = Column(BigInteger)
    last_id = Column(BigInteger)
    count = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<MarketTickers(symbol='{self.symbol}', close_time={self.close_time})>"


class MarketOpenInterestHist(Base):

    __tablename__ = 'market_open_interest_hist'
    __table_args__ = (
        UniqueConstraint('exchange', 'symbol', 'period', 'event_time', name='uk_moih_exchange_symbol_period_time'),
        Index('idx_moih_exchange_symbol_period_time', 'exchange', 'symbol', 'period', 'event_time'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False)
    symbol = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    event_time = Column(BigInteger, nullable=False)
    sum_open_interest = Column(DECIMAL(30, 8))
    sum_open_interest_value = Column(DECIMAL(30, 8))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class MarketKline(Base):

    __tablename__ = 'market_klines'
    __table_args__ = (
        UniqueConstraint('exchange', 'symbol', 'period', 'open_time', name='uk_mk_exchange_symbol_period_open_time'),
        Index('idx_mk_exchange_symbol_period_open_time', 'exchange', 'symbol', 'period', 'open_time'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False)
    symbol = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    open_time = Column(BigInteger, nullable=False)
    close_time = Column(BigInteger, nullable=False)
    open_price = Column(DECIMAL(30, 8), nullable=False)
    high_price = Column(DECIMAL(30, 8), nullable=False)
    low_price = Column(DECIMAL(30, 8), nullable=False)
    close_price = Column(DECIMAL(30, 8), nullable=False)
    volume = Column(DECIMAL(30, 8))
    quote_volume = Column(DECIMAL(30, 8))
    trade_count = Column(BigInteger)
    taker_buy_base_volume = Column(DECIMAL(30, 8))
    taker_buy_quote_volume = Column(DECIMAL(30, 8))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class MarketTakerBuySellVol(Base):

    __tablename__ = 'market_taker_buy_sell_vol'
    __table_args__ = (
        UniqueConstraint('exchange', 'symbol', 'period', 'event_time', name='uk_mtbsv_exchange_symbol_period_time'),
        Index('idx_mtbsv_exchange_symbol_period_time', 'exchange', 'symbol', 'period', 'event_time'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    exchange = Column(String(20), nullable=False)
    symbol = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    event_time = Column(BigInteger, nullable=False)
    buy_sell_ratio = Column(DECIMAL(20, 8))
    buy_vol = Column(DECIMAL(30, 8))
    sell_vol = Column(DECIMAL(30, 8))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<MarketTakerBuySellVol(exchange='{self.exchange}', symbol='{self.symbol}', period='{self.period}')>"


class MarketFundingRate(Base):
    """资金费率历史表"""

    __tablename__ = 'market_funding_rate'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_symbol_period_time'),
        Index('idx_symbol_period', 'symbol', 'period'),
        Index('idx_symbol_time', 'symbol', 'event_time'),
        {'comment': '资金费率历史'}
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, comment='交易对名称')
    period = Column(String(10), nullable=False, default='5m', comment='采集周期')
    event_time = Column(BigInteger, nullable=False, comment='采集时间戳（毫秒）')
    funding_rate = Column(Numeric(20, 8), comment='上次结算费率')
    predicted_rate = Column(Numeric(20, 8), comment='预测费率（下次结算）')
    next_funding_time = Column(BigInteger, comment='下次结算时间戳（毫秒）')
    mark_price = Column(Numeric(20, 8), comment='标记价格')
    exchange = Column(String(20), nullable=False, default='binance', comment='交易所')
    created_at = Column(DateTime, server_default=func.now(), comment='创建时间')

    def __repr__(self):
        return f"<MarketFundingRate(symbol='{self.symbol}', predicted_rate={self.predicted_rate})>"
