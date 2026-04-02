from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    DECIMAL,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
)

from coinx.database import Base


SQLITE_BIGINT_PK = BigInteger().with_variant(Integer, 'sqlite')


class Coin(Base):
    """币种配置模型"""

    __tablename__ = 'coins'

    symbol = Column(String(50), primary_key=True, comment='交易对符号')
    is_tracking = Column(Boolean, default=True, comment='是否启用跟踪')

    base_asset = Column(String(100), comment='基础资产')
    quote_asset = Column(String(100), comment='计价资产')
    margin_asset = Column(String(100), comment='保证金资产')

    price_precision = Column(Integer, comment='价格精度')
    quantity_precision = Column(Integer, comment='数量精度')
    base_asset_precision = Column(Integer, comment='基础资产精度')
    quote_precision = Column(Integer, comment='报价精度')

    status = Column(String(50), comment='交易状态')
    onboard_date = Column(BigInteger, comment='上线时间')
    delivery_date = Column(BigInteger, comment='交割时间')

    contract_type = Column(String(50), comment='合约类型')
    underlying_type = Column(String(50), comment='标的类型')

    liquidation_fee = Column(DECIMAL(10, 6), comment='强平费率')
    maint_margin_percent = Column(DECIMAL(10, 4), comment='维持保证金率')
    required_margin_percent = Column(DECIMAL(10, 4), comment='所需保证金率')

    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    def __repr__(self):
        return f"<Coin(symbol='{self.symbol}', is_tracking={self.is_tracking})>"


class MarketSnapshot(Base):
    """市场数据快照模型"""

    __tablename__ = 'market_snapshots'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='自增ID')
    batch_id = Column(String(50), nullable=False, index=True, comment='批次ID')
    symbol = Column(String(20), nullable=False, index=True, comment='交易对符号')
    price = Column(DECIMAL(24, 8), comment='当前价格')
    open_interest = Column(DECIMAL(24, 8), comment='持仓量')
    open_interest_value = Column(DECIMAL(24, 8), comment='持仓价值')
    data_json = Column(JSON, comment='完整数据JSON')
    snapshot_time = Column(BigInteger, nullable=False, index=True, comment='快照时间戳，毫秒')
    created_at = Column(DateTime, default=datetime.now, comment='记录创建时间')

    def __repr__(self):
        return f"<MarketSnapshot(symbol='{self.symbol}', time={self.snapshot_time})>"


class BinanceTopLongShortPositionRatio(Base):
    """Binance 大户持仓量多空比历史表"""

    __tablename__ = 'binance_top_long_short_position_ratio'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_btlspr_symbol_period_time'),
        Index('idx_btlspr_symbol_period_time', 'symbol', 'period', 'event_time'),
        {'comment': 'Binance 大户持仓量多空比历史数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对，例如 BTCUSDT')
    period = Column(String(10), nullable=False, comment='时间周期，例如 5m、15m、1h')
    event_time = Column(BigInteger, nullable=False, comment='数据时间戳，毫秒')
    long_short_ratio = Column(DECIMAL(20, 8), comment='大户持仓量多空比')
    long_account = Column(DECIMAL(20, 8), comment='大户多头持仓占比')
    short_account = Column(DECIMAL(20, 8), comment='大户空头持仓占比')
    raw_json = Column(JSON, comment='接口原始返回数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')


class BinanceTopLongShortAccountRatio(Base):
    """Binance 大户账户数多空比历史表"""

    __tablename__ = 'binance_top_long_short_account_ratio'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_btlsar_symbol_period_time'),
        Index('idx_btlsar_symbol_period_time', 'symbol', 'period', 'event_time'),
        {'comment': 'Binance 大户账户数多空比历史数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对，例如 BTCUSDT')
    period = Column(String(10), nullable=False, comment='时间周期，例如 5m、15m、1h')
    event_time = Column(BigInteger, nullable=False, comment='数据时间戳，毫秒')
    long_short_ratio = Column(DECIMAL(20, 8), comment='大户账户数多空比')
    long_account = Column(DECIMAL(20, 8), comment='大户多头账户占比')
    short_account = Column(DECIMAL(20, 8), comment='大户空头账户占比')
    raw_json = Column(JSON, comment='接口原始返回数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')


class BinanceOpenInterestHist(Base):
    """Binance 合约持仓量历史表"""

    __tablename__ = 'binance_open_interest_hist'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_boih_symbol_period_time'),
        Index('idx_boih_symbol_period_time', 'symbol', 'period', 'event_time'),
        {'comment': 'Binance 合约持仓量历史数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对，例如 BTCUSDT')
    period = Column(String(10), nullable=False, comment='时间周期，例如 5m、15m、1h')
    event_time = Column(BigInteger, nullable=False, comment='数据时间戳，毫秒')
    sum_open_interest = Column(DECIMAL(30, 8), comment='总持仓量')
    sum_open_interest_value = Column(DECIMAL(30, 8), comment='总持仓价值')
    cmc_circulating_supply = Column(DECIMAL(30, 8), comment='CMC 流通供应量')
    raw_json = Column(JSON, comment='接口原始返回数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')


class BinanceKline(Base):
    """Binance K线历史表"""

    __tablename__ = 'binance_klines'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'open_time', name='uk_bk_symbol_period_open_time'),
        Index('idx_bk_symbol_period_open_time', 'symbol', 'period', 'open_time'),
        {'comment': 'Binance K线历史数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对，例如 BTCUSDT')
    period = Column(String(10), nullable=False, comment='K线周期，例如 5m、15m、1h')
    open_time = Column(BigInteger, nullable=False, comment='开盘时间，毫秒')
    close_time = Column(BigInteger, nullable=False, comment='收盘时间，毫秒')
    open_price = Column(DECIMAL(30, 8), nullable=False, comment='开盘价')
    high_price = Column(DECIMAL(30, 8), nullable=False, comment='最高价')
    low_price = Column(DECIMAL(30, 8), nullable=False, comment='最低价')
    close_price = Column(DECIMAL(30, 8), nullable=False, comment='收盘价')
    volume = Column(DECIMAL(30, 8), comment='成交量')
    quote_volume = Column(DECIMAL(30, 8), comment='成交额')
    trade_count = Column(BigInteger, comment='成交笔数')
    taker_buy_base_volume = Column(DECIMAL(30, 8), comment='主动买入成交量')
    taker_buy_quote_volume = Column(DECIMAL(30, 8), comment='主动买入成交额')
    raw_json = Column(JSON, comment='接口原始K线数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')


class MarketTickers(Base):
    """行情快照原始数据表"""

    __tablename__ = 'market_tickers'
    __table_args__ = (
        Index('idx_mt_symbol', 'symbol'),
        Index('idx_mt_close_time', 'close_time'),
        {'comment': '行情快照原始数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对')
    price_change = Column(DECIMAL(24, 8), comment='价格变动')
    price_change_percent = Column(DECIMAL(20, 8), comment='涨跌幅')
    weighted_avg_price = Column(DECIMAL(24, 8), comment='加权平均价')
    last_price = Column(DECIMAL(24, 8), comment='最新价')
    last_qty = Column(DECIMAL(24, 8), comment='最新成交量')
    open_price = Column(DECIMAL(24, 8), comment='开盘价')
    high_price = Column(DECIMAL(24, 8), comment='最高价')
    low_price = Column(DECIMAL(24, 8), comment='最低价')
    volume = Column(DECIMAL(30, 8), comment='成交量')
    quote_volume = Column(DECIMAL(30, 8), comment='成交额')
    open_time = Column(BigInteger, comment='24h窗口开始时间')
    close_time = Column(BigInteger, comment='24h窗口结束时间')
    first_id = Column(BigInteger, comment='首笔交易ID')
    last_id = Column(BigInteger, comment='末笔交易ID')
    count = Column(BigInteger, comment='交易笔数')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    def __repr__(self):
        return f"<MarketTickers(symbol='{self.symbol}', close_time={self.close_time})>"


class BinanceGlobalLongShortAccountRatio(Base):
    """Binance 全市场多空账户数比历史表"""

    __tablename__ = 'binance_global_long_short_account_ratio'
    __table_args__ = (
        UniqueConstraint('symbol', 'period', 'event_time', name='uk_bglsar_symbol_period_time'),
        Index('idx_bglsar_symbol_period_time', 'symbol', 'period', 'event_time'),
        {'comment': 'Binance 全市场多空账户数比历史数据表'},
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True, comment='主键ID')
    symbol = Column(String(20), nullable=False, comment='交易对，例如 BTCUSDT')
    period = Column(String(10), nullable=False, comment='时间周期，例如 5m、15m、1h')
    event_time = Column(BigInteger, nullable=False, comment='数据时间戳，毫秒')
    long_short_ratio = Column(DECIMAL(20, 8), comment='全市场多空账户数比')
    long_account = Column(DECIMAL(20, 8), comment='全市场多头账户占比')
    short_account = Column(DECIMAL(20, 8), comment='全市场空头账户占比')
    raw_json = Column(JSON, comment='接口原始返回数据')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    def __repr__(self):
        return f"<BinanceGlobalLongShortAccountRatio(symbol='{self.symbol}', period='{self.period}')>"
