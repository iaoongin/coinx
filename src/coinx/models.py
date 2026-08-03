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
    Text,
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
        Index('idx_moih_symbol_period_exchange_time', 'symbol', 'period', 'exchange', 'event_time'),
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
        Index('idx_mk_symbol_period_exchange_open_time', 'symbol', 'period', 'exchange', 'open_time'),
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
        Index('idx_mtbsv_symbol_period_exchange_time', 'symbol', 'period', 'exchange', 'event_time'),
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


class NotificationChannel(Base):
    __tablename__ = 'notification_channels'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    channel_type = Column(String(30), nullable=False, default='apprise')
    enabled = Column(Boolean, nullable=False, default=True)
    config_encrypted = Column(Text, nullable=False)
    key_version = Column(String(30), nullable=False, default='v1')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AlertRule(Base):
    __tablename__ = 'alert_rules'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    name = Column(String(120), nullable=False, unique=True)
    event_type = Column(String(80), nullable=False, index=True)
    scope_type = Column(String(40), nullable=False)
    scope_json = Column(JSON, nullable=False, default=dict)
    params_json = Column(JSON, nullable=False, default=dict)
    cooldown_seconds = Column(Integer, nullable=False, default=1800)
    recovery_enabled = Column(Boolean, nullable=False, default=True)
    enabled = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AlertRuleChannel(Base):
    __tablename__ = 'alert_rule_channels'
    __table_args__ = (UniqueConstraint('rule_id', 'channel_id', name='uk_alert_rule_channel'),)

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    rule_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    channel_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)


class AlertState(Base):
    __tablename__ = 'alert_states'
    __table_args__ = (UniqueConstraint('rule_id', 'subject_key', 'dimension_key', name='uk_alert_state'),)

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    rule_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    subject_key = Column(String(80), nullable=False)
    dimension_key = Column(String(80), nullable=False)
    state = Column(String(20), nullable=False, default='normal')
    consecutive_matches = Column(Integer, nullable=False, default=0)
    last_value_json = Column(JSON)
    last_triggered_at = Column(BigInteger)
    last_notified_at = Column(BigInteger)
    last_recovered_at = Column(BigInteger)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class NotificationDelivery(Base):
    __tablename__ = 'notification_deliveries'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    rule_id = Column(SQLITE_BIGINT_PK, nullable=True, index=True)
    channel_id = Column(SQLITE_BIGINT_PK, nullable=True, index=True)
    event_key = Column(String(255), nullable=False, index=True)
    event_status = Column(String(20), nullable=False)
    payload_json = Column(JSON, nullable=False, default=dict)
    delivery_status = Column(String(20), nullable=False)
    response_code = Column(Integer)
    error_message = Column(String(500))
    sent_at = Column(BigInteger, nullable=False, index=True)


class AlertEvaluationRun(Base):
    __tablename__ = 'alert_evaluation_runs'

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    rule_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    trigger_source = Column(String(20), nullable=False, default='manual')
    status = Column(String(20), nullable=False, default='running')
    checked_count = Column(Integer, nullable=False, default=0)
    matched_count = Column(Integer, nullable=False, default=0)
    sent_count = Column(Integer, nullable=False, default=0)
    error_message = Column(String(500))
    started_at = Column(BigInteger, nullable=False, index=True)
    completed_at = Column(BigInteger)


class AlertEvaluationMetric(Base):
    """Persist timing details separately so existing run rows need no migration."""

    __tablename__ = 'alert_evaluation_metrics'
    __table_args__ = (UniqueConstraint('run_id', name='uk_alert_evaluation_metric_run'),)

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    run_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    metrics_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.now)


class ScheduledJobRun(Base):
    """Persistent execution record for an APScheduler job."""

    __tablename__ = 'scheduled_job_runs'
    __table_args__ = (
        Index('idx_scheduled_job_runs_job_started', 'job_id', 'started_at'),
        Index('idx_scheduled_job_runs_job_started_id', 'job_id', 'started_at', 'id'),
        Index('idx_scheduled_job_runs_started_at', 'started_at'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    job_id = Column(String(120), nullable=False, index=True)
    status = Column(String(20), nullable=False, default='running')
    summary_json = Column(JSON)
    error_message = Column(String(500))
    started_at = Column(BigInteger, nullable=False)
    completed_at = Column(BigInteger)
    duration_ms = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)


class RssSubscription(Base):
    """A feed that can be fetched and optionally delivered as notifications."""

    __tablename__ = 'rss_subscriptions'
    __table_args__ = (
        Index('idx_rss_subscriptions_enabled', 'enabled'),
        Index('idx_rss_subscriptions_monitor_enabled', 'monitor_enabled'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    name = Column(String(160), nullable=False)
    url = Column(String(500), nullable=False, unique=True)
    site_url = Column(String(1000))
    feed_title = Column(String(255))
    enabled = Column(Boolean, nullable=False, default=True)
    monitor_enabled = Column(Boolean, nullable=False, default=True)
    notification_channel_ids = Column(JSON)
    last_checked_at = Column(BigInteger)
    last_success_at = Column(BigInteger)
    last_error = Column(String(500))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class RssArticle(Base):
    """Normalized RSS item retained for browsing and notification de-duplication."""

    __tablename__ = 'rss_articles'
    __table_args__ = (
        UniqueConstraint('subscription_id', 'guid', name='uk_rss_article_subscription_guid'),
        Index('idx_rss_articles_subscription_published', 'subscription_id', 'published_at'),
        Index('idx_rss_articles_published', 'published_at'),
    )

    id = Column(SQLITE_BIGINT_PK, primary_key=True, autoincrement=True)
    subscription_id = Column(SQLITE_BIGINT_PK, nullable=False, index=True)
    guid = Column(String(512), nullable=False)
    title = Column(String(1000), nullable=False)
    link = Column(String(2000), nullable=False)
    author = Column(String(255))
    summary = Column(Text)
    content = Column(Text)
    published_at = Column(BigInteger, index=True)
    notified_at = Column(BigInteger)
    fetched_at = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
