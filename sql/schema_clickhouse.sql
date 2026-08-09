-- ClickHouse 行情时序表 DDL。
-- 业务时区统一使用 Asia/Shanghai；open_time、close_time、event_time 均为毫秒时间戳。

CREATE DATABASE IF NOT EXISTS coinx;

CREATE TABLE IF NOT EXISTS coinx.market_klines
(
    exchange LowCardinality(String) COMMENT '交易所标识，例如 binance、okx',
    symbol LowCardinality(String) COMMENT '内部交易对符号，例如 BTCUSDT',
    period LowCardinality(String) COMMENT 'K 线周期，例如 5m、15m、1h',
    open_time UInt64 COMMENT '开盘时间戳，毫秒',
    close_time UInt64 COMMENT '收盘时间戳，毫秒',
    open_price Decimal(30, 8) COMMENT '开盘价格',
    high_price Decimal(30, 8) COMMENT '最高价格',
    low_price Decimal(30, 8) COMMENT '最低价格',
    close_price Decimal(30, 8) COMMENT '收盘价格',
    volume Nullable(Decimal(30, 8)) COMMENT '成交量',
    quote_volume Nullable(Decimal(30, 8)) COMMENT '成交额',
    trade_count Nullable(UInt64) COMMENT '成交笔数',
    taker_buy_base_volume Nullable(Decimal(30, 8)) COMMENT '主动买入基础资产成交量',
    taker_buy_quote_volume Nullable(Decimal(30, 8)) COMMENT '主动买入计价资产成交额',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间',
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(open_time, 1000), 'Asia/Shanghai'))
ORDER BY (exchange, symbol, period, open_time)
COMMENT '多交易所 K 线历史数据';

CREATE TABLE IF NOT EXISTS coinx.market_open_interest_hist
(
    exchange LowCardinality(String) COMMENT '交易所标识，例如 binance、okx',
    symbol LowCardinality(String) COMMENT '内部交易对符号，例如 BTCUSDT',
    period LowCardinality(String) COMMENT '采集周期，例如 5m、15m、1h',
    event_time UInt64 COMMENT '数据时间戳，毫秒',
    sum_open_interest Nullable(Decimal(30, 8)) COMMENT '持仓量',
    sum_open_interest_value Nullable(Decimal(30, 8)) COMMENT '持仓价值',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间',
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(event_time, 1000), 'Asia/Shanghai'))
ORDER BY (exchange, symbol, period, event_time)
COMMENT '多交易所持仓量历史数据';

CREATE TABLE IF NOT EXISTS coinx.market_taker_buy_sell_vol
(
    exchange LowCardinality(String) COMMENT '交易所标识，例如 binance、okx',
    symbol LowCardinality(String) COMMENT '内部交易对符号，例如 BTCUSDT',
    period LowCardinality(String) COMMENT '采集周期，例如 5m、15m、1h',
    event_time UInt64 COMMENT '数据时间戳，毫秒',
    buy_sell_ratio Nullable(Decimal(20, 8)) COMMENT '主动买入与卖出比率',
    buy_vol Nullable(Decimal(30, 8)) COMMENT '主动买入成交量或成交额',
    sell_vol Nullable(Decimal(30, 8)) COMMENT '主动卖出成交量或成交额',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间',
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(event_time, 1000), 'Asia/Shanghai'))
ORDER BY (exchange, symbol, period, event_time)
COMMENT '多交易所主动买入卖出量历史数据';

CREATE TABLE IF NOT EXISTS coinx.market_funding_rate
(
    exchange LowCardinality(String) DEFAULT 'binance' COMMENT '交易所标识',
    symbol LowCardinality(String) COMMENT '交易对名称，例如 BTCUSDT',
    period LowCardinality(String) COMMENT '采集周期，例如 5m',
    event_time UInt64 COMMENT '采集时间戳，毫秒',
    funding_rate Nullable(Decimal(20, 8)) COMMENT '上次结算资金费率',
    predicted_rate Nullable(Decimal(20, 8)) COMMENT '预测的下一次结算资金费率',
    next_funding_time Nullable(UInt64) COMMENT '下次结算时间戳，毫秒',
    mark_price Nullable(Decimal(20, 8)) COMMENT '标记价格',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间',
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(event_time, 1000), 'Asia/Shanghai'))
ORDER BY (exchange, symbol, period, event_time)
COMMENT '资金费率历史数据';

CREATE TABLE IF NOT EXISTS coinx.market_snapshots
(
    snapshot_time UInt64 COMMENT '快照时间戳，毫秒',
    symbol LowCardinality(String) COMMENT '交易对符号，例如 BTCUSDT',
    batch_id String COMMENT '采集批次 ID，用于标识同一批次的数据',
    price Nullable(Decimal(24, 8)) COMMENT '当前价格',
    open_interest Nullable(Decimal(24, 8)) COMMENT '持仓量',
    open_interest_value Nullable(Decimal(24, 8)) COMMENT '持仓价值',
    data_json String DEFAULT '{}' COMMENT '完整原始数据 JSON',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间'
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(snapshot_time, 1000), 'Asia/Shanghai'))
ORDER BY (symbol, snapshot_time, batch_id)
COMMENT '市场数据快照';

CREATE TABLE IF NOT EXISTS coinx.market_tickers
(
    close_time UInt64 COMMENT '24 小时统计窗口结束时间戳，毫秒',
    symbol LowCardinality(String) COMMENT '交易对符号，例如 BTCUSDT',
    price_change Nullable(Decimal(24, 8)) COMMENT '价格变动',
    price_change_percent Nullable(Decimal(20, 8)) COMMENT '涨跌幅百分比',
    weighted_avg_price Nullable(Decimal(24, 8)) COMMENT '加权平均价格',
    last_price Nullable(Decimal(24, 8)) COMMENT '最新价格',
    last_qty Nullable(Decimal(24, 8)) COMMENT '最新成交数量',
    open_price Nullable(Decimal(24, 8)) COMMENT '开盘价格',
    high_price Nullable(Decimal(24, 8)) COMMENT '最高价格',
    low_price Nullable(Decimal(24, 8)) COMMENT '最低价格',
    volume Nullable(Decimal(30, 8)) COMMENT '成交量',
    quote_volume Nullable(Decimal(30, 8)) COMMENT '成交额',
    open_time Nullable(UInt64) COMMENT '24 小时统计窗口开始时间戳，毫秒',
    first_id Nullable(UInt64) COMMENT '首笔交易 ID',
    last_id Nullable(UInt64) COMMENT '末笔交易 ID',
    count Nullable(UInt64) COMMENT '交易笔数',
    created_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录创建时间',
    updated_at DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3) COMMENT '记录更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(intDiv(close_time, 1000), 'Asia/Shanghai'))
ORDER BY (symbol, close_time)
COMMENT '行情快照原始数据';
