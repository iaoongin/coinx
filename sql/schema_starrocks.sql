-- StarRocks 建表 DDL（与 MySQL schema.sql 功能等价）
-- 所有表使用 Primary Key 模型，支持 INSERT ... ON DUPLICATE KEY UPDATE
-- 分布键选择原则：选查询最常用的过滤列，确保数据均匀分布

-- 币种配置表
CREATE TABLE IF NOT EXISTS coins (
    symbol VARCHAR(20) NOT NULL COMMENT '交易对符号，例如 BTCUSDT',
    is_tracking BOOLEAN COMMENT '是否启用跟踪',
    base_asset VARCHAR(100) COMMENT '基础资产',
    quote_asset VARCHAR(100) COMMENT '计价资产',
    margin_asset VARCHAR(100) COMMENT '保证金资产',
    price_precision INT COMMENT '价格精度',
    quantity_precision INT COMMENT '数量精度',
    base_asset_precision INT COMMENT '基础资产精度',
    quote_precision INT COMMENT '报价精度',
    status VARCHAR(20) COMMENT '交易状态',
    onboard_date BIGINT COMMENT '上线时间',
    delivery_date BIGINT COMMENT '交割时间',
    contract_type VARCHAR(20) COMMENT '合约类型',
    underlying_type VARCHAR(20) COMMENT '标的类型',
    liquidation_fee DECIMAL(10, 6) COMMENT '强平费率',
    maint_margin_percent DECIMAL(10, 4) COMMENT '维持保证金率',
    required_margin_percent DECIMAL(10, 4) COMMENT '所需保证金率',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) PRIMARY KEY (symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 市场数据快照表（DUPLICATE KEY 模型，支持按 symbol 分布）
CREATE TABLE IF NOT EXISTS market_snapshots (
    snapshot_time BIGINT NOT NULL COMMENT '快照时间戳，毫秒',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对符号',
    batch_id VARCHAR(50) NOT NULL COMMENT '批次ID，用于标识同一批次的数据',
    price DECIMAL(24, 8) COMMENT '当前价格',
    open_interest DECIMAL(24, 8) COMMENT '持仓量',
    open_interest_value DECIMAL(24, 8) COMMENT '持仓价值',
    data_json JSON COMMENT '完整数据JSON',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
) DUPLICATE KEY (snapshot_time, symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- 行情快照原始数据表（DUPLICATE KEY 模型，支持按 symbol 分布）
CREATE TABLE IF NOT EXISTS market_tickers (
    close_time BIGINT COMMENT '24h窗口结束时间',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对',
    price_change DECIMAL(24, 8) COMMENT '价格变动',
    price_change_percent DECIMAL(20, 8) COMMENT '涨跌幅',
    weighted_avg_price DECIMAL(24, 8) COMMENT '加权平均价',
    last_price DECIMAL(24, 8) COMMENT '最新价',
    last_qty DECIMAL(24, 8) COMMENT '最新成交量',
    open_price DECIMAL(24, 8) COMMENT '开盘价',
    high_price DECIMAL(24, 8) COMMENT '最高价',
    low_price DECIMAL(24, 8) COMMENT '最低价',
    volume DECIMAL(30, 8) COMMENT '成交量',
    quote_volume DECIMAL(30, 8) COMMENT '成交额',
    open_time BIGINT COMMENT '24h窗口开始时间',
    first_id BIGINT COMMENT '首笔交易ID',
    last_id BIGINT COMMENT '末笔交易ID',
    count BIGINT COMMENT '交易笔数',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) DUPLICATE KEY (close_time, symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- 多交易所持仓量历史数据表（KEY 列必须在最前面）
CREATE TABLE IF NOT EXISTS market_open_interest_hist (
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    sum_open_interest DECIMAL(30, 8) COMMENT '持仓量',
    sum_open_interest_value DECIMAL(30, 8) COMMENT '持仓价值',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) PRIMARY KEY (exchange, symbol, period, event_time)
DISTRIBUTED BY HASH(exchange, symbol) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 多交易所K线历史数据表（KEY 列必须在最前面）
CREATE TABLE IF NOT EXISTS market_klines (
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT 'K线周期，例如 5m、15m、1h',
    open_time BIGINT NOT NULL COMMENT '开盘时间戳，毫秒',
    close_time BIGINT NOT NULL COMMENT '收盘时间戳，毫秒',
    open_price DECIMAL(30, 8) NOT NULL COMMENT '开盘价',
    high_price DECIMAL(30, 8) NOT NULL COMMENT '最高价',
    low_price DECIMAL(30, 8) NOT NULL COMMENT '最低价',
    close_price DECIMAL(30, 8) NOT NULL COMMENT '收盘价',
    volume DECIMAL(30, 8) COMMENT '成交量',
    quote_volume DECIMAL(30, 8) COMMENT '成交额',
    trade_count BIGINT COMMENT '成交笔数',
    taker_buy_base_volume DECIMAL(30, 8) COMMENT '主动买入基础资产成交量',
    taker_buy_quote_volume DECIMAL(30, 8) COMMENT '主动买入计价资产成交额',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) PRIMARY KEY (exchange, symbol, period, event_time)
DISTRIBUTED BY HASH(exchange, symbol) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 多交易所主动买入卖出量历史数据表（KEY 列必须在最前面）
CREATE TABLE IF NOT EXISTS market_taker_buy_sell_vol (
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    buy_sell_ratio DECIMAL(20, 8) COMMENT '主动买入卖出比',
    buy_vol DECIMAL(30, 8) COMMENT '主动买入成交量或成交额',
    sell_vol DECIMAL(30, 8) COMMENT '主动卖出成交量或成交额',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) PRIMARY KEY (exchange, symbol, period, event_time)
DISTRIBUTED BY HASH(exchange, symbol) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- 资金费率历史表
CREATE TABLE IF NOT EXISTS market_funding_rate (
    symbol VARCHAR(20) NOT NULL COMMENT '交易对名称',
    period VARCHAR(10) NOT NULL COMMENT '采集周期',
    event_time BIGINT NOT NULL COMMENT '采集时间戳（毫秒）',
    funding_rate DECIMAL(20, 8) COMMENT '上次结算费率',
    predicted_rate DECIMAL(20, 8) COMMENT '预测费率（下次结算）',
    next_funding_time BIGINT COMMENT '下次结算时间戳（毫秒）',
    mark_price DECIMAL(20, 8) COMMENT '标记价格',
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance' COMMENT '交易所',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) PRIMARY KEY (symbol, period, event_time)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1");
