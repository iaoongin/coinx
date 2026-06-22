-- 币种配置表
CREATE TABLE IF NOT EXISTS coins (
    symbol VARCHAR(20) PRIMARY KEY COMMENT '交易对符号，例如 BTCUSDT',
    is_tracking BOOLEAN DEFAULT TRUE COMMENT '是否启用跟踪',
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='币种配置表';

-- 市场数据快照表
CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
    batch_id VARCHAR(50) NOT NULL COMMENT '批次ID，用于标识同一批次的数据',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对符号',
    price DECIMAL(24, 8) COMMENT '当前价格',
    open_interest DECIMAL(24, 8) COMMENT '持仓量',
    open_interest_value DECIMAL(24, 8) COMMENT '持仓价值',
    data_json JSON COMMENT '完整数据JSON',
    snapshot_time BIGINT NOT NULL COMMENT '快照时间戳，毫秒',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    INDEX idx_snapshot_time (snapshot_time),
    INDEX idx_batch_id (batch_id),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场数据快照表';

-- 行情快照原始数据表
CREATE TABLE IF NOT EXISTS market_tickers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '交易对',
    price_change DECIMAL(24, 8) DEFAULT NULL COMMENT '价格变动',
    price_change_percent DECIMAL(20, 8) DEFAULT NULL COMMENT '涨跌幅',
    weighted_avg_price DECIMAL(24, 8) DEFAULT NULL COMMENT '加权平均价',
    last_price DECIMAL(24, 8) DEFAULT NULL COMMENT '最新价',
    last_qty DECIMAL(24, 8) DEFAULT NULL COMMENT '最新成交量',
    open_price DECIMAL(24, 8) DEFAULT NULL COMMENT '开盘价',
    high_price DECIMAL(24, 8) DEFAULT NULL COMMENT '最高价',
    low_price DECIMAL(24, 8) DEFAULT NULL COMMENT '最低价',
    volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交量',
    quote_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交额',
    open_time BIGINT DEFAULT NULL COMMENT '24h窗口开始时间',
    close_time BIGINT DEFAULT NULL COMMENT '24h窗口结束时间',
    first_id BIGINT DEFAULT NULL COMMENT '首笔交易ID',
    last_id BIGINT DEFAULT NULL COMMENT '末笔交易ID',
    count BIGINT DEFAULT NULL COMMENT '交易笔数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_symbol (symbol),
    KEY idx_close_time (close_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行情快照原始数据表';

-- 多交易所持仓量历史数据表
CREATE TABLE IF NOT EXISTS market_open_interest_hist (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    sum_open_interest DECIMAL(30, 8) DEFAULT NULL COMMENT '持仓量',
    sum_open_interest_value DECIMAL(30, 8) DEFAULT NULL COMMENT '持仓价值',
    raw_json JSON DEFAULT NULL COMMENT '交易所原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_moih_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_moih_exchange_symbol_period_time (exchange, symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多交易所持仓量历史数据表';

-- 多交易所K线历史数据表
CREATE TABLE IF NOT EXISTS market_klines (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT 'K线周期，例如 5m、15m、1h',
    open_time BIGINT NOT NULL COMMENT '开盘时间戳，毫秒',
    close_time BIGINT NOT NULL COMMENT '收盘时间戳，毫秒',
    open_price DECIMAL(30, 8) NOT NULL COMMENT '开盘价',
    high_price DECIMAL(30, 8) NOT NULL COMMENT '最高价',
    low_price DECIMAL(30, 8) NOT NULL COMMENT '最低价',
    close_price DECIMAL(30, 8) NOT NULL COMMENT '收盘价',
    volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交量',
    quote_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交额',
    trade_count BIGINT DEFAULT NULL COMMENT '成交笔数',
    taker_buy_base_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入基础资产成交量',
    taker_buy_quote_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入计价资产成交额',
    raw_json JSON DEFAULT NULL COMMENT '交易所原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_mk_exchange_symbol_period_open_time (exchange, symbol, period, open_time),
    KEY idx_mk_exchange_symbol_period_open_time (exchange, symbol, period, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多交易所K线历史数据表';

-- 多交易所主动买入卖出量历史数据表
CREATE TABLE IF NOT EXISTS market_taker_buy_sell_vol (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    buy_sell_ratio DECIMAL(20, 8) DEFAULT NULL COMMENT '主动买入卖出比',
    buy_vol DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入成交量或成交额',
    sell_vol DECIMAL(30, 8) DEFAULT NULL COMMENT '主动卖出成交量或成交额',
    raw_json JSON DEFAULT NULL COMMENT '交易所原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_mtbsv_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_mtbsv_exchange_symbol_period_time (exchange, symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多交易所主动买入卖出量历史数据表';

-- 资金费率历史表
CREATE TABLE IF NOT EXISTS market_funding_rate (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(20) NOT NULL COMMENT '交易对名称',
    period VARCHAR(10) NOT NULL DEFAULT '5m' COMMENT '采集周期',
    event_time BIGINT NOT NULL COMMENT '采集时间戳（毫秒）',
    funding_rate DECIMAL(20, 8) COMMENT '上次结算费率',
    predicted_rate DECIMAL(20, 8) COMMENT '预测费率（下次结算）',
    next_funding_time BIGINT COMMENT '下次结算时间戳（毫秒）',
    mark_price DECIMAL(20, 8) COMMENT '标记价格',
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance' COMMENT '交易所',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_symbol_period_time (symbol, period, event_time),
    INDEX idx_symbol_period (symbol, period),
    INDEX idx_symbol_time (symbol, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金费率历史';
