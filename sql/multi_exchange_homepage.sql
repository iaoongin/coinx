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
