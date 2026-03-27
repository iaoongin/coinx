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

-- Binance 大户持仓量多空比历史数据表
CREATE TABLE IF NOT EXISTS binance_top_long_short_position_ratio (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    long_short_ratio DECIMAL(20, 8) DEFAULT NULL COMMENT '大户持仓量多空比',
    long_account DECIMAL(20, 8) DEFAULT NULL COMMENT '大户多头持仓占比',
    short_account DECIMAL(20, 8) DEFAULT NULL COMMENT '大户空头持仓占比',
    raw_json JSON DEFAULT NULL COMMENT '接口原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_btlspr_symbol_period_time (symbol, period, event_time),
    KEY idx_btlspr_symbol_period_time (symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Binance 大户持仓量多空比历史数据表';

-- Binance 大户账户数多空比历史数据表
CREATE TABLE IF NOT EXISTS binance_top_long_short_account_ratio (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    long_short_ratio DECIMAL(20, 8) DEFAULT NULL COMMENT '大户账户数多空比',
    long_account DECIMAL(20, 8) DEFAULT NULL COMMENT '大户多头账户占比',
    short_account DECIMAL(20, 8) DEFAULT NULL COMMENT '大户空头账户占比',
    raw_json JSON DEFAULT NULL COMMENT '接口原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_btlsar_symbol_period_time (symbol, period, event_time),
    KEY idx_btlsar_symbol_period_time (symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Binance 大户账户数多空比历史数据表';

-- Binance 合约持仓量历史数据表
CREATE TABLE IF NOT EXISTS binance_open_interest_hist (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    sum_open_interest DECIMAL(30, 8) DEFAULT NULL COMMENT '总持仓量',
    sum_open_interest_value DECIMAL(30, 8) DEFAULT NULL COMMENT '总持仓价值',
    cmc_circulating_supply DECIMAL(30, 8) DEFAULT NULL COMMENT 'CMC 流通供应量',
    raw_json JSON DEFAULT NULL COMMENT '接口原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_boih_symbol_period_time (symbol, period, event_time),
    KEY idx_boih_symbol_period_time (symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Binance 合约持仓量历史数据表';

-- Binance K线历史数据表
CREATE TABLE IF NOT EXISTS binance_klines (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT 'K线周期，例如 5m、15m、1h',
    open_time BIGINT NOT NULL COMMENT '开盘时间，毫秒',
    close_time BIGINT NOT NULL COMMENT '收盘时间，毫秒',
    open_price DECIMAL(30, 8) NOT NULL COMMENT '开盘价',
    high_price DECIMAL(30, 8) NOT NULL COMMENT '最高价',
    low_price DECIMAL(30, 8) NOT NULL COMMENT '最低价',
    close_price DECIMAL(30, 8) NOT NULL COMMENT '收盘价',
    volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交量',
    quote_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '成交额',
    trade_count BIGINT DEFAULT NULL COMMENT '成交笔数',
    taker_buy_base_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入成交量',
    taker_buy_quote_volume DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入成交额',
    raw_json JSON DEFAULT NULL COMMENT '接口原始K线数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_bk_symbol_period_open_time (symbol, period, open_time),
    KEY idx_bk_symbol_period_open_time (symbol, period, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Binance K线历史数据表';

-- Binance 全市场多空账户数比历史数据表
CREATE TABLE IF NOT EXISTS binance_global_long_short_account_ratio (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    long_short_ratio DECIMAL(20, 8) DEFAULT NULL COMMENT '全市场多空账户数比',
    long_account DECIMAL(20, 8) DEFAULT NULL COMMENT '全市场多头账户占比',
    short_account DECIMAL(20, 8) DEFAULT NULL COMMENT '全市场空头账户占比',
    raw_json JSON DEFAULT NULL COMMENT '接口原始返回数据',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_bglsar_symbol_period_time (symbol, period, event_time),
    KEY idx_bglsar_symbol_period_time (symbol, period, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Binance 全市场多空账户数比历史数据表';
