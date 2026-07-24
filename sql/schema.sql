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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_coins_is_tracking (is_tracking)
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_moih_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_moih_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_moih_symbol_period_exchange_time (symbol, period, exchange, event_time)
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_mk_exchange_symbol_period_open_time (exchange, symbol, period, open_time),
    KEY idx_mk_exchange_symbol_period_open_time (exchange, symbol, period, open_time),
    KEY idx_mk_symbol_period_exchange_open_time (symbol, period, exchange, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多交易所K线历史数据表';

-- 多交易所主动买入卖出量历史数据表
CREATE TABLE IF NOT EXISTS market_taker_buy_sell_vol (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    exchange VARCHAR(20) NOT NULL COMMENT '交易所标识，例如 binance、okx',
    symbol VARCHAR(20) NOT NULL COMMENT '内部交易对符号，例如 BTCUSDT',
    period VARCHAR(10) NOT NULL COMMENT '时间周期，例如 5m、15m、1h',
    event_time BIGINT NOT NULL COMMENT '数据时间戳，毫秒',
    buy_sell_ratio DECIMAL(20, 8) DEFAULT NULL COMMENT '主动买入卖出比',
    buy_vol DECIMAL(30, 8) DEFAULT NULL COMMENT '主动买入成交量',
    sell_vol DECIMAL(30, 8) DEFAULT NULL COMMENT '主动卖出成交量',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_mtbsv_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_mtbsv_exchange_symbol_period_time (exchange, symbol, period, event_time),
    KEY idx_mtbsv_symbol_period_exchange_time (symbol, period, exchange, event_time)
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
    INDEX idx_symbol_time (symbol, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金费率历史';

-- 通知渠道：Apprise URL 仅保存为应用层 Fernet 密文。
CREATE TABLE IF NOT EXISTS notification_channels (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    name VARCHAR(100) NOT NULL UNIQUE COMMENT '渠道名称',
    channel_type VARCHAR(30) NOT NULL DEFAULT 'apprise' COMMENT '通知渠道类型',
    enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT '是否启用渠道',
    config_encrypted TEXT NOT NULL COMMENT '加密后的渠道配置',
    key_version VARCHAR(30) NOT NULL DEFAULT 'v1' COMMENT '配置加密密钥版本',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='加密通知渠道配置';

CREATE TABLE IF NOT EXISTS alert_rules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    name VARCHAR(120) NOT NULL UNIQUE COMMENT '规则名称',
    event_type VARCHAR(80) NOT NULL COMMENT '事件类型',
    scope_type VARCHAR(40) NOT NULL COMMENT '适用对象范围类型',
    scope_json JSON NOT NULL COMMENT '适用对象范围配置',
    params_json JSON NOT NULL COMMENT '规则参数配置',
    cooldown_seconds INT NOT NULL DEFAULT 1800 COMMENT '同一对象重复通知冷却时间（秒）',
    recovery_enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT '是否发送恢复通知',
    enabled BOOLEAN NOT NULL DEFAULT FALSE COMMENT '是否启用规则',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY idx_alert_rules_event_type (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警规则';

CREATE TABLE IF NOT EXISTS alert_rule_channels (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    rule_id BIGINT NOT NULL COMMENT '告警规则ID',
    channel_id BIGINT NOT NULL COMMENT '通知渠道ID',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_alert_rule_channel (rule_id, channel_id),
    KEY idx_alert_rule_channels_rule (rule_id),
    KEY idx_alert_rule_channels_channel (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='规则与渠道关联';

CREATE TABLE IF NOT EXISTS alert_states (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    rule_id BIGINT NOT NULL COMMENT '告警规则ID',
    subject_key VARCHAR(80) NOT NULL COMMENT '监控对象标识，例如交易对或任务ID',
    dimension_key VARCHAR(80) NOT NULL COMMENT '规则判定维度标识',
    state VARCHAR(20) NOT NULL DEFAULT 'normal' COMMENT '当前状态：normal 或 triggered',
    consecutive_matches INT NOT NULL DEFAULT 0 COMMENT '连续满足恢复条件的次数',
    last_value_json JSON COMMENT '最近一次判定指标值',
    last_triggered_at BIGINT COMMENT '最近一次触发异常时间戳（毫秒）',
    last_notified_at BIGINT COMMENT '最近一次发送通知时间戳（毫秒）',
    last_recovered_at BIGINT COMMENT '最近一次恢复正常时间戳（毫秒）',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '状态更新时间',
    UNIQUE KEY uk_alert_state (rule_id, subject_key, dimension_key),
    KEY idx_alert_states_rule (rule_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警状态与去重';

CREATE TABLE IF NOT EXISTS notification_deliveries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    rule_id BIGINT NULL COMMENT '关联告警规则ID；渠道测试时为空',
    channel_id BIGINT NULL COMMENT '目标通知渠道ID',
    event_key VARCHAR(255) NOT NULL COMMENT '投递事件幂等标识',
    event_status VARCHAR(20) NOT NULL COMMENT '事件状态，例如 triggered、recovered、test',
    payload_json JSON NOT NULL COMMENT '投递消息内容与上下文',
    delivery_status VARCHAR(20) NOT NULL COMMENT '投递结果：success 或 failed',
    response_code INT NULL COMMENT '渠道响应状态码',
    error_message VARCHAR(500) NULL COMMENT '投递失败原因',
    sent_at BIGINT NOT NULL COMMENT '投递尝试时间戳（毫秒）',
    KEY idx_notification_deliveries_rule (rule_id),
    KEY idx_notification_deliveries_channel (channel_id),
    KEY idx_notification_deliveries_event_key (event_key),
    KEY idx_notification_deliveries_sent_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='通知发送记录';

CREATE TABLE IF NOT EXISTS alert_evaluation_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    rule_id BIGINT NOT NULL COMMENT '被评估的告警规则ID',
    trigger_source VARCHAR(20) NOT NULL DEFAULT 'manual' COMMENT '评估触发来源：manual 或 scheduled',
    status VARCHAR(20) NOT NULL DEFAULT 'running' COMMENT '评估状态：running、success、error 或 skipped',
    checked_count INT NOT NULL DEFAULT 0 COMMENT '本次检查对象数量',
    matched_count INT NOT NULL DEFAULT 0 COMMENT '本次命中规则条件的对象数量',
    sent_count INT NOT NULL DEFAULT 0 COMMENT '本次成功投递的渠道消息数量',
    error_message VARCHAR(500) NULL COMMENT '评估失败或跳过原因',
    started_at BIGINT NOT NULL COMMENT '评估开始时间戳（毫秒）',
    completed_at BIGINT NULL COMMENT '评估完成时间戳（毫秒）',
    KEY idx_alert_evaluation_runs_rule (rule_id),
    KEY idx_alert_evaluation_runs_started_at (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警规则评估记录';

CREATE TABLE IF NOT EXISTS alert_evaluation_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    run_id BIGINT NOT NULL COMMENT '关联评估记录ID',
    metrics_json JSON NOT NULL COMMENT '评估耗时与分阶段指标',
    created_at DATETIME NULL COMMENT '指标记录创建时间',
    UNIQUE KEY uk_alert_evaluation_metric_run (run_id),
    KEY idx_alert_evaluation_metrics_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警评估耗时指标';
