-- 币种配置表
CREATE TABLE IF NOT EXISTS coins (
    symbol VARCHAR(20) PRIMARY KEY COMMENT '交易对符号',
    is_tracking BOOLEAN DEFAULT TRUE COMMENT '是否启用跟踪',
    
    base_asset VARCHAR(100) COMMENT '标的资产',
    quote_asset VARCHAR(100) COMMENT '报价资产',
    margin_asset VARCHAR(100) COMMENT '保证金资产',
    
    price_precision INT COMMENT '价格精度',
    quantity_precision INT COMMENT '数量精度',
    base_asset_precision INT COMMENT '标的资产精度',
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
    data_json JSON COMMENT '完整数据JSON (包含intervals, net_inflow等)',
    snapshot_time BIGINT NOT NULL COMMENT '快照时间戳(毫秒)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    INDEX idx_snapshot_time (snapshot_time),
    INDEX idx_batch_id (batch_id),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场数据快照表';
