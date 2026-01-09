from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, DECIMAL, BigInteger, Text, Index
from sqlalchemy.dialects.mysql import JSON
from coinx.database import Base

class Coin(Base):
    """币种配置模型"""
    __tablename__ = 'coins'

    symbol = Column(String(50), primary_key=True, comment='交易对符号')
    is_tracking = Column(Boolean, default=True, comment='是否启用跟踪')
    
    # 基础信息
    base_asset = Column(String(100), comment='标的资产')
    quote_asset = Column(String(100), comment='报价资产')
    margin_asset = Column(String(100), comment='保证金资产')
    
    # 精度信息
    price_precision = Column(Integer, comment='价格精度')
    quantity_precision = Column(Integer, comment='数量精度')
    base_asset_precision = Column(Integer, comment='标的资产精度')
    quote_precision = Column(Integer, comment='报价精度')
    
    # 状态信息
    status = Column(String(50), comment='交易状态')
    onboard_date = Column(BigInteger, comment='上线时间')
    delivery_date = Column(BigInteger, comment='交割时间')
    
    # 合约信息
    contract_type = Column(String(50), comment='合约类型')
    underlying_type = Column(String(50), comment='标的类型')
    
    # 手续费和风控
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

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='自增ID')
    batch_id = Column(String(50), nullable=False, index=True, comment='批次ID')
    symbol = Column(String(20), nullable=False, index=True, comment='交易对符号')
    price = Column(DECIMAL(24, 8), comment='当前价格')
    open_interest = Column(DECIMAL(24, 8), comment='持仓量')
    open_interest_value = Column(DECIMAL(24, 8), comment='持仓价值')
    # 使用JSON类型存储复杂结构，兼容现有逻辑
    data_json = Column(JSON, comment='完整数据JSON')
    snapshot_time = Column(BigInteger, nullable=False, index=True, comment='快照时间戳(毫秒)')
    created_at = Column(DateTime, default=datetime.now, comment='记录创建时间')

    def __repr__(self):
        return f"<MarketSnapshot(symbol='{self.symbol}', time={self.snapshot_time})>"
