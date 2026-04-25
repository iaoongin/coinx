from coinx.utils import logger
from .market import (
    get_open_interest,
    get_latest_price
)

def get_exchange_distribution_real(symbol):
    """使用币安U本位合约的持仓量与价格估算持仓价值，单一来源：binance=100%"""
    try:
        oi = get_open_interest(symbol) or {}
        value = float(oi.get('openInterestValue', 0) or 0)
        if value == 0:
            # 若接口未返回持仓价值，则用最新价格估算
            latest_price = get_latest_price(symbol)
            if latest_price is not None:
                value = float(oi.get('openInterest', 0) or 0) * latest_price
        # 单一来源（仅币安）
        return {
            'binance': {
                'value': value,
                'percentage': 100.0
            }
        }
    except Exception as e:
        logger.error(f"计算交易所持仓分布失败: {symbol}, 错误: {e}")
        return {
            'binance': {
                'value': 0,
                'percentage': 100.0
            }
        }
