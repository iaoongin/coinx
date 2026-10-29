from coinx.utils import logger
from .market import (
    get_open_interest,
    get_latest_price
)

def get_exchange_distribution_real(symbol):
    """使用币安U本位合约的持仓量与价格估算持仓价值，单一来源：binance=100%"""
    try:
        oi = get_open_interest(symbol)
        if not oi:
            return {}
        value = oi.get('openInterestValue')
        if value is None:
            # 若接口未返回持仓价值，则用最新价格估算
            latest_price = get_latest_price(symbol)
            open_interest = oi.get('openInterest')
            if latest_price is not None and open_interest is not None:
                value = float(open_interest) * latest_price
        if value is None:
            return {'binance': {'value': None, 'percentage': None}}
        # 单一来源（仅币安）
        return {
            'binance': {
                'value': float(value),
                'percentage': 100.0
            }
        }
    except Exception as e:
        logger.error(f"计算交易所持仓分布失败: {symbol}, 错误: {e}")
        return {}
