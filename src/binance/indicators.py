from src.utils import logger
from .market import (
    get_futures_kline_latest,
    aggregate_futures_kline,
    get_open_interest,
    get_latest_price
)

def get_net_inflow_data(symbol):
    """基于期货K线的主动买入成交额估算主力净流入（单位：报价货币）"""
    period_to_interval = {
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '4h': '4h',
        '8h': '8h',
        '12h': '12h',
        '24h': '1d',
        '72h': '3d',
        '168h': '1w'
    }
    result = {}
    for period, interval in period_to_interval.items():
        # 先直接取目标周期最新一根
        k = get_futures_kline_latest(symbol, interval)
        if not k:
            # 回退：用细粒度聚合
            agg = None
            try:
                if period.endswith('m'):
                    minutes = int(period[:-1])
                    # 聚合 5m 作为基础
                    count = max(1, minutes // 5)
                    agg = aggregate_futures_kline(symbol, '5m', count)
                elif period.endswith('h'):
                    hours = int(period[:-1])
                    # 聚合 1h 作为基础
                    count = max(1, hours)
                    agg = aggregate_futures_kline(symbol, '1h', count)
                elif period.endswith('d'):
                    days = int(period[:-1])
                    count = max(1, days * 24)
                    agg = aggregate_futures_kline(symbol, '1h', count)
            except Exception as e:
                logger.error(f"净流入回退聚合失败: {symbol}, {period}, 错误: {e}")
                agg = None
            if agg:
                quote_vol = agg.get('quoteVolume', 0.0)
                taker_buy_quote = agg.get('takerBuyQuoteVolume', 0.0)
                result[period] = 2.0 * taker_buy_quote - quote_vol
            else:
                result[period] = None
            continue
        quote_vol = k.get('quoteVolume', 0.0)
        taker_buy_quote = k.get('takerBuyQuoteVolume', 0.0)
        # 估算净流入 = 主动买入额 - 主动卖出额 = 2 * takerBuyQuoteVolume - quoteVolume
        net_inflow = 2.0 * taker_buy_quote - quote_vol
        result[period] = net_inflow
    return result

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
