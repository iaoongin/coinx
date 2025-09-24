import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from src.utils import load_all_coins_data, logger
from src.config import TIME_INTERVALS

def format_number(num):
    """
    将数字格式化为易读的格式 (k, m, b)
    :param num: 数字
    :return: 格式化后的字符串
    """
    if num is None:
        return "N/A"
    
    abs_num = abs(num)
    if abs_num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}b"
    elif abs_num >= 1_000_000:
        return f"{num / 1_000_000:.2f}m"
    elif abs_num >= 1_000:
        return f"{num / 1_000:.2f}k"
    elif abs_num >= 1:
        return f"{num:.2f}"
    # elif abs_num >= 0.1:
    #     return f"{num:.3f}"
    # elif abs_num >= 0.01:
    #     return f"{num:.4f}"
    # elif abs_num >= 0.001:
    #     return f"{num:.5f}"
    # elif abs_num >= 0.0001:
    #     return f"{num:.6f}"
    else:
        # 对于非常小的数字，使用科学计数法
        return f"{num:.5e}"

def get_coin_data(symbol='BTCUSDT'):
    """
    获取币种数据用于展示
    :param symbol: 币种
    :return: 包含当前持仓量和变化比例的数据
    """
    # 加载所有币种数据
    all_coins_data = load_all_coins_data()
    
    # 查找指定币种的数据
    symbol_data = None
    for coin_data in all_coins_data:
        if coin_data.get('symbol') == symbol:
            symbol_data = coin_data
            break
    
    if not symbol_data:
        return {
            'symbol': symbol,
            'current_open_interest': None,
            'current_open_interest_formatted': "N/A",
            'current_open_interest_value': None,
            'current_open_interest_value_formatted': "N/A",
            'current_price': None,
            'current_price_formatted': "N/A",
            'price_change': None,
            'price_change_percent': None,
            'price_change_formatted': "N/A",
            'changes': {}
        }
    
    current_interest = symbol_data.get('current', {}).get('openInterest') if symbol_data.get('current') else None
    current_interest_value = symbol_data.get('current', {}).get('openInterestValue') if symbol_data.get('current') else None
    price_change_data = symbol_data.get('price_change', {})
    # 通过持仓价值除以持仓量计算当前价格
    current_price = None
    if current_interest is not None and current_interest != 0 and current_interest_value is not None:
        current_price = current_interest_value / current_interest
    price_change = price_change_data.get('priceChange') if price_change_data else None
    price_change_percent = price_change_data.get('priceChangePercent') if price_change_data else None
    
    result = {
        'symbol': symbol,
        'current_open_interest': current_interest,
        'current_open_interest_formatted': format_number(current_interest),
        'current_open_interest_value': current_interest_value,
        'current_open_interest_value_formatted': format_number(current_interest_value),
        'current_price': current_price,
        'current_price_formatted': format_number(current_price) if current_price is not None else "N/A",
        'price_change': price_change,
        'price_change_percent': price_change_percent,
        'price_change_formatted': format_number(price_change) if price_change is not None else "N/A",
        'changes': {}
    }
    
    # 计算各时间间隔的变化比例
    if current_interest is not None:
        # 创建一个字典来存储每个时间间隔的数据，便于查找
        interval_data_map = {}
        for item in symbol_data.get('intervals', []):
            interval = item.get('interval')
            if interval:
                interval_data_map[interval] = item
        
        # 包含5m数据
        for interval in TIME_INTERVALS:
            # 从历史数据中获取指定时间间隔的数据
            interval_data = interval_data_map.get(interval)
            
            if interval_data:
                # 使用历史数据中的当前值和前一个值
                current_interval_interest = interval_data.get('openInterest')
                current_interval_interest_value = interval_data.get('openInterestValue')
                
                ratio = None
                value_ratio = None
                price_change = None
                price_change_percent = None
                
                current_interval_price = None
                
                # 计算当前价格（持仓价值/持仓量）
                if current_interval_interest is not None and current_interval_interest != 0 and current_interval_interest_value is not None:
                    current_interval_price = current_interval_interest_value / current_interval_interest
                
                # 计算价格变化和价格变化百分比（当前价格与历史价格比较）
                price_change = None
                price_change_percent = None
                if current_interval_price is not None and current_price is not None:
                    price_change =  current_price - current_interval_price
                    if current_price != 0:
                        price_change_percent = (price_change / current_price) * 100
                
                # 计算持仓量变化比例（当前与前一个时间点比较）
                if current_interest is not None and current_interval_interest != 0:
                    ratio = (( current_interest - current_interval_interest) / current_interest) * 100
                
                # 计算持仓价值变化比例（当前与前一个时间点比较）
                if current_interval_interest_value is not None and current_interest_value is not None and current_interval_interest_value != 0:
                    value_ratio = ((current_interest_value - current_interval_interest_value ) / current_interest_value) * 100
                
                result['changes'][interval] = {
                    'ratio': round(ratio, 2) if ratio is not None else (0 if current_interval_interest == 0 else None),
                    'value_ratio': round(value_ratio, 2) if value_ratio is not None else None,
                    'open_interest': current_interval_interest,
                    'open_interest_formatted': format_number(current_interval_interest),
                    'open_interest_value': current_interval_interest_value,
                    'open_interest_value_formatted': format_number(current_interval_interest_value),
                    'price_change': price_change,
                    'price_change_percent': price_change_percent,
                    'price_change_formatted': format_number(price_change) if price_change is not None else "N/A",
                    'current_price': current_interval_price,
                    'current_price_formatted': format_number(current_interval_price) if current_interval_price is not None else "N/A",
                }
            else:
                result['changes'][interval] = {
                    'ratio': None,
                    'value_ratio': None,
                    'open_interest': None,
                    'open_interest_formatted': "N/A",
                    'open_interest_value': None,
                    'open_interest_value_formatted': "N/A",
                    'price_change': None,
                    'price_change_percent': None,
                    'price_change_formatted': "N/A",
                    'current_price': None,
                    'current_price_formatted': "N/A",
                }
    
    return result

def get_all_coins_data(symbols=['BTCUSDT']):
    """
    获取所有币种的数据
    :param symbols: 币种列表
    :return: 所有币种数据列表
    """
    coins_data = []
    for symbol in symbols:
        coin_data = get_coin_data(symbol)
        coins_data.append(coin_data)
    return coins_data
