from .utils import load_all_coins_data, logger
from .config import TIME_INTERVALS

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
            'changes': {}
        }
    
    result = {
        'symbol': symbol,
        'current_open_interest': symbol_data.get('current', {}).get('openInterest') if symbol_data.get('current') else None,
        'changes': {}
    }
    
    # 计算各时间间隔的变化比例
    current_interest = result['current_open_interest']
    if current_interest is not None:
        for interval in TIME_INTERVALS:
            if interval == '5m':
                continue  # 跳过5m自身
            
            # 从历史数据中获取指定时间间隔的数据
            interval_data = None
            for item in symbol_data.get('intervals', []):
                if item.get('interval') == interval:
                    interval_data = item
                    break
            
            if interval_data:
                past_interest = interval_data.get('openInterest')
                if past_interest is not None and past_interest != 0:
                    ratio = ((current_interest - past_interest) / past_interest) * 100
                    result['changes'][interval] = round(ratio, 2)
                else:
                    result['changes'][interval] = 0 if past_interest == 0 else None
            else:
                result['changes'][interval] = None
    
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