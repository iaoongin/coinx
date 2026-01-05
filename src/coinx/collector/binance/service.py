import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from coinx.config import TIME_INTERVALS
from coinx.utils import save_all_coins_data, logger
# 避免循环引用，如果是从外部导入 binance_api，而 coin_manager 可能也导入 binance_api
# 但是 coin_manager 的 get_all_coins_from_binance 似乎没有依赖 binance_api 的核心逻辑
from coinx.coin_manager import get_active_coins, get_all_coins_from_binance
from .market import (
    get_open_interest,
    get_latest_price,
    get_24hr_ticker,
    get_open_interest_history
)
from .indicators import get_net_inflow_data
from .cache import should_update_cache, save_cached_data, get_cache_key, save_drop_list_cache, should_update_drop_list_cache, load_drop_list_cache

def get_all_coins_list():
    """获取所有币种列表"""
    try:
        all_coins = get_all_coins_from_binance()
        if all_coins:
            return [coin['symbol'] for coin in all_coins]
        else:
            # 如果无法从币安获取，使用已知的币种列表
            return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'DOTUSDT']
    except Exception as e:
        logger.error(f"获取所有币种列表失败: {e}")
        # 返回默认列表
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

def update_single_coin_data(symbol):
    """
    更新单个币种的数据（并行处理多个时间周期）
    :param symbol: 币种
    """
    try:
        # 获取当前持仓量
        current_data = get_open_interest(symbol)
        if not current_data:
            logger.warning(f"未获取到 {symbol} 当前持仓量数据")
            return None
        
        # 获取最新价格并计算持仓价值
        if current_data and current_data.get('openInterestValue', 0) == 0:
            latest_price = get_latest_price(symbol)
            if latest_price is not None:
                open_interest = current_data.get('openInterest', 0)
                calculated_value = open_interest * latest_price
                current_data['openInterestValue'] = calculated_value
                logger.info(f"使用最新价格计算 {symbol} 持仓价值: {open_interest} * {latest_price} = {calculated_value}")
            else:
                logger.warning(f"无法获取 {symbol} 最新价格，持仓价值仍为0")
        
        # 获取24小时价格变化数据
        price_change_data = get_24hr_ticker(symbol)
        
        # 并行获取各时间间隔的历史数据，减少线程数避免资源耗尽
        intervals_data = []
        
        try:
            # 降低时间周期请求并发，缓解风控
            with ThreadPoolExecutor(max_workers=min(2, len(TIME_INTERVALS) or 1)) as executor:
                # 提交所有时间周期的任务
                future_to_interval = {
                    executor.submit(get_open_interest_history, symbol, interval, limit=2): interval 
                    for interval in TIME_INTERVALS
                }
                
                # 收集结果
                for future in as_completed(future_to_interval):
                    interval = future_to_interval[future]
                    try:
                        history_data = future.result(timeout=30)  # 设置超时时间
                        if history_data:
                            # 保存历史数据用于计算变化比例
                            intervals_data.append(history_data)
                            
                            logger.info(f"已获取 {symbol} {interval} 数据: {history_data['openInterest']}")
                        else:
                            logger.warning(f"未获取到 {symbol} {interval} 历史数据")
                    except Exception as e:
                        logger.error(f"获取 {symbol} {interval} 数据时出错: {e}")
                        logger.exception(e)
        except RuntimeError as e:
            if "cannot schedule new futures after interpreter shutdown" in str(e):
                logger.error(f"检测到解释器关闭，停止并行处理 {symbol} 的时间周期数据")
                # 降级为串行处理
                for interval in TIME_INTERVALS:
                    history_data = get_open_interest_history(symbol, interval, limit=2)
                    if history_data:
                        intervals_data.append(history_data)
                        
                        logger.info(f"已获取 {symbol} {interval} 数据（串行）: {history_data['openInterest']}")
                    else:
                        logger.warning(f"未获取到 {symbol} {interval} 历史数据（串行）")
            else:
                logger.error(f"运行时错误: {e}")
                logger.exception(e)
        except Exception as e:
            logger.error(f"并行处理时间周期数据过程中出现未预期的错误: {e}")
            logger.exception(e)
        
        # 组装币种数据
        coin_data = {
            'symbol': symbol,
            'current': current_data,
            'intervals': intervals_data,
            'price_change': price_change_data,  # 添加价格变化数据
            'net_inflow': get_net_inflow_data(symbol),  # 添加主力净流入数据
            'update_time': int(time.time() * 1000)
        }
        
        return coin_data
    except Exception as e:
        logger.error(f"更新 {symbol} 数据时出错: {e}")
        logger.exception(e)
        return None

def update_all_data(symbols=None, force_update=False):
    """
    更新所有币种的数据（并行处理）
    :param symbols: 币种列表，如果为None则从配置中获取
    :param force_update: 是否强制更新，忽略缓存
    """
    # 检查是否需要更新缓存
    if not force_update and not should_update_cache():
        logger.info("当前5分钟周期内已有缓存数据，跳过更新")
        return None
    
    # 如果没有提供币种列表，则仅获取启用跟踪/订阅的币种
    if symbols is None:
        try:
            symbols = get_active_coins()
            logger.info(f"使用订阅币种进行数据更新，共 {len(symbols)} 个")
        except Exception as e:
            logger.error(f"获取订阅币种失败，回退到空列表: {e}")
            symbols = []
    
    logger.info(f"开始更新数据，共 {len(symbols)} 个币种...")
    
    all_coins_data = []
    
    # 使用线程池并行处理多个币种，减少线程数避免资源耗尽
    try:
        # 降低币种层面的并发，避免同时跑太多请求
        with ThreadPoolExecutor(max_workers=min(2, len(symbols) or 1)) as executor:
            # 提交所有币种的任务
            future_to_symbol = {
                executor.submit(update_single_coin_data, symbol): symbol 
                for symbol in symbols
            }
            
            # 收集结果
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    coin_data = future.result(timeout=30)  # 设置超时时间
                    if coin_data:
                        all_coins_data.append(coin_data)
                        logger.info(f"已更新 {symbol} 数据")
                    else:
                        logger.warning(f"未获取到 {symbol} 数据")
                except Exception as e:
                    logger.error(f"更新 {symbol} 数据时出错: {e}")
                    logger.exception(e)
    except RuntimeError as e:
        if "cannot schedule new futures after interpreter shutdown" in str(e):
            logger.error("检测到解释器关闭，停止并行处理")
            # 降级为串行处理
            for symbol in symbols:
                coin_data = update_single_coin_data(symbol)
                if coin_data:
                    all_coins_data.append(coin_data)
                    logger.info(f"已更新 {symbol} 数据（串行）")
                else:
                    logger.warning(f"未获取到 {symbol} 数据（串行）")
        else:
            logger.error(f"运行时错误: {e}")
            logger.exception(e)
    except Exception as e:
        logger.error(f"并行处理过程中出现未预期的错误: {e}")
        logger.exception(e)
    
    # 保存所有币种数据
    if all_coins_data:
        try:
            save_all_coins_data(all_coins_data)
            # 保存到缓存
            cache_key = get_cache_key()
            save_cached_data(cache_key, all_coins_data)
            logger.info("所有币种数据已保存")
        except Exception as e:
            logger.error(f"保存所有币种数据失败: {e}")
            logger.exception(e)
    else:
        logger.warning("没有币种数据需要保存")
    
    logger.info("数据更新完成")
    return all_coins_data

def update_drop_list_data(force_update=False):
    """
    更新跌幅榜数据并缓存
    :param force_update: 是否强制更新
    """
    try:
        # Check if cache update is needed
        if not force_update and not should_update_drop_list_cache():
            logger.info("当前5分钟周期内已有跌幅榜缓存数据，跳过更新")
            # Try to return cached data
            cache_data = load_drop_list_cache()
            cache_key = get_cache_key()
            if str(cache_key) in cache_data:
                return cache_data[str(cache_key)]['data']
        
        logger.info("开始更新跌幅榜数据...")
        
        from coinx.collector.binance.market import get_all_24hr_tickers
        
        # 1. 获取当前所有处于 TRADING 状态的 USDT 合约
        valid_coins_list = get_all_coins_from_binance()
        valid_symbols = set()
        if valid_coins_list:
             valid_symbols = {c['symbol'] for c in valid_coins_list}
        
        # 2. 获取所有币种的24小时数据
        all_tickers = get_all_24hr_tickers()
        
        # 3. 过滤只保留有效的 TRADING 币种
        if valid_symbols:
            filtered_tickers = [t for t in all_tickers if t['symbol'] in valid_symbols]
        else:
            # 如果获取交易对信息失败，暂时不过滤，或者保留所有
            filtered_tickers = all_tickers
        
        # 按跌幅排序（涨幅百分比升序）
        filtered_tickers.sort(key=lambda x: x['priceChangePercent'])
        
        # 取前100名
        top_losers = filtered_tickers[:100]
        
        # 格式化数据以适应前端展示
        formatted_data = []
        for coin in top_losers:
            formatted_coin = {
                'symbol': coin['symbol'],
                'current_price': coin['lastPrice'],
                'current_price_formatted': str(coin['lastPrice']),
                'price_change_percent': coin['priceChangePercent'],
                'price_change_formatted': f"{coin['priceChange']:.4f}",
                'volume': coin['volume'],
                'quote_volume': coin['quoteVolume'],
                
                # 填充 renderData 需要的字段
                'current_open_interest': None,
                'current_open_interest_formatted': 'N/A',
                'current_open_interest_value': None,
                'current_open_interest_value_formatted': 'N/A',
                'net_inflow': {},
                'changes': []
            }
            formatted_data.append(formatted_coin)
            
        # 保存到缓存
        try:
            cache_key = get_cache_key()
            save_drop_list_cache(cache_key, formatted_data)
            logger.info(f"跌幅榜数据已更新并缓存，主要数据 {len(formatted_data)} 条")
        except Exception as e:
            logger.error(f"保存跌幅榜缓存失败: {e}")
            
        return formatted_data
        
    except Exception as e:
        logger.error(f"更新跌幅榜数据失败: {e}")
        logger.exception(e)
        return []
