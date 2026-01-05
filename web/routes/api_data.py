from flask import Blueprint, jsonify, request
import threading
from src.utils import logger, get_cache_update_time
from src.data_processor import get_all_coins_data
from src.coin_manager import get_active_coins
from src.binance_api import (
    should_update_cache, update_all_data,
    get_latest_price, get_24hr_ticker, get_all_24hr_tickers, get_open_interest,
    get_funding_rate, get_long_short_ratio,
    get_exchange_distribution_real, get_net_inflow_data as get_net_inflow_data_real
)

api_data_bp = Blueprint('api_data', __name__)

@api_data_bp.route('/api/coins')
def get_coins():
    """获取所有活跃币种的数据"""
    logger.info("获取所有币种数据")
    try:
        # 获取活跃的币种列表
        active_coins = get_active_coins()
        logger.info(f"活跃币种: {active_coins}")
        
        # 获取所有币种数据
        coins_data = get_all_coins_data(active_coins)
        logger.info(f"获取到 {len(coins_data)} 个币种的数据")
        logger.info(f"原始数据示例: {coins_data[:2] if coins_data else '无数据'}")
        
        # 获取缓存更新时间
        cache_update_time = get_cache_update_time()
        
        # 处理数据格式，使其更适合前端展示
        formatted_data = []

        for coin in coins_data:
            formatted_coin = {
                'symbol': coin['symbol'],
                'current_open_interest': coin['current_open_interest'],
                'current_open_interest_formatted': coin['current_open_interest_formatted'],
                'current_open_interest_value': coin['current_open_interest_value'],
                'current_open_interest_value_formatted': coin['current_open_interest_value_formatted'],
                'current_price': coin['current_price'],
                'current_price_formatted': coin['current_price_formatted'],
                'price_change': coin['price_change'],
                'price_change_percent': coin['price_change_percent'],
                'price_change_formatted': coin['price_change_formatted']
            }

            # 主力净流入（多时间间隔）
            formatted_coin['net_inflow'] = coin.get('net_inflow', {})
            
            # 处理变化数据，转换为数组格式
            changes = []
            if coin['changes']:
                for interval, data in coin['changes'].items():
                    changes.append({
                        'interval': interval,
                        'ratio': data['ratio'],
                        'value_ratio': data['value_ratio'],
                        'open_interest': data['open_interest'],
                        'open_interest_formatted': data['open_interest_formatted'],
                        'open_interest_value': data['open_interest_value'],
                        'open_interest_value_formatted': data['open_interest_value_formatted'],
                        'price_change': data['price_change'],
                        'price_change_percent': data['price_change_percent'],
                        'price_change_formatted': data['price_change_formatted'],
                        'current_price': data['current_price'],
                        # 'past_price': data['past_price'],
                        'current_price_formatted': data['current_price_formatted'],
                        # 'past_price_formatted': data['past_price_formatted']
                    })
            
            # 按时间间隔排序
            changes.sort(key=lambda x: (
                x['interval'].endswith('m') and int(x['interval'][:-1]) or
                x['interval'].endswith('h') and int(x['interval'][:-1]) * 60 or
                x['interval'].endswith('d') and int(x['interval'][:-1]) * 1440 or
                0
            ))
            
            formatted_coin['changes'] = changes
            formatted_data.append(formatted_coin)
        
        logger.info(f"格式化后的数据数量: {len(formatted_data)}")
        logger.info(f"格式化后的数据示例: {formatted_data[:2] if formatted_data else '无数据'}")
        
        response_data = {
            'status': 'success',
            'message': '获取币种数据成功',
            'data': formatted_data,
            'cache_update_time': cache_update_time  # 添加缓存更新时间
        }
        logger.info(f"返回 {len(formatted_data)} 个币种数据")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'获取币种数据失败: {str(e)}'
        }
        logger.error(f"获取币种数据失败: {e}")
        logger.exception(e)
        return jsonify(error_response), 500

@api_data_bp.route('/api/update')
def update_data():
    """手动更新数据"""
    logger.info("手动更新数据")
    try:
        # Check for force parameter
        force = request.args.get('force', 'false').lower() == 'true'
        
        # 如果不需要更新，且不是强制更新，直接返回成功消息
        if not force and not should_update_cache():
            response_data = {
                'status': 'success',
                'message': '数据已是最新，无需更新'
            }
            logger.info("数据已是最新，无需更新")
            return jsonify(response_data)
        
        # 在后台线程中执行数据更新，不阻塞Web请求（仅更新订阅币种）
        symbols = []
        try:
            symbols = get_active_coins()
            logger.info(f"手动更新仅针对订阅币种: {symbols}")
        except Exception as e:
            logger.error(f"获取订阅币种失败，将使用空列表: {e}")
            symbols = []

        update_thread = threading.Thread(target=update_all_data, kwargs={'symbols': symbols, 'force_update': True})
        update_thread.daemon = True
        update_thread.start()
        
        response_data = {
            'status': 'success',
            'message': '已触发数据更新，请稍后刷新页面查看最新数据'
        }
        logger.info(f"更新响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'数据更新失败: {str(e)}'
        }
        logger.error(f"更新数据失败: {e}")
        return jsonify(error_response), 500

@api_data_bp.route('/api/coin-detail/<symbol>')
def get_coin_detail(symbol):
    """获取指定币种的详细数据"""
    logger.info(f"获取币种详情: {symbol}")
    try:
        # 获取基础数据
        latest_price = get_latest_price(symbol)
        ticker_data = get_24hr_ticker(symbol)
        open_interest_data = get_open_interest(symbol)
        funding_rate = get_funding_rate(symbol)
        long_short_ratio = get_long_short_ratio(symbol)
        
        detail_data = {
            'symbol': symbol,
            'latest_price': latest_price,
            'funding_rate': funding_rate,
            'ticker_data': ticker_data,
            'open_interest_data': open_interest_data,
            'long_short_ratio': long_short_ratio,
            'exchange_distribution': get_exchange_distribution_real(symbol),
            'net_inflow_data': get_net_inflow_data_real(symbol)
        }
        
        response_data = {
            'status': 'success',
            'message': '获取币种详情成功',
            'data': detail_data
        }
        logger.info(f"返回 {symbol} 详情数据")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'获取币种详情失败: {str(e)}'
        }
        logger.error(f"获取币种详情失败: {e}")
        return jsonify(error_response), 500
        return jsonify(error_response), 500

@api_data_bp.route('/api/drop-list')
@api_data_bp.route('/api/drop-list')
def get_drop_list():
    """获取跌幅榜数据"""
    logger.info("获取跌幅榜数据")
    try:
        from src.binance_api import update_drop_list_data
        
        # Check for force parameter
        force = request.args.get('force', 'false').lower() == 'true'
        
        # Get data (with cache logic handled inside update_drop_list_data)
        data = update_drop_list_data(force_update=force)
        
        if not data:
             # Try to load from cache even if update failed or returned empty (though update_drop_list_data tries to return cache if not updating)
             pass 

        response_data = {
            'status': 'success',
            'message': '获取跌幅榜数据成功',
            'data': data
        }
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'获取跌幅榜数据失败: {str(e)}'
        }
        logger.error(f"获取跌幅榜数据失败: {e}")
        return jsonify(error_response), 500
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'获取跌幅榜数据失败: {str(e)}'
        }
        logger.error(f"获取跌幅榜数据失败: {e}")
        return jsonify(error_response), 500
