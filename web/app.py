import sys
import os
from flask import Flask, render_template, jsonify, request
import json

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from src.binance_api import update_all_data
from src.data_processor import get_all_coins_data
from src.coin_manager import get_active_coins, load_coins_config_dict, save_coins_config_dict, add_coin, remove_coin, set_coin_tracking, update_coins_config
from src.utils import logger

app = Flask(__name__, template_folder='templates', static_folder='static')

@app.before_request
def log_request_info():
    """记录请求信息"""
    logger.info(f"请求: {request.method} {request.url}")
    if request.data:
        try:
            logger.info(f"请求数据: {request.get_json()}")
        except:
            logger.info(f"请求数据: {request.data}")

@app.after_request
def log_response_info(response):
    """记录响应信息"""
    logger.info(f"响应状态: {response.status}")
    return response

@app.route('/')
def index():
    logger.info("访问首页")
    return render_template('index.html')

@app.route('/api/coins')
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
            'success': True,
            'data': formatted_data
        }
        logger.info(f"返回 {len(formatted_data)} 个币种数据")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"获取币种数据失败: {e}")
        raise e
        return jsonify(error_response), 500

@app.route('/api/update')
def update_data():
    """手动更新数据"""
    logger.info("手动更新数据")
    try:
        update_all_data()
        response_data = {
            'success': True,
            'message': '数据更新成功'
        }
        logger.info(f"更新响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"更新数据失败: {e}")
        return jsonify(error_response), 500

@app.route('/coins-config')
def coins_config():
    """币种配置页面"""
    logger.info("访问币种配置页面")
    return render_template('coins_config.html')

@app.route('/api/coins-config')
def get_coins_config():
    """获取币种配置"""
    logger.info("获取币种配置")
    try:
        config = load_coins_config_dict()
        logger.info(f"获取到 {len(config)} 个币种配置")
        
        response_data = {
            'status': 'success',
            'data': config
        }
        logger.info(f"返回币种配置数据")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': str(e)
        }
        logger.error(f"获取币种配置失败: {e}")
        return jsonify(error_response), 500

@app.route('/api/coins-config', methods=['POST'])
def update_coins_config():
    """更新币种配置"""
    logger.info("更新币种配置")
    try:
        data = request.get_json()
        logger.info(f"更新配置请求数据: {data}")
        symbol = data.get('symbol')
        tracked = data.get('tracked')
        
        if not symbol:
            error_response = {
                'success': False,
                'error': '缺少symbol参数'
            }
            logger.error("更新币种配置失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        set_coin_tracking(symbol, tracked)
        
        response_data = {
            'success': True,
            'message': '配置更新成功'
        }
        logger.info(f"配置更新响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"更新币种配置失败: {e}")
        return jsonify(error_response), 500

@app.route('/api/coins-config/add', methods=['POST'])
def add_coin_api():
    """添加新币种"""
    logger.info("添加新币种")
    try:
        data = request.get_json()
        logger.info(f"添加币种请求数据: {data}")
        symbol = data.get('symbol')
        
        if not symbol:
            error_response = {
                'success': False,
                'error': '缺少symbol参数'
            }
            logger.error("添加币种失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        add_coin(symbol)
        
        response_data = {
            'success': True,
            'message': '币种添加成功'
        }
        logger.info(f"添加币种响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"添加币种失败: {e}")
        return jsonify(error_response), 500

@app.route('/api/coins-config/delete', methods=['POST'])
def delete_coin():
    """删除币种"""
    logger.info("删除币种")
    try:
        data = request.get_json()
        logger.info(f"删除币种请求数据: {data}")
        symbol = data.get('symbol')
        
        if not symbol:
            error_response = {
                'success': False,
                'error': '缺少symbol参数'
            }
            logger.error("删除币种失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        remove_coin(symbol)
        
        response_data = {
            'success': True,
            'message': '币种删除成功'
        }
        logger.info(f"删除币种响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"删除币种失败: {e}")
        return jsonify(error_response), 500

@app.route('/api/coins-config/track', methods=['POST'])
def set_coin_track():
    """设置币种跟踪状态"""
    logger.info("设置币种跟踪状态")
    try:
        data = request.get_json()
        logger.info(f"设置跟踪状态请求数据: {data}")
        symbol = data.get('symbol')
        tracked = data.get('tracked')
        
        if not symbol:
            error_response = {
                'success': False,
                'error': '缺少symbol参数'
            }
            logger.error("设置币种跟踪状态失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        set_coin_tracking(symbol, tracked)
        
        response_data = {
            'success': True,
            'message': '跟踪状态设置成功'
        }
        logger.info(f"设置跟踪状态响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'success': False,
            'error': str(e)
        }
        logger.error(f"设置币种跟踪状态失败: {e}")
        return jsonify(error_response), 500

@app.route('/api/coins-config/update-from-binance', methods=['POST'])
def update_coins_config_api():
    """从币安更新币种配置"""
    logger.info("从币安更新币种配置")
    try:
        # 调用币种管理模块的更新函数
        success = update_coins_config()
        
        if success:
            response_data = {
                'status': 'success',
                'message': '币种配置更新成功'
            }
            logger.info("币种配置更新成功")
            return jsonify(response_data)
        else:
            error_response = {
                'status': 'error',
                'message': '币种配置更新失败'
            }
            logger.error("币种配置更新失败")
            return jsonify(error_response), 500
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': str(e)
        }
        logger.error(f"更新币种配置失败: {e}")
        return jsonify(error_response), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)