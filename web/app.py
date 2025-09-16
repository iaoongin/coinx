from flask import Flask, render_template, jsonify, request
import sys
import os
import time

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.data_processor import get_all_coins_data
from src.binance_api import update_all_data

app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'static'))

# 缓存数据和时间戳
cached_data = None
last_update_time = 0
CACHE_DURATION = 60  # 缓存60秒

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/coins')
def coins_api():
    """获取币种数据的API"""
    global cached_data, last_update_time
    
    # 检查是否有有效的缓存数据
    current_time = time.time()
    if cached_data and (current_time - last_update_time) < CACHE_DURATION:
        # 返回缓存数据
        return jsonify(cached_data)
    
    # 更新数据
    try:
        update_all_data(['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'])
    except Exception as e:
        pass  # 即使更新失败也继续返回现有数据
    
    # 获取所有币种数据
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
    coins_data = get_all_coins_data(symbols)
    
    # 更新缓存
    cached_data = coins_data
    last_update_time = current_time
    
    return jsonify(coins_data)

@app.route('/api/update')
def update_api():
    """手动更新数据的API"""
    global cached_data, last_update_time
    
    try:
        update_all_data(['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'])
        
        # 获取所有币种数据
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
        coins_data = get_all_coins_data(symbols)
        
        # 更新缓存
        cached_data = coins_data
        last_update_time = time.time()
        
        return jsonify({'status': 'success', 'message': '数据更新成功'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'数据更新失败: {str(e)}'})

@app.route('/api/force-update')
def force_update_api():
    """强制更新数据的API（忽略缓存）"""
    global cached_data, last_update_time
    
    try:
        update_all_data(['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'])
        
        # 获取所有币种数据
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
        coins_data = get_all_coins_data(symbols)
        
        # 更新缓存
        cached_data = coins_data
        last_update_time = time.time()
        
        return jsonify({'status': 'success', 'message': '数据强制更新成功', 'data': coins_data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'数据更新失败: {str(e)}'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)