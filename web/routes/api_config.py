from flask import Blueprint, jsonify, request
from src.utils import logger
from src.coin_manager import (
    load_coins_config_dict, 
    add_coin, 
    remove_coin, 
    set_coin_tracking, 
    update_coins_config as update_coins_list_from_binance
)

api_config_bp = Blueprint('api_config', __name__)

@api_config_bp.route('/api/coins-config')
def get_coins_config():
    """获取币种配置"""
    logger.info("获取币种配置")
    try:
        config = load_coins_config_dict()
        logger.info(f"获取到 {len(config)} 个币种配置")
        
        response_data = {
            'status': 'success',
            'message': '获取币种配置成功',
            'data': config
        }
        logger.info(f"返回币种配置数据")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'获取币种配置失败: {str(e)}'
        }
        logger.error(f"获取币种配置失败: {e}")
        return jsonify(error_response), 500

@api_config_bp.route('/api/coins-config', methods=['POST'])
def update_coins_config():
    """更新币种配置(设置跟踪状态)"""
    logger.info("更新币种配置")
    try:
        data = request.get_json()
        logger.info(f"更新配置请求数据: {data}")
        symbol = data.get('symbol')
        tracked = data.get('tracked')
        
        if not symbol:
            error_response = {
                'status': 'error',
                'message': '缺少symbol参数'
            }
            logger.error("更新币种配置失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        set_coin_tracking(symbol, tracked)
        
        response_data = {
            'status': 'success',
            'message': '配置更新成功'
        }
        logger.info(f"配置更新响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'更新币种配置失败: {str(e)}'
        }
        logger.error(f"更新币种配置失败: {e}")
        return jsonify(error_response), 500

@api_config_bp.route('/api/coins-config/add', methods=['POST'])
def add_coin_api():
    """添加新币种"""
    logger.info("添加新币种")
    try:
        data = request.get_json()
        logger.info(f"添加币种请求数据: {data}")
        symbol = data.get('symbol')
        
        if not symbol:
            error_response = {
                'status': 'error',
                'message': '缺少symbol参数'
            }
            logger.error("添加币种失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        add_coin(symbol)
        
        response_data = {
            'status': 'success',
            'message': '币种添加成功'
        }
        logger.info(f"添加币种响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'添加币种失败: {str(e)}'
        }
        logger.error(f"添加币种失败: {e}")
        return jsonify(error_response), 500

@api_config_bp.route('/api/coins-config/delete', methods=['POST'])
def delete_coin():
    """删除币种"""
    logger.info("删除币种")
    try:
        data = request.get_json()
        logger.info(f"删除币种请求数据: {data}")
        symbol = data.get('symbol')
        
        if not symbol:
            error_response = {
                'status': 'error',
                'message': '缺少symbol参数'
            }
            logger.error("删除币种失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        remove_coin(symbol)
        
        response_data = {
            'status': 'success',
            'message': '币种删除成功'
        }
        logger.info(f"删除币种响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'删除币种失败: {str(e)}'
        }
        logger.error(f"删除币种失败: {e}")
        return jsonify(error_response), 500

@api_config_bp.route('/api/coins-config/track', methods=['POST'])
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
                'status': 'error',
                'message': '缺少symbol参数'
            }
            logger.error("设置币种跟踪状态失败: 缺少symbol参数")
            return jsonify(error_response), 400
            
        set_coin_tracking(symbol, tracked)
        
        response_data = {
            'status': 'success',
            'message': '跟踪状态设置成功'
        }
        logger.info(f"设置跟踪状态响应: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        error_response = {
            'status': 'error',
            'message': f'设置币种跟踪状态失败: {str(e)}'
        }
        logger.error(f"设置币种跟踪状态失败: {e}")
        return jsonify(error_response), 500

@api_config_bp.route('/api/coins-config/update-from-binance', methods=['POST'])
def update_coins_config_api():
    """从币安更新币种配置"""
    logger.info("从币安更新币种配置")
    try:
        # 调用币种管理模块的更新函数
        success = update_coins_list_from_binance()
        
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
            'message': f'更新币种配置失败: {str(e)}'
        }
        logger.error(f"更新币种配置失败: {e}")
        return jsonify(error_response), 500
