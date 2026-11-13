"""Database storage statistics API routes."""

from flask import Blueprint, jsonify

from coinx.repositories.database_stats import get_database_stats
from coinx.utils import logger


api_database_stats_bp = Blueprint('api_database_stats', __name__)


@api_database_stats_bp.route('/api/database-stats')
def database_stats():
    try:
        return jsonify({
            'status': 'success',
            'message': '数据库存储统计加载成功',
            'data': get_database_stats(),
        })
    except Exception as exc:
        logger.error('加载数据库存储统计失败: %s', exc)
        logger.exception(exc)
        return jsonify({
            'status': 'error',
            'message': f'加载数据库存储统计失败：{exc}',
        }), 500
