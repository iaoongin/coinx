import time

from flask import Blueprint, jsonify, request

from coinx.config import FUNDING_RATE_ABNORMAL_THRESHOLD
from coinx.repositories.funding_rate import (
    collect_funding_rates,
    load_abnormal_funding_rates,
    load_funding_rate_history,
    load_funding_rate_sparklines,
    load_latest_funding_rate_page,
)
from coinx.repositories.homepage_series import (
    format_funding_countdown,
    format_funding_rate,
)
from coinx.utils import logger


api_funding_rate_bp = Blueprint('api_funding_rate', __name__)


@api_funding_rate_bp.route('/api/funding-rate')
def get_funding_rates():
    """获取资金费率排行榜数据（支持搜索和分页）"""
    logger.info('开始加载资金费率数据')
    try:
        keyword = request.args.get('keyword', '').strip()
        show_abnormal_only = request.args.get('show_abnormal_only', '').lower() in ('true', '1')
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', type=int)
        page_size = request.args.get('page_size', type=int)
        if page_size is None:
            page_size = limit if limit is not None else 50
        page_size = min(max(page_size, 1), 200)
        sort_by = request.args.get('sort_by', 'funding_rate')
        sort_order = request.args.get('sort_order', 'desc')

        page_result = load_latest_funding_rate_page(
            keyword=keyword,
            show_abnormal_only=show_abnormal_only,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            threshold=FUNDING_RATE_ABNORMAL_THRESHOLD,
        )

        page_data = []
        for rate_obj in page_result['data']:
            predicted_rate = rate_obj['predicted_rate']
            funding_rate = rate_obj['funding_rate']
            next_funding_time = rate_obj['next_funding_time']

            page_data.append({
                'symbol': rate_obj['symbol'],
                'predicted_rate': predicted_rate,
                'predicted_rate_formatted': format_funding_rate(predicted_rate),
                'funding_rate': funding_rate,
                'funding_rate_formatted': format_funding_rate(funding_rate),
                'next_funding_time': next_funding_time,
                'next_funding_time_formatted': format_funding_countdown(next_funding_time),
                'mark_price': rate_obj['mark_price'],
                'is_abnormal': rate_obj['is_abnormal'],
                'event_time': rate_obj['event_time'],
            })

        visible_symbols = [item['symbol'] for item in page_data]
        sparkline_map = load_funding_rate_sparklines(visible_symbols, hours=24)
        for item in page_data:
            item['sparkline'] = sparkline_map.get(item['symbol'], [])

        return jsonify({
            'status': 'success',
            'message': 'funding rates loaded',
            'data': page_data,
            'total_count': page_result['total_count'],
            'page': page,
            'page_size': page_size,
            'threshold': FUNDING_RATE_ABNORMAL_THRESHOLD,
            'stats': page_result['stats'],
        })
    except Exception as e:
        logger.error(f'加载资金费率数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load funding rates: {str(e)}'}), 500


@api_funding_rate_bp.route('/api/funding-rate/abnormal')
def get_abnormal_funding_rates():
    """获取异常资金费率数据"""
    logger.info('开始加载异常资金费率数据')
    try:
        threshold = request.args.get('threshold', FUNDING_RATE_ABNORMAL_THRESHOLD, type=float)

        abnormal_rates = load_abnormal_funding_rates(threshold=threshold)

        data = []
        for rate_obj in abnormal_rates:
            predicted_rate = float(rate_obj['predicted_rate']) if rate_obj['predicted_rate'] is not None else None
            funding_rate = float(rate_obj['funding_rate']) if rate_obj['funding_rate'] is not None else None
            next_funding_time = int(rate_obj['next_funding_time']) if rate_obj['next_funding_time'] is not None else None

            data.append({
                'symbol': rate_obj['symbol'],
                'predicted_rate': predicted_rate,
                'predicted_rate_formatted': format_funding_rate(predicted_rate),
                'funding_rate': funding_rate,
                'funding_rate_formatted': format_funding_rate(funding_rate),
                'next_funding_time': next_funding_time,
                'next_funding_time_formatted': format_funding_countdown(next_funding_time),
                'mark_price': float(rate_obj['mark_price']) if rate_obj['mark_price'] is not None else None,
                'event_time': int(rate_obj['event_time']) if rate_obj['event_time'] else None,
            })

        data.sort(key=lambda x: abs(x['predicted_rate'] or 0), reverse=True)

        return jsonify({
            'status': 'success',
            'message': 'abnormal funding rates loaded',
            'data': data,
            'threshold': threshold,
        })
    except Exception as e:
        logger.error(f'加载异常资金费率数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load abnormal funding rates: {str(e)}'}), 500


@api_funding_rate_bp.route('/api/funding-rate/refresh')
def refresh_funding_rates():
    """手动触发资金费率采集"""
    logger.info('手动触发资金费率采集')
    try:
        count = collect_funding_rates()
        return jsonify({'status': 'success', 'message': 'funding rates collected', 'count': count})
    except Exception as e:
        logger.error(f'资金费率采集失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to collect funding rates: {str(e)}'}), 500


@api_funding_rate_bp.route('/api/funding-rate/history/<symbol>')
def get_funding_rate_history(symbol):
    """获取单个币种的资金费率历史"""
    logger.info(f'开始加载资金费率历史: {symbol}')
    try:
        hours = request.args.get('hours', 1, type=int)
        hours = min(max(hours, 1), 168)

        history = load_funding_rate_history(symbol, hours=hours)

        data = []
        for rate_obj in history:
            predicted_rate = float(rate_obj['predicted_rate']) if rate_obj['predicted_rate'] is not None else None
            funding_rate = float(rate_obj['funding_rate']) if rate_obj['funding_rate'] is not None else None

            data.append({
                'symbol': rate_obj['symbol'],
                'event_time': int(rate_obj['event_time']),
                'predicted_rate': predicted_rate,
                'predicted_rate_formatted': format_funding_rate(predicted_rate),
                'funding_rate': funding_rate,
                'funding_rate_formatted': format_funding_rate(funding_rate),
                'mark_price': float(rate_obj['mark_price']) if rate_obj['mark_price'] is not None else None,
            })

        return jsonify({
            'status': 'success',
            'message': f'funding rate history loaded for {symbol}',
            'data': data,
            'symbol': symbol,
            'hours': hours,
        })
    except Exception as e:
        logger.error(f'加载资金费率历史失败: {symbol}, 错误: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load funding rate history: {str(e)}'}), 500
