import time

from flask import Blueprint, jsonify, request

from coinx.coin_manager import get_active_coins
from coinx.collector.binance.market import get_exchange_info
from coinx.config import FUNDING_RATE_ABNORMAL_THRESHOLD
from coinx.repositories.funding_rate import (
    collect_funding_rates,
    load_abnormal_funding_rates,
    load_funding_rate_history,
    load_latest_funding_rates,
)
from coinx.repositories.homepage_series import (
    format_funding_countdown,
    format_funding_rate,
)
from coinx.utils import logger


api_funding_rate_bp = Blueprint('api_funding_rate', __name__)


@api_funding_rate_bp.route('/api/funding-rate')
def get_funding_rates():
    """获取资金费率排行榜数据"""
    logger.info('开始加载资金费率数据')
    try:
        limit = request.args.get('limit', type=int)
        sort_by = request.args.get('sort_by', 'funding_rate')
        sort_order = request.args.get('sort_order', 'desc')

        exchange_info = get_exchange_info()
        if not exchange_info:
            return jsonify({
                'status': 'success',
                'message': 'exchange info unavailable',
                'data': [],
            })

        all_symbols = [s['symbol'] for s in exchange_info]
        funding_rate_map = load_latest_funding_rates(all_symbols)

        data = []
        for symbol, rate_obj in funding_rate_map.items():
            if rate_obj is None:
                continue

            predicted_rate = float(rate_obj['predicted_rate']) if rate_obj['predicted_rate'] is not None else None
            funding_rate = float(rate_obj['funding_rate']) if rate_obj['funding_rate'] is not None else None
            next_funding_time = int(rate_obj['next_funding_time']) if rate_obj['next_funding_time'] is not None else None
            mark_price = float(rate_obj['mark_price']) if rate_obj['mark_price'] is not None else None

            is_abnormal = (
                (predicted_rate is not None and abs(predicted_rate) >= FUNDING_RATE_ABNORMAL_THRESHOLD)
                or (funding_rate is not None and abs(funding_rate) >= FUNDING_RATE_ABNORMAL_THRESHOLD)
            )

            data.append({
                'symbol': symbol,
                'predicted_rate': predicted_rate,
                'predicted_rate_formatted': format_funding_rate(predicted_rate),
                'funding_rate': funding_rate,
                'funding_rate_formatted': format_funding_rate(funding_rate),
                'next_funding_time': next_funding_time,
                'next_funding_time_formatted': format_funding_countdown(next_funding_time),
                'mark_price': mark_price,
                'is_abnormal': is_abnormal,
                'event_time': int(rate_obj['event_time']) if rate_obj['event_time'] else None,
            })

        reverse = sort_order == 'desc'
        if sort_by == 'abs_predicted_rate':
            data.sort(key=lambda x: abs(x['predicted_rate'] or 0), reverse=reverse)
        elif sort_by == 'funding_rate':
            data.sort(key=lambda x: x['funding_rate'] or 0, reverse=reverse)
        elif sort_by == 'abs_funding_rate':
            data.sort(key=lambda x: abs(x['funding_rate'] or 0), reverse=reverse)
        else:
            data.sort(key=lambda x: x['predicted_rate'] or 0, reverse=reverse)

        if limit:
            data = data[:limit]

        return jsonify({
            'status': 'success',
            'message': 'funding rates loaded',
            'data': data,
            'threshold': FUNDING_RATE_ABNORMAL_THRESHOLD,
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
