from flask import Blueprint, jsonify, request

from coinx.repositories.smart_money import load_smart_money_page
from coinx.utils import logger


api_smart_money_bp = Blueprint('api_smart_money', __name__)


@api_smart_money_bp.route('/api/smart-money')
def get_smart_money():
    """Load the latest persisted Binance Futures Smart Money snapshot."""
    try:
        requested_time_range = request.args.get('time_range', 'ALL').strip()
        if requested_time_range.upper() == 'ALL':
            time_range = 'ALL'
        elif requested_time_range.lower() == '30m':
            time_range = '30m'
        else:
            time_range = requested_time_range
        result = load_smart_money_page(
            keyword=request.args.get('keyword', ''),
            side=request.args.get('side', '').strip().upper(),
            sort_by=request.args.get('sort_by', 'net_notional'),
            sort_order=request.args.get('sort_order', 'desc'),
            page=request.args.get('page', 1, type=int),
            page_size=request.args.get('page_size', 100, type=int),
            snapshot_time=request.args.get('snapshot_time', type=int),
            min_net_notional=request.args.get('min_net_notional', '10000000'),
            max_mark_price_spread_percent=request.args.get('max_mark_price_spread_percent', '5'),
            max_price_trend_percent=request.args.get('max_price_trend_percent', ''),
            max_price_drawdown_percent=request.args.get('max_price_drawdown_percent', '-10'),
            underlying_type=request.args.get('underlying_type', 'COIN'),
            time_range=time_range,
        )
        return jsonify({
            'status': 'success',
            'message': 'smart money loaded',
            **result,
        })
    except ValueError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception as exc:
        logger.error(
            '加载合约聪明钱失败: error_type=%s error=%s',
            type(exc).__name__,
            str(exc)[:500],
        )
        return jsonify({'status': 'error', 'message': '加载合约聪明钱失败，请稍后重试'}), 500
