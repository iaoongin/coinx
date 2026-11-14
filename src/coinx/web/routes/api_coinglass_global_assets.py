"""CoinGlass global-assets API routes."""

from flask import Blueprint, jsonify, request

from coinx import config
from coinx.config import COLLECTION_SCHEDULER_ONLY
from coinx.repositories.coinglass_global_assets import (
    collect_and_save_global_assets,
    load_global_asset_history,
    load_global_assets_page,
)
from coinx.utils import logger


api_coinglass_global_assets_bp = Blueprint('api_coinglass_global_assets', __name__)


def _get_float_filter(name: str, default: float | None) -> float | None:
    """Use defaults only when a filter is absent; an empty filter means unset."""
    raw_value = request.args.get(name)
    if raw_value is None:
        return default
    if not raw_value.strip():
        return None
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ValueError(f'{name} 必须是数字') from exc


@api_coinglass_global_assets_bp.route('/api/global-assets')
def get_global_assets():
    try:
        page_size = request.args.get(
            'page_size',
            request.args.get('limit', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_PAGE_SIZE, type=int),
            type=int,
        )
        result = load_global_assets_page(
            keyword=request.args.get('keyword', ''),
            sort_by=request.args.get('sort_by', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_SORT_BY),
            sort_order=request.args.get('sort_order', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_SORT_ORDER),
            page=request.args.get('page', 1, type=int),
            page_size=page_size,
            snapshot_time=request.args.get('snapshot_time', type=int),
            min_market_cap=_get_float_filter(
                'min_market_cap', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_MARKET_CAP,
            ),
            min_oi_market_cap_ratio=_get_float_filter(
                'min_oi_market_cap_ratio',
                config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_OI_MARKET_CAP_RATIO,
            ),
            min_h1_oi_change=_get_float_filter(
                'min_h1_oi_change', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H1_OI_CHANGE,
            ),
            min_h4_oi_change=_get_float_filter(
                'min_h4_oi_change', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H4_OI_CHANGE,
            ),
            min_h24_oi_change=_get_float_filter(
                'min_h24_oi_change', config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H24_OI_CHANGE,
            ),
        )
        return jsonify({'status': 'success', 'message': 'global assets loaded', **result})
    except ValueError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception as exc:
        logger.exception('加载 CoinGlass 全球资产失败')
        return jsonify({'status': 'error', 'message': '加载全球资产失败，请稍后重试'}), 500


@api_coinglass_global_assets_bp.route('/api/global-assets/history/<symbol>')
def get_global_asset_history(symbol):
    try:
        limit = min(max(request.args.get('limit', 100, type=int), 1), 500)
        data = load_global_asset_history(symbol, limit=limit)
        return jsonify({
            'status': 'success',
            'message': 'global asset history loaded',
            'symbol': symbol,
            'data': data,
        })
    except ValueError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception:
        logger.exception('加载 CoinGlass 全球资产历史失败: symbol=%s', symbol)
        return jsonify({'status': 'error', 'message': '加载资产历史失败，请稍后重试'}), 500


@api_coinglass_global_assets_bp.route('/api/global-assets/refresh', methods=['POST'])
def refresh_global_assets():
    if COLLECTION_SCHEDULER_ONLY:
        return jsonify({
            'status': 'error',
            'code': 'COLLECTION_SCHEDULER_ONLY',
            'message': 'global asset collection is disabled: collection is scheduler-only',
        }), 409
    try:
        summary = collect_and_save_global_assets()
        return jsonify({'status': 'success', 'message': 'global assets collected', 'data': summary})
    except Exception as exc:
        logger.exception('手动采集 CoinGlass 全球资产失败')
        return jsonify({'status': 'error', 'message': f'采集全球资产失败：{exc}'}), 500
