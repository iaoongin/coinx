from flask import Blueprint, jsonify, request
import threading

from coinx.utils import logger
from coinx.data_processor import get_all_coins_data
from coinx.coin_manager import get_active_coins
from coinx.collector import (
    should_update_cache,
    update_all_data,
    update_drop_list_data,
    get_latest_price,
    get_24hr_ticker,
    get_open_interest,
    get_funding_rate,
    get_long_short_ratio,
    get_exchange_distribution_real,
    get_net_inflow_data as get_net_inflow_data_real,
    get_cache_update_time,
    collect_and_store_series,
    collect_series_batch,
)
from coinx.collector.binance.cache import get_drop_list_cache_update_time
from coinx.config import (
    BINANCE_SERIES_ENABLED,
    BINANCE_SERIES_INTERVAL,
    BINANCE_SERIES_LIMIT,
    BINANCE_SERIES_TYPES,
    BINANCE_SERIES_PERIODS,
)


api_data_bp = Blueprint('api_data', __name__)

SUPPORTED_SERIES_TYPES = {
    'top_long_short_position_ratio',
    'top_long_short_account_ratio',
    'open_interest_hist',
    'klines',
    'global_long_short_account_ratio',
}


@api_data_bp.route('/api/binance-series/config')
def get_binance_series_config():
    """获取 Binance 历史序列页面默认配置。"""
    return jsonify(
        {
            'status': 'success',
            'message': '获取 Binance 历史序列配置成功',
            'data': {
                'enabled': BINANCE_SERIES_ENABLED,
                'interval': BINANCE_SERIES_INTERVAL,
                'limit': BINANCE_SERIES_LIMIT,
                'series_types': BINANCE_SERIES_TYPES,
                'periods': BINANCE_SERIES_PERIODS,
            },
        }
    )


@api_data_bp.route('/api/coins')
def get_coins():
    """获取所有活跃币种的数据。"""
    logger.info("获取所有币种数据")
    try:
        active_coins = get_active_coins()
        coins_data = get_all_coins_data(active_coins)
        cache_update_time = get_cache_update_time()

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
                'price_change_formatted': coin['price_change_formatted'],
                'net_inflow': coin.get('net_inflow', {}),
            }

            changes = []
            for interval, data in (coin.get('changes') or {}).items():
                changes.append(
                    {
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
                        'current_price_formatted': data['current_price_formatted'],
                    }
                )

            changes.sort(
                key=lambda x: (
                    x['interval'].endswith('m') and int(x['interval'][:-1])
                    or x['interval'].endswith('h') and int(x['interval'][:-1]) * 60
                    or x['interval'].endswith('d') and int(x['interval'][:-1]) * 1440
                    or 0
                )
            )
            formatted_coin['changes'] = changes
            formatted_data.append(formatted_coin)

        return jsonify(
            {
                'status': 'success',
                'message': '获取币种数据成功',
                'data': formatted_data,
                'cache_update_time': cache_update_time,
            }
        )
    except Exception as e:
        logger.error(f"获取币种数据失败: {e}")
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'获取币种数据失败: {str(e)}'}), 500


@api_data_bp.route('/api/update')
def update_data():
    """手动更新展示数据。"""
    logger.info("手动更新数据")
    try:
        force = request.args.get('force', 'false').lower() == 'true'
        if not force and not should_update_cache():
            return jsonify({'status': 'success', 'message': '数据已经是最新，无需更新'})

        try:
            symbols = get_active_coins()
        except Exception as e:
            logger.error(f"获取订阅币种失败，将使用空列表: {e}")
            symbols = []

        update_thread = threading.Thread(
            target=update_all_data,
            kwargs={'symbols': symbols, 'force_update': True},
        )
        update_thread.daemon = True
        update_thread.start()

        return jsonify({'status': 'success', 'message': '已触发数据更新，请稍后刷新页面查看'})
    except Exception as e:
        logger.error(f"更新数据失败: {e}")
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'数据更新失败: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>')
def get_coin_detail(symbol):
    """获取指定币种的详情数据。"""
    logger.info(f"获取币种详情: {symbol}")
    try:
        detail_data = {
            'symbol': symbol,
            'latest_price': get_latest_price(symbol),
            'funding_rate': get_funding_rate(symbol),
            'ticker_data': get_24hr_ticker(symbol),
            'open_interest_data': get_open_interest(symbol),
            'long_short_ratio': get_long_short_ratio(symbol),
            'exchange_distribution': get_exchange_distribution_real(symbol),
            'net_inflow_data': get_net_inflow_data_real(symbol),
        }
        return jsonify({'status': 'success', 'message': '获取币种详情成功', 'data': detail_data})
    except Exception as e:
        logger.error(f"获取币种详情失败: {e}")
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'获取币种详情失败: {str(e)}'}), 500


@api_data_bp.route('/api/drop-list')
def get_drop_list():
    """获取跌幅榜数据。"""
    logger.info("获取跌幅榜数据")
    try:
        force = request.args.get('force', 'false').lower() == 'true'
        data = update_drop_list_data(force_update=force)
        cache_update_time = get_drop_list_cache_update_time()
        return jsonify(
            {
                'status': 'success',
                'message': '获取跌幅榜数据成功',
                'data': data,
                'cache_update_time': cache_update_time,
            }
        )
    except Exception as e:
        logger.error(f"获取跌幅榜数据失败: {e}")
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'获取跌幅榜数据失败: {str(e)}'}), 500


@api_data_bp.route('/api/binance-series/collect', methods=['POST'])
def collect_binance_series():
    """手动采集单个 Binance 历史序列并写入数据库。"""
    payload = request.get_json(silent=True) or {}
    series_type = payload.get('series_type')
    symbol = payload.get('symbol')
    period = payload.get('period')
    limit = payload.get('limit')

    if not series_type or not symbol or not period or limit is None:
        return jsonify(
            {
                'status': 'error',
                'message': '缺少必要参数：series_type、symbol、period、limit',
            }
        ), 400

    if series_type not in SUPPORTED_SERIES_TYPES:
        return jsonify(
            {
                'status': 'error',
                'message': f'不支持的 series_type: {series_type}',
            }
        ), 400

    try:
        result = collect_and_store_series(
            series_type=series_type,
            symbol=symbol,
            period=period,
            limit=int(limit),
        )
        return jsonify(
            {
                'status': 'success',
                'message': '历史序列采集成功',
                'data': result,
            }
        )
    except Exception as e:
        logger.error(f"采集 Binance 历史序列失败: {e}")
        logger.exception(e)
        return jsonify(
            {
                'status': 'error',
                'message': f'采集 Binance 历史序列失败: {str(e)}',
            }
        ), 500


@api_data_bp.route('/api/binance-series/batch-collect', methods=['POST'])
def batch_collect_binance_series():
    """手动批量采集 Binance 历史序列并写入数据库。"""
    payload = request.get_json(silent=True) or {}
    symbols = payload.get('symbols')
    periods = payload.get('periods')
    series_types = payload.get('series_types')
    limit = payload.get('limit')

    if not symbols or not periods or limit is None:
        return jsonify(
            {
                'status': 'error',
                'message': '缺少必要参数：symbols、periods、limit',
            }
        ), 400

    if not isinstance(symbols, list) or not isinstance(periods, list):
        return jsonify(
            {
                'status': 'error',
                'message': 'symbols 和 periods 必须是数组',
            }
        ), 400

    if series_types is not None:
        if not isinstance(series_types, list):
            return jsonify(
                {
                    'status': 'error',
                    'message': 'series_types 必须是数组',
                }
            ), 400
        invalid_types = [series_type for series_type in series_types if series_type not in SUPPORTED_SERIES_TYPES]
        if invalid_types:
            return jsonify(
                {
                    'status': 'error',
                    'message': f'存在不支持的 series_type: {invalid_types}',
                }
            ), 400

    try:
        result = collect_series_batch(
            symbols=symbols,
            periods=periods,
            series_types=series_types,
            limit=int(limit),
        )
        return jsonify(
            {
                'status': 'success',
                'message': '批量历史序列采集成功',
                'data': result,
            }
        )
    except Exception as e:
        logger.error(f"批量采集 Binance 历史序列失败: {e}")
        logger.exception(e)
        return jsonify(
            {
                'status': 'error',
                'message': f'批量采集 Binance 历史序列失败: {str(e)}',
            }
        ), 500
