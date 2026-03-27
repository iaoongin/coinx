from flask import Blueprint, render_template

from coinx.utils import logger


pages_bp = Blueprint('pages', __name__)


@pages_bp.route('/')
def index():
    logger.info("访问首页")
    return render_template('index.html')


@pages_bp.route('/coins-config')
def coins_config():
    """币种配置页面"""
    logger.info("访问币种配置页面")
    return render_template('coins_config.html')


@pages_bp.route('/coin-detail')
def coin_detail():
    """币种详情页面"""
    logger.info("访问币种详情页面")
    return render_template('coin_detail.html')


@pages_bp.route('/drop-list')
def drop_list():
    """跌幅榜页面"""
    logger.info("访问跌幅榜页面")
    return render_template('drop_list.html')


@pages_bp.route('/binance-series')
def binance_series():
    """Binance 历史序列管理页面"""
    logger.info("访问 Binance 历史序列管理页面")
    return render_template('binance_series.html')
