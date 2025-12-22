from flask import Blueprint, render_template
from src.utils import logger

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
