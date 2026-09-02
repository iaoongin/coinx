from flask import Blueprint, redirect, render_template, url_for

from coinx.config import TIME_INTERVALS
from coinx.utils import logger


pages_bp = Blueprint('pages', __name__)


@pages_bp.route('/')
def index():
    logger.info("访问多周期矩阵页面")
    return render_template('index.html', time_intervals=TIME_INTERVALS)


@pages_bp.route('/pwa-start')
def pwa_start():
    return render_template('pwa_start.html')


@pages_bp.route('/new-home')
def new_home():
    logger.info("访问多周期矩阵兼容入口，重定向到默认首页")
    return redirect(url_for('pages.index'))


@pages_bp.route('/legacy-home')
def legacy_home():
    logger.info("访问旧首页")
    return render_template('legacy_home.html', time_intervals=TIME_INTERVALS)


@pages_bp.route('/coins-config')
def coins_config():
    """币种配置页面"""
    logger.info("访问币种配置页面")
    return render_template('coins_config.html')


@pages_bp.route('/coin-detail')
def coin_detail():
    """币种详情页面"""
    logger.info("访问币种详情页面")
    return render_template('coin_detail.html', time_intervals=TIME_INTERVALS)


@pages_bp.route('/market-rank')
def market_rank():
    """行情榜页面"""
    logger.info("访问行情榜页面")
    return render_template('market_rank.html')


@pages_bp.route('/hedge-calculator')
def hedge_calculator():
    """对冲计算器页面"""
    logger.info("访问对冲计算器页面")
    return render_template('hedge_calculator.html')


@pages_bp.route('/market-structure-score')
def market_structure_score():
    """合约市场结构评分页面"""
    logger.info("访问合约市场结构评分页面")
    return render_template('market_structure_score.html')


@pages_bp.route('/trade-opportunities')
def trade_opportunities():
    """交易机会扫描页面。"""
    return render_template('trade_opportunities.html')


@pages_bp.route('/funding-rate')
def funding_rate():
    """资金费率页面"""
    logger.info("访问资金费率页面")
    return render_template('funding_rate.html')


@pages_bp.route('/task-jobs')
def task_jobs():
    """任务管理页面"""
    logger.info("访问任务管理页面")
    return render_template('task_jobs.html')


@pages_bp.route('/notification-management')
def notification_management():
    """告警与通知管理页面。"""
    logger.info('访问告警管理页面')
    return render_template('notification_management.html')


@pages_bp.route('/rss')
def rss_management():
    """RSS subscription management and article reader."""
    logger.info('访问 RSS 订阅管理页面')
    return render_template('rss_management.html')
