from types import SimpleNamespace

from flask import Flask

from coinx.repositories import database_stats
from coinx.web.routes.api_database_stats import api_database_stats_bp
from coinx.web.routes.pages import pages_bp
from coinx import models


TABLE_COMMENT_EXPECTATIONS = {
    'Coin': '币种配置表',
    'MarketSnapshot': '市场数据快照表',
    'MarketTickers': '行情快照原始数据表',
    'MarketOpenInterestHist': '持仓量历史',
    'MarketKline': '多交易所K线历史数据表',
    'MarketTakerBuySellVol': '主动买卖历史',
    'MarketFundingRate': '资金费率历史',
    'MarketFuturesSmartMoneySignal': '聪明钱快照',
    'NotificationChannel': '通知渠道',
    'AlertRule': '告警规则',
    'AlertRuleChannel': '规则与渠道关联',
    'AlertState': '告警状态',
    'NotificationDelivery': '通知发送记录',
    'AlertEvaluationRun': '告警规则评估记录',
    'AlertEvaluationMetric': '告警评估指标',
    'ScheduledJobRun': '定时任务执行记录',
    'RssSubscription': 'RSS订阅源配置',
    'RssArticle': 'RSS文章记录',
}


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar_value


class FakeSession:
    def __init__(self, rows, row_counts=None):
        self.rows = rows
        self.row_counts = row_counts or {}
        self.statements = []

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name='mysql'))

    def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if 'SELECT DATABASE()' in sql:
            return FakeResult(scalar_value='coinx')
        if 'COUNT(*)' in sql:
            table_name = sql.rsplit('`', 2)[1]
            return FakeResult(scalar_value=self.row_counts[table_name])
        return FakeResult(rows=self.rows)


def test_models_define_table_comments():
    for model_name, expected_comment in TABLE_COMMENT_EXPECTATIONS.items():
        assert getattr(models, model_name).__table__.comment == expected_comment


def test_get_database_stats_aggregates_table_space_and_comment():
    session = FakeSession([
        {
            'table_name': 'market_klines',
            'table_comment': '多交易所K线历史数据表',
            'data_bytes': 800,
            'index_bytes': 200,
            'free_bytes': 100,
        },
        {
            'table_name': 'coins',
            'table_comment': None,
            'data_bytes': 100,
            'index_bytes': 50,
            'free_bytes': 0,
        },
    ], row_counts={'market_klines': 1200, 'coins': 25})

    result = database_stats.get_database_stats(session=session)

    assert result['database'] == 'coinx'
    assert result['summary']['table_count'] == 2
    assert result['summary']['row_count'] == 1225
    assert result['summary']['data_bytes'] == 900
    assert result['summary']['index_bytes'] == 250
    assert result['summary']['used_bytes'] == 1150
    assert result['summary']['free_bytes'] == 100
    assert result['summary']['largest_table_name'] == 'market_klines'
    assert result['tables'][0]['table_comment'] == '多交易所K线历史数据表'
    assert result['tables'][0]['free_percent'] == 9.09
    assert result['tables'][1]['table_comment'] == ''
    assert 'TABLE_COMMENT' in session.statements[0]
    assert 'TABLE_ROWS' not in session.statements[0]
    assert 'SELECT COUNT(*) FROM `market_klines`' in session.statements
    assert 'SELECT COUNT(*) FROM `coins`' in session.statements


def test_get_database_stats_rejects_non_mysql_session():
    session = SimpleNamespace(get_bind=lambda: SimpleNamespace(dialect=SimpleNamespace(name='sqlite')))

    try:
        database_stats.get_database_stats(session=session)
    except RuntimeError as exc:
        assert '仅支持 MySQL' in str(exc)
    else:
        raise AssertionError('expected non-MySQL session to be rejected')


def test_database_stats_api_returns_repository_payload(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(api_database_stats_bp)
    payload = {
        'database': 'coinx',
        'refreshed_at_ms': 1,
        'query_duration_ms': 2,
        'summary': {'table_count': 1},
        'tables': [],
    }
    monkeypatch.setattr(
        'coinx.web.routes.api_database_stats.get_database_stats',
        lambda: payload,
    )

    response = app.test_client().get('/api/database-stats')

    assert response.status_code == 200
    assert response.get_json() == {
        'status': 'success',
        'message': '数据库存储统计加载成功',
        'data': payload,
    }


def test_database_stats_page_renders():
    app = Flask(__name__, template_folder='../src/coinx/web/templates', static_folder='../src/coinx/web/static')
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/database-stats')

    assert response.status_code == 200
    assert '数据存储'.encode('utf-8') in response.data
    assert '/api/database-stats'.encode('utf-8') in response.data
    assert '搜索表名或表注释'.encode('utf-8') in response.data
    assert 'COUNT(*)'.encode('utf-8') in response.data
    assert 'var(--page-max-width)'.encode('utf-8') in response.data
