from pathlib import Path

from flask import Flask

from coinx.web.routes.api_smart_money import api_smart_money_bp
from coinx.web.routes.pages import pages_bp


def test_smart_money_api_passes_display_filters(monkeypatch):
    calls = {}

    def fake_load(**kwargs):
        calls.update(kwargs)
        return {
            'data': [],
            'total_count': 0,
            'snapshot_time': None,
            'snapshots': [],
            'stats': {'net_notional': 0, 'long_notional': 0, 'short_notional': 0},
        }

    monkeypatch.setattr('coinx.web.routes.api_smart_money.load_smart_money_page', fake_load)
    app = Flask(__name__)
    app.register_blueprint(api_smart_money_bp)

    response = app.test_client().get(
        '/api/smart-money?keyword=btc&side=buy&sort_by=long_whales&sort_order=asc&page=2&page_size=50&snapshot_time=1700000000000&min_net_notional=10000000'
    )

    assert response.status_code == 200
    assert calls == {
        'keyword': 'btc',
        'side': 'BUY',
        'sort_by': 'long_whales',
        'sort_order': 'asc',
        'page': 2,
        'page_size': 50,
        'snapshot_time': 1700000000000,
        'min_net_notional': '10000000',
        'max_mark_price_spread_percent': '5',
        'max_price_trend_percent': '',
        'max_price_drawdown_percent': '-10',
        'underlying_type': 'COIN',
        'time_range': 'ALL',
    }


def test_smart_money_page_is_registered():
    template_dir = Path(__file__).resolve().parents[1] / 'src' / 'coinx' / 'web' / 'templates'
    app = Flask(__name__, template_folder=str(template_dir))
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/smart-money')

    assert response.status_code == 200
    assert '合约聪明钱' in response.get_data(as_text=True)
    assert 'value="ALL"' in response.get_data(as_text=True)
    assert 'smart-money-asset-type'.encode('utf-8') in response.data
    assert '<option value="COIN">加密货币</option>'.encode('utf-8') in response.data
    assert '<option value="TRADITIONAL">传统金融</option>'.encode('utf-8') in response.data
    assert '<option value="">不限</option>'.encode('utf-8') in response.data
    assert 'underlyingType'.encode('utf-8') in response.data
    assert 'value="long_short_ratio"' in response.get_data(as_text=True)
    assert '当前标记价'.encode('utf-8') in response.data
    assert '7天走势'.encode('utf-8') in response.data
    assert '7天高点回撤'.encode('utf-8') in response.data
    assert '<th class="drawdown-header">7天高点回撤</th>'.encode('utf-8') in response.data
    assert 'font-weight: 600; text-align: center;'.encode('utf-8') in response.data
    assert 'padding: var(--page-padding, 20px);'.encode('utf-8') in response.data
    assert 'max-width: var(--page-max-width, 1536px);'.encode('utf-8') in response.data
    assert 'padding: 0 0 28px;'.encode('utf-8') in response.data
    assert 'padding: 20px;'.encode('utf-8') in response.data
    assert 'max-width: 1500px;'.encode('utf-8') not in response.data
    assert 'border-radius: var(--page-surface-radius, var(--card-radius-lg, 10px));'.encode('utf-8') in response.data
    assert '@media (min-width: 1800px)'.encode('utf-8') in response.data
    assert '.table-shell { overflow-x: hidden; }'.encode('utf-8') in response.data
    assert '.signal-table th { font-size: 10px; line-height: 1.25; white-space: nowrap; }'.encode('utf-8') in response.data
    assert '.signal-table td { white-space: normal; overflow-wrap: anywhere; line-height: 1.35; }'.encode('utf-8') in response.data
    assert 'sparklinePoints'.encode('utf-8') in response.data
    assert '多空名义比'.encode('utf-8') in response.data
    assert '净名义'.encode('utf-8') in response.data
    assert '<th>多头名义</th><th>空头名义</th><th>净名义</th>'.encode('utf-8') in response.data
    assert 'formatProfitPercent'.encode('utf-8') in response.data
    assert "(markNumber - averageNumber) / averageNumber * 100".encode('utf-8') in response.data
    assert "(averageNumber - markNumber) / averageNumber * 100".encode('utf-8') in response.data
    assert 'profitPercentClass'.encode('utf-8') in response.data
    assert 'formatPnl(row, \'long\')'.encode('utf-8') in response.data
    assert '(markNumber - averageNumber) * Math.abs(quantityNumber)'.encode('utf-8') in response.data
    assert '(averageNumber - markNumber) * Math.abs(quantityNumber)'.encode('utf-8') in response.data
    assert 'pnlClass(row, \'short\')'.encode('utf-8') in response.data
    assert 'metricClass(stats.net_notional)'.encode('utf-8') not in response.data
    assert 'metricClass(row.net_notional)'.encode('utf-8') not in response.data
    assert 'const pageSize = 10;'.encode('utf-8') in response.data
    assert response.data.decode('utf-8').count('加载聪明钱数据中') == 1
    assert '暂无符合当前筛选条件的数据。'.encode('utf-8') in response.data
    assert 'const showStats = computed'.encode('utf-8') in response.data
    assert '>币种<'.encode('utf-8') in response.data
    assert 'smart-money-query-scope'.encode('utf-8') in response.data
    assert '>筛选模式<'.encode('utf-8') in response.data
    assert '全部条件'.encode('utf-8') in response.data
    assert '仅按币种'.encode('utf-8') in response.data
    assert '筛选条件'.encode('utf-8') in response.data
    assert '排序与快照'.encode('utf-8') in response.data
    assert 'queryKeyword'.encode('utf-8') in response.data
    assert 'ignoreFilters'.encode('utf-8') in response.data
    assert '@change="load(1)"'.encode('utf-8') not in response.data
    assert '@change="searchSymbols"'.encode('utf-8') not in response.data
    assert 'const searchSymbols = () =>'.encode('utf-8') in response.data
    assert 'locatedSymbol'.encode('utf-8') not in response.data
    assert 'showAdvancedFilters'.encode('utf-8') not in response.data
    assert "underlying_type: ignoreOtherFilters ? '' : underlyingType.value".encode('utf-8') in response.data
    assert "max_price_drawdown_percent: ignoreOtherFilters ? ''".encode('utf-8') in response.data
    assert 'value="1000000"'.encode('utf-8') in response.data
    assert 'value="10000000"'.encode('utf-8') in response.data
    page_html = response.get_data(as_text=True)
    net_notional_options = [
        '<option value="">不限</option>',
        '<option value="1000000">&gt; 1M</option>',
        '<option value="10000000">&gt; 10M</option>',
        '<option value="50000000">&gt; 50M</option>',
        '<option value="100000000">&gt; 100M</option>',
        '<option value="custom">自定义</option>',
    ]
    option_positions = [page_html.index(option) for option in net_notional_options]
    assert option_positions == sorted(option_positions)
    assert '自定义净名义最小值' in page_html
    assert 'smart-money-price-spread'.encode('utf-8') in response.data
    assert '均价距当前价'.encode('utf-8') in response.data
    assert '<option value="-10">≤ -10%</option>'.encode('utf-8') in response.data
    assert '<option value="-20">≤ -20%</option>'.encode('utf-8') in response.data
    assert '<option value="">不限</option>'.encode('utf-8') in response.data
    assert '<option value="10">≤ +10%</option>'.encode('utf-8') in response.data
    assert '7天高点回撤'.encode('utf-8') in response.data
    assert '<option value="-5">≤ -5%</option>'.encode('utf-8') in response.data
    assert '<option value="-30">≤ -30%</option>'.encode('utf-8') in response.data
    assert 'maxPriceTrendPercent'.encode('utf-8') in response.data
    assert 'formatTrendPercent'.encode('utf-8') not in response.data
    assert 'formatDrawdownPercent'.encode('utf-8') in response.data
    assert 'value="5"'.encode('utf-8') in response.data
    assert 'value="10"'.encode('utf-8') in response.data


def test_smart_money_api_preserves_binance_time_range_casing(monkeypatch):
    calls = {}

    def fake_load(**kwargs):
        calls.update(kwargs)
        return {'data': [], 'total_count': 0, 'snapshot_time': None, 'snapshots': [], 'stats': {}}

    monkeypatch.setattr('coinx.web.routes.api_smart_money.load_smart_money_page', fake_load)
    app = Flask(__name__)
    app.register_blueprint(api_smart_money_bp)

    response = app.test_client().get('/api/smart-money?time_range=30m')

    assert response.status_code == 200
    assert calls['time_range'] == '30m'
    assert calls['min_net_notional'] == '10000000'
