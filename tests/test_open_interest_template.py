from pathlib import Path

from flask import Flask

from coinx.web.routes.pages import pages_bp


def test_open_interest_template_contains_filters_and_aggregate_columns():
    template = Path('src/coinx/web/templates/open_interest.html').read_text(encoding='utf-8')

    assert '持仓量概览' in template
    assert '跨交易所累计持仓量' in template
    assert 'id="oi-period"' in template
    assert 'id="oi-min-ratio"' in template
    assert 'id="oi-max-ratio"' in template
    assert 'id="oi-min-amount"' in template
    assert 'id="oi-max-amount"' in template
    assert 'id="oi-min-market-cap"' in template
    assert 'id="oi-order"' in template
    assert '仅显示时间对齐' in template
    assert 'stale-data' in template
    assert '含旧数据' not in template
    assert 'flex-wrap: nowrap' in template
    assert 'white-space: nowrap' in template
    assert '数据时间：${formatTime(detail.event_time)}' in template
    assert 'formatTime(detail.event_time)' in template
    assert '存在时间差异' not in template
    assert '最新价格' in template
    assert '流通市值' in template
    assert '持仓 / 流通' in template
    assert 'formatPrice(row.latest_price)' in template
    assert 'formatAmount(row.circulating_market_cap)' in template
    assert 'formatRatio(row.oi_circulating_ratio)' in template
    assert "row.oi_circulating_ratio > 1 ? 'ratio-high' : ''" in template
    assert 'formatAmount(detail.open_interest_value)' in template
    assert 'height: 30px' in template
    assert 'grid-column: span 3' in template
    assert '持仓流通比例下限' in template
    assert "minRatio: '0.6'" in template
    assert "minMarketCap: '3M'" in template
    assert "min_market_cap'" in template
    assert 'parseCompactAmount' in template
    assert 'filter-actions-label' in template
    assert '>操作</span>' in template


def test_open_interest_page_renders_template():
    app = Flask(
        __name__,
        template_folder=str(Path('src/coinx/web/templates').resolve()),
        static_folder=str(Path('src/coinx/web/static').resolve()),
    )
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/open-interest')

    assert response.status_code == 200
    assert '持仓量概览' in response.get_data(as_text=True)
