from pathlib import Path


def test_homepage_shows_taker_source_tags_for_single_source():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')
    component = Path('src/coinx/web/static/js/components/PeriodMatrix.js').read_text(encoding='utf-8')
    nav = Path('src/coinx/web/templates/components/nav.html').read_text(encoding='utf-8')
    api_client = Path('src/coinx/web/static/js/api_client.js').read_text(encoding='utf-8')

    assert '<period-matrix :coin="coin"></period-matrix>' in template
    assert "filename='css/period-matrix.css'" in template
    assert "filename='js/components/PeriodMatrix.js'" in template
    assert "app.component('PeriodMatrix', PeriodMatrix)" in template
    assert "filename='js/api_client.js'" in nav
    assert 'window.CoinxApi.requestJson' in template
    assert "window.addEventListener('coinx:api-error'" not in template
    assert 'coinx:api-error' in api_client
    assert 'v-if="allTakerExchanges(coin).length"' in component
    assert 'v-for="item in allTakerExchanges(coin)"' in component


def test_homepage_funding_label_opens_24_hour_chart_modal():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert '@click.stop="openFundingModal(coin, $event)"' in template
    assert 'role="dialog"' in template
    assert "'/api/funding-rate/history/' + encodeURIComponent(symbol) + '?hours=24'" in template
    assert "name: '结算费率'" in template
    assert "name: '预测费率'" in template
    assert "renderer: 'svg'" in template
    assert "visualViewport.addEventListener('resize', handleFundingResize)" in template


def test_homepage_loads_pinned_echarts_from_unpkg():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert "https://unpkg.com/echarts@6.0.0/dist/echarts.min.js" in template
    assert "cdn.jsdelivr.net/npm/echarts" not in template


def test_homepage_search_persists_in_url_and_supports_recent_history():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert 'type="search"' in template
    assert "SEARCH_QUERY_PARAM = 'symbol'" in template
    assert 'history.replaceState' in template
    assert "SEARCH_HISTORY_STORAGE_KEY = 'coinx.homepage.search-history'" in template
    assert 'role="listbox"' in template
    assert 'aria-label="清除搜索"' in template
