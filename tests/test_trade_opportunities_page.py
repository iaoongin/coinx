from pathlib import Path


def test_trade_opportunities_page_uses_the_opportunity_api_and_score_fields():
    template = Path('src/coinx/web/templates/trade_opportunities.html').read_text(encoding='utf-8')

    assert "/api/trade-opportunities?scope=${next === 'all' ? 'all' : 'candidates'}" in template
    assert '搜索币种，例如 BTC' in template
    assert 'entryFilter' in template
    assert 'trendFilter' in template
    assert 'riskFilter' in template
    assert 'filteredRows' in template
    assert "scope === 'actionable'" in template
    assert "['可做多', '可做空']" in template
    assert 'class="filter-row"' in template
    assert "scope = ref('candidates')" in template
    assert "load('candidates');" in template
    assert 'entry_score' in template
    assert 'trend_score' in template
    assert 'timing_score' in template
    assert 'risk_score' in template
    assert 'cache_update_time' in template
    assert '数据时间' in template
    assert 'current_price' in template
    assert '当前价' in template
    assert '下方列出触发的具体原因' in template
    assert '交易机会' in template
    assert 'trade_plan' in template
    assert '止盈1' in template
    assert '止盈2' in template
    assert '止盈3' in template
    assert template.count('class="help-tip"') == 9
    assert '方向分＝趋势分＋时机分' in template
    assert '入场分≥65：可做多' in template
    assert '资金费率、OI 过快增长和短周期波动的扣分' in template
    assert 'planPercent' in template
    assert 'planR' in template
    assert 'space_status' in template
    assert '空间不足' in template
    assert 'text-overflow: ellipsis' in template
    assert 'class="reason-text"' in template
    assert 'risk-score' in template
    assert 'riskClass' in template
    assert 'signal-strong-long' in template
    assert 'metricClass' in template
    assert ':data-tip="(row.risk_reasons || []).join' in template
    assert 'colspan="5" class="plan-group"' in template
    assert 'class="plan-cell"' in template
    assert '展开交易计划' not in template
    assert 'expandedSymbol' not in template
    assert 'togglePlan' not in template
    assert "}).mount('#app');" in template


def test_navigation_exposes_trade_opportunities_first_in_analysis_menu():
    nav = Path('src/coinx/web/templates/components/nav.html').read_text(encoding='utf-8')

    assert nav.index('href="/trade-opportunities"') < nav.index('href="/market-structure-score"')
