from pathlib import Path


def test_coin_detail_uses_stored_detail_contract_without_placeholder_values():
    template = Path('src/coinx/web/templates/coin_detail.html').read_text(encoding='utf-8')
    assert 'intervalChanges' not in template
    assert 'SPKUSDT' not in template
    assert 'detail.intervals' in template
    assert 'detail.exchange_distribution' in template
    assert '<h2 class="overview-heading">基础信息</h2>' in template
    assert template.index('<h2 class="overview-heading">交易所持仓分布</h2>') < template.index('<h2>市场结构评分</h2>')
    assert 'grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px;' in template
    assert 'structureScore.value?.total_score' in template
    assert 'detail.data_status' in template
    assert '当前合约状态：[[ statusText ]]' in template
    assert '<span v-if="detail" class="status"' not in template
    assert 'coinx.contract-detail.recent' in template
    assert 'loadSymbolOptions' in template
    assert "fetch('/api/coins-config')" in template
    assert 'switchSymbol' in template
    assert '搜索并切换合约' in template
    assert 'recentSymbols.value[0]' in template
    assert 'selectionRequired.value = true' in template
    assert '.chart, .chart-state { height: 260px; }' in template
    assert '.range-bar { align-items: stretch; flex-direction: column; gap: 8px; }' in template
    assert '.score-layout > div { min-width: 0; }' in template
    assert '.table-wrap { max-width: 100%; overflow-x: auto;' in template
    assert '@media (max-width: 768px)' in template
    assert '.detail-header { align-items: stretch; flex-direction: column;' in template
    assert 'width: min(320px, calc(100vw - 32px));' in template
    assert '/series?range=${requestedRange}' in template
    assert 'priceChartEl' in template
    assert 'openInterestChartEl' in template
    assert 'flowChartEl' in template
    assert 'fundingChartEl' in template
    assert 'scoreComponents' in template
    assert 'chartCompact' in template
    assert 'chartMoney' in template
    assert '.chart { height: 360px; width: 100%; }' in template
    assert 'tooltip:{valueFormatter:value=>chartMoney(value)}' in template
    assert 'tooltip:{valueFormatter:value=>chartCompact(value)}' in template
    assert "name:'成交量',type:'bar',yAxisIndex:1" in template
    assert "name:'持仓量',type:'line',showSymbol:false,yAxisIndex:1" in template
    assert "name:'主动买入',type:'line',showSymbol:false,lineStyle:{color:'#4ade80'}" in template
    assert "name:'主动卖出',type:'line',showSymbol:false,lineStyle:{color:'#f87171'}" in template
    assert "color:x.net_inflow >= 0 ? '#4ade80' : '#f87171'" in template
    assert "visualMap:{show:false,dimension:1,seriesIndex:[0]" in template
    assert 'x.open_interest])' in template
    assert 'tooltip:{valueFormatter:value=>formatRate(value)}' in template
    assert '/structure-score`' in template
    assert 'loadStructureScore(); loadSeries();' in template
    assert "bottom: 68" in template
    assert 'grid: { left: 12, right: 12, top: 34, bottom: 68, containLabel: true }' in template
    assert "legend: { bottom: 4" in template
    assert 'const disposeCharts' in template
    assert 'el.clientWidth === 0' in template
    assert 'detail.value = result.data; recordRecentSymbol(requestedSymbol); await nextTick(); renderCharts();' in template
    assert 'v-if="loading && !detail"' not in template
    assert '历史趋势' in template
    assert '正在加载摘要...' in template
