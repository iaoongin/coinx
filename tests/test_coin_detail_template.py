from pathlib import Path


def test_coin_detail_uses_stored_detail_contract_without_placeholder_values():
    template = Path('src/coinx/web/templates/coin_detail.html').read_text(encoding='utf-8')
    assert 'intervalChanges' not in template
    assert 'SPKUSDT' not in template
    assert 'detail.intervals' in template
    assert 'detail.exchange_distribution' in template
    assert 'structureScore.value?.total_score' in template
    assert 'detail.data_status' in template
    assert '/series?range=${selectedRange.value}' in template
    assert 'marketChartEl' in template
    assert 'flowChartEl' in template
    assert 'fundingChartEl' in template
    assert 'scoreComponents' in template
    assert 'chartCompact' in template
    assert 'chartMoney' in template
    assert 'tooltip:{valueFormatter:value=>chartMoney(value)}' in template
    assert 'tooltip:{valueFormatter:value=>chartCompact(value)}' in template
    assert 'tooltip:{valueFormatter:value=>formatRate(value)}' in template
    assert '/structure-score`' in template
    assert 'loadStructureScore(); loadSeries();' in template
    assert "bottom: 68" in template
    assert "legend: { bottom: 4" in template
    assert 'const disposeCharts' in template
    assert 'el.clientWidth === 0' in template
    assert 'detail.value = result.data; await nextTick(); renderCharts();' in template
    assert 'v-if="loading && !detail"' not in template
    assert '历史趋势' in template
    assert '正在加载摘要...' in template
