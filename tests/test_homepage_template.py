from pathlib import Path


def test_homepage_shows_taker_source_tags_for_single_source():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert 'v-if="allTakerExchanges(coin).length"' in template
    assert 'v-for="item in allTakerExchanges(coin)"' in template


def test_homepage_funding_label_opens_24_hour_chart_modal():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert '@click.stop="openFundingModal(coin, $event)"' in template
    assert 'role="dialog"' in template
    assert "'/api/funding-rate/history/' + encodeURIComponent(symbol) + '?hours=24'" in template
    assert "name: '结算费率'" in template
    assert "name: '预测费率'" in template
