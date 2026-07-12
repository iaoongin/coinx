from pathlib import Path


def test_coin_detail_uses_stored_detail_contract_without_placeholder_values():
    template = Path('src/coinx/web/templates/coin_detail.html').read_text(encoding='utf-8')
    assert 'intervalChanges' not in template
    assert 'SPKUSDT' not in template
    assert 'detail.intervals' in template
    assert 'detail.exchange_distribution' in template
    assert 'detail.value?.structure_score' in template
    assert 'detail.data_status' in template
