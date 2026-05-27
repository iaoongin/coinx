from pathlib import Path


def test_homepage_shows_taker_source_tags_for_single_source():
    template = Path('src/coinx/web/templates/index.html').read_text(encoding='utf-8')

    assert 'v-if="takerExchanges(coin).length > 0"' in template
