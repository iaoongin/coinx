from pathlib import Path


def test_funding_rate_initial_load_uses_overlay_instead_of_empty_text():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert 'class="table-container" :class="{ loading: loading }"' in template
    assert 'v-if="loading" class="table-loading-overlay"' in template
    assert '>加载中<' in template
    assert '<p v-if="loading">' not in template


def test_funding_rate_mobile_layout_keeps_filters_and_stats_compact():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert 'grid-template-columns: minmax(0, 1fr) minmax(0, 1fr)' in template
    assert 'grid-template-columns: repeat(4, minmax(0, 1fr))' in template
    assert 'justify-content: flex-end;' in template
    assert 'align-items: flex-end;' in template


def test_funding_rate_filters_use_matching_labels_and_controls():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert 'for="funding-sort"' in template
    assert 'for="funding-order"' in template
    assert 'for="funding-keyword"' in template
    assert 'for="funding-abnormal-only"' in template
    assert 'filter-threshold' in template


def test_funding_rate_symbol_links_have_high_contrast_color():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert '.symbol-cell a {' in template
    assert 'color: var(--text-primary);' in template


def test_funding_rate_displays_symbols_without_usdt_suffix():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert "const displaySymbol = (symbol) => String(symbol || '').replace(/USDT$/, '');" in template
    assert "displaySymbol(item.symbol)" in template


def test_funding_rate_refresh_loading_state_does_not_use_primary_button_color():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert ":class=\"{ 'btn-primary': loading }\"" not in template
