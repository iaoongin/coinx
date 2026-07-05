from pathlib import Path


def test_funding_rate_initial_load_uses_overlay_instead_of_empty_text():
    template = Path("src/coinx/web/templates/funding_rate.html").read_text(encoding="utf-8")

    assert 'class="table-container" :class="{ loading: loading }"' in template
    assert 'v-if="loading" class="table-loading-overlay"' in template
    assert '>加载中<' in template
    assert '<p v-if="loading">' not in template
