from pathlib import Path

from flask import Flask
import werkzeug

from coinx import config
from coinx.web.routes.api_coinglass_global_assets import api_coinglass_global_assets_bp
from coinx.web.routes.pages import pages_bp


if not hasattr(werkzeug, '__version__'):
    werkzeug.__version__ = '3'


def _client():
    app = Flask(__name__)
    app.register_blueprint(api_coinglass_global_assets_bp)
    return app.test_client()


def test_global_assets_page_renders_default_filter_controls():
    template_dir = Path(__file__).resolve().parents[1] / 'src' / 'coinx' / 'web' / 'templates'
    app = Flask(__name__, template_folder=str(template_dir))
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/global-assets')
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'id="asset-min-market-cap"' in page
    assert 'id="asset-min-ratio"' in page
    assert 'id="asset-min-h1-oi"' in page
    assert 'id="asset-min-h4-oi"' in page
    assert 'id="asset-min-h24-oi"' in page
    assert 'value="oi_market_cap_ratio"' in page
    assert '暂无符合当前筛选条件的数据' in page
    assert 'min_h1_oi_change: null' in page
    assert 'min_h4_oi_change: null' in page


def test_global_assets_api_uses_configured_default_filters(monkeypatch):
    calls = {}

    def fake_load(**kwargs):
        calls.update(kwargs)
        return {'data': [], 'total_count': 0, 'snapshot_time': None, 'snapshots': [], 'stats': {}}

    monkeypatch.setattr(
        'coinx.web.routes.api_coinglass_global_assets.load_global_assets_page',
        fake_load,
    )

    response = _client().get('/api/global-assets')

    assert response.status_code == 200
    assert calls == {
        'keyword': '',
        'sort_by': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_SORT_BY,
        'sort_order': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_SORT_ORDER,
        'page': 1,
        'page_size': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_PAGE_SIZE,
        'snapshot_time': None,
        'min_market_cap': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_MARKET_CAP,
        'min_oi_market_cap_ratio': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_OI_MARKET_CAP_RATIO,
        'min_h1_oi_change': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H1_OI_CHANGE,
        'min_h4_oi_change': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H4_OI_CHANGE,
        'min_h24_oi_change': config.COINGLASS_GLOBAL_ASSETS_DEFAULT_MIN_H24_OI_CHANGE,
    }


def test_global_assets_api_passes_explicit_filters(monkeypatch):
    calls = {}

    def fake_load(**kwargs):
        calls.update(kwargs)
        return {'data': [], 'total_count': 0, 'snapshot_time': None, 'snapshots': [], 'stats': {}}

    monkeypatch.setattr(
        'coinx.web.routes.api_coinglass_global_assets.load_global_assets_page',
        fake_load,
    )

    response = _client().get(
        '/api/global-assets?page=2&page_size=10&sort_by=oi_market_cap_ratio'
        '&min_market_cap=5000000&min_oi_market_cap_ratio=0.8'
        '&min_h1_oi_change=6&min_h4_oi_change=11&min_h24_oi_change=21'
    )

    assert response.status_code == 200
    assert calls['page'] == 2
    assert calls['page_size'] == 10
    assert calls['min_market_cap'] == 5_000_000
    assert calls['min_oi_market_cap_ratio'] == 0.8
    assert calls['min_h1_oi_change'] == 6
    assert calls['min_h4_oi_change'] == 11
    assert calls['min_h24_oi_change'] == 21


def test_global_assets_api_treats_blank_filters_as_unset(monkeypatch):
    calls = {}

    def fake_load(**kwargs):
        calls.update(kwargs)
        return {'data': [], 'total_count': 0, 'snapshot_time': None, 'snapshots': [], 'stats': {}}

    monkeypatch.setattr(
        'coinx.web.routes.api_coinglass_global_assets.load_global_assets_page',
        fake_load,
    )

    response = _client().get(
        '/api/global-assets?min_market_cap=&min_oi_market_cap_ratio='
        '&min_h1_oi_change=&min_h4_oi_change=&min_h24_oi_change='
    )

    assert response.status_code == 200
    assert calls['min_market_cap'] is None
    assert calls['min_oi_market_cap_ratio'] is None
    assert calls['min_h1_oi_change'] is None
    assert calls['min_h4_oi_change'] is None
    assert calls['min_h24_oi_change'] is None
