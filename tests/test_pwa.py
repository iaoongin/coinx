import json
from pathlib import Path

import coinx.web.app as app_module
from coinx.web.app import create_app


def create_test_client():
    app = create_app()
    app.config['TESTING'] = True
    return app.test_client()


def test_manifest_exposes_install_metadata_and_existing_icons():
    client = create_test_client()

    response = client.get('/static/manifest.webmanifest')

    assert response.status_code == 200
    manifest = json.loads(response.data)
    assert manifest['start_url'] == '/pwa-start'
    assert manifest['scope'] == '/'
    assert manifest['display'] == 'standalone'
    assert manifest['theme_color'] == '#080808'
    assert {icon['sizes'] for icon in manifest['icons']} == {'192x192', '512x512'}
    for icon in manifest['icons']:
        icon_response = client.get(icon['src'])
        assert icon_response.status_code == 200
        assert icon_response.mimetype == 'image/png'


def test_service_worker_is_public_and_does_not_cache_requests(monkeypatch):
    monkeypatch.setattr(app_module, 'WEB_AUTH_DISABLED', False)
    client = create_test_client()

    response = client.get('/service-worker.js')

    assert response.status_code == 200
    assert response.mimetype == 'application/javascript'
    assert "addEventListener('fetch'" not in response.get_data(as_text=True)
    assert response.headers['Cache-Control'] == 'no-cache'


def test_pwa_start_page_is_public(monkeypatch):
    monkeypatch.setattr(app_module, 'WEB_AUTH_DISABLED', False)
    client = create_test_client()

    response = client.get('/pwa-start')

    assert response.status_code == 200
    assert b'/static/dark-theme.css' in response.data
    assert b'/static/brand/coinx-mark.svg' in response.data
    assert b'/static/manifest.webmanifest' in response.data
    assert b'data-pwa-start-status' in response.data
    assert '进入首页'.encode('utf-8') in response.data


def test_pwa_head_component_registers_manifest_and_worker():
    template = Path('src/coinx/web/templates/components/pwa_head.html').read_text(encoding='utf-8')

    assert 'rel="manifest"' in template
    assert 'apple-touch-icon' in template
    assert "register('/service-worker.js', { scope: '/' })" in Path(
        'src/coinx/web/static/js/pwa.js'
    ).read_text(encoding='utf-8')
