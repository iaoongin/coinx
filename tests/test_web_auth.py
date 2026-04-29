import importlib

import werkzeug

import coinx.config as config_module
import coinx.web.auth as auth_module
from coinx.web.app import create_app


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = create_app()
    app.config['TESTING'] = True
    return app.test_client()


def test_requires_login_for_pages():
    client = create_test_client()

    response = client.get('/')

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/login?next=/')


def test_requires_login_for_api():
    client = create_test_client()

    response = client.get('/api/coins')

    assert response.status_code == 401
    payload = response.get_json()
    assert payload['status'] == 'error'
    assert payload['message'] == '需要先登录'


def test_login_allows_access_to_protected_page():
    client = create_test_client()
    auth_context = auth_module.get_auth_context()

    response = client.post(
        '/login',
        data={'username': auth_context['username'], 'password': auth_context['password'], 'next': '/binance-series'},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/binance-series')

    page_response = client.get('/binance-series')
    assert page_response.status_code == 200


def test_login_allows_access_to_new_home_page():
    client = create_test_client()
    auth_context = auth_module.get_auth_context()

    response = client.post(
        '/login',
        data={'username': auth_context['username'], 'password': auth_context['password'], 'next': '/new-home'},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/new-home')

    page_response = client.get('/new-home')
    assert page_response.status_code == 200
    assert '新首页'.encode('utf-8') in page_response.data


def test_login_rejects_wrong_username():
    client = create_test_client()
    auth_context = auth_module.get_auth_context()

    response = client.post(
        '/login',
        data={'username': 'wrong-user', 'password': auth_context['password'], 'next': '/binance-series'},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert '用户名或密码错误，请重试。'.encode('utf-8') in response.data


def test_generated_password_is_created_when_missing(monkeypatch):
    original_password = config_module.WEB_PASSWORD
    monkeypatch.setattr(config_module, 'WEB_PASSWORD', None, raising=False)
    reloaded = importlib.reload(auth_module)

    auth_context = reloaded.get_auth_context()

    assert auth_context['password_source'] == '自动生成'
    assert auth_context['password']

    monkeypatch.setattr(config_module, 'WEB_PASSWORD', original_password, raising=False)
    importlib.reload(auth_module)
