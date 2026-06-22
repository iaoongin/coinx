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
        data={'username': auth_context['username'], 'password': auth_context['password'], 'next': '/market-rank'},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/market-rank')

    page_response = client.get('/market-rank')
    assert page_response.status_code == 200


def test_new_home_redirects_to_default_home():
    client = create_test_client()
    auth_context = auth_module.get_auth_context()

    response = client.post(
        '/login',
        data={'username': auth_context['username'], 'password': auth_context['password'], 'next': '/new-home'},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/new-home')

    page_response = client.get('/new-home', follow_redirects=False)
    assert page_response.status_code == 302
    assert page_response.headers['Location'].endswith('/')


def test_login_rejects_wrong_username():
    client = create_test_client()
    auth_context = auth_module.get_auth_context()

    response = client.post(
        '/login',
        data={'username': 'wrong-user', 'password': auth_context['password'], 'next': '/market-rank'},
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


# ---- JWT 专属测试 ----

def _login(client):
    """登录并返回响应，用于后续测试"""
    auth_context = auth_module.get_auth_context()
    return client.post(
        '/login',
        data={'username': auth_context['username'], 'password': auth_context['password']},
        follow_redirects=False,
    )


def _get_cookie_value(response, name):
    """从 Set-Cookie 头中提取指定 cookie 的值"""
    for header in response.headers.getlist('Set-Cookie'):
        if header.startswith(f'{name}='):
            return header.split('=', 1)[1].split(';')[0]
    return None


def test_login_sets_jwt_cookies():
    client = create_test_client()
    response = _login(client)

    assert response.status_code == 302
    # 检查 Set-Cookie 头包含 JWT cookie 名称
    set_cookie_headers = response.headers.getlist('Set-Cookie')
    cookie_names = ' '.join(set_cookie_headers)
    assert 'access_token_cookie' in cookie_names
    assert 'refresh_token_cookie' in cookie_names


def test_logout_clears_jwt_cookies():
    client = create_test_client()
    _login(client)

    response = client.get('/logout', follow_redirects=False)

    set_cookie_headers = response.headers.getlist('Set-Cookie')
    cookie_text = ' '.join(set_cookie_headers)
    # unset_jwt_cookies 将 cookie 值设为空并 max-age=0
    assert 'access_token_cookie' in cookie_text
    assert 'refresh_token_cookie' in cookie_text


def test_api_accepts_bearer_header(monkeypatch):
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: {'data': [], 'cache_update_time': None},
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_active_coins',
        lambda: ['BTCUSDT'],
    )
    client = create_test_client()
    login_response = _login(client)

    # 从 Set-Cookie 头中提取 access token
    token_value = _get_cookie_value(login_response, 'access_token_cookie')
    assert token_value is not None, '登录后应设置 access_token_cookie'

    # 新客户端，不带 cookie，仅用 Bearer header
    new_client = create_test_client()
    response = new_client.get(
        '/api/coins',
        headers={'Authorization': f'Bearer {token_value}'},
    )

    assert response.status_code == 200


def test_auth_refresh_endpoint():
    client = create_test_client()
    _login(client)

    response = client.post('/auth/refresh')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'

    # 应设置新的 access token cookie
    set_cookie_headers = response.headers.getlist('Set-Cookie')
    cookie_names = ' '.join(set_cookie_headers)
    assert 'access_token_cookie' in cookie_names


def test_login_page_silent_refresh_with_valid_refresh_token():
    client = create_test_client()
    _login(client)

    # 清除 access token cookie（模拟过期），保留 refresh token
    client.delete_cookie(key='access_token_cookie')

    # 访问 /login，应利用 refresh token 静默续期并跳转
    response = client.get('/login', follow_redirects=False)

    # 有效 refresh token → 302 跳转 + 新 access token
    assert response.status_code == 302
    set_cookie_headers = response.headers.getlist('Set-Cookie')
    cookie_names = ' '.join(set_cookie_headers)
    assert 'access_token_cookie' in cookie_names
