import secrets
from datetime import timedelta
from urllib.parse import urlsplit

from flask import jsonify, redirect, render_template, request, url_for
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_jwt_identity,
    unset_jwt_cookies,
    verify_jwt_in_request,
)
from werkzeug.security import check_password_hash, generate_password_hash

from coinx.config import (
    WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES,
    WEB_JWT_COOKIE_DOMAIN,
    WEB_JWT_COOKIE_SECURE,
    WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS,
    WEB_JWT_SECRET_KEY,
    WEB_PASSWORD,
    WEB_USERNAME,
)
from coinx.utils import logger


_resolved_password = WEB_PASSWORD or secrets.token_urlsafe(12)
_password_source = '环境变量' if WEB_PASSWORD else '自动生成'
_password_hash = generate_password_hash(_resolved_password)

# JWT 密钥：优先使用配置，未配置时自动生成
_jwt_secret_key = WEB_JWT_SECRET_KEY or secrets.token_urlsafe(64)


def configure_app(app):
    """配置网页服务的 JWT 和会话密钥"""
    # 保留 secret_key 作为安全网（其他 Flask 扩展可能需要）
    app.secret_key = _jwt_secret_key

    # JWT 配置
    app.config['JWT_SECRET_KEY'] = _jwt_secret_key
    app.config['JWT_TOKEN_LOCATION'] = ['cookies', 'headers']
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES)
    app.config['JWT_REFRESH_TOKEN_EXPIRES'] = timedelta(days=WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS)
    app.config['JWT_SESSION_COOKIE'] = False  # 关闭浏览器后cookie仍保留
    app.config['JWT_COOKIE_SECURE'] = WEB_JWT_COOKIE_SECURE
    app.config['JWT_COOKIE_HTTPONLY'] = True
    app.config['JWT_COOKIE_SAMESITE'] = 'Lax'
    # 单用户内部仪表盘，SameSite=Lax 已提供 CSRF 防护，无需额外 CSRF token
    app.config['JWT_COOKIE_CSRF_PROTECT'] = False
    if WEB_JWT_COOKIE_DOMAIN:
        app.config['JWT_COOKIE_DOMAIN'] = WEB_JWT_COOKIE_DOMAIN


def get_auth_context():
    return {
        'username': WEB_USERNAME,
        'password': _resolved_password,
        'password_source': _password_source,
    }


def log_startup_credentials():
    if _password_source == '自动生成':
        logger.warning('未配置 Web 登录密码，已为用户 %s 自动生成临时密码: %s', WEB_USERNAME, _resolved_password)
    else:
        logger.info('已启用 Web 登录，用户 %s 的密码来自环境变量 WEB_PASSWORD', WEB_USERNAME)


def is_authenticated():
    try:
        verify_jwt_in_request(optional=True)
        return get_jwt_identity() is not None
    except Exception:
        return False


def create_auth_tokens():
    """创建 JWT access 和 refresh token"""
    access_token = create_access_token(identity=WEB_USERNAME)
    refresh_token = create_refresh_token(identity=WEB_USERNAME)
    return access_token, refresh_token


def clear_auth_cookies(response):
    """清除 JWT Cookie"""
    unset_jwt_cookies(response)
    return response


def verify_password(password):
    return check_password_hash(_password_hash, password or '')


def verify_username(username):
    return (username or '').strip() == WEB_USERNAME


def is_safe_redirect_target(target):
    if not target:
        return False
    parts = urlsplit(target)
    return not parts.scheme and not parts.netloc and target.startswith('/')


def build_login_redirect():
    next_target = request.full_path if request.query_string else request.path
    return redirect(url_for('auth.login', next=next_target))


def unauthorized_response():
    if request.path.startswith('/api/'):
        return jsonify({'status': 'error', 'message': '需要先登录'}), 401
    return build_login_redirect()


def render_login_page(error_message=None):
    next_target = request.args.get('next') or request.form.get('next') or '/'
    if not is_safe_redirect_target(next_target):
        next_target = '/'
    return render_template('login.html', next_target=next_target, error_message=error_message)
