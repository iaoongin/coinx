import secrets
from urllib.parse import urlsplit

from flask import jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from coinx.config import WEB_PASSWORD, WEB_SESSION_SECRET, WEB_USERNAME
from coinx.utils import logger


_resolved_password = WEB_PASSWORD or secrets.token_urlsafe(12)
_password_source = '环境变量' if WEB_PASSWORD else '自动生成'
_password_hash = generate_password_hash(_resolved_password)
_session_secret = WEB_SESSION_SECRET or secrets.token_urlsafe(32)


def configure_app(app):
    """配置网页服务的会话密钥"""
    app.secret_key = _session_secret


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
    return session.get('authenticated') is True


def login_user():
    session.clear()
    session['authenticated'] = True
    session['username'] = WEB_USERNAME


def logout_user():
    session.clear()


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
