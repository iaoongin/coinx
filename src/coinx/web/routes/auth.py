from flask import Blueprint, redirect, request, url_for

from coinx.config import WEB_USERNAME
from coinx.utils import logger
from coinx.web.auth import (
    is_authenticated,
    is_safe_redirect_target,
    login_user,
    logout_user,
    render_login_page,
    verify_password,
    verify_username,
)


auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    next_target = request.args.get('next') or request.form.get('next') or '/'
    if not is_safe_redirect_target(next_target):
        next_target = '/'

    if request.method == 'GET' and is_authenticated():
        return redirect(next_target)

    if request.method == 'GET':
        return render_login_page()

    username = request.form.get('username', '')
    password = request.form.get('password', '')
    if not verify_username(username) or not verify_password(password):
        logger.warning('Web 登录失败')
        return render_login_page(error_message='用户名或密码错误，请重试。')

    login_user()
    logger.info('Web 登录成功，用户: %s', WEB_USERNAME)
    return redirect(next_target)


@auth_bp.route('/logout', methods=['GET', 'POST'])
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
