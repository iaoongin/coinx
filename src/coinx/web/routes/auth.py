from flask import Blueprint, jsonify, make_response, redirect, request, url_for
from flask_jwt_extended import (
    create_access_token,
    get_jwt_identity,
    jwt_required,
    set_access_cookies,
    set_refresh_cookies,
    unset_jwt_cookies,
    verify_jwt_in_request,
)

from coinx.config import WEB_USERNAME
from coinx.utils import logger
from coinx.web.auth import (
    create_auth_tokens,
    is_authenticated,
    is_safe_redirect_target,
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

    if request.method == 'GET':
        # 已登录则直接跳转
        if is_authenticated():
            return redirect(next_target)

        # access token 过期但 refresh token 有效时，静默续期
        try:
            verify_jwt_in_request(refresh=True, optional=True)
            identity = get_jwt_identity()
            if identity:
                new_access_token = create_access_token(identity=identity)
                response = make_response(redirect(next_target))
                set_access_cookies(response, new_access_token)
                return response
        except Exception:
            pass

        return render_login_page()

    username = request.form.get('username', '')
    password = request.form.get('password', '')
    if not verify_username(username) or not verify_password(password):
        logger.warning('Web 登录失败')
        return render_login_page(error_message='用户名或密码错误，请重试。')

    access_token, refresh_token = create_auth_tokens()
    logger.info('Web 登录成功，用户: %s', WEB_USERNAME)
    response = make_response(redirect(next_target))
    set_access_cookies(response, access_token)
    set_refresh_cookies(response, refresh_token)
    return response


@auth_bp.route('/logout', methods=['GET', 'POST'])
def logout():
    response = make_response(redirect(url_for('auth.login')))
    unset_jwt_cookies(response)
    return response


@auth_bp.route('/auth/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    identity = get_jwt_identity()
    new_access_token = create_access_token(identity=identity)
    response = jsonify({'status': 'success'})
    set_access_cookies(response, new_access_token)
    return response
