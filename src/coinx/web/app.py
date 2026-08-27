import os
import sys

from flask import Flask, jsonify, request, send_from_directory
from flask_jwt_extended import JWTManager

# 添加项目根目录到路径
# 添加项目根目录到路径（兼容直接运行当前模块）
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from coinx.config import WEB_AUTH_DISABLED, WEB_DEBUG, WEB_HOST, WEB_PORT
from coinx.database import db_session
from coinx.runtime import start_runtime_services
from coinx.read_backend import get_read_backend, is_clickhouse_read, read_backend_health
from coinx.write_backend import is_clickhouse_write
from coinx.utils import logger
from coinx.web.auth import configure_app, is_authenticated, log_startup_credentials, unauthorized_response

# 导入蓝图
# 注意：必须在添加项目根目录到 sys.path 之后导入
# 使用 try-except 兼容不同运行入口下的导入路径
try:
    from coinx.web.routes.auth import auth_bp
    from coinx.web.routes.pages import pages_bp
    from coinx.web.routes.api_data import api_data_bp
    from coinx.web.routes.api_config import api_config_bp
    from coinx.web.routes.api_funding_rate import api_funding_rate_bp
    from coinx.web.routes.api_notifications import api_notifications_bp
    from coinx.web.routes.api_rss import api_rss_bp
except ImportError:
    # 如果在当前目录运行，可能需要使用相对导入路径
    from routes.auth import auth_bp
    from routes.pages import pages_bp
    from routes.api_data import api_data_bp
    from routes.api_config import api_config_bp
    from routes.api_funding_rate import api_funding_rate_bp
    from routes.api_notifications import api_notifications_bp
    from routes.api_rss import api_rss_bp


def create_app():
    # 创建 Flask 应用并注册页面、接口与登录路由
    app = Flask(__name__, template_folder='templates', static_folder='static')
    
    # 开发环境：禁用静态文件缓存，启用模板自动重载
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    
    configure_app(app)
    JWTManager(app)

    app.register_blueprint(auth_bp)
    app.register_blueprint(pages_bp)
    app.register_blueprint(api_data_bp)
    app.register_blueprint(api_config_bp)
    app.register_blueprint(api_funding_rate_bp)
    app.register_blueprint(api_notifications_bp)
    app.register_blueprint(api_rss_bp)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()

    @app.before_request
    def require_login():
        if WEB_AUTH_DISABLED:
            return None
        # 登录页、退出接口和静态资源不需要登录，其余请求统一拦截
        endpoint = request.endpoint or ''
        if endpoint in {'auth.login', 'auth.logout', 'auth.refresh', 'static', 'service_worker', 'pages.pwa_start', 'read_backend_health_endpoint'}:
            return None
        if is_authenticated():
            return None
        return unauthorized_response()

    @app.route('/service-worker.js')
    def service_worker():
        response = send_from_directory(
            os.path.dirname(__file__),
            'service-worker.js',
            mimetype='application/javascript',
            max_age=0,
        )
        response.headers['Cache-Control'] = 'no-cache'
        return response

    @app.before_request
    def enforce_clickhouse_read_only():
        # A CK read-only instance is still useful for dual-instance API
        # verification.  In the production migration mode, however,
        # MARKET_WRITE_BACKEND=clickhouse means market writes are intentional,
        # while control-plane and alert writes continue to use MySQL.
        if (
            not is_clickhouse_read()
            or is_clickhouse_write()
            or request.method in {'GET', 'HEAD', 'OPTIONS'}
        ):
            return None
        endpoint = request.endpoint or ''
        if endpoint.startswith('auth.'):
            return None
        return jsonify({
            'status': 'error',
            'message': 'ClickHouse read instance is read-only; send writes to the MySQL instance',
            'read_backend': get_read_backend(),
        }), 503

    @app.before_request
    def log_request_info():
        logger.debug('请求: %s %s', request.method, request.url)
        if request.data:
            if request.path.startswith('/api/notification-channels'):
                logger.debug('请求数据: [REDACTED]')
                return None
            try:
                json_data = request.get_json(force=True, silent=True)
                if json_data:
                    logger.debug('请求数据: %s', json_data)
                else:
                    logger.debug('请求数据: %s', request.data)
            except Exception:
                logger.debug('请求数据: %s', request.data)

    @app.route('/api/health/read-backend', methods=['GET'])
    def read_backend_health_endpoint():
        payload = read_backend_health()
        return jsonify(payload), (200 if payload.get('healthy') else 503)

    @app.after_request
    def log_response_info(response):
        response.headers['X-Read-Backend'] = get_read_backend()
        logger.debug('响应状态: %s', response.status)
        return response

    return app


app = create_app()
log_startup_credentials()


if __name__ == '__main__':
    if not WEB_DEBUG or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_runtime_services(with_startup_repair=True, startup_delay_seconds=1)
    app.run(host=WEB_HOST, port=WEB_PORT, debug=WEB_DEBUG)
