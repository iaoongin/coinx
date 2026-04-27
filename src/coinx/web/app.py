import os
import sys

from flask import Flask, request

# 添加项目根目录到路径
# 添加项目根目录到路径（兼容直接运行当前模块）
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from coinx.config import WEB_DEBUG, WEB_HOST, WEB_PORT
from coinx.database import db_session
from coinx.runtime import start_runtime_services
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
except ImportError:
    # 如果在当前目录运行，可能需要使用相对导入路径
    from routes.auth import auth_bp
    from routes.pages import pages_bp
    from routes.api_data import api_data_bp
    from routes.api_config import api_config_bp


def create_app():
    # 创建 Flask 应用并注册页面、接口与登录路由
    app = Flask(__name__, template_folder='templates', static_folder='static')
    
    # 开发环境：禁用静态文件缓存，启用模板自动重载
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    
    configure_app(app)

    app.register_blueprint(auth_bp)
    app.register_blueprint(pages_bp)
    app.register_blueprint(api_data_bp)
    app.register_blueprint(api_config_bp)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()

    @app.before_request
    def require_login():
        # 登录页、退出接口和静态资源不需要登录，其余请求统一拦截
        endpoint = request.endpoint or ''
        if endpoint in {'auth.login', 'auth.logout', 'static'}:
            return None
        if is_authenticated():
            return None
        return unauthorized_response()

    @app.before_request
    def log_request_info():
        logger.info('请求: %s %s', request.method, request.url)
        if request.data:
            try:
                json_data = request.get_json(force=True, silent=True)
                if json_data:
                    logger.info('请求数据: %s', json_data)
                else:
                    logger.info('请求数据: %s', request.data)
            except Exception:
                logger.info('请求数据: %s', request.data)

    @app.after_request
    def log_response_info(response):
        logger.info('响应状态: %s', response.status)
        return response

    return app


app = create_app()
log_startup_credentials()


if __name__ == '__main__':
    if not WEB_DEBUG or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_runtime_services(with_startup_repair=True, startup_delay_seconds=1)
    app.run(host=WEB_HOST, port=WEB_PORT, debug=WEB_DEBUG)
