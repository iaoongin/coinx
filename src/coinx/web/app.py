import sys
import os
from flask import Flask, request

# 添加项目根目录到路径
# 添加项目根目录到路径 (src目录)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from coinx.utils import logger

# Import blueprints
# 注意：必须在添加项目根目录到sys.path之后导入
# 使用 try-except 块来处理可能的导入路径问题
try:
    from coinx.web.routes.pages import pages_bp
    from coinx.web.routes.api_data import api_data_bp
    from coinx.web.routes.api_config import api_config_bp
except ImportError:
    #如果在当前目录运行，可能需要 adjustments
    from routes.pages import pages_bp
    from routes.api_data import api_data_bp
    from routes.api_config import api_config_bp

app = Flask(__name__, template_folder='templates', static_folder='static')

# 注册 Blueprints
app.register_blueprint(pages_bp)
app.register_blueprint(api_data_bp)
app.register_blueprint(api_config_bp)

@app.before_request
def log_request_info():
    """记录请求信息"""
    logger.info(f"请求: {request.method} {request.url}")
    if request.data:
        try:
            # 使用 force=True 避免Content-Type检查，silent=True 避免解析失败时抛出异常
            json_data = request.get_json(force=True, silent=True)
            if json_data:
                logger.info(f"请求数据: {json_data}")
            else:
                logger.info(f"请求数据: {request.data}")
        except:
            logger.info(f"请求数据: {request.data}")

@app.after_request
def log_response_info(response):
    """记录响应信息"""
    logger.info(f"响应状态: {response.status}")
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
