import werkzeug
from flask import Flask

from coinx.web.routes.pages import pages_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__, template_folder='../src/coinx/web/templates')
    app.register_blueprint(pages_bp)
    return app.test_client()


def test_task_jobs_page_renders():
    client = create_test_client()
    response = client.get('/task-jobs')

    assert response.status_code == 200
    assert '任务管理'.encode('utf-8') in response.data
    assert '任务调度控制台'.encode('utf-8') in response.data
