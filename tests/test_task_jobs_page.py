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
    assert 'border-radius: var(--page-surface-radius);'.encode('utf-8') in response.data
    assert 'SCHEDULER_ENABLED=false'.encode('utf-8') in response.data
    assert '部分成功'.encode('utf-8') in response.data
    assert '限流等待'.encode('utf-8') in response.data
    assert '冷却剩余'.encode('utf-8') in response.data
    assert '执行记录'.encode('utf-8') in response.data
    assert '下一页'.encode('utf-8') in response.data
    assert '/runs?limit=5&offset='.encode('utf-8') in response.data
    assert 'history-modal'.encode('utf-8') in response.data
    assert 'formatRunDetail'.encode('utf-8') in response.data
    assert 'summary-content'.encode('utf-8') in response.data
    assert 'summary-modal'.encode('utf-8') in response.data
    assert 'history-summary-line'.encode('utf-8') in response.data
    assert 'window.CoinxToast'.encode('utf-8') in response.data
    assert 'coinxSuppressGlobalError'.encode('utf-8') in response.data
    assert ':class="[\'message-box\', flash.type]"'.encode('utf-8') not in response.data
    assert "openErrorDetail('历史摘要'".encode('utf-8') in response.data
