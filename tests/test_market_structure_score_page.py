import werkzeug
from flask import Flask

from coinx.web.routes.pages import pages_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__, template_folder='../src/coinx/web/templates')
    app.register_blueprint(pages_bp)
    return app.test_client()


def test_market_structure_score_page_renders():
    client = create_test_client()
    response = client.get('/market-structure-score')

    assert response.status_code == 200
    assert '合约市场结构评分'.encode('utf-8') in response.data
    assert '结构评分'.encode('utf-8') in response.data
    assert '情绪健康度'.encode('utf-8') in response.data
