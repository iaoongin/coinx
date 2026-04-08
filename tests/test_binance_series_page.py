import werkzeug
from flask import Flask

from coinx.web.routes.pages import pages_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__, template_folder='../src/coinx/web/templates')
    app.register_blueprint(pages_bp)
    return app.test_client()


def test_binance_series_page_renders():
    client = create_test_client()
    response = client.get('/binance-series')

    assert response.status_code == 200
    assert 'Binance 历史序列管理'.encode('utf-8') in response.data
    assert '手动采集'.encode('utf-8') in response.data
    assert '缺口修补'.encode('utf-8') in response.data


def test_hedge_calculator_page_renders():
    client = create_test_client()
    response = client.get('/hedge-calculator')

    assert response.status_code == 200
    assert '对冲计算器'.encode('utf-8') in response.data
    assert 'USDT 本位线性合约'.encode('utf-8') in response.data
    assert '平衡市场价'.encode('utf-8') in response.data
