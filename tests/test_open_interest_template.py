from pathlib import Path

from flask import Flask

from coinx.web.routes.pages import pages_bp


def test_open_interest_template_contains_filters_and_aggregate_columns():
    template = Path('src/coinx/web/templates/open_interest.html').read_text(encoding='utf-8')

    assert '持仓量概览' in template
    assert '跨交易所累计持仓量' in template
    assert 'id="oi-period"' in template
    assert 'id="oi-min-ratio"' in template
    assert 'id="oi-max-ratio"' in template
    assert 'id="oi-min-amount"' in template
    assert 'id="oi-max-amount"' in template
    assert 'id="oi-order"' in template
    assert '累计持仓量' in template
    assert '流通量' in template
    assert '持仓量 / 流通量' in template


def test_open_interest_page_renders_template():
    app = Flask(
        __name__,
        template_folder=str(Path('src/coinx/web/templates').resolve()),
        static_folder=str(Path('src/coinx/web/static').resolve()),
    )
    app.register_blueprint(pages_bp)

    response = app.test_client().get('/open-interest')

    assert response.status_code == 200
    assert '持仓量概览' in response.get_data(as_text=True)
