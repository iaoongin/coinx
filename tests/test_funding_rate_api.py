"""Tests for funding rate API routes."""
from unittest.mock import patch

import pytest
import werkzeug
from flask import Flask

from coinx.web.routes.api_funding_rate import api_funding_rate_bp


@pytest.fixture
def client():
    """Create test client with funding rate blueprint."""
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_funding_rate_bp)
    app.config['TESTING'] = True
    return app.test_client()


class TestGetFundingRates:
    """Test /api/funding-rate endpoint."""

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_get_funding_rates_success(self, mock_load_page, mock_sparklines, client):
        """List endpoint returns formatted rows and sparklines."""
        mock_load_page.return_value = {
            'data': [
                {
                    'symbol': 'BTCUSDT',
                    'predicted_rate': 0.001,
                    'funding_rate': 0.0005,
                    'next_funding_time': 1698796800000,
                    'mark_price': 34000.0,
                    'event_time': 1698768000000,
                    'is_abnormal': True,
                },
                {
                    'symbol': 'ETHUSDT',
                    'predicted_rate': -0.002,
                    'funding_rate': -0.001,
                    'next_funding_time': 1698796800000,
                    'mark_price': 1800.0,
                    'event_time': 1698768000000,
                    'is_abnormal': True,
                },
            ],
            'total_count': 2,
            'stats': {
                'total': 2,
                'abnormal': 2,
                'positive': 1,
                'negative': 1,
            },
        }
        mock_sparklines.return_value = {'BTCUSDT': [0.1, 0.2]}

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['total_count'] == 2
        assert data['page'] == 1
        assert data['page_size'] == 50
        assert len(data['data']) == 2
        assert data['data'][0]['symbol'] == 'BTCUSDT'
        assert data['data'][0]['sparkline'] == [0.1, 0.2]
        assert data['data'][1]['sparkline'] == []

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_get_funding_rates_empty(self, mock_load_page, mock_sparklines, client):
        """Empty page payload returns empty list."""
        mock_load_page.return_value = {
            'data': [],
            'total_count': 0,
            'stats': {
                'total': 0,
                'abnormal': 0,
                'positive': 0,
                'negative': 0,
            },
        }
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['data'] == []

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_limit_compat(self, mock_load_page, mock_sparklines, client):
        """limit parameter maps to page_size for backward compatibility."""
        mock_load_page.return_value = {
            'data': [
                {'symbol': 'SOLUSDT', 'predicted_rate': 0.003, 'funding_rate': 0.0015, 'next_funding_time': 1698796800000, 'mark_price': 50.0, 'event_time': 1698768000000, 'is_abnormal': True},
                {'symbol': 'ETHUSDT', 'predicted_rate': 0.002, 'funding_rate': 0.001, 'next_funding_time': 1698796800000, 'mark_price': 1800.0, 'event_time': 1698768000000, 'is_abnormal': True},
                {'symbol': 'BTCUSDT', 'predicted_rate': 0.001, 'funding_rate': 0.0005, 'next_funding_time': 1698796800000, 'mark_price': 34000.0, 'event_time': 1698768000000, 'is_abnormal': False},
            ],
            'total_count': 3,
            'stats': {
                'total': 3,
                'abnormal': 2,
                'positive': 3,
                'negative': 0,
            },
        }
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?limit=3')
        data = response.get_json()

        assert response.status_code == 200
        assert data['page_size'] == 3
        assert len(data['data']) == 3

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_endpoint_passes_paging_filters_to_repository(self, mock_load_page, mock_sparklines, client):
        """Route forwards keyword/filter/sort/paging to the single-query repository."""
        mock_load_page.return_value = {
            'data': [],
            'total_count': 0,
            'stats': {
                'total': 0,
                'abnormal': 0,
                'positive': 0,
                'negative': 0,
            },
        }
        mock_sparklines.return_value = {}

        response = client.get(
            '/api/funding-rate?keyword=doge&show_abnormal_only=true&sort_by=abs_funding_rate&sort_order=asc&page=2&page_size=3'
        )

        assert response.status_code == 200
        mock_load_page.assert_called_once_with(
            keyword='doge',
            show_abnormal_only=True,
            sort_by='abs_funding_rate',
            sort_order='asc',
            page=2,
            page_size=3,
            threshold=0.001,
        )


class TestGetFundingRatesPagination:
    """Test pagination, search, and filter features of /api/funding-rate."""

    SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'DOGEUSDT', 'XRPUSDT',
               'ADAUSDT', 'DOTUSDT', 'MATICUSDT', 'AVAXUSDT', 'BNBUSDT']

    TEST_DATA = {
        'BTCUSDT': (0.0005, 0.0010),
        'ETHUSDT': (-0.0003, -0.0005),
        'SOLUSDT': (0.0020, 0.0025),
        'DOGEUSDT': (-0.0015, -0.0018),
        'XRPUSDT': (0.0001, 0.0002),
        'ADAUSDT': (-0.0008, -0.0012),
        'DOTUSDT': (0.0004, 0.0006),
        'MATICUSDT': (-0.0002, -0.0004),
        'AVAXUSDT': (0.0018, 0.0022),
        'BNBUSDT': (-0.0005, -0.0007),
    }

    @classmethod
    def _build_page_payload(cls, symbols, total_count=None):
        data = []
        abnormal_count = 0
        positive_count = 0
        negative_count = 0

        for sym in symbols:
            predicted_rate, funding_rate = cls.TEST_DATA[sym]
            predicted_abs = abs(predicted_rate) if predicted_rate is not None else 0
            funding_abs = abs(funding_rate) if funding_rate is not None else 0
            is_abnormal = predicted_abs >= 0.001 or funding_abs >= 0.001
            abnormal_count += int(is_abnormal)
            positive_count += int(funding_rate is not None and funding_rate > 0)
            negative_count += int(funding_rate is not None and funding_rate < 0)
            data.append({
                'symbol': sym,
                'predicted_rate': predicted_rate,
                'funding_rate': funding_rate,
                'next_funding_time': 1698796800000,
                'mark_price': 100.0,
                'event_time': 1698768000000,
                'is_abnormal': is_abnormal,
            })

        total = len(symbols) if total_count is None else total_count
        return {
            'data': data,
            'total_count': total,
            'stats': {
                'total': total,
                'abnormal': abnormal_count,
                'positive': positive_count,
                'negative': negative_count,
            },
        }

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_pagination_defaults(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(self.SYMBOLS)
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert response.status_code == 200
        assert data['total_count'] == 10
        assert data['page'] == 1
        assert data['page_size'] == 50
        assert len(data['data']) == 10
        assert data['stats'] == {
            'total': 10,
            'abnormal': 5,
            'positive': 5,
            'negative': 5,
        }

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_pagination_page_size(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(self.SYMBOLS[:3], total_count=10)
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?page_size=3')
        data = response.get_json()

        assert len(data['data']) == 3
        assert data['total_count'] == 10
        assert data['page_size'] == 3

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_pagination_page_2(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(['DOTUSDT', 'XRPUSDT', 'MATICUSDT'], total_count=10)
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?page_size=3&page=2')
        data = response.get_json()

        assert len(data['data']) == 3
        assert data['page'] == 2
        assert data['data'][0]['symbol'] == 'DOTUSDT'
        assert data['data'][1]['symbol'] == 'XRPUSDT'
        assert data['data'][2]['symbol'] == 'MATICUSDT'

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_pagination_last_page(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = {
            'data': [],
            'total_count': 10,
            'stats': {'total': 10, 'abnormal': 5, 'positive': 5, 'negative': 5},
        }
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?page_size=3&page=10')
        data = response.get_json()

        assert len(data['data']) == 0
        assert data['total_count'] == 10

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_keyword_search(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(['BTCUSDT'])
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?keyword=BTC')
        data = response.get_json()

        assert len(data['data']) == 1
        assert data['data'][0]['symbol'] == 'BTCUSDT'
        assert data['total_count'] == 1

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_keyword_search_lowercase(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(['BTCUSDT'])
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?keyword=btc')
        data = response.get_json()

        assert len(data['data']) == 1
        assert data['data'][0]['symbol'] == 'BTCUSDT'

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_keyword_search_partial(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(self.SYMBOLS)
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?keyword=USDT')
        data = response.get_json()

        assert len(data['data']) == 10

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_keyword_no_match(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = {
            'data': [],
            'total_count': 0,
            'stats': {'total': 0, 'abnormal': 0, 'positive': 0, 'negative': 0},
        }
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?keyword=ZZZ')
        data = response.get_json()

        assert len(data['data']) == 0
        assert data['total_count'] == 0

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_show_abnormal_only(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(['BTCUSDT', 'SOLUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT'])
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?show_abnormal_only=true')
        data = response.get_json()

        assert data['total_count'] == 5
        assert all(d['is_abnormal'] for d in data['data'])

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_keyword_plus_abnormal(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(['DOGEUSDT'])
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?keyword=DOGE&show_abnormal_only=true')
        data = response.get_json()

        assert len(data['data']) == 1
        assert data['data'][0]['symbol'] == 'DOGEUSDT'
        assert data['data'][0]['is_abnormal'] is True

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_sort_by_funding_rate_asc(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(
            ['DOGEUSDT', 'ADAUSDT', 'BNBUSDT', 'ETHUSDT', 'MATICUSDT', 'XRPUSDT', 'DOTUSDT', 'BTCUSDT', 'AVAXUSDT', 'SOLUSDT']
        )
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?sort_by=funding_rate&sort_order=asc')
        data = response.get_json()

        rates = [item['funding_rate'] for item in data['data']]
        assert rates == sorted(rates)

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_sparklines')
    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rate_page')
    def test_sort_by_abs_funding_rate(self, mock_load_page, mock_sparklines, client):
        mock_load_page.return_value = self._build_page_payload(
            ['SOLUSDT', 'AVAXUSDT', 'DOGEUSDT', 'ADAUSDT', 'BTCUSDT', 'BNBUSDT', 'DOTUSDT', 'ETHUSDT', 'MATICUSDT', 'XRPUSDT']
        )
        mock_sparklines.return_value = {}

        response = client.get('/api/funding-rate?sort_by=abs_funding_rate')
        data = response.get_json()

        abs_rates = [abs(item['funding_rate']) for item in data['data']]
        assert abs_rates == sorted(abs_rates, reverse=True)


class TestGetAbnormalFundingRates:
    """Test /api/funding-rate/abnormal endpoint."""

    @patch('coinx.web.routes.api_funding_rate.load_abnormal_funding_rates')
    def test_get_abnormal_rates(self, mock_load_abnormal, client):
        mock_load_abnormal.return_value = [
            {
                'symbol': 'BTCUSDT',
                'predicted_rate': 0.002,
                'funding_rate': 0.0005,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'event_time': 1698768000000,
            },
        ]

        response = client.get('/api/funding-rate/abnormal')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert len(data['data']) == 1
        assert data['data'][0]['symbol'] == 'BTCUSDT'

    @patch('coinx.web.routes.api_funding_rate.load_abnormal_funding_rates')
    def test_get_abnormal_rates_empty(self, mock_load_abnormal, client):
        mock_load_abnormal.return_value = []

        response = client.get('/api/funding-rate/abnormal')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert len(data['data']) == 0


class TestGetFundingRateHistory:
    """Test /api/funding-rate/history/<symbol> endpoint."""

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_history')
    def test_get_history_success(self, mock_load_history, client):
        mock_load_history.return_value = [
            {
                'symbol': 'BTCUSDT',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'mark_price': 34000.0,
            },
            {
                'symbol': 'BTCUSDT',
                'event_time': 1698771600000,
                'funding_rate': 0.0002,
                'predicted_rate': 0.00025,
                'mark_price': 35000.0,
            },
        ]

        response = client.get('/api/funding-rate/history/BTCUSDT')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['symbol'] == 'BTCUSDT'
        assert len(data['data']) == 2

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_history')
    def test_get_history_with_hours_param(self, mock_load_history, client):
        mock_load_history.return_value = []

        response = client.get('/api/funding-rate/history/BTCUSDT?hours=24')
        data = response.get_json()

        assert response.status_code == 200
        assert data['hours'] == 24
        mock_load_history.assert_called_once_with('BTCUSDT', hours=24)

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_history')
    def test_get_history_hours_clamped(self, mock_load_history, client):
        mock_load_history.return_value = []

        response = client.get('/api/funding-rate/history/BTCUSDT?hours=0')
        data = response.get_json()
        assert data['hours'] == 1

        response = client.get('/api/funding-rate/history/BTCUSDT?hours=200')
        data = response.get_json()
        assert data['hours'] == 168


class TestRefreshFundingRates:
    """Test /api/funding-rate/refresh endpoint."""

    @patch('coinx.web.routes.api_funding_rate.collect_funding_rates')
    def test_refresh_success(self, mock_collect, client):
        mock_collect.return_value = 806

        response = client.get('/api/funding-rate/refresh')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['count'] == 806
        mock_collect.assert_called_once_with()

    @patch('coinx.web.routes.api_funding_rate.collect_funding_rates')
    def test_refresh_failure(self, mock_collect, client):
        mock_collect.side_effect = Exception('API error')

        response = client.get('/api/funding-rate/refresh')
        data = response.get_json()

        assert response.status_code == 500
        assert data['status'] == 'error'
