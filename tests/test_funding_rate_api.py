"""Tests for funding rate API routes"""
import pytest
from unittest.mock import patch
from flask import Flask
import werkzeug

from coinx.web.routes.api_funding_rate import api_funding_rate_bp


@pytest.fixture
def client():
    """Create test client with funding rate blueprint"""
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_funding_rate_bp)
    app.config['TESTING'] = True
    return app.test_client()


class TestGetFundingRates:
    """Test /api/funding-rate endpoint"""

    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rates')
    @patch('coinx.web.routes.api_funding_rate.get_exchange_info')
    def test_get_funding_rates_success(self, mock_exchange_info, mock_load_rates, client):
        """Test successful retrieval of funding rates"""
        mock_exchange_info.return_value = [
            {'symbol': 'BTCUSDT'},
            {'symbol': 'ETHUSDT'},
        ]
        mock_load_rates.return_value = {
            'BTCUSDT': {
                'predicted_rate': 0.001,
                'funding_rate': 0.0005,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'event_time': 1698768000000,
            },
            'ETHUSDT': {
                'predicted_rate': -0.002,
                'funding_rate': -0.001,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
                'event_time': 1698768000000,
            },
        }

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert len(data['data']) == 2
        # Should be sorted by predicted_rate descending (default)
        assert data['data'][0]['symbol'] == 'BTCUSDT'
        assert data['data'][1]['symbol'] == 'ETHUSDT'

    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rates')
    @patch('coinx.web.routes.api_funding_rate.get_exchange_info')
    def test_get_funding_rates_empty(self, mock_exchange_info, mock_load_rates, client):
        """Test when no exchange info available"""
        mock_exchange_info.return_value = None

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['data'] == []

    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rates')
    @patch('coinx.web.routes.api_funding_rate.get_exchange_info')
    def test_get_funding_rates_with_limit(self, mock_exchange_info, mock_load_rates, client):
        """Test limit parameter"""
        mock_exchange_info.return_value = [
            {'symbol': 'BTCUSDT'},
            {'symbol': 'ETHUSDT'},
            {'symbol': 'SOLUSDT'},
        ]
        mock_load_rates.return_value = {
            'BTCUSDT': {
                'predicted_rate': 0.001,
                'funding_rate': 0.0005,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'event_time': 1698768000000,
            },
            'ETHUSDT': {
                'predicted_rate': 0.002,
                'funding_rate': 0.001,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
                'event_time': 1698768000000,
            },
            'SOLUSDT': {
                'predicted_rate': 0.003,
                'funding_rate': 0.0015,
                'next_funding_time': 1698796800000,
                'mark_price': 50.0,
                'event_time': 1698768000000,
            },
        }

        response = client.get('/api/funding-rate?limit=2')
        data = response.get_json()

        assert response.status_code == 200
        assert len(data['data']) == 2

    @patch('coinx.web.routes.api_funding_rate.load_latest_funding_rates')
    @patch('coinx.web.routes.api_funding_rate.get_exchange_info')
    def test_get_funding_rates_abnormal_marked(self, mock_exchange_info, mock_load_rates, client):
        """Test that abnormal rates are marked correctly"""
        mock_exchange_info.return_value = [{'symbol': 'BTCUSDT'}]
        mock_load_rates.return_value = {
            'BTCUSDT': {
                'predicted_rate': 0.002,  # Above threshold (0.001)
                'funding_rate': 0.0005,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'event_time': 1698768000000,
            },
        }

        response = client.get('/api/funding-rate')
        data = response.get_json()

        assert data['data'][0]['is_abnormal'] is True


class TestGetAbnormalFundingRates:
    """Test /api/funding-rate/abnormal endpoint"""

    @patch('coinx.web.routes.api_funding_rate.load_abnormal_funding_rates')
    def test_get_abnormal_rates(self, mock_load_abnormal, client):
        """Test successful retrieval of abnormal rates"""
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
        """Test when no abnormal rates"""
        mock_load_abnormal.return_value = []

        response = client.get('/api/funding-rate/abnormal')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert len(data['data']) == 0


class TestGetFundingRateHistory:
    """Test /api/funding-rate/history/<symbol> endpoint"""

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_history')
    def test_get_history_success(self, mock_load_history, client):
        """Test successful retrieval of history"""
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
        """Test history with hours parameter"""
        mock_load_history.return_value = []

        response = client.get('/api/funding-rate/history/BTCUSDT?hours=24')
        data = response.get_json()

        assert response.status_code == 200
        assert data['hours'] == 24
        mock_load_history.assert_called_once_with('BTCUSDT', hours=24)

    @patch('coinx.web.routes.api_funding_rate.load_funding_rate_history')
    def test_get_history_hours_clamped(self, mock_load_history, client):
        """Test that hours parameter is clamped to 1-168"""
        mock_load_history.return_value = []

        # Test lower bound
        response = client.get('/api/funding-rate/history/BTCUSDT?hours=0')
        data = response.get_json()
        assert data['hours'] == 1

        # Test upper bound
        response = client.get('/api/funding-rate/history/BTCUSDT?hours=200')
        data = response.get_json()
        assert data['hours'] == 168


class TestRefreshFundingRates:
    """Test /api/funding-rate/refresh endpoint"""

    @patch('coinx.web.routes.api_funding_rate.collect_funding_rates')
    def test_refresh_success(self, mock_collect, client):
        """Test successful refresh"""
        mock_collect.return_value = 806

        response = client.get('/api/funding-rate/refresh')
        data = response.get_json()

        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['count'] == 806
        mock_collect.assert_called_once_with()

    @patch('coinx.web.routes.api_funding_rate.collect_funding_rates')
    def test_refresh_failure(self, mock_collect, client):
        """Test refresh when collection fails"""
        mock_collect.side_effect = Exception('API error')

        response = client.get('/api/funding-rate/refresh')
        data = response.get_json()

        assert response.status_code == 500
        assert data['status'] == 'error'
