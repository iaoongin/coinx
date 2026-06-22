"""Tests for Binance funding rate collector"""
import pytest
from unittest.mock import Mock, patch
from coinx.collector.binance.funding_rate import fetch_premium_index, parse_funding_rate


class TestFetchPremiumIndex:
    """Test fetch_premium_index function"""

    @patch('coinx.collector.binance.funding_rate.get_session')
    @patch('coinx.collector.binance.funding_rate.request_with_binance_retry')
    def test_fetch_premium_index_success(self, mock_request, mock_get_session):
        """Test successful API call"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'symbol': 'BTCUSDT',
            'markPrice': '34000.00',
            'lastFundingRate': '0.00010000',
            'nextFundingRate': '0.00012000',
            'nextFundingTime': 1698796800000,
            'time': 1698768000000,
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        result = fetch_premium_index('BTCUSDT')

        assert result['symbol'] == 'BTCUSDT'
        assert result['period'] == '5m'
        assert result['funding_rate'] == 0.0001
        assert result['predicted_rate'] == 0.00012
        assert result['next_funding_time'] == 1698796800000
        assert result['mark_price'] == 34000.0


class TestParseFundingRate:
    """Test parse_funding_rate function"""

    def test_parse_funding_rate(self):
        """Test parsing API response"""
        payload = {
            'symbol': 'BTCUSDT',
            'markPrice': '34000.00',
            'lastFundingRate': '0.00010000',
            'nextFundingRate': '0.00012000',
            'nextFundingTime': 1698796800000,
            'time': 1698768000000,
        }

        result = parse_funding_rate(payload, 'BTCUSDT', '5m')

        assert len(result) == 1
        assert result[0]['symbol'] == 'BTCUSDT'
        assert result[0]['period'] == '5m'
        assert result[0]['funding_rate'] == 0.0001
        assert result[0]['predicted_rate'] == 0.00012

    def test_parse_funding_rate_without_predicted(self):
        """Test parsing API response when nextFundingRate is missing"""
        payload = {
            'symbol': 'BTCUSDT',
            'markPrice': '34000.00',
            'lastFundingRate': '0.00010000',
            'nextFundingTime': 1698796800000,
            'time': 1698768000000,
        }

        result = parse_funding_rate(payload, 'BTCUSDT', '5m')

        assert len(result) == 1
        assert result[0]['symbol'] == 'BTCUSDT'
        assert result[0]['funding_rate'] == 0.0001
        assert result[0]['predicted_rate'] is None
