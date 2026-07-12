"""Tests for funding rate repository"""
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from coinx.repositories.funding_rate import (
    save_funding_rates,
    load_latest_funding_rates,
    load_funding_rate_history,
    load_abnormal_funding_rates,
    collect_funding_rates,
)
from coinx.models import MarketFundingRate
from coinx.repositories import funding_rate as funding_rate_repository


def test_funding_history_cutoff_uses_unix_time_without_timezone_offset(monkeypatch):
    now_seconds = 1_783_839_735.0
    monkeypatch.setattr(funding_rate_repository.time, 'time', lambda: now_seconds)

    cutoff = funding_rate_repository._history_cutoff_time_ms(24)

    assert cutoff == int(now_seconds * 1000) - 24 * 60 * 60 * 1000


class TestSaveFundingRates:
    """Test save_funding_rates function"""

    def test_save_empty_records(self):
        """Test saving empty records returns 0"""
        result = save_funding_rates([])
        assert result == 0

    def test_save_single_record_new(self, db_session):
        """Test saving a new record"""
        records = [{
            'symbol': 'BTCUSDT',
            'period': '5m',
            'event_time': 1698768000000,
            'funding_rate': 0.0001,
            'predicted_rate': 0.00012,
            'next_funding_time': 1698796800000,
            'mark_price': 34000.0,
            'exchange': 'binance',
        }]

        result = save_funding_rates(records, session=db_session)
        assert result == 1

        # Verify record was saved
        saved = db_session.query(MarketFundingRate).first()
        assert saved is not None
        assert saved.symbol == 'BTCUSDT'
        assert float(saved.funding_rate) == 0.0001
        assert float(saved.predicted_rate) == 0.00012

    def test_save_single_record_update_existing(self, db_session):
        """Test updating an existing record"""
        # First insert
        initial_records = [{
            'symbol': 'BTCUSDT',
            'period': '5m',
            'event_time': 1698768000000,
            'funding_rate': 0.0001,
            'predicted_rate': 0.00012,
            'next_funding_time': 1698796800000,
            'mark_price': 34000.0,
            'exchange': 'binance',
        }]
        save_funding_rates(initial_records, session=db_session)

        # Update with same symbol, period, event_time
        update_records = [{
            'symbol': 'BTCUSDT',
            'period': '5m',
            'event_time': 1698768000000,
            'funding_rate': 0.0002,
            'predicted_rate': 0.00025,
            'next_funding_time': 1698796800000,
            'mark_price': 35000.0,
            'exchange': 'binance',
        }]
        result = save_funding_rates(update_records, session=db_session)
        assert result == 1

        # Verify record was updated, not duplicated
        saved = db_session.query(MarketFundingRate).all()
        assert len(saved) == 1
        assert float(saved[0].funding_rate) == 0.0002
        assert float(saved[0].predicted_rate) == 0.00025

    def test_save_multiple_records(self, db_session):
        """Test saving multiple records"""
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.00015,
                'predicted_rate': 0.00018,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
                'exchange': 'binance',
            },
        ]

        result = save_funding_rates(records, session=db_session)
        assert result == 2

        saved = db_session.query(MarketFundingRate).all()
        assert len(saved) == 2


class TestLoadLatestFundingRates:
    """Test load_latest_funding_rates function"""

    def test_load_empty_symbols(self):
        """Test loading with empty symbols returns empty dict"""
        result = load_latest_funding_rates([])
        assert result == {}

    def test_load_single_symbol(self, db_session):
        """Test loading latest rate for a single symbol"""
        # Insert records
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698771600000,  # Later timestamp
                'funding_rate': 0.0002,
                'predicted_rate': 0.00025,
                'next_funding_time': 1698800400000,
                'mark_price': 35000.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_latest_funding_rates(['BTCUSDT'], session=db_session)

        assert 'BTCUSDT' in result
        assert result['BTCUSDT']['funding_rate'] == 0.0002
        assert result['BTCUSDT']['predicted_rate'] == 0.00025
        assert result['BTCUSDT']['event_time'] == 1698771600000

    def test_load_multiple_symbols(self, db_session):
        """Test loading latest rates for multiple symbols"""
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.00015,
                'predicted_rate': 0.00018,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_latest_funding_rates(['BTCUSDT', 'ETHUSDT'], session=db_session)

        assert 'BTCUSDT' in result
        assert 'ETHUSDT' in result
        assert result['BTCUSDT']['funding_rate'] == 0.0001
        assert result['ETHUSDT']['funding_rate'] == 0.00015

    def test_load_nonexistent_symbol(self, db_session):
        """Test loading a symbol that doesn't exist in database"""
        result = load_latest_funding_rates(['NONEXISTUSDT'], session=db_session)
        assert result == {}


class TestLoadFundingRateHistory:
    """Test load_funding_rate_history function"""

    def test_load_history_empty(self, db_session):
        """Test loading history when no data exists"""
        result = load_funding_rate_history('BTCUSDT', session=db_session)
        assert result == []

    def test_load_history_with_data(self, db_session):
        """Test loading history with multiple records"""
        now_ms = int(time.time() * 1000)
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': now_ms - 3600000,  # 1 hour ago
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': now_ms - 3500000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': now_ms - 1800000,  # 30 min ago
                'funding_rate': 0.0002,
                'predicted_rate': 0.00025,
                'next_funding_time': now_ms - 1700000,
                'mark_price': 35000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': now_ms,
                'funding_rate': 0.0003,
                'predicted_rate': 0.00035,
                'next_funding_time': now_ms + 100000,
                'mark_price': 36000.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_funding_rate_history('BTCUSDT', hours=2, session=db_session)

        assert len(result) == 3
        # Should be sorted by event_time ascending
        assert result[0]['event_time'] < result[1]['event_time'] < result[2]['event_time']

    def test_load_history_different_symbol(self, db_session):
        """Test that history only returns data for specified symbol"""
        now_ms = int(time.time() * 1000)
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': now_ms,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': now_ms + 100000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': now_ms,
                'funding_rate': 0.00015,
                'predicted_rate': 0.00018,
                'next_funding_time': now_ms + 100000,
                'mark_price': 1800.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_funding_rate_history('BTCUSDT', hours=1, session=db_session)
        assert len(result) == 1
        assert result[0]['funding_rate'] == 0.0001


class TestLoadAbnormalFundingRates:
    """Test load_abnormal_funding_rates function"""

    def test_load_abnormal_empty(self, db_session):
        """Test loading abnormal rates when no data exists"""
        result = load_abnormal_funding_rates(threshold=0.001, session=db_session)
        assert result == []

    def test_load_abnormal_with_threshold(self, db_session):
        """Test loading abnormal rates with threshold filtering"""
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.0005,  # Below threshold
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0015,
                'predicted_rate': 0.002,  # Above threshold
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
                'exchange': 'binance',
            },
            {
                'symbol': 'SOLUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': -0.001,
                'predicted_rate': -0.003,  # Above threshold (absolute value)
                'next_funding_time': 1698796800000,
                'mark_price': 50.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_abnormal_funding_rates(threshold=0.001, session=db_session)

        assert len(result) == 2
        # Should be sorted by absolute value descending
        assert abs(result[0]['predicted_rate']) >= abs(result[1]['predicted_rate'])

    def test_load_abnormal_negative_rates(self, db_session):
        """Test that negative rates with large absolute values are included"""
        records = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': -0.001,
                'predicted_rate': -0.002,  # Negative but above threshold
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
                'exchange': 'binance',
            },
        ]
        save_funding_rates(records, session=db_session)

        result = load_abnormal_funding_rates(threshold=0.001, session=db_session)
        assert len(result) == 1
        assert result[0]['predicted_rate'] == -0.002


class TestCollectFundingRates:
    """Test collect_funding_rates function"""

    @patch('coinx.repositories.funding_rate.fetch_all_premium_index')
    @patch('coinx.repositories.funding_rate.get_http_session')
    def test_collect_success(self, mock_http_session, mock_fetch_all, db_session):
        """Test successful collection of funding rates"""
        mock_fetch_all.return_value = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0002,
                'predicted_rate': 0.00025,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
            },
        ]

        result = collect_funding_rates(
            symbols=['BTCUSDT'],
            db_session=db_session,
        )

        # 只有 BTCUSDT 被过滤保存
        assert result == 1
        saved = db_session.query(MarketFundingRate).first()
        assert saved is not None
        assert saved.symbol == 'BTCUSDT'

    @patch('coinx.repositories.funding_rate.fetch_all_premium_index')
    @patch('coinx.repositories.funding_rate.get_http_session')
    def test_collect_all_symbols(self, mock_http_session, mock_fetch_all, db_session):
        """Test collection without symbol filter saves all"""
        mock_fetch_all.return_value = [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0001,
                'predicted_rate': 0.00012,
                'next_funding_time': 1698796800000,
                'mark_price': 34000.0,
            },
            {
                'symbol': 'ETHUSDT',
                'period': '5m',
                'event_time': 1698768000000,
                'funding_rate': 0.0002,
                'predicted_rate': 0.00025,
                'next_funding_time': 1698796800000,
                'mark_price': 1800.0,
            },
        ]

        result = collect_funding_rates(db_session=db_session)

        assert result == 2
