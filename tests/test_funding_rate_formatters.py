"""Tests for funding rate format functions"""
import pytest
from coinx.repositories.homepage_series import (
    format_funding_rate,
    format_funding_countdown,
)


class TestFormatFundingRate:
    """Test format_funding_rate function"""

    def test_format_positive_rate(self):
        """Test formatting positive rate"""
        assert format_funding_rate(0.001) == '0.100%'
        assert format_funding_rate(0.0001) == '0.010%'
        assert format_funding_rate(0.01) == '1.000%'

    def test_format_negative_rate(self):
        """Test formatting negative rate"""
        assert format_funding_rate(-0.001) == '-0.100%'
        assert format_funding_rate(-0.0001) == '-0.010%'
        assert format_funding_rate(-0.01) == '-1.000%'

    def test_format_zero_rate(self):
        """Test formatting zero rate"""
        assert format_funding_rate(0) == '0.000%'
        assert format_funding_rate(0.0) == '0.000%'

    def test_format_none_rate(self):
        """Test formatting None rate"""
        assert format_funding_rate(None) == 'N/A'

    def test_format_small_rate(self):
        """Test formatting very small rate"""
        assert format_funding_rate(0.00001) == '0.001%'
        assert format_funding_rate(-0.00001) == '-0.001%'

    def test_format_large_rate(self):
        """Test formatting large rate"""
        assert format_funding_rate(0.1) == '10.000%'
        assert format_funding_rate(-0.1) == '-10.000%'


class TestFormatFundingCountdown:
    """Test format_funding_countdown function"""

    def test_format_countdown_hours_minutes(self):
        """Test formatting countdown with hours and minutes"""
        import time
        now_ms = int(time.time() * 1000)
        # 2 hours 30 minutes from now
        next_time = now_ms + (2 * 3600 + 30 * 60) * 1000
        result = format_funding_countdown(next_time)
        assert result == '2h30m'

    def test_format_countdown_minutes_only(self):
        """Test formatting countdown with only minutes"""
        import time
        now_ms = int(time.time() * 1000)
        # 45 minutes from now
        next_time = now_ms + 45 * 60 * 1000
        result = format_funding_countdown(next_time)
        assert result == '45m'

    def test_format_countdown_zero(self):
        """Test formatting countdown when time has passed"""
        import time
        now_ms = int(time.time() * 1000)
        # 1 hour ago
        next_time = now_ms - 3600000
        result = format_funding_countdown(next_time)
        assert result == '已结算'

    def test_format_countdown_none(self):
        """Test formatting None countdown"""
        assert format_funding_countdown(None) == 'N/A'

    def test_format_countdown_exactly_zero_diff(self):
        """Test formatting countdown when diff is exactly 0"""
        import time
        now_ms = int(time.time() * 1000)
        result = format_funding_countdown(now_ms)
        # Should be '已结算' since diff is 0 or very close to 0
        assert result in ['已结算', '0m']

    def test_format_countdown_one_hour(self):
        """Test formatting countdown for exactly one hour"""
        import time
        now_ms = int(time.time() * 1000)
        next_time = now_ms + 3600000  # Exactly 1 hour
        result = format_funding_countdown(next_time)
        assert result == '1h00m'

    def test_format_countdown_less_than_one_minute(self):
        """Test formatting countdown for less than one minute"""
        import time
        now_ms = int(time.time() * 1000)
        next_time = now_ms + 30000  # 30 seconds
        result = format_funding_countdown(next_time)
        assert result == '0m'
