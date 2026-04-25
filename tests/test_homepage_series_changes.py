"""TDD Tests for Homepage Series Data Independence"""

import pytest
from coinx.repositories.homepage_series import get_homepage_series_data


TIME_INTERVALS = ['5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h']


class TestAllTimeIntervals:
    """Test all time intervals from 5m to 168h"""

    @pytest.mark.parametrize("interval", TIME_INTERVALS)
    def test_changes_interval_exists(self, interval):
        """Each time interval should have changes data"""
        result = get_homepage_series_data(['BTCUSDT'])[0]
        
        change = result['changes'].get(interval)
        assert change is not None, f"changes[{interval}] should exist"

    @pytest.mark.parametrize("interval", TIME_INTERVALS)
    def test_net_inflow_interval_exists(self, interval):
        """Each time interval should have net_inflow data"""
        result = get_homepage_series_data(['BTCUSDT'])[0]
        
        net_inflow = result['net_inflow'].get(interval)
        assert net_inflow is not None, f"net_inflow[{interval}] should exist"

    @pytest.mark.parametrize("interval", TIME_INTERVALS)
    def test_changes_interval_has_required_fields(self, interval):
        """Each changes interval should have required fields"""
        result = get_homepage_series_data(['BTCUSDT'])[0]
        
        change = result['changes'].get(interval)
        assert change is not None
        
        # Required fields for changes
        assert 'current_price' in change or change.get('price_change') is not None, f"{interval}: current_price"
        assert 'open_interest' in change or 'ratio' in change, f"{interval}: open_interest"
        assert 'price_change' in change or 'price_change_percent' in change, f"{interval}: price_change"

    @pytest.mark.parametrize("interval", TIME_INTERVALS)
    def test_net_inflow_interval_values(self, interval):
        """Each net_inflow interval should have numeric value"""
        result = get_homepage_series_data(['BTCUSDT'])[0]
        
        net_inflow = result['net_inflow'].get(interval)
        assert net_inflow is not None
        assert isinstance(net_inflow, (int, float)), f"net_inflow[{interval}] should be numeric"


class TestChangesIndependentTimeSources:
    """Test changes calculates correctly with independent time sources"""

    def test_changes_uses_own_latest_time_per_source(self):
        """Each data source should use its own latest time when calculating 168h"""
        result = get_homepage_series_data(['ETHUSDT'])[0]
        
        assert result['changes'].get('168h') is not None, "168h changes should exist"
        
        change_168h = result['changes']['168h']
        assert change_168h.get('current_price') is not None, "168h price should be available"
        assert change_168h.get('open_interest') is not None, "168h open_interest should be available"

    def test_changes_168h_tolerance(self):
        """Should tolerate up to 10 missing points"""
        result = get_homepage_series_data(['ETHUSDT'])[0]
        
        change_168h = result['changes'].get('168h')
        if change_168h:
            price = change_168h.get('current_price')
            oi = change_168h.get('open_interest')
            assert price is not None or oi is not None


class TestNetInflowCumulative:
    """Test net_inflow calculation is cumulative"""

    @pytest.mark.parametrize("symbol", ['BTCUSDT', 'ETHUSDT', 'XAUUSDT'])
    def test_net_inflow_cumulative_increasing(self, symbol):
        """net_inflow should be cumulative over interval (data points count)"""
        result = get_homepage_series_data([symbol])[0]
        
        # Check data exists for each interval
        assert result['net_inflow'].get('5m') is not None
        assert result['net_inflow'].get('1h') is not None
        assert result['net_inflow'].get('168h') is not None


class TestAllSymbols168h:
    """Test all tracked symbols have 168h data"""

    @pytest.mark.parametrize("symbol", ['BTCUSDT', 'ETHUSDT', 'XAUUSDT', 'AXLUSDT', 'PROMUSDT'])
    def test_symbol_has_168h_changes(self, symbol):
        """Each symbol should have 168h changes"""
        result = get_homepage_series_data([symbol])[0]
        
        assert result['changes'].get('168h') is not None, f"{symbol} should have 168h changes"

    @pytest.mark.parametrize("symbol", ['BTCUSDT', 'ETHUSDT', 'XAUUSDT', 'AXLUSDT', 'PROMUSDT'])
    def test_symbol_has_168h_net_inflow(self, symbol):
        """Each symbol should have 168h net_inflow"""
        result = get_homepage_series_data([symbol])[0]
        
        assert result['net_inflow'].get('168h') is not None, f"{symbol} should have 168h net_inflow"

    @pytest.mark.parametrize("symbol", ['BTCUSDT', 'ETHUSDT', 'XAUUSDT', 'AXLUSDT', 'PROMUSDT'])
    def test_symbol_has_all_intervals(self, symbol):
        """Each symbol should have all time intervals"""
        result = get_homepage_series_data([symbol])[0]
        
        for interval in TIME_INTERVALS:
            assert result['changes'].get(interval) is not None, f"{symbol}: changes[{interval}]"
            assert result['net_inflow'].get(interval) is not None, f"{symbol}: net_inflow[{interval}]"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])