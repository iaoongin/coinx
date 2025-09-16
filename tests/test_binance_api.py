import pytest
import time
from unittest.mock import patch, MagicMock
from src.binance_api import get_open_interest, get_open_interest_history, update_all_data

class TestBinanceAPI:
    """测试币安API模块"""
    
    def test_get_open_interest_success(self):
        """测试成功获取当前持仓量数据"""
        # 模拟API响应
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'openInterest': '123456.789',
            'time': 1234567890123
        }
        mock_response.raise_for_status.return_value = None
        
        with patch('src.binance_api.requests.get', return_value=mock_response):
            result = get_open_interest('BTCUSDT')
            
            assert result is not None
            assert result['symbol'] == 'BTCUSDT'
            assert result['openInterest'] == 123456.789
            assert result['time'] == 1234567890123
    
    def test_get_open_interest_failure(self):
        """测试获取当前持仓量数据失败时返回None"""
        with patch('src.binance_api.requests.get', side_effect=Exception('Network error')):
            result = get_open_interest('BTCUSDT')
            
            assert result is None
    
    def test_get_open_interest_history_success(self):
        """测试成功获取历史持仓量数据"""
        # 模拟API响应
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                'timestamp': 1234567890123,
                'sumOpenInterest': '98765.432'
            }
        ]
        mock_response.raise_for_status.return_value = None
        
        with patch('src.binance_api.requests.get', return_value=mock_response):
            result = get_open_interest_history('BTCUSDT', '5m')
            
            assert result is not None
            assert result['symbol'] == 'BTCUSDT'
            assert result['interval'] == '5m'
            assert result['openInterest'] == 98765.432
            assert result['timestamp'] == 1234567890123
    
    def test_get_open_interest_history_failure(self):
        """测试获取历史持仓量数据失败时返回None"""
        with patch('src.binance_api.requests.get', side_effect=Exception('Network error')):
            result = get_open_interest_history('BTCUSDT', '5m')
            
            assert result is None
    
    def test_get_open_interest_history_empty_data(self):
        """测试获取历史持仓量数据为空时返回None"""
        # 模拟API响应为空数据
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        
        with patch('src.binance_api.requests.get', return_value=mock_response):
            result = get_open_interest_history('BTCUSDT', '5m')
            
            assert result is None
    
    def test_update_all_data_success(self):
        """测试成功更新所有币种数据"""
        # 模拟获取当前持仓量数据
        mock_current_response = MagicMock()
        mock_current_response.json.return_value = {
            'openInterest': '123456.789',
            'time': 1234567890123
        }
        mock_current_response.raise_for_status.return_value = None
        
        # 模拟获取历史持仓量数据
        mock_history_response = MagicMock()
        mock_history_response.json.return_value = [
            {
                'timestamp': 1234567890123,
                'sumOpenInterest': '98765.432'
            }
        ]
        mock_history_response.raise_for_status.return_value = None
        
        with patch('src.binance_api.requests.get') as mock_get:
            # 设置不同的响应
            mock_get.side_effect = [mock_current_response, mock_history_response]
            
            result = update_all_data(['BTCUSDT'])
            
            assert len(result) == 1
            assert result[0]['symbol'] == 'BTCUSDT'
            assert 'current' in result[0]
            assert 'intervals' in result[0]
    
    def test_update_all_data_empty_symbols(self):
        """测试空币种列表"""
        result = update_all_data([])
        assert result == []
    
    def test_update_all_data_network_error(self):
        """测试网络错误时的处理"""
        with patch('src.binance_api.requests.get', side_effect=Exception('Network error')):
            result = update_all_data(['BTCUSDT'])
            
            # 网络错误时应该返回空列表
            assert result == []
    
    def test_update_all_data_current_data_none(self):
        """测试当前数据为None时的处理"""
        with patch('src.binance_api.get_open_interest', return_value=None):
            result = update_all_data(['BTCUSDT'])
            
            # 当前数据为None时应该返回空列表
            assert result == []
    
    def test_update_all_data_history_data_none(self):
        """测试历史数据为None时的处理"""
        # 模拟获取当前持仓量数据成功
        mock_current_response = MagicMock()
        mock_current_response.json.return_value = {
            'openInterest': '123456.789',
            'time': 1234567890123
        }
        mock_current_response.raise_for_status.return_value = None
        
        with patch('src.binance_api.requests.get', return_value=mock_current_response):
            with patch('src.binance_api.get_open_interest_history', return_value=None):
                result = update_all_data(['BTCUSDT'])
                
                # 历史数据为None时仍应返回币种数据，但intervals为空
                assert len(result) == 1
                assert result[0]['symbol'] == 'BTCUSDT'
                assert result[0]['intervals'] == []