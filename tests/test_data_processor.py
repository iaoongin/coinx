import pytest
import time
from unittest.mock import patch, MagicMock
from src.data_processor import get_coin_data, get_all_coins_data

class TestDataProcessor:
    """测试数据处理模块"""
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_success(self, mock_load_data):
        """测试成功获取币种数据"""
        # 模拟加载的数据
        mock_load_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current': {
                    'openInterest': 150000.0
                },
                'intervals': [
                    {
                        'interval': '5m',
                        'openInterest': 140000.0
                    },
                    {
                        'interval': '1h',
                        'openInterest': 130000.0
                    }
                ]
            }
        ]
        
        result = get_coin_data('BTCUSDT')
        
        assert result['symbol'] == 'BTCUSDT'
        assert result['current_open_interest'] == 150000.0
        assert '5m' not in result['changes']  # 5m应该被跳过
        assert '1h' in result['changes']
        # 计算变化比例: (150000 - 130000) / 130000 * 100 = 15.38%
        assert abs(result['changes']['1h'] - 15.38) < 0.01
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_not_found(self, mock_load_data):
        """测试找不到币种数据的情况"""
        mock_load_data.return_value = [
            {
                'symbol': 'ETHUSDT',
                'current': {
                    'openInterest': 80000.0
                },
                'intervals': []
            }
        ]
        
        result = get_coin_data('BTCUSDT')
        
        assert result['symbol'] == 'BTCUSDT'
        assert result['current_open_interest'] is None
        assert result['changes'] == {}
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_empty_data(self, mock_load_data):
        """测试空数据的情况"""
        mock_load_data.return_value = []
        
        result = get_coin_data('BTCUSDT')
        
        assert result['symbol'] == 'BTCUSDT'
        assert result['current_open_interest'] is None
        assert result['changes'] == {}
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_zero_division(self, mock_load_data):
        """测试除零情况的处理"""
        mock_load_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current': {
                    'openInterest': 150000.0
                },
                'intervals': [
                    {
                        'interval': '1h',
                        'openInterest': 0.0
                    }
                ]
            }
        ]
        
        result = get_coin_data('BTCUSDT')
        
        assert result['changes']['1h'] == 0
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_none_current_interest(self, mock_load_data):
        """测试当前持仓量为None的情况"""
        mock_load_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current': None,
                'intervals': [
                    {
                        'interval': '1h',
                        'openInterest': 130000.0
                    }
                ]
            }
        ]
        
        result = get_coin_data('BTCUSDT')
        
        assert result['current_open_interest'] is None
        # 当前持仓量为None时，变化比例应该为空
        assert result['changes'] == {}
    
    @patch('src.data_processor.load_all_coins_data')
    def test_get_coin_data_none_interval_data(self, mock_load_data):
        """测试时间间隔数据为None的情况"""
        mock_load_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current': {
                    'openInterest': 150000.0
                },
                'intervals': [
                    {
                        'interval': '1h',
                        'openInterest': None
                    }
                ]
            }
        ]
        
        result = get_coin_data('BTCUSDT')
        
        assert result['changes']['1h'] is None
    
    @patch('src.data_processor.get_coin_data')
    def test_get_all_coins_data_success(self, mock_get_coin_data):
        """测试获取所有币种数据"""
        # 模拟get_coin_data的返回值
        mock_get_coin_data.return_value = {
            'symbol': 'BTCUSDT',
            'current_open_interest': 150000.0,
            'changes': {
                '1h': 15.38
            }
        }
        
        result = get_all_coins_data(['BTCUSDT'])
        
        assert len(result) == 1
        assert result[0]['symbol'] == 'BTCUSDT'
        assert result[0]['current_open_interest'] == 150000.0
    
    @patch('src.data_processor.get_coin_data')
    def test_get_all_coins_data_multiple_symbols(self, mock_get_coin_data):
        """测试获取多个币种数据"""
        # 模拟get_coin_data的返回值
        mock_get_coin_data.side_effect = [
            {
                'symbol': 'BTCUSDT',
                'current_open_interest': 150000.0,
                'changes': {
                    '1h': 15.38
                }
            },
            {
                'symbol': 'ETHUSDT',
                'current_open_interest': 80000.0,
                'changes': {
                    '1h': -5.23
                }
            }
        ]
        
        result = get_all_coins_data(['BTCUSDT', 'ETHUSDT'])
        
        assert len(result) == 2
        assert result[0]['symbol'] == 'BTCUSDT'
        assert result[1]['symbol'] == 'ETHUSDT'
    
    @patch('src.data_processor.get_coin_data')
    def test_get_all_coins_data_empty_symbols(self, mock_get_coin_data):
        """测试空币种列表"""
        result = get_all_coins_data([])
        
        assert result == []
        mock_get_coin_data.assert_not_called()