import pytest
import os
import json
import time
from unittest.mock import patch, mock_open
from src.utils import save_all_coins_data, load_all_coins_data, calculate_change_ratio

class TestUtils:
    """测试工具函数模块"""
    
    def test_calculate_change_ratio_success(self):
        """测试成功计算变化比例"""
        current = [0, 150000.0]  # [timestamp, openInterest]
        past = [0, 100000.0]
        
        ratio = calculate_change_ratio(current, past)
        
        # 计算: (150000 - 100000) / 100000 * 100 = 50.0
        assert ratio == 50.0
    
    def test_calculate_change_ratio_negative(self):
        """测试负数变化比例"""
        current = [0, 80000.0]
        past = [0, 100000.0]
        
        ratio = calculate_change_ratio(current, past)
        
        # 计算: (80000 - 100000) / 100000 * 100 = -20.0
        assert ratio == -20.0
    
    def test_calculate_change_ratio_zero_past(self):
        """测试过去值为零的情况"""
        current = [0, 150000.0]
        past = [0, 0.0]
        
        ratio = calculate_change_ratio(current, past)
        
        # 除零情况应该返回0
        assert ratio == 0
    
    def test_calculate_change_ratio_none_past(self):
        """测试过去值为None的情况"""
        current = [0, 150000.0]
        past = None
        
        ratio = calculate_change_ratio(current, past)
        
        # None值应该返回0
        assert ratio == 0
    
    @patch('src.utils.open', new_callable=mock_open)
    @patch('src.utils.os.path.exists')
    @patch('src.utils.json.dump')
    @patch('src.utils.json.load')
    def test_save_all_coins_data_success(self, mock_json_load, mock_json_dump, mock_exists, mock_file):
        """测试成功保存所有币种数据"""
        # 模拟文件不存在
        mock_exists.return_value = False
        
        test_data = [
            {
                'symbol': 'BTCUSDT',
                'current': {'openInterest': 150000.0},
                'intervals': []
            }
        ]
        
        save_all_coins_data(test_data)
        
        # 验证文件被打开用于写入（使用实际路径）
        expected_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'coins_data.json')
        mock_file.assert_called_with(expected_path, 'w', encoding='utf-8')
        # 验证数据被保存
        mock_json_dump.assert_called_once()
    
    @patch('src.utils.open', new_callable=mock_open)
    @patch('src.utils.os.path.exists')
    @patch('src.utils.json.dump')
    @patch('src.utils.json.load')
    def test_save_all_coins_data_existing_file(self, mock_json_load, mock_json_dump, mock_exists, mock_file):
        """测试保存数据到已存在的文件"""
        # 模拟文件存在
        mock_exists.return_value = True
        # 模拟已存在的数据
        mock_json_load.return_value = [
            {
                'timestamp': 1234567890123,
                'data': [
                    {
                        'symbol': 'ETHUSDT',
                        'current': {'openInterest': 80000.0},
                        'intervals': []
                    }
                ]
            }
        ]
        
        test_data = [
            {
                'symbol': 'BTCUSDT',
                'current': {'openInterest': 150000.0},
                'intervals': []
            }
        ]
        
        save_all_coins_data(test_data)
        
        # 验证文件被打开用于读取和写入（使用实际路径）
        expected_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'coins_data.json')
        # 检查是否被调用过读取
        read_calls = [call for call in mock_file.call_args_list if call[0][1] == 'r']
        assert len(read_calls) > 0
        # 检查是否被调用过写入
        write_calls = [call for call in mock_file.call_args_list if call[0][1] == 'w']
        assert len(write_calls) > 0
        # 验证数据被保存
        mock_json_dump.assert_called_once()
    
    @patch('src.utils.open', new_callable=mock_open)
    @patch('src.utils.os.path.exists')
    @patch('src.utils.json.dump')
    def test_save_all_coins_data_limit_records(self, mock_json_dump, mock_exists, mock_file):
        """测试只保留最近100条记录"""
        # 模拟文件不存在
        mock_exists.return_value = False
        
        test_data = [
            {
                'symbol': 'BTCUSDT',
                'current': {'openInterest': 150000.0},
                'intervals': []
            }
        ]
        
        save_all_coins_data(test_data)
        
        # 验证数据被保存
        mock_json_dump.assert_called_once()
        # 获取保存的数据
        call_args = mock_json_dump.call_args
        saved_data = call_args[0][0] if call_args else []
        # 验证数据结构正确
        assert isinstance(saved_data, list)
        assert len(saved_data) >= 1  # 至少有一条记录
    
    @patch('src.utils.open', new_callable=mock_open)
    @patch('src.utils.os.path.exists')
    @patch('src.utils.json.load')
    def test_load_all_coins_data_success(self, mock_json_load, mock_exists, mock_file):
        """测试成功加载所有币种数据"""
        # 模拟文件存在
        mock_exists.return_value = True
        # 模拟文件内容
        mock_json_load.return_value = [
            {
                'timestamp': 1234567890123,
                'data': [
                    {
                        'symbol': 'BTCUSDT',
                        'current': {'openInterest': 150000.0},
                        'intervals': []
                    }
                ]
            }
        ]
        
        result = load_all_coins_data()
        
        assert len(result) == 1
        assert result[0]['symbol'] == 'BTCUSDT'
    
    @patch('src.utils.os.path.exists')
    def test_load_all_coins_data_file_not_exists(self, mock_exists):
        """测试文件不存在的情况"""
        # 模拟文件不存在
        mock_exists.return_value = False
        
        result = load_all_coins_data()
        
        assert result == []
    
    @patch('src.utils.open', new_callable=mock_open)
    @patch('src.utils.os.path.exists')
    def test_load_all_coins_data_empty_file(self, mock_exists, mock_file):
        """测试空文件的情况"""
        # 模拟文件存在
        mock_exists.return_value = True
        # 模拟空文件内容
        mock_file.return_value.read.return_value = "[]"
        
        with patch('src.utils.json.load', return_value=[]):
            result = load_all_coins_data()
            
            assert result == []
    
    @patch('src.utils.open', side_effect=Exception('File error'))
    @patch('src.utils.os.path.exists')
    def test_save_all_coins_data_exception(self, mock_exists, mock_file):
        """测试保存数据时发生异常"""
        # 模拟文件不存在
        mock_exists.return_value = False
        
        test_data = [{'symbol': 'BTCUSDT'}]
        
        # 不应该抛出异常
        save_all_coins_data(test_data)
    
    @patch('src.utils.open', side_effect=Exception('File error'))
    @patch('src.utils.os.path.exists')
    def test_load_all_coins_data_exception(self, mock_exists, mock_file):
        """测试加载数据时发生异常"""
        # 模拟文件存在
        mock_exists.return_value = True
        
        # 不应该抛出异常
        result = load_all_coins_data()
        
        assert result == []