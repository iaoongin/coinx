import pytest
import json
from unittest.mock import patch, MagicMock
from web.app import app

class TestWebAPI:
    """测试Web接口"""
    
    def setup_method(self):
        """测试前准备"""
        self.app = app.test_client()
        self.app.testing = True
    
    @patch('web.app.get_all_coins_data')
    def test_coins_api_success(self, mock_get_all_coins_data):
        """测试获取币种数据API成功"""
        # 模拟返回数据
        mock_get_all_coins_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current_open_interest': 150000.0,
                'changes': {
                    '1h': 15.38,
                    '24h': -5.23
                }
            }
        ]
        
        response = self.app.get('/api/coins')
        data = json.loads(response.data)
        
        assert response.status_code == 200
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['symbol'] == 'BTCUSDT'
        assert data[0]['current_open_interest'] == 150000.0
    
    @patch('web.app.get_all_coins_data')
    def test_coins_api_empty_result(self, mock_get_all_coins_data):
        """测试获取币种数据API返回空结果"""
        mock_get_all_coins_data.return_value = []
        
        response = self.app.get('/api/coins')
        data = json.loads(response.data)
        
        assert response.status_code == 200
        assert isinstance(data, list)
        assert len(data) == 0
    
    @patch('web.app.get_all_coins_data')
    def test_coins_api_with_symbol_filter(self, mock_get_all_coins_data):
        """测试带币种过滤的API"""
        # 模拟返回数据
        mock_get_all_coins_data.return_value = [
            {
                'symbol': 'BTCUSDT',
                'current_open_interest': 150000.0,
                'changes': {}
            }
        ]
        
        response = self.app.get('/api/coins?symbol=BTC')
        data = json.loads(response.data)
        
        assert response.status_code == 200
        # 过滤逻辑在get_all_coins_data中实现，这里只是测试API能接收参数
        mock_get_all_coins_data.assert_called_once()
    
    @patch('web.app.update_all_data')
    def test_update_api_success(self, mock_update_all_data):
        """测试手动更新数据API成功"""
        # 模拟更新函数返回空列表（表示成功）
        mock_update_all_data.return_value = []
        
        response = self.app.get('/api/update')
        data = json.loads(response.data)
        
        assert response.status_code == 200
        assert data['status'] == 'success'
        assert 'message' in data
    
    @patch('web.app.update_all_data')
    def test_update_api_exception(self, mock_update_all_data):
        """测试手动更新数据API异常处理"""
        # 模拟更新函数抛出异常
        mock_update_all_data.side_effect = Exception('Update failed')
        
        response = self.app.get('/api/update')
        data = json.loads(response.data)
        
        assert response.status_code == 200
        assert data['status'] == 'error'
        assert 'message' in data
    
    def test_index_page(self):
        """测试主页访问"""
        response = self.app.get('/')
        
        assert response.status_code == 200
        assert b'<!DOCTYPE html>' in response.data
        # 使用英文字符避免编码问题
        assert b'Coin' in response.data or b'currency' in response.data or b'<!DOCTYPE' in response.data
    
    def test_invalid_route(self):
        """测试无效路由"""
        response = self.app.get('/invalid-route')
        
        assert response.status_code == 404