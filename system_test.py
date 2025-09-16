import os
import sys
import time
import json

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.binance_api import update_all_data
from src.data_processor import get_all_coins_data
from src.utils import save_all_coins_data
from web.app import app

def test_data_acquisition():
    """测试数据获取功能"""
    print("=== 测试数据获取功能 ===")
    try:
        data = update_all_data(['BTCUSDT'])
        print(f"获取到的数据: {len(data) if data else 0} 个币种")
        return len(data) > 0
    except Exception as e:
        print(f"数据获取测试失败: {e}")
        return False

def test_data_persistence():
    """测试数据持久化功能"""
    print("\n=== 测试数据持久化功能 ===")
    try:
        # 创建测试数据
        test_data = [{
            'symbol': 'TEST',
            'current': {
                'timestamp': int(time.time() * 1000),
                'symbol': 'TEST',
                'openInterest': 150000.0,
                'time': int(time.time() * 1000)
            },
            'intervals': [],
            'update_time': int(time.time() * 1000)
        }]
        
        save_all_coins_data(test_data)
        
        # 检查文件是否存在
        data_file = os.path.join('data', 'coins_data.json')
        if os.path.exists(data_file):
            print("数据持久化成功")
            return True
        else:
            print("数据持久化失败")
            return False
    except Exception as e:
        print(f"数据持久化测试失败: {e}")
        return False

def test_data_processing():
    """测试数据处理功能"""
    print("\n=== 测试数据处理功能 ===")
    try:
        coins_data = get_all_coins_data(['BTCUSDT'])
        print(f"处理后的数据: {coins_data[0] if coins_data else '无数据'}")
        return len(coins_data) > 0
    except Exception as e:
        print(f"数据处理测试失败: {e}")
        return False

def test_web_api():
    """测试Web API功能"""
    print("\n=== 测试Web API功能 ===")
    try:
        with app.test_client() as c:
            # 测试获取所有币种数据
            response = c.get('/api/coins')
            status_code = response.status_code
            data = response.get_json()
            
            print(f"API状态码: {status_code}")
            print(f"API返回数据: {str(data)[:100]}...")
            
            # 测试过滤功能
            response = c.get('/api/coins?symbol=BTC')
            filtered_data = response.get_json()
            print(f"过滤BTC后的数据: {str(filtered_data)[:100]}...")
            
            return status_code == 200
    except Exception as e:
        print(f"Web API测试失败: {e}")
        return False

def test_manual_update():
    """测试手动更新功能"""
    print("\n=== 测试手动更新功能 ===")
    try:
        with app.test_client() as c:
            response = c.get('/api/update')
            status_code = response.status_code
            data = response.get_json()
            
            print(f"手动更新API状态码: {status_code}")
            print(f"手动更新返回数据: {data}")
            
            return status_code == 200
    except Exception as e:
        print(f"手动更新测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("币种数据监控系统测试")
    print("=" * 50)
    
    tests = [
        test_data_acquisition,
        test_data_persistence,
        test_data_processing,
        test_web_api,
        test_manual_update
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
                print("✓ 测试通过")
            else:
                print("✗ 测试失败")
        except Exception as e:
            print(f"✗ 测试执行出错: {e}")
        print()
    
    print("=" * 50)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过，系统运行正常！")
    else:
        print("⚠️  部分测试未通过，请检查系统配置。")

if __name__ == "__main__":
    main()