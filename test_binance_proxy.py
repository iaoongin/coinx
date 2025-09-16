import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.binance_api import get_open_interest, get_open_interest_history

def test_binance_with_proxy():
    """测试币安API通过代理访问"""
    print("测试币安API通过代理访问...")
    
    # 测试获取BTCUSDT当前持仓量
    print("\n1. 测试获取BTCUSDT当前持仓量...")
    try:
        current_data = get_open_interest('BTCUSDT')
        if current_data:
            print(f"   成功获取当前持仓量: {current_data}")
        else:
            print("   未能获取当前持仓量数据")
    except Exception as e:
        print(f"   获取当前持仓量失败: {e}")
    
    # 测试获取BTCUSDT历史持仓量
    print("\n2. 测试获取BTCUSDT历史持仓量(5m)...")
    try:
        history_data = get_open_interest_history('BTCUSDT', '5m')
        if history_data:
            print(f"   成功获取历史持仓量: {history_data}")
        else:
            print("   未能获取历史持仓量数据")
    except Exception as e:
        print(f"   获取历史持仓量失败: {e}")

if __name__ == "__main__":
    test_binance_with_proxy()