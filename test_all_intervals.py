import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.binance_api import get_open_interest_history

def test_all_intervals():
    """测试所有时间间隔"""
    symbol = 'BTCUSDT'
    intervals = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h']
    
    print("测试所有时间间隔...")
    for interval in intervals:
        print(f"\n测试 {interval}...")
        try:
            data = get_open_interest_history(symbol, interval)
            if data:
                print(f"   成功: {data['openInterest']}")
            else:
                print("   无数据返回")
        except Exception as e:
            print(f"   失败: {e}")

if __name__ == "__main__":
    test_all_intervals()