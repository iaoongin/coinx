import requests
import time
from src.config import BINANCE_BASE_URL, USE_PROXY, PROXY_URL

def test_proxy_connection():
    """测试代理连接"""
    print("测试代理连接...")
    print(f"使用代理: {USE_PROXY}")
    print(f"代理地址: {PROXY_URL}")
    
    try:
        # 测试直接连接
        print("\n1. 测试直接连接...")
        start_time = time.time()
        response = requests.get("http://www.baidu.com", timeout=5)
        direct_time = time.time() - start_time
        print(f"   直接连接成功，耗时: {direct_time:.2f}秒")
        
        # 测试代理连接
        if USE_PROXY:
            print("\n2. 测试代理连接...")
            start_time = time.time()
            proxies = {'http': PROXY_URL, 'https': PROXY_URL}
            response = requests.get("http://www.baidu.com", timeout=5, proxies=proxies)
            proxy_time = time.time() - start_time
            print(f"   代理连接成功，耗时: {proxy_time:.2f}秒")
            
            # 比较速度
            if proxy_time < direct_time:
                print("   代理连接更快")
            else:
                print("   直接连接更快")
        
    except Exception as e:
        print(f"连接测试失败: {e}")

def test_binance_api_with_proxy():
    """测试币安API调用"""
    print("\n3. 测试币安API调用...")
    
    try:
        # 测试获取BTCUSDT持仓量
        url = f"{BINANCE_BASE_URL}/fapi/v1/openInterest"
        params = {'symbol': 'BTCUSDT'}
        
        # 直接连接
        print("   测试直接连接...")
        start_time = time.time()
        try:
            response = requests.get(url, params=params, timeout=10)
            direct_time = time.time() - start_time
            direct_data = response.json()
            print(f"   直接获取BTC持仓量成功: {direct_data.get('openInterest')}, 耗时: {direct_time:.2f}秒")
        except Exception as e:
            print(f"   直接连接失败: {e}")
        
        # 代理连接
        if USE_PROXY:
            print("   测试代理连接...")
            start_time = time.time()
            try:
                proxies = {'http': PROXY_URL, 'https': PROXY_URL}
                response = requests.get(url, params=params, timeout=10, proxies=proxies)
                proxy_time = time.time() - start_time
                proxy_data = response.json()
                print(f"   代理获取BTC持仓量成功: {proxy_data.get('openInterest')}, 耗时: {proxy_time:.2f}秒")
            except Exception as e:
                print(f"   代理连接失败: {e}")
            
    except Exception as e:
        print(f"币安API测试失败: {e}")

def test_proxy_only():
    """仅测试代理功能"""
    print("\n4. 仅测试代理功能...")
    
    if USE_PROXY:
        try:
            proxies = {'http': PROXY_URL, 'https': PROXY_URL}
            response = requests.get("https://httpbin.org/ip", timeout=10, proxies=proxies)
            data = response.json()
            print(f"   代理IP信息: {data}")
            print("   代理配置正确!")
        except Exception as e:
            print(f"   代理测试失败: {e}")
    else:
        print("   未启用代理")

if __name__ == "__main__":
    test_proxy_connection()
    test_binance_api_with_proxy()
    test_proxy_only()