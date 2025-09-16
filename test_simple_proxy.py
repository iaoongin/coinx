import requests
from src.config import USE_PROXY, PROXY_URL, HTTPS_PROXY_URL

def test_simple_proxy():
    """简单测试代理"""
    print("简单代理测试...")
    print(f"使用代理: {USE_PROXY}")
    print(f"HTTP代理: {PROXY_URL}")
    print(f"HTTPS代理: {HTTPS_PROXY_URL}")
    
    if not USE_PROXY:
        print("未启用代理")
        return
    
    try:
        # 测试HTTP
        print("\n1. 测试HTTP代理...")
        proxies = {
            'http': PROXY_URL,
            'https': HTTPS_PROXY_URL
        }
        
        response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=10)
        print(f"   HTTP代理成功: {response.json()}")
        
        # 测试HTTPS
        print("\n2. 测试HTTPS代理...")
        response = requests.get('https://httpbin.org/ip', proxies=proxies, timeout=10)
        print(f"   HTTPS代理成功: {response.json()}")
        
        print("\n✅ 代理测试成功!")
        
    except Exception as e:
        print(f"❌ 代理测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_proxy()