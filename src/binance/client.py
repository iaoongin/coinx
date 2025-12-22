import requests
import time
from src.config import USE_PROXY, PROXY_URL, HTTPS_PROXY_URL
from src.utils import logger

# 创建一个全局会话对象，用于复用连接
_global_session = None

def get_session():
    """创建带代理配置的会话"""
    global _global_session
    if _global_session is None:
        _global_session = requests.Session()
        # 合理的默认请求头，降低被风控概率
        _global_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive'
        })
        
        if USE_PROXY:
            proxies = {
                'http': PROXY_URL,
                'https': HTTPS_PROXY_URL
            }
            _global_session.proxies.update(proxies)
            logger.info(f"使用代理: {PROXY_URL}")
    
    return _global_session

def request_with_retry(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
    """带指数退避的请求封装，处理 403/429/超时等情况"""
    attempt = 0
    while True:
        try:
            response = session.get(url, params=params, timeout=timeout)
            # 对于 429/403 主动做退避重试
            if response.status_code in (403, 429):
                raise requests.exceptions.HTTPError(f"{response.status_code} {response.reason}")
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            attempt += 1
            if attempt > max_retries:
                raise e
            delay = base_delay * (2 ** (attempt - 1))
            # 上限1.5秒左右，避免过长阻塞
            if delay > 1.5:
                delay = 1.5
            logger.warning(f"请求失败，将在 {delay:.2f}s 后重试（第{attempt}/{max_retries}次）: {url}, 错误: {e}")
            time.sleep(delay)
