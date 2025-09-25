#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import requests

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.coin_manager import get_session, get_all_coins_from_binance
from src.config import BINANCE_BASE_URL, USE_PROXY, PROXY_URL, HTTPS_PROXY_URL
from src.utils import logger


def debug_request():
    """调试网络请求"""
    print("=== 币安API请求调试 ===")

    # 显示配置
    print(f"代理配置: {'启用' if USE_PROXY else '禁用'}")
    if USE_PROXY:
        print(f"代理地址: {PROXY_URL}")
        print(f"HTTPS代理地址: {HTTPS_PROXY_URL}")

    print(f"币安API地址: {BINANCE_BASE_URL}")

    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/exchangeInfo"
        print(f"\n请求URL: {url}")

        # 使用会话
        session = get_session()

        print("\n发送请求...")
        response = session.get(url, timeout=15)  # 增加超时时间

        print(f"响应状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")

        # 检查响应内容类型
        content_type = response.headers.get("content-type", "")
        print(f"内容类型: {content_type}")

        # 显示响应内容的前500个字符
        content_preview = response.text[:500]
        print(f"响应内容预览: {content_preview}")

        # 如果是JSON，尝试解析
        if "application/json" in content_type:
            print("尝试解析JSON...")
            data = response.json()
            print(f"解析成功，数据类型: {type(data)}")
            if isinstance(data, dict) and "symbols" in data:
                print(f"交易对数量: {len(data['symbols'])}")
        else:
            print("响应不是JSON格式")
            if content_preview.strip().startswith("<"):
                print("响应似乎是HTML格式，可能是代理错误页面")

        return response
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"错误响应状态码: {e.response.status_code}")
            print(f"错误响应内容: {e.response.text[:500]}")
        return None
    except Exception as e:
        print(f"其他异常: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_get_all_coins():
    """测试获取所有币种"""
    print("\n=== 测试获取所有币种 ===")
    try:
        result = get_all_coins_from_binance()
        if result is None:
            print("❌ 获取币种列表失败")
        elif len(result) == 0:
            print("⚠️ 未找到USDT交易对")
        else:
            print(f"✅ 成功获取到 {len(result)} 个USDT交易对")
            print(f"前5个交易对: {[coin['symbol'] for coin in result[:5]]}")
        return result
    except Exception as e:
        print(f"❌ 获取币种列表时出错: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("开始调试币安API请求...")

    # 调试请求
    response = debug_request()

    # 测试获取币种
    coins = test_get_all_coins()

    print("\n调试完成。")
