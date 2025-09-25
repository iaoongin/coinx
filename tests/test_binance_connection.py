#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 重新加载配置模块以确保获取最新的环境变量
if 'src.config' in sys.modules:
    del sys.modules['src.config']

from src.coin_manager import update_coins_config, get_all_coins_from_binance
from src.config import USE_PROXY, PROXY_URL
from src.utils import logger

def test_binance_connection():
    """测试与币安的连接"""
    print("测试与币安的连接...")
    
    # 显示代理配置
    env_proxy = os.getenv('USE_PROXY', '未设置')
    print(f"环境变量 USE_PROXY: {env_proxy}")
    print(f"当前代理配置: {'启用' if USE_PROXY else '禁用'}")
    if USE_PROXY:
        print(f"代理地址: {PROXY_URL}")
    
    try:
        # 测试获取所有币种列表
        print("1. 测试获取币种列表...")
        all_coins = get_all_coins_from_binance()
        if all_coins is None:
            print("❌ 从币安获取交易对列表失败")
            return False
        elif len(all_coins) == 0:
            print("⚠️ 未找到符合条件的USDT交易对")
            return True
        else:
            print(f"✅ 成功获取到 {len(all_coins)} 个USDT交易对")
            print(f"前5个交易对: {[coin['symbol'] for coin in all_coins[:5]]}")
            return True
    except Exception as e:
        print(f"❌ 获取币种列表时出错: {e}")
        return False

def test_update_coins_config():
    """测试更新币种配置"""
    print("\n2. 测试更新币种配置...")
    
    try:
        success = update_coins_config()
        if success:
            print("✅ 币种配置更新成功")
            return True
        else:
            print("❌ 币种配置更新失败")
            return False
    except Exception as e:
        print(f"❌ 更新币种配置时出错: {e}")
        return False

if __name__ == "__main__":
    print("=== 币安更新功能测试 ===")
    
    # 测试连接
    connection_ok = test_binance_connection()
    
    if connection_ok:
        # 测试更新功能
        update_ok = test_update_coins_config()
        
        if update_ok:
            print("\n🎉 所有测试通过！")
        else:
            print("\n💥 更新功能测试失败！")
    else:
        print("\n💥 连接测试失败！请检查网络连接和代理设置。")