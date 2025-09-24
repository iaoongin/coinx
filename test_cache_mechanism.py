#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
from datetime import datetime

# 添加项目根目录到路径
project_root = os.path.dirname(__file__)
sys.path.insert(0, project_root)

from src.binance_api import update_all_data, load_cached_data, get_cache_key
from src.coin_manager import get_all_coins_list

def test_cache_mechanism():
    """测试缓存机制"""
    print("开始测试缓存机制...")
    
    # 获取当前缓存键
    cache_key = get_cache_key()
    print(f"当前缓存键: {cache_key} ({datetime.fromtimestamp(cache_key)})")
    
    # 查看现有缓存
    cache_data = load_cached_data()
    print(f"现有缓存条目数: {len(cache_data)}")
    
    # 获取所有币种列表
    all_coins = get_all_coins_list()
    print(f"所有币种数量: {len(all_coins)}")
    print(f"前10个币种: {all_coins[:10]}")
    
    # 强制更新数据（忽略缓存）
    print("\n开始强制更新数据...")
    start_time = time.time()
    result = update_all_data(force_update=True)
    end_time = time.time()
    
    print(f"数据更新完成，耗时: {end_time - start_time:.2f}秒")
    print(f"更新结果: {'成功' if result else '失败'}")
    
    if result:
        print(f"更新的币种数量: {len(result)}")
        print(f"第一个币种: {result[0]['symbol'] if result else '无'}")
    
    # 再次查看缓存
    cache_data = load_cached_data()
    print(f"\n更新后缓存条目数: {len(cache_data)}")
    
    # 测试缓存命中（不应该更新数据）
    print("\n测试缓存命中...")
    start_time = time.time()
    result = update_all_data()
    end_time = time.time()
    
    print(f"缓存命中测试完成，耗时: {end_time - start_time:.2f}秒")
    print(f"更新结果: {'成功' if result else '失败（使用缓存）'}")
    
    print("\n测试完成!")

if __name__ == "__main__":
    test_cache_mechanism()