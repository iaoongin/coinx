#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试数据获取功能
"""

import sys
import os
import json
import time

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)


def test_data_fetch():
    """测试数据获取功能"""
    print("测试数据获取功能...")

    try:
        # 导入相关模块
        from src.binance_api import update_all_data
        from src.coin_manager import get_active_coins

        # 获取活跃币种
        active_coins = get_active_coins()
        print(f"活跃币种数量: {len(active_coins)}")
        print(f"前5个活跃币种: {active_coins[:5]}")

        if len(active_coins) == 0:
            print("没有活跃币种，尝试从币安获取默认币种列表...")
            from src.binance_api import get_all_coins_list

            all_coins = get_all_coins_list()
            print(f"从币安获取到 {len(all_coins)} 个币种")
            if len(all_coins) > 0:
                # 使用前5个币种进行测试
                test_coins = all_coins[:5]
                print(f"使用前5个币种进行测试: {test_coins}")

                # 更新数据
                print("开始更新数据...")
                result = update_all_data(symbols=test_coins, force_update=True)
                print(f"数据更新结果: {result is not None}")

                if result:
                    print(f"成功更新 {len(result)} 个币种的数据")
                    # 检查数据目录
                    data_dir = os.path.join(project_root, "data")
                    if os.path.exists(data_dir):
                        files = os.listdir(data_dir)
                        print(f"数据目录文件: {files}")

                        # 检查缓存文件
                        cache_file = os.path.join(data_dir, "open_interest_cache.json")
                        if os.path.exists(cache_file):
                            with open(cache_file, "r", encoding="utf-8") as f:
                                cache_data = json.load(f)
                                print(f"缓存数据键数量: {len(cache_data)}")
                                if cache_data:
                                    first_key = list(cache_data.keys())[0]
                                    print(
                                        f"缓存示例数据: {first_key} -> {type(cache_data[first_key])}"
                                    )
                        else:
                            print("缓存文件不存在")
                    else:
                        print("数据目录不存在")
                else:
                    print("数据更新失败")
            else:
                print("无法从币安获取币种列表")
        else:
            print("有活跃币种，直接更新数据...")
            # 更新数据
            print("开始更新数据...")
            result = update_all_data(force_update=True)
            print(f"数据更新结果: {result is not None}")

    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_data_fetch()
