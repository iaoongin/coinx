#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.coin_manager import (
    update_coins_config,
    load_coins_config_dict,
    get_all_coins_from_binance,
)
from src.config import DATA_DIR


def test_get_all_coins():
    """测试获取所有币种"""
    print("=== 测试获取所有币种 ===")
    try:
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
        import traceback

        traceback.print_exc()
        return False


def test_update_coins_config():
    """测试更新币种配置"""
    print("\n=== 测试更新币种配置 ===")
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
        import traceback

        traceback.print_exc()
        return False


def check_config_file():
    """检查配置文件内容"""
    print("\n=== 检查配置文件 ===")
    config_file = os.path.join(DATA_DIR, "coins_config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            print(f"配置文件存在，包含 {len(config.get('coins', {}))} 个币种")
            print("配置文件内容预览:")
            # 只显示前10个币种
            coins = config.get("coins", {})
            for i, (symbol, tracked) in enumerate(coins.items()):
                if i >= 10:
                    print(f"... 还有 {len(coins) - 10} 个币种")
                    break
                print(f"  {symbol}: {'跟踪' if tracked else '不跟踪'}")
            return True
        except Exception as e:
            print(f"读取配置文件时出错: {e}")
            return False
    else:
        print("配置文件不存在")
        return False


if __name__ == "__main__":
    print("开始测试币种更新功能...")

    # 1. 测试获取所有币种
    get_success = test_get_all_coins()

    # 2. 测试更新币种配置
    if get_success:
        update_success = test_update_coins_config()

        # 3. 检查配置文件
        check_config_file()

        if update_success:
            print("\n🎉 所有测试通过！")
        else:
            print("\n💥 更新功能测试失败！")
    else:
        print("\n💥 获取币种测试失败！")
