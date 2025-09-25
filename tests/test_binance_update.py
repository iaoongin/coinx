#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试从币安更新功能
"""

import sys
import os
import requests
import json

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)


def test_update_from_binance():
    """测试从币安更新功能"""
    print("测试从币安更新功能...")

    try:
        # 发送POST请求到更新接口
        response = requests.post(
            "http://localhost:5000/api/coins-config/update-from-binance"
        )

        print(f"响应状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")

        # 尝试解析JSON响应
        try:
            data = response.json()
            print(f"响应数据: {json.dumps(data, indent=2, ensure_ascii=False)}")

            if data.get("status") == "success":
                print("✓ 从币安更新功能正常工作")
                return True
            else:
                print("✗ 从币安更新功能出现问题")
                return False
        except json.JSONDecodeError:
            print(f"响应内容不是JSON格式: {response.text}")
            return False

    except requests.exceptions.ConnectionError:
        print("✗ 无法连接到服务器，请确保Flask应用正在运行")
        return False
    except Exception as e:
        print(f"✗ 测试过程中出现错误: {e}")
        return False


if __name__ == "__main__":
    test_update_from_binance()
