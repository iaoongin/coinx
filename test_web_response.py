#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import requests

# 添加项目根目录到路径
project_root = os.path.dirname(__file__)
sys.path.insert(0, project_root)

def test_web_response():
    """测试Web响应速度"""
    print("测试Web响应速度...")
    
    # 测试获取币种数据的响应时间
    start_time = time.time()
    try:
        response = requests.get('http://localhost:5000/api/coins', timeout=5)
        end_time = time.time()
        
        print(f"HTTP状态码: {response.status_code}")
        print(f"响应时间: {end_time - start_time:.2f}秒")
        
        if response.status_code == 200:
            data = response.json()
            print(f"成功获取数据: {data.get('success', False)}")
            if data.get('success', False):
                coins_data = data.get('data', [])
                print(f"获取到 {len(coins_data)} 个币种的数据")
                if coins_data:
                    print(f"第一个币种: {coins_data[0].get('symbol', '未知')}")
            else:
                print(f"错误信息: {data.get('error', '未知错误')}")
        else:
            print(f"请求失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        end_time = time.time()
        print(f"请求失败: {e}")
        print(f"耗时: {end_time - start_time:.2f}秒")
    except Exception as e:
        end_time = time.time()
        print(f"发生错误: {e}")
        print(f"耗时: {end_time - start_time:.2f}秒")

def test_manual_update():
    """测试手动触发更新"""
    print("\n测试手动触发更新...")
    
    start_time = time.time()
    try:
        response = requests.get('http://localhost:5000/api/update', timeout=5)
        end_time = time.time()
        
        print(f"HTTP状态码: {response.status_code}")
        print(f"响应时间: {end_time - start_time:.2f}秒")
        
        if response.status_code == 200:
            data = response.json()
            print(f"成功触发更新: {data.get('success', False)}")
            print(f"消息: {data.get('message', '无消息')}")
        else:
            print(f"请求失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        end_time = time.time()
        print(f"请求失败: {e}")
        print(f"耗时: {end_time - start_time:.2f}秒")
    except Exception as e:
        end_time = time.time()
        print(f"发生错误: {e}")
        print(f"耗时: {end_time - start_time:.2f}秒")

if __name__ == "__main__":
    test_web_response()
    test_manual_update()