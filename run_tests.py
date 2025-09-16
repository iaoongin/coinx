#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试运行脚本
用于运行所有单元测试并生成覆盖率报告
"""

import subprocess
import sys
import os

def run_tests():
    """运行所有测试"""
    print("开始运行单元测试...")
    
    # 切换到项目根目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    try:
        # 运行测试并生成覆盖率报告
        cmd = [
            sys.executable, "-m", "pytest", 
            "tests/", 
            "--cov=src", 
            "--cov-report=html",
            "--cov-report=term",
            "-v"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print("✅ 所有测试通过!")
            print(f"📊 覆盖率报告已生成在 {os.path.join(project_root, 'htmlcov')} 目录中")
            print("   可以打开 htmlcov/index.html 查看详细报告")
        else:
            print("❌ 测试失败!")
            return False
            
    except Exception as e:
        print(f"运行测试时出错: {e}")
        return False
    
    return True

def run_tests_without_coverage():
    """运行测试但不生成覆盖率报告"""
    print("开始运行单元测试(无覆盖率)...")
    
    # 切换到项目根目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    try:
        # 运行测试
        cmd = [sys.executable, "-m", "pytest", "tests/", "-v"]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
            
    except Exception as e:
        print(f"运行测试时出错: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--no-cov":
        success = run_tests_without_coverage()
    else:
        success = run_tests()
    
    sys.exit(0 if success else 1)