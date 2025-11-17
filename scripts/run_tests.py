#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试运行脚本
用于批量执行所有测试文件
"""

import os
import sys
import subprocess
from pathlib import Path

def run_test(test_file):
    """运行单个测试文件"""
    print(f"\n{'='*60}")
    print(f"运行测试: {test_file}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=False,
            text=True,
            cwd=os.getcwd()
        )
        
        if result.returncode == 0:
            print(f"✅ {test_file} 测试通过")
            return True
        else:
            print(f"❌ {test_file} 测试失败 (退出码: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"❌ {test_file} 测试异常: {e}")
        return False

def main():
    """主函数"""
    print("开始运行所有测试...")
    
    # 切换到项目根目录
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)
    
    # 测试文件列表
    test_files = [
        "test/test_system.py",
        "test/test_stock_list_simple.py",
        "test/test_stock_list.py",
        "test/test_api.py",
        "test/test_api_get_stock_list.py",
        "test/test_clean_db.py",
        "test/test_jqdata_stocks.py",
        "test/test_real_stocks.py",
        "test/test_real_bj_stocks.py",
        "test/test_sz_sh_stocks.py"
    ]
    
    results = []
    
    for test_file in test_files:
        if os.path.exists(test_file):
            success = run_test(test_file)
            results.append((test_file, success))
        else:
            print(f"⚠️ 测试文件不存在: {test_file}")
            results.append((test_file, False))
    
    # 输出汇总结果
    print(f"\n{'='*60}")
    print("测试结果汇总")
    print(f"{'='*60}")
    
    passed = 0
    for test_file, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{test_file}: {status}")
        if success:
            passed += 1
    
    print(f"\n总计: {passed}/{len(results)} 项测试通过")
    
    if passed == len(results):
        print("🎉 所有测试通过！")
        return 0
    else:
        print("⚠️ 部分测试失败")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)