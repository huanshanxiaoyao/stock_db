#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行JQDataSource单元测试的脚本
"""

import sys
import os
import unittest

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 确保tests目录存在
tests_dir = os.path.join(project_root, 'tests')
if not os.path.exists(tests_dir):
    os.makedirs(tests_dir)
    print(f"创建测试目录: {tests_dir}")

# 创建__init__.py文件
init_file = os.path.join(tests_dir, '__init__.py')
if not os.path.exists(init_file):
    with open(init_file, 'w') as f:
        f.write('# Tests package\n')
    print(f"创建__init__.py: {init_file}")

def run_jqdata_tests():
    """运行JQDataSource相关测试"""
    print("开始运行JQDataSource单元测试...")
    print("=" * 50)
    
    try:
        # 导入测试模块
        from tests.test_jqdata import TestJQDataSource
        
        # 创建测试套件
        suite = unittest.TestLoader().loadTestsFromTestCase(TestJQDataSource)
        
        # 运行测试
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        # 输出结果摘要
        print("\n" + "=" * 50)
        print(f"测试运行完成!")
        print(f"总测试数: {result.testsRun}")
        print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
        print(f"失败: {len(result.failures)}")
        print(f"错误: {len(result.errors)}")
        
        if result.failures:
            print("\n失败的测试:")
            for test, traceback in result.failures:
                print(f"- {test}: {traceback.split('AssertionError:')[-1].strip()}")
        
        if result.errors:
            print("\n错误的测试:")
            for test, traceback in result.errors:
                print(f"- {test}: {traceback.split('Exception:')[-1].strip()}")
        
        return result.wasSuccessful()
        
    except ImportError as e:
        print(f"导入测试模块失败: {e}")
        print("请确保所有依赖都已正确安装")
        return False
    except Exception as e:
        print(f"运行测试时发生错误: {e}")
        return False

def main():
    """主函数"""
    print("JQDataSource单元测试运行器")
    print(f"项目根目录: {project_root}")
    print(f"Python版本: {sys.version}")
    print()
    
    success = run_jqdata_tests()
    
    if success:
        print("\n✅ 所有测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败")
        sys.exit(1)

if __name__ == '__main__':
    main()