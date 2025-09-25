#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API端点功能测试脚本
测试所有API端点的基本功能和响应格式
"""

import requests
import json
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# API服务器配置
API_BASE_URL = "http://localhost:5001"

class APIEndpointTester:
    """API端点测试类"""

    def __init__(self, base_url=API_BASE_URL):
        self.base_url = base_url
        self.results = []

    def test_endpoint(self, name, method, path, **kwargs):
        """测试单个端点"""
        url = f"{self.base_url}{path}"
        print(f"\n测试: {name}")
        print(f"  URL: {method} {url}")

        try:
            response = requests.request(method, url, **kwargs)
            print(f"  状态码: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                if 'success' in data:
                    if data['success']:
                        print(f"  ✅ 成功: {data.get('message', '响应正常')}")
                        if 'data' in data:
                            print(f"  数据条数: {len(data['data']) if isinstance(data['data'], list) else 1}")
                    else:
                        print(f"  ❌ 失败: {data.get('error', '未知错误')}")
                else:
                    print(f"  ✅ 响应正常")
                self.results.append((name, True, response.status_code))
            else:
                print(f"  ❌ HTTP错误: {response.status_code}")
                self.results.append((name, False, response.status_code))

            return response

        except requests.exceptions.ConnectionError:
            print(f"  ❌ 连接失败: API服务器未启动")
            self.results.append((name, False, 0))
            return None
        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.results.append((name, False, -1))
            return None

    def run_all_tests(self):
        """运行所有测试"""
        print("=" * 60)
        print("API端点功能测试")
        print("=" * 60)

        # 1. 健康检查
        self.test_endpoint("健康检查", "GET", "/health")

        # 2. API信息
        self.test_endpoint("API信息", "GET", "/api/v1/info")

        # 3. 股票列表
        self.test_endpoint("获取股票列表", "GET", "/api/v1/stocks?limit=10")
        self.test_endpoint("按交易所筛选", "GET", "/api/v1/stocks?exchange=XSHG&limit=5")
        self.test_endpoint("按市场筛选", "GET", "/api/v1/stocks?market=main&limit=5")

        # 4. 股票信息
        self.test_endpoint("获取股票信息", "GET", "/api/v1/stocks/000001.SZ")

        # 5. 价格数据
        end_date = date.today().strftime('%Y-%m-%d')
        start_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.test_endpoint("获取价格数据", "GET",
                          f"/api/v1/stocks/000001.SZ/price?start_date={start_date}&end_date={end_date}")

        # 6. 批量价格数据
        self.test_endpoint("批量获取价格(POST)", "POST", "/api/v1/stocks/batch/price",
                          json={
                              "codes": ["000001.SZ", "000002.SZ"],
                              "start_date": start_date,
                              "end_date": end_date
                          })

        self.test_endpoint("批量获取价格(新接口)", "POST", "/api/v1/stocks/batch/prices",
                          json={
                              "codes": ["000001.SZ", "000002.SZ"],
                              "start_date": start_date,
                              "end_date": end_date,
                              "fields": ["open", "close", "volume"]
                          })

        # 7. 财务数据
        self.test_endpoint("获取财务数据", "GET", "/api/v1/stocks/000001.SZ/financial")

        # 8. 数据库信息
        self.test_endpoint("获取数据库信息", "GET", "/api/v1/database/info")

        # 9. 自定义查询
        self.test_endpoint("执行SQL查询", "POST", "/api/v1/database/query",
                          json={
                              "sql": "SELECT code, name FROM stock_list LIMIT 5"
                          })

        # 10. 股票筛选
        self.test_endpoint("股票筛选", "POST", "/api/v1/analysis/screen",
                          json={
                              "market_cap": {"min": 1000000000}
                          })

        # 11. 排行榜
        self.test_endpoint("市值排行榜", "GET", "/api/v1/analysis/ranking?metric=market_cap&limit=10")

        # 12. 用户持仓
        self.test_endpoint("获取用户持仓", "GET", "/api/v1/positions?user_id=test_user&limit=10")

        # 13. 账户信息
        self.test_endpoint("获取账户信息", "GET", "/api/v1/accounts?user_id=test_user")

        # 14. 持仓汇总
        self.test_endpoint("获取持仓汇总", "GET", "/api/v1/positions/summary?user_id=test_user")

        # 输出测试结果汇总
        self.print_summary()

    def print_summary(self):
        """打印测试结果汇总"""
        print("\n" + "=" * 60)
        print("测试结果汇总")
        print("=" * 60)

        passed = sum(1 for _, success, _ in self.results if success)
        total = len(self.results)

        for name, success, code in self.results:
            status = "✅ 通过" if success else "❌ 失败"
            code_str = f"({code})" if code > 0 else ""
            print(f"{status} {name} {code_str}")

        print(f"\n总计: {passed}/{total} 项测试通过")

        if passed == total:
            print("🎉 所有端点测试通过！")
        else:
            print("⚠️ 部分端点测试失败")

def check_api_server():
    """检查API服务器是否运行"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def main():
    """主函数"""
    # 检查API服务器
    if not check_api_server():
        print("❌ API服务器未启动")
        print("请先运行: python start_api.py")
        return 1

    print("✅ API服务器已启动")

    # 运行测试
    tester = APIEndpointTester()
    tester.run_all_tests()

    # 返回状态码
    passed = sum(1 for _, success, _ in tester.results if success)
    return 0 if passed == len(tester.results) else 1

if __name__ == '__main__':
    sys.exit(main())