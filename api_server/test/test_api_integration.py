#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API集成测试
全面测试API的功能性、性能和稳定性
"""

import unittest
import requests
import json
from datetime import date, timedelta
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class TestStockDataAPI(unittest.TestCase):
    """股票数据API集成测试"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls.base_url = "http://localhost:5001/api/v1"
        cls.test_code = "000001.SZ"
        cls.test_codes = ["000001.SZ", "000002.SZ", "000858.SZ"]

        # 检查API服务器是否运行
        try:
            response = requests.get(f"http://localhost:5001/health", timeout=2)
            if response.status_code != 200:
                raise Exception("API服务器未正常运行")
        except Exception as e:
            print(f"错误: API服务器未启动。请运行: python start_api.py --port 5001")
            sys.exit(1)

    def test_01_health_check(self):
        """测试健康检查接口"""
        response = requests.get(f"http://localhost:5001/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("status", data)
        self.assertEqual(data["status"], "healthy")

    def test_02_api_info(self):
        """测试API信息接口"""
        response = requests.get(f"{self.base_url}/info")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("name", data)
        self.assertIn("version", data)
        self.assertIn("endpoints", data)

    def test_03_get_stock_list(self):
        """测试获取股票列表"""
        response = requests.get(f"{self.base_url}/stocks", params={"limit": 10})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        self.assertIn("pagination", data)
        self.assertLessEqual(len(data["data"]), 10)

    def test_04_get_stock_info(self):
        """测试获取股票信息"""
        response = requests.get(f"{self.base_url}/stocks/{self.test_code}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        stock_info = data["data"]
        self.assertEqual(stock_info["code"], self.test_code)

    def test_05_get_price_data(self):
        """测试获取价格数据"""
        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        response = requests.get(
            f"{self.base_url}/stocks/{self.test_code}/price",
            params={
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            }
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        self.assertIn("count", data)
        self.assertGreater(data["count"], 0)

    def test_06_batch_price_data(self):
        """测试批量获取价格数据"""
        end_date = date.today()
        start_date = end_date - timedelta(days=10)

        # 测试新接口
        response = requests.post(
            f"{self.base_url}/stocks/batch/prices",
            json={
                "codes": self.test_codes,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "fields": ["open", "close", "volume"]
            }
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        self.assertIn("count", data)

        # 验证返回数据包含所有请求字段
        if data["count"] > 0:
            first_item = data["data"][0]
            self.assertIn("code", first_item)
            self.assertIn("date", first_item)
            self.assertIn("open", first_item)
            self.assertIn("close", first_item)
            self.assertIn("volume", first_item)

    def test_07_database_info(self):
        """测试数据库信息接口"""
        response = requests.get(f"{self.base_url}/database/info")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        db_info = data["data"]
        self.assertIn("tables", db_info)

    def test_08_custom_query(self):
        """测试自定义SQL查询"""
        response = requests.post(
            f"{self.base_url}/database/query",
            json={
                "sql": "SELECT COUNT(*) as count FROM price_data WHERE code = ?",
                "params": [self.test_code]
            }
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)

    def test_09_stock_screen(self):
        """测试股票筛选"""
        response = requests.post(
            f"{self.base_url}/analysis/screen",
            json={
                "market_cap_min": 1000000000
            }
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)

    def test_10_ranking(self):
        """测试排行榜"""
        response = requests.get(
            f"{self.base_url}/analysis/ranking",
            params={
                "metric": "market_cap",
                "order": "desc",
                "limit": 10
            }
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("data", data)
        self.assertLessEqual(len(data["data"]), 10)

    def test_11_error_handling(self):
        """测试错误处理"""
        # 测试不存在的股票
        response = requests.get(f"{self.base_url}/stocks/INVALID_CODE")
        self.assertEqual(response.status_code, 404)
        data = response.json()
        self.assertFalse(data["success"])
        self.assertIn("error", data)

        # 测试无效的日期格式
        response = requests.get(
            f"{self.base_url}/stocks/{self.test_code}/price",
            params={"start_date": "invalid_date"}
        )
        self.assertEqual(response.status_code, 400)

        # 测试非SELECT查询
        response = requests.post(
            f"{self.base_url}/database/query",
            json={"sql": "DELETE FROM price_data"}
        )
        self.assertEqual(response.status_code, 400)

    def test_12_performance(self):
        """测试性能"""
        # 测试单个请求响应时间
        start_time = time.time()
        response = requests.get(f"{self.base_url}/stocks/{self.test_code}/price")
        end_time = time.time()
        response_time = end_time - start_time

        self.assertEqual(response.status_code, 200)
        self.assertLess(response_time, 2.0, "响应时间超过2秒")

        # 测试批量请求性能
        start_time = time.time()
        response = requests.post(
            f"{self.base_url}/stocks/batch/prices",
            json={
                "codes": self.test_codes[:10],
                "start_date": "2025-09-01",
                "end_date": "2025-09-19"
            }
        )
        end_time = time.time()
        batch_response_time = end_time - start_time

        self.assertEqual(response.status_code, 200)
        self.assertLess(batch_response_time, 5.0, "批量请求响应时间超过5秒")

    def test_13_pagination(self):
        """测试分页功能"""
        # 第一页
        response1 = requests.get(
            f"{self.base_url}/stocks",
            params={"limit": 5, "offset": 0}
        )
        self.assertEqual(response1.status_code, 200)
        data1 = response1.json()
        self.assertTrue(data1["success"])

        # 第二页
        response2 = requests.get(
            f"{self.base_url}/stocks",
            params={"limit": 5, "offset": 5}
        )
        self.assertEqual(response2.status_code, 200)
        data2 = response2.json()
        self.assertTrue(data2["success"])

        # 确保两页数据不同
        if data1["data"] and data2["data"]:
            self.assertNotEqual(data1["data"][0]["code"], data2["data"][0]["code"])


class TestPythonAPI(unittest.TestCase):
    """Python API客户端测试"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        from api import create_api
        cls.api = create_api()
        cls.test_code = "000001.SZ"

    def test_01_get_stock_list(self):
        """测试获取股票列表"""
        stocks = self.api.get_stock_list(limit=10)
        self.assertIsInstance(stocks, list)
        self.assertLessEqual(len(stocks), 10)

    def test_02_get_price_data(self):
        """测试获取价格数据"""
        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        df = self.api.get_price_data(
            self.test_code,
            start_date,
            end_date
        )
        self.assertFalse(df.empty)
        self.assertIn("trade_date", df.columns)
        self.assertIn("close", df.columns)

    def test_03_batch_operations(self):
        """测试批量操作"""
        codes = ["000001.SZ", "000002.SZ"]
        batch_data = self.api.get_batch_price_data(codes)

        self.assertIsInstance(batch_data, dict)
        for code in codes:
            if code in batch_data:
                self.assertFalse(batch_data[code].empty)

    def test_04_screen_stocks(self):
        """测试股票筛选"""
        results = self.api.screen_stocks({
            "market_cap_min": 1000000000
        })
        self.assertIsInstance(results, list)


def run_tests():
    """运行所有测试"""
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestStockDataAPI))
    suite.addTests(loader.loadTestsFromTestCase(TestPythonAPI))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # 返回测试结果
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)