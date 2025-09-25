#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发访问测试脚本

测试修改后的副本机制是否能正确处理以下场景：
1. main.py daily 更新脚本（写入主数据库）
2. api_server.py API服务（只读副本数据库）同时运行

使用方法：
python test_concurrent_access.py
"""

import os
import sys
import time
import logging
import threading
import subprocess
from datetime import datetime
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api import create_api
from config import get_config

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConcurrentAccessTest:
    """并发访问测试类"""

    def __init__(self):
        self.config = get_config()
        self.db_path = self.config.database.path
        self.test_duration = 60  # 测试持续时间（秒）
        self.results = {
            'writer_operations': 0,
            'writer_errors': 0,
            'reader_operations': 0,
            'reader_errors': 0,
            'start_time': None,
            'end_time': None
        }

    def writer_thread(self):
        """写入线程 - 模拟 main.py daily 更新"""
        logger.info("🔄 启动写入线程（模拟 main.py daily 更新）")

        try:
            # 使用直接连接模式（与 main.py 相同）
            api = create_api(self.db_path, use_replica=False)
            api.initialize()

            start_time = time.time()
            while time.time() - start_time < self.test_duration:
                try:
                    # 模拟查询操作（类似daily update中的检查）
                    stocks = api.get_stock_list()
                    if stocks:
                        # 模拟获取价格数据
                        sample_stock = stocks[0] if stocks else None
                        if sample_stock:
                            price_data = api.get_price_data(sample_stock)
                            self.results['writer_operations'] += 1

                    time.sleep(0.5)  # 模拟处理间隔

                except Exception as e:
                    logger.error(f"写入线程操作失败: {e}")
                    self.results['writer_errors'] += 1

            api.close()
            logger.info("✅ 写入线程完成")

        except Exception as e:
            logger.error(f"写入线程启动失败: {e}")

    def reader_thread(self):
        """读取线程 - 模拟 api_server.py 查询"""
        logger.info("📖 启动读取线程（模拟 api_server.py 查询）")

        try:
            # 使用副本模式（与 api_server.py 相同）
            api = create_api(self.db_path, use_replica=True)
            api.initialize()

            start_time = time.time()
            while time.time() - start_time < self.test_duration:
                try:
                    # 模拟API查询操作
                    stocks = api.get_stock_list()
                    if stocks:
                        # 模拟批量查询
                        sample_stocks = stocks[:5] if len(stocks) >= 5 else stocks
                        for stock in sample_stocks:
                            try:
                                stock_info = api.get_stock_info(stock)
                                price_data = api.get_price_data(stock)
                                self.results['reader_operations'] += 1
                            except Exception as e:
                                logger.debug(f"单个股票查询失败: {e}")
                                continue

                    time.sleep(0.1)  # 模拟API请求间隔

                except Exception as e:
                    logger.error(f"读取线程操作失败: {e}")
                    self.results['reader_errors'] += 1

            api.close()
            logger.info("✅ 读取线程完成")

        except Exception as e:
            logger.error(f"读取线程启动失败: {e}")

    def test_readonly_enforcement(self):
        """测试只读模式强制执行"""
        logger.info("🔒 测试副本只读模式强制执行")

        try:
            api = create_api(self.db_path, use_replica=True)
            api.initialize()

            # 尝试执行写入操作，应该抛出异常
            test_cases = [
                ("插入操作", lambda: api.db.db.insert_dataframe(None, "test_table")),
                ("删除操作", lambda: api.db.db.delete_data("test_table", {})),
                ("更新操作", lambda: api.db.db.update_data("test_table", {}, {})),
                ("创建表操作", lambda: api.db.db.create_tables()),
                ("危险SQL", lambda: api.query("DELETE FROM stock_list WHERE code = '000001.SZ'"))
            ]

            for test_name, test_func in test_cases:
                try:
                    test_func()
                    logger.error(f"❌ {test_name}: 应该被禁止但执行成功了！")
                except RuntimeError as e:
                    if "只读模式" in str(e):
                        logger.info(f"✅ {test_name}: 正确被阻止 - {e}")
                    else:
                        logger.warning(f"⚠️ {test_name}: 被阻止但原因不明确 - {e}")
                except Exception as e:
                    logger.warning(f"⚠️ {test_name}: 出现其他错误 - {e}")

            api.close()

        except Exception as e:
            logger.error(f"只读模式测试失败: {e}")

    def run_test(self):
        """运行并发访问测试"""
        logger.info(f"🚀 开始并发访问测试，持续时间: {self.test_duration} 秒")
        logger.info(f"数据库路径: {self.db_path}")

        # 检查数据库是否存在
        if not Path(self.db_path).exists():
            logger.error(f"数据库文件不存在: {self.db_path}")
            logger.info("请先运行 'python main.py init' 初始化数据库")
            return False

        self.results['start_time'] = datetime.now()

        # 先测试只读模式强制执行
        self.test_readonly_enforcement()

        # 创建并启动线程
        writer_thread = threading.Thread(target=self.writer_thread, name="WriterThread")
        reader_thread = threading.Thread(target=self.reader_thread, name="ReaderThread")

        writer_thread.start()
        reader_thread.start()

        # 等待测试完成
        writer_thread.join()
        reader_thread.join()

        self.results['end_time'] = datetime.now()

        # 输出测试结果
        self.print_results()

        return self.results['writer_errors'] == 0 and self.results['reader_errors'] == 0

    def print_results(self):
        """打印测试结果"""
        duration = (self.results['end_time'] - self.results['start_time']).total_seconds()

        logger.info("📊 测试结果统计:")
        logger.info(f"测试持续时间: {duration:.1f} 秒")
        logger.info(f"写入操作: {self.results['writer_operations']} 次成功, {self.results['writer_errors']} 次失败")
        logger.info(f"读取操作: {self.results['reader_operations']} 次成功, {self.results['reader_errors']} 次失败")

        if self.results['writer_errors'] == 0 and self.results['reader_errors'] == 0:
            logger.info("✅ 测试通过：并发访问正常工作！")
        else:
            logger.error("❌ 测试失败：发现并发访问问题")

        # 计算性能指标
        if duration > 0:
            writer_ops_per_sec = self.results['writer_operations'] / duration
            reader_ops_per_sec = self.results['reader_operations'] / duration
            logger.info(f"性能指标：写入 {writer_ops_per_sec:.1f} ops/s，读取 {reader_ops_per_sec:.1f} ops/s")

def main():
    """主函数"""
    logger.info("🧪 DuckDB 并发访问测试")
    logger.info("=" * 50)

    test = ConcurrentAccessTest()
    success = test.run_test()

    logger.info("=" * 50)
    if success:
        logger.info("🎉 所有测试通过！并发访问问题已解决。")
        return 0
    else:
        logger.error("💥 测试失败！仍存在并发访问问题。")
        return 1

if __name__ == '__main__':
    sys.exit(main())