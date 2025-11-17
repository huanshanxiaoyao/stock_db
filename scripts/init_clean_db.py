#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化干净的数据库，添加深交所和上交所示例股票数据
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from duckdb_impl import DuckDBDatabase
import pandas as pd
from datetime import date, datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_stock_data():
    """创建示例股票数据"""
    
    # 深交所和上交所示例股票
    sample_stocks = [
        # 深交所主板
        {'code': '000001.SZ', 'name': '平安银行', 'exchange': 'SZSE', 'market': 'main'},
        {'code': '000002.SZ', 'name': '万科A', 'exchange': 'SZSE', 'market': 'main'},
        {'code': '000858.SZ', 'name': '五粮液', 'exchange': 'SZSE', 'market': 'main'},
        
        # 深交所创业板
        {'code': '300001.SZ', 'name': '特锐德', 'exchange': 'SZSE', 'market': 'gem'},
        {'code': '300015.SZ', 'name': '爱尔眼科', 'exchange': 'SZSE', 'market': 'gem'},
        {'code': '300059.SZ', 'name': '东方财富', 'exchange': 'SZSE', 'market': 'gem'},
        
        # 上交所主板
        {'code': '600000.SH', 'name': '浦发银行', 'exchange': 'SSE', 'market': 'main'},
        {'code': '600036.SH', 'name': '招商银行', 'exchange': 'SSE', 'market': 'main'},
        {'code': '600519.SH', 'name': '贵州茅台', 'exchange': 'SSE', 'market': 'main'},
        
        # 上交所科创板
        {'code': '688001.SH', 'name': '华兴源创', 'exchange': 'SSE', 'market': 'star'},
        {'code': '688009.SH', 'name': '中国通号', 'exchange': 'SSE', 'market': 'star'},
        {'code': '688036.SH', 'name': '传音控股', 'exchange': 'SSE', 'market': 'star'},
    ]
    
    return sample_stocks

def init_database():
    """初始化数据库"""
    logger.info("开始初始化数据库...")
    
    # 创建数据库实例
    db = DuckDBDatabase("stock_data.db")
    
    try:
        # 连接数据库
        db.connect()
        logger.info("数据库连接成功")
        
        # 创建表结构
        db.create_tables()
        logger.info("数据表创建成功")
        
        # 获取示例股票数据
        sample_stocks = create_sample_stock_data()
        
        # 创建股票列表数据
        stock_list_df = pd.DataFrame(sample_stocks)
        
        # 插入股票列表数据
        db.conn.execute("DROP TABLE IF EXISTS stock_list")
        db.conn.execute("""
            CREATE TABLE stock_list (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                exchange VARCHAR,
                market VARCHAR
            )
        """)
        
        # 插入数据
        for stock in sample_stocks:
            db.conn.execute("""
                INSERT INTO stock_list (code, name, exchange, market)
                VALUES (?, ?, ?, ?)
            """, [stock['code'], stock['name'], stock['exchange'], stock['market']])
        
        logger.info(f"成功插入 {len(sample_stocks)} 只股票数据")
        
        # 验证数据
        result = db.conn.execute("SELECT COUNT(*) as count FROM stock_list").fetchone()
        logger.info(f"数据库中共有 {result[0]} 只股票")
        
        # 显示股票列表
        stocks = db.conn.execute("SELECT * FROM stock_list ORDER BY code").fetchall()
        logger.info("股票列表:")
        for stock in stocks:
            logger.info(f"  {stock[0]} - {stock[1]} ({stock[2]})")
        
        logger.info("数据库初始化完成!")
        
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    init_database()