#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from duckdb_impl import DuckDBDatabase
from config import get_config

def add_price_data_indexes():
    """为price_data表添加索引"""
    config = get_config()
    db = DuckDBDatabase(config.database.path)
    db.connect()
    
    try:
        print("开始为price_data表创建索引...")
        
        # 创建日期索引
        print("创建日期索引: idx_price_data_day")
        db.conn.execute("CREATE INDEX IF NOT EXISTS idx_price_data_day ON price_data(day)")
        
        # 创建复合索引：股票代码 + 日期
        print("创建复合索引: idx_price_data_code_day")
        db.conn.execute("CREATE INDEX IF NOT EXISTS idx_price_data_code_day ON price_data(code, day)")
        
        print("✅ price_data表索引创建成功！")
        
        # 验证索引是否创建成功
        print("\n验证索引创建情况:")
        indexes = db.conn.execute("""
            SELECT index_name, table_name, column_names 
            FROM duckdb_indexes() 
            WHERE table_name = 'price_data'
        """).fetchall()
        
        for idx in indexes:
            print(f"- {idx[0]}: {idx[2]}")
            
    except Exception as e:
        print(f"❌ 创建索引失败: {e}")
    finally:
        db.close()

if __name__ == '__main__':
    add_price_data_indexes()