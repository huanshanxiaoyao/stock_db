#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证price_data表结构脚本
确认所有JQData API字段都已正确添加
"""

from api import StockDataAPI
from duckdb_impl import DuckDBDatabase
from config import Config

def verify_price_data_structure():
    """验证price_data表结构"""
    print("=== 验证price_data表结构 ===")
    
    # 方法1: 使用StockDataAPI
print("\n1. 通过StockDataAPI查看表结构:")
try:
    api = StockDataAPI()
        api.initialize()
        result = api.db.query('DESCRIBE price_data')
        print(f"   字段数量: {len(result)}")
        for _, row in result.iterrows():
            print(f"   {row[0]}: {row[1]}")
    except Exception as e:
        print(f"   错误: {e}")
    
    # 方法2: 使用配置文件
    print("\n2. 通过配置文件查看表结构:")
    try:
        config = Config()
        db = DuckDBDatabase(config.database.path)
        db.connect()
        result = db.conn.execute('DESCRIBE price_data').fetchall()
        print(f"   字段数量: {len(result)}")
        for row in result:
            print(f"   {row[0]}: {row[1]}")
        db.close()
    except Exception as e:
        print(f"   错误: {e}")
    
    # 方法3: 直接使用DuckDBDatabase
    print("\n3. 直接使用DuckDBDatabase查看表结构:")
    try:
        db = DuckDBDatabase()
        db.connect()
        result = db.conn.execute('DESCRIBE price_data').fetchall()
        print(f"   字段数量: {len(result)}")
        for row in result:
            print(f"   {row[0]}: {row[1]}")
        db.close()
    except Exception as e:
        print(f"   错误: {e}")
    
    # 验证必需字段
    print("\n4. 验证JQData API必需字段:")
    required_fields = [
        'code', 'day', 'open', 'close', 'high', 'low', 'pre_close',
        'volume', 'money', 'factor', 'high_limit', 'low_limit', 
        'avg', 'paused', 'adj_close', 'adj_factor'
    ]
    
    try:
        api = StockDataAPI()
        api.initialize()
        result = api.db.query('DESCRIBE price_data')
        existing_fields = result.iloc[:, 0].tolist()
        
        missing_fields = []
        for field in required_fields:
            if field not in existing_fields:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"   ❌ 缺少字段: {missing_fields}")
        else:
            print(f"   ✅ 所有必需字段都存在 ({len(required_fields)}个)")
            
        # 显示字段分组
        print("\n5. 字段分组说明:")
        field_groups = {
            '基础信息': ['code', 'day'],
            '价格数据': ['open', 'close', 'high', 'low', 'pre_close'],
            '成交数据': ['volume', 'money'],
            '复权相关': ['factor', 'adj_close', 'adj_factor'],
            '价格限制': ['high_limit', 'low_limit'],
            '其他指标': ['avg', 'paused']
        }
        
        for group_name, fields in field_groups.items():
            existing_in_group = [f for f in fields if f in existing_fields]
            print(f"   {group_name}: {existing_in_group} ({len(existing_in_group)}/{len(fields)})")
            
    except Exception as e:
        print(f"   错误: {e}")

if __name__ == "__main__":
    verify_price_data_structure()