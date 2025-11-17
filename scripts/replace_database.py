#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库替换脚本 - 用新的数据库文件替换损坏的数据库文件
"""

import os
import shutil
from datetime import datetime

def replace_database():
    """替换损坏的数据库文件"""
    # 获取数据库文件路径
    import sys
    sys.path.append('..')
    from config import get_config
    config = get_config()
    old_db = config.database.path
    new_db = "data/stock_data_new.duckdb"
    backup_db = f"data/stock_data_corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    print("开始替换数据库文件...")
    
    # 检查新数据库是否存在
    if not os.path.exists(new_db):
        print(f"错误: 新数据库文件不存在: {new_db}")
        return False
    
    try:
        # 如果旧数据库存在，重命名为备份文件
        if os.path.exists(old_db):
            print(f"备份损坏的数据库文件到: {backup_db}")
            os.rename(old_db, backup_db)
        
        # 将新数据库重命名为主数据库
        print(f"将新数据库 {new_db} 设置为主数据库 {old_db}")
        os.rename(new_db, old_db)
        
        print("数据库替换完成!")
        print(f"主数据库: {old_db}")
        print(f"备份文件: {backup_db}")
        
        return True
        
    except Exception as e:
        print(f"替换失败: {e}")
        return False

def verify_database():
    """验证数据库表结构"""
    from duckdb_impl import DuckDBDatabase
    
    # 获取数据库路径
    config = get_config()
    db_path = config.database.path
    print(f"验证数据库表结构: {db_path}")
    
    try:
        db = DuckDBDatabase(db_path)
        db.connect()
        
        # 检查price_data表结构
        result = db.conn.execute("DESCRIBE price_data").fetchall()
        print("\nprice_data表结构:")
        for row in result:
            print(f"  {row[0]}: {row[1]}")
        
        # 检查所有表
        tables = db.conn.execute("SHOW TABLES").fetchall()
        print(f"\n数据库包含 {len(tables)} 个表:")
        for table in tables:
            print(f"  - {table[0]}")
        
        db.close()
        print("\n数据库验证完成!")
        return True
        
    except Exception as e:
        print(f"验证失败: {e}")
        return False

def main():
    """主函数"""
    print("=== 数据库替换和验证工具 ===")
    
    # 替换数据库
    if replace_database():
        print("\n" + "="*50)
        # 验证新数据库
        verify_database()
    else:
        print("数据库替换失败")

if __name__ == "__main__":
    main()