#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本 - 更新price_data表结构
添加JQData API的新字段：factor, high_limit, low_limit, avg, paused
"""

import os
import shutil
from datetime import datetime
from duckdb_impl import DuckDBDatabase

def backup_database(db_path):
    """备份现有数据库"""
    if os.path.exists(db_path):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{db_path}.backup_{timestamp}"
        shutil.copy2(db_path, backup_path)
        print(f"数据库已备份到: {backup_path}")
        return backup_path
    return None

def migrate_price_data_table(db_path):
    """迁移price_data表结构"""
    print(f"开始迁移数据库: {db_path}")
    
    try:
        # 连接数据库
        db = DuckDBDatabase(db_path)
        db.connect()
        
        # 检查表是否存在
        tables = db.conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        if 'price_data' not in table_names:
            print("price_data表不存在，创建新表...")
            db.create_tables()
            print("新表创建完成")
        else:
            print("检查现有表结构...")
            
            # 获取现有表结构
            current_schema = db.conn.execute("DESCRIBE price_data").fetchall()
            current_columns = [col[0] for col in current_schema]
            print(f"当前字段: {current_columns}")
            
            # 需要添加的新字段
            new_fields = {
                'pre_close': 'DOUBLE',
                'factor': 'DOUBLE', 
                'high_limit': 'DOUBLE',
                'low_limit': 'DOUBLE',
                'avg': 'DOUBLE',
                'paused': 'INTEGER'
            }
            
            # 检查并添加缺失的字段
            missing_fields = []
            for field, field_type in new_fields.items():
                if field not in current_columns:
                    missing_fields.append((field, field_type))
            
            if missing_fields:
                print(f"需要添加的字段: {[f[0] for f in missing_fields]}")
                
                # 添加缺失字段
                for field, field_type in missing_fields:
                    try:
                        sql = f"ALTER TABLE price_data ADD COLUMN {field} {field_type}"
                        db.conn.execute(sql)
                        print(f"已添加字段: {field} ({field_type})")
                    except Exception as e:
                        print(f"添加字段 {field} 失败: {e}")
                
                # 验证更新后的表结构
                updated_schema = db.conn.execute("DESCRIBE price_data").fetchall()
                updated_columns = [col[0] for col in updated_schema]
                print(f"更新后字段: {updated_columns}")
                
                print("表结构迁移完成")
            else:
                print("表结构已是最新版本，无需迁移")
        
        db.close()
        print("迁移成功完成")
        
    except Exception as e:
        print(f"迁移失败: {e}")
        # 如果数据库损坏，尝试创建新的数据库
        if "utf-8" in str(e) or "decode" in str(e):
            print("数据库文件可能损坏，创建新的数据库文件...")
            new_db_path = db_path.replace('.duckdb', '_new.duckdb')
            new_db = DuckDBDatabase(new_db_path)
            new_db.connect()
            new_db.create_tables()
            new_db.close()
            print(f"新数据库已创建: {new_db_path}")
        else:
            raise

def main():
    """主函数"""
    # 获取数据库路径
    import sys
    sys.path.append('..')
    from config import get_config
    config = get_config()
    db_path = config.database.path
    
    if not os.path.exists(db_path):
        print(f"数据库文件不存在: {db_path}")
        print("创建新数据库...")
        db = DuckDBDatabase(db_path)
        db.connect()
        db.create_tables()
        db.close()
        print("新数据库创建完成")
    else:
        migrate_price_data_table(db_path)

if __name__ == "__main__":
    main()