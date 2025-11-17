#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清空user_transactions表中的所有数据
"""

from duckdb_impl import DuckDBDatabase

def clear_user_transactions():
    """清空user_transactions表"""
    try:
        # 连接数据库
        db = DuckDBDatabase()
        
        # 连接数据库
        db.connect()
        
        # 查询当前记录数
        result = db.query_data("SELECT COUNT(*) as count FROM user_transactions")
        current_count = result.iloc[0]['count'] if not result.empty else 0
        print(f"当前user_transactions表中有 {current_count} 条记录")
        
        if current_count > 0:
            # 清空表
            db.query_data("DELETE FROM user_transactions")
            print(f"已清空user_transactions表，删除了 {current_count} 条记录")
        else:
            print("user_transactions表已经是空的")
            
        # 验证清空结果
        result = db.query_data("SELECT COUNT(*) as count FROM user_transactions")
        final_count = result.iloc[0]['count'] if not result.empty else 0
        print(f"清空后user_transactions表中有 {final_count} 条记录")
        
        # 关闭连接
        db.close()
        
        return True
        
    except Exception as e:
        print(f"清空user_transactions表失败: {e}")
        return False

if __name__ == "__main__":
    clear_user_transactions()