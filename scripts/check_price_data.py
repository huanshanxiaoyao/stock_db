#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from duckdb_impl import DuckDBDatabase

def check_price_data():
    db = DuckDBDatabase('stock_data.db')
    db.connect()
    
    # 检查2025年价格数据统计
    result = db.conn.execute("""
        SELECT MIN(day) as min_date, MAX(day) as max_date, 
               COUNT(*) as total_records, COUNT(DISTINCT code) as unique_stocks 
        FROM price_data 
        WHERE day >= '2025-01-01'
    """).fetchone()
    
    print("=== 2025年价格数据统计 ===")
    print(f"最早日期: {result[0]}")
    print(f"最晚日期: {result[1]}")
    print(f"总记录数: {result[2]}")
    print(f"股票数量: {result[3]}")
    
    # 查看数据样本
    samples = db.conn.execute("""
        SELECT code, day, open, high, low, close, volume 
        FROM price_data 
        WHERE day >= '2025-01-01' 
        ORDER BY code, day 
        LIMIT 10
    """).fetchall()
    
    print("\n=== 2025年价格数据样本 ===")
    for row in samples:
        print(f"股票: {row[0]}, 日期: {row[1]}, 开盘: {row[2]:.2f}, 最高: {row[3]:.2f}, 最低: {row[4]:.2f}, 收盘: {row[5]:.2f}, 成交量: {row[6]}")
    
    # 按股票统计
    stock_stats = db.conn.execute("""
        SELECT code, COUNT(*) as record_count, MIN(day) as start_date, MAX(day) as end_date
        FROM price_data 
        WHERE day >= '2025-01-01'
        GROUP BY code
        ORDER BY code
    """).fetchall()
    
    print("\n=== 各股票数据统计 ===")
    for row in stock_stats:
        print(f"股票: {row[0]}, 记录数: {row[1]}, 起始日期: {row[2]}, 结束日期: {row[3]}")
    
    db.close()

if __name__ == '__main__':
    check_price_data()