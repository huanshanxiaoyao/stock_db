#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查股票的活跃状态
"""

import duckdb
from datetime import datetime, date

def check_active_stocks():
    """检查股票的活跃状态"""
    print("=== 检查股票活跃状态 ===")
    
    try:
        conn = duckdb.connect('stock_data.duckdb')
        
        # 查看当前日期
        current_date = date.today()
        print(f"当前日期: {current_date}")
        
        # 查看end_date字段的分布
        print("\nend_date字段分布:")
        end_date_stats = conn.execute("""
            SELECT 
                CASE 
                    WHEN end_date IS NULL THEN 'NULL'
                    WHEN end_date > CURRENT_DATE THEN '未来日期'
                    WHEN end_date <= CURRENT_DATE THEN '过去日期'
                END as date_category,
                COUNT(*) as count
            FROM stock_list 
            GROUP BY date_category
            ORDER BY count DESC
        """).fetchall()
        
        for category, count in end_date_stats:
            print(f"  {category}: {count} 只股票")
        
        # 查看具体的end_date值
        print("\nend_date值样本:")
        end_date_samples = conn.execute("""
            SELECT code, name, end_date, 
                   CASE WHEN end_date > CURRENT_DATE THEN '活跃' ELSE '非活跃' END as status
            FROM stock_list 
            ORDER BY end_date DESC
            LIMIT 10
        """).fetchall()
        
        for code, name, end_date, status in end_date_samples:
            print(f"  {code} - {name}: {end_date} ({status})")
        
        # 测试active_only=True的查询
        print("\n测试active_only=True查询:")
        active_stocks = conn.execute("""
            SELECT COUNT(*) as count
            FROM stock_list 
            WHERE (end_date IS NULL OR end_date > CURRENT_DATE)
        """).fetchone()[0]
        print(f"活跃股票数量: {active_stocks}")
        
        # 测试active_only=False的查询
        print("\n测试active_only=False查询:")
        all_stocks = conn.execute("""
            SELECT COUNT(*) as count
            FROM stock_list
        """).fetchone()[0]
        print(f"所有股票数量: {all_stocks}")
        
        # 显示一些活跃股票的代码
        if active_stocks > 0:
            print("\n活跃股票样本:")
            active_samples = conn.execute("""
                SELECT code, name, end_date
                FROM stock_list 
                WHERE (end_date IS NULL OR end_date > CURRENT_DATE)
                LIMIT 10
            """).fetchall()
            
            for code, name, end_date in active_samples:
                print(f"  {code} - {name} (end_date: {end_date})")
        else:
            print("\n没有找到活跃股票！")
            print("\n所有股票样本:")
            all_samples = conn.execute("""
                SELECT code, name, end_date
                FROM stock_list 
                LIMIT 10
            """).fetchall()
            
            for code, name, end_date in all_samples:
                print(f"  {code} - {name} (end_date: {end_date})")
        
        conn.close()
        
    except Exception as e:
        print(f"查询失败: {e}")

if __name__ == "__main__":
    check_active_stocks()