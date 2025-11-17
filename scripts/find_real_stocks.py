#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查找数据库中的真实A股代码
"""

import duckdb

def find_real_stocks():
    """查找数据库中的真实A股代码"""
    print("=== 查找真实A股代码 ===")
    
    try:
        conn = duckdb.connect('stock_data.duckdb')
        
        # 查看stock_list表的总数
        total = conn.execute("SELECT COUNT(*) FROM stock_list").fetchone()[0]
        print(f"stock_list表总记录数: {total}")
        
        # 按交易所分组统计
        print("\n按交易所分组:")
        exchanges = conn.execute("""
            SELECT exchange, COUNT(*) as count 
            FROM stock_list 
            GROUP BY exchange 
            ORDER BY count DESC
        """).fetchall()
        for exchange, count in exchanges:
            print(f"  {exchange}: {count} 只股票")
        
        # 查找深交所股票（.SZ结尾）
        print("\n深交所股票（.SZ结尾）:")
        sz_stocks = conn.execute("""
            SELECT code, name, exchange, market 
            FROM stock_list 
            WHERE code LIKE '%.SZ' 
            LIMIT 10
        """).fetchall()
        for code, name, exchange, market in sz_stocks:
            print(f"  {code} - {name} ({exchange}/{market})")
        
        # 查找上交所股票（.SH结尾）
        print("\n上交所股票（.SH结尾）:")
        sh_stocks = conn.execute("""
            SELECT code, name, exchange, market 
            FROM stock_list 
            WHERE code LIKE '%.SH' 
            LIMIT 10
        """).fetchall()
        for code, name, exchange, market in sh_stocks:
            print(f"  {code} - {name} ({exchange}/{market})")
        
        # 查找特定的A股代码
        print("\n查找特定A股代码:")
        target_codes = ['000001.SZ', '688585.SH', '000002.SZ', '600000.SH', '300001.SZ']
        for code in target_codes:
            result = conn.execute("""
                SELECT code, name, exchange, market, status 
                FROM stock_list 
                WHERE code = ?
            """, [code]).fetchall()
            if result:
                code, name, exchange, market, status = result[0]
                print(f"  ✓ {code} - {name} ({exchange}/{market}, {status})")
            else:
                print(f"  ✗ {code} - 未找到")
        
        # 查看所有市场类型
        print("\n所有市场类型:")
        markets = conn.execute("""
            SELECT market, COUNT(*) as count 
            FROM stock_list 
            GROUP BY market 
            ORDER BY count DESC
        """).fetchall()
        for market, count in markets:
            print(f"  {market}: {count} 只股票")
        
        # 查看主板股票（可能是A股）
        print("\n主板股票样本:")
        main_stocks = conn.execute("""
            SELECT code, name, exchange, market 
            FROM stock_list 
            WHERE market IN ('main', 'sz_main', 'sh_main', 'szse', 'sse') 
            LIMIT 10
        """).fetchall()
        for code, name, exchange, market in main_stocks:
            print(f"  {code} - {name} ({exchange}/{market})")
        
        conn.close()
        
    except Exception as e:
        print(f"查询失败: {e}")

if __name__ == "__main__":
    find_real_stocks()