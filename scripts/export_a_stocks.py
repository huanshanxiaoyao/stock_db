#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import duckdb
import pandas as pd

def export_a_stocks():
    """导出A股股票列表"""
    conn = duckdb.connect('stock_data.duckdb')
    
    # 查看所有股票代码格式
    print("=== 股票代码样本 ===")
    samples = conn.execute('SELECT code, name FROM stock_list LIMIT 20').fetchall()
    for code, name in samples:
        print(f'{code} - {name}')
    
    # 查看交易所分布
    print("\n=== 交易所分布 ===")
    exchanges = conn.execute("""
        SELECT 
            CASE 
                WHEN code LIKE '%.SZ' THEN '深交所'
                WHEN code LIKE '%.SH' THEN '上交所'
                WHEN code LIKE '%.BJ' THEN '北交所'
                ELSE '其他'
            END as exchange,
            COUNT(*) as count
        FROM stock_list 
        GROUP BY 
            CASE 
                WHEN code LIKE '%.SZ' THEN '深交所'
                WHEN code LIKE '%.SH' THEN '上交所'
                WHEN code LIKE '%.BJ' THEN '北交所'
                ELSE '其他'
            END
        ORDER BY count DESC
    """).fetchall()
    
    for exchange, count in exchanges:
        print(f'{exchange}: {count}只')
    
    # 导出A股股票（深交所和上交所）
    print("\n=== 导出A股股票 ===")
    a_stocks = conn.execute("""
        SELECT code, name 
        FROM stock_list 
        WHERE code LIKE '%.SZ' OR code LIKE '%.SH'
        ORDER BY code
    """).fetchdf()
    
    if len(a_stocks) > 0:
        a_stocks.to_csv('all_a_stocks.csv', index=False)
        print(f'成功导出 {len(a_stocks)} 只A股股票到 all_a_stocks.csv')
        
        # 显示前10只股票
        print("\n前10只A股股票:")
        for _, row in a_stocks.head(10).iterrows():
            print(f'{row["code"]} - {row["name"]}')
    else:
        print('未找到A股股票')
    
    conn.close()

if __name__ == '__main__':
    export_a_stocks()