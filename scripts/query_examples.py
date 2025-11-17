#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户交易记录查询示例脚本

展示如何使用 query_user_transactions.py 进行各种查询操作
"""

import sys
import os
from datetime import datetime, date, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.query_user_transactions import UserTransactionQuery


def example_queries():
    """演示各种查询操作"""
    
    print("=" * 60)
    print("用户交易记录查询示例")
    print("=" * 60)
    
    try:
        query = UserTransactionQuery()
        
        # 1. 查询今天的所有交易记录
        print("\n1. 查询今天的所有交易记录：")
        today = date.today().strftime('%Y-%m-%d')
        df_today = query.query_by_date(today)
        print(f"今天({today})共有 {len(df_today)} 条交易记录")
        if not df_today.empty:
            print(df_today[['user_id', 'stock_code', 'trade_type', 'quantity', 'price', 'amount']].head())
        
        # 2. 查询最近7天的交易记录
        print("\n2. 查询最近7天的交易记录：")
        df_recent = query.get_recent_trades(days=7, limit=10)
        print(f"最近7天共有 {len(df_recent)} 条交易记录（显示前10条）")
        if not df_recent.empty:
            print(df_recent[['trade_date', 'user_id', 'stock_code', 'trade_type', 'quantity', 'amount']].head(10))
        
        # 3. 查询特定用户的交易记录
        print("\n3. 查询特定用户的交易记录：")
        # 先获取一个用户ID
        all_users = query.conn.execute("SELECT DISTINCT user_id FROM user_transactions LIMIT 1").fetchall()
        if all_users:
            user_id = all_users[0][0]
            df_user = query.query_by_user(user_id, limit=5)
            print(f"用户 {user_id} 的交易记录（最近5条）：")
            if not df_user.empty:
                print(df_user[['trade_date', 'stock_code', 'trade_type', 'quantity', 'price', 'amount']].head())
        else:
            print("暂无用户交易记录")
        
        # 4. 查询特定股票的交易记录
        print("\n4. 查询特定股票的交易记录：")
        # 先获取一个股票代码
        all_stocks = query.conn.execute("SELECT DISTINCT stock_code FROM user_transactions LIMIT 1").fetchall()
        if all_stocks:
            stock_code = all_stocks[0][0]
            df_stock = query.query_by_stock(stock_code, limit=5)
            print(f"股票 {stock_code} 的交易记录（最近5条）：")
            if not df_stock.empty:
                print(df_stock[['trade_date', 'user_id', 'trade_type', 'quantity', 'price', 'amount']].head())
        else:
            print("暂无股票交易记录")
        
        # 5. 查询买入交易记录
        print("\n5. 查询买入交易记录（trade_type=23）：")
        df_buy = query.query_by_trade_type(23, limit=5)
        print(f"买入交易记录（最近5条）：")
        if not df_buy.empty:
            print(df_buy[['trade_date', 'user_id', 'stock_code', 'quantity', 'price', 'amount']].head())
        
        # 6. 查询卖出交易记录
        print("\n6. 查询卖出交易记录（trade_type=24）：")
        df_sell = query.query_by_trade_type(24, limit=5)
        print(f"卖出交易记录（最近5条）：")
        if not df_sell.empty:
            print(df_sell[['trade_date', 'user_id', 'stock_code', 'quantity', 'price', 'amount']].head())
        
        # 7. 获取今天的交易汇总
        print("\n7. 获取今天的交易汇总：")
        df_summary = query.get_daily_summary(today)
        print(f"今天的交易汇总：")
        if not df_summary.empty:
            print(df_summary[['user_id', 'stock_code', 'trade_type_name', 'trade_count', 'total_quantity', 'total_amount']].head())
        
        # 8. 获取用户交易汇总（如果有用户数据）
        if all_users:
            print("\n8. 获取用户交易汇总：")
            user_id = all_users[0][0]
            df_user_summary = query.get_user_summary(user_id)
            print(f"用户 {user_id} 的交易汇总：")
            if not df_user_summary.empty:
                print(df_user_summary[['stock_code', 'trade_count', 'buy_quantity', 'sell_quantity', 'buy_amount', 'sell_amount']].head())
        
        # 9. 按日期范围查询
        print("\n9. 按日期范围查询（最近3天）：")
        end_date = date.today()
        start_date = end_date - timedelta(days=3)
        df_range = query.query_by_date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        print(f"最近3天({start_date} 到 {end_date})共有 {len(df_range)} 条交易记录")
        if not df_range.empty:
            print(df_range[['trade_date', 'user_id', 'stock_code', 'trade_type', 'quantity', 'amount']].head())
        
        # 10. 数据库统计信息
        print("\n10. 数据库统计信息：")
        stats = query.conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT user_id) as total_users,
                COUNT(DISTINCT stock_code) as total_stocks,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date,
                SUM(CASE WHEN trade_type = 23 THEN 1 ELSE 0 END) as buy_count,
                SUM(CASE WHEN trade_type = 24 THEN 1 ELSE 0 END) as sell_count
            FROM user_transactions
        """).fetchone()
        
        if stats:
            print(f"总交易记录数：{stats[0]}")
            print(f"总用户数：{stats[1]}")
            print(f"总股票数：{stats[2]}")
            print(f"最早交易日期：{stats[3]}")
            print(f"最晚交易日期：{stats[4]}")
            print(f"买入交易数：{stats[5]}")
            print(f"卖出交易数：{stats[6]}")
        
    except Exception as e:
        print(f"查询失败：{e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("查询示例完成")
    print("=" * 60)


def print_usage_examples():
    """打印命令行使用示例"""
    
    print("\n" + "=" * 60)
    print("命令行使用示例")
    print("=" * 60)
    
    examples = [
        {
            "description": "查询今天的所有交易记录",
            "command": "python scripts/query_user_transactions.py --type date --date 2024-01-15"
        },
        {
            "description": "查询指定用户今天的交易记录",
            "command": "python scripts/query_user_transactions.py --type date --date 2024-01-15 --user-id user123"
        },
        {
            "description": "查询最近7天的交易记录",
            "command": "python scripts/query_user_transactions.py --type recent --days 7 --limit 50"
        },
        {
            "description": "查询指定日期范围的交易记录",
            "command": "python scripts/query_user_transactions.py --type date-range --start-date 2024-01-01 --end-date 2024-01-15"
        },
        {
            "description": "查询指定用户的交易记录",
            "command": "python scripts/query_user_transactions.py --type user --user-id user123 --limit 20"
        },
        {
            "description": "查询指定股票的交易记录",
            "command": "python scripts/query_user_transactions.py --type stock --stock-code 000001.SZ --limit 10"
        },
        {
            "description": "查询买入交易记录",
            "command": "python scripts/query_user_transactions.py --type trade-type --trade-type 23 --limit 20"
        },
        {
            "description": "查询卖出交易记录",
            "command": "python scripts/query_user_transactions.py --type trade-type --trade-type 24 --limit 20"
        },
        {
            "description": "获取指定日期的交易汇总",
            "command": "python scripts/query_user_transactions.py --type daily-summary --date 2024-01-15"
        },
        {
            "description": "获取用户交易汇总",
            "command": "python scripts/query_user_transactions.py --type user-summary --user-id user123"
        },
        {
            "description": "查询结果保存到CSV文件",
            "command": "python scripts/query_user_transactions.py --type date --date 2024-01-15 --output results.csv"
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"\n{i}. {example['description']}：")
        print(f"   {example['command']}")
    
    print("\n" + "=" * 60)


if __name__ == '__main__':
    print("用户交易记录查询工具演示")
    
    # 打印命令行使用示例
    print_usage_examples()
    
    # 执行查询示例
    example_queries()