#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户交易记录查询脚本

提供对user_transactions表的常见查询功能：
- 按日期查询
- 按用户查询
- 按股票代码查询
- 按交易类型查询
- 统计分析查询

使用方法：
    python scripts/query_user_transactions.py --help
"""

import argparse
import sys
import os
from datetime import datetime, date
from typing import Optional, List
import pandas as pd

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from duckdb_impl import DuckDBDatabase
from config import get_config


class UserTransactionQuery:
    """用户交易记录查询类"""
    
    def __init__(self):
        """初始化数据库连接"""
        config = get_config()
        self.db = DuckDBDatabase(config.database.path)
        self.db.connect()
        self.conn = self.db.conn
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def query_by_date(self, target_date: str, user_id: Optional[str] = None) -> pd.DataFrame:
        """
        按日期查询交易记录
        
        Args:
            target_date: 目标日期，格式：YYYY-MM-DD
            user_id: 可选的用户ID过滤
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE trade_date = ?
        """
        params = [target_date]
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += " ORDER BY trade_time"
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def query_by_date_range(self, start_date: str, end_date: str, 
                           user_id: Optional[str] = None) -> pd.DataFrame:
        """
        按日期范围查询交易记录
        
        Args:
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            user_id: 可选的用户ID过滤
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE trade_date BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += " ORDER BY trade_date, trade_time"
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def query_by_user(self, user_id: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        按用户ID查询交易记录
        
        Args:
            user_id: 用户ID
            limit: 限制返回记录数
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE user_id = ?
        ORDER BY trade_date DESC, trade_time DESC
        """
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return pd.read_sql_query(sql, self.conn, params=[user_id])
    
    def query_by_stock(self, stock_code: str, user_id: Optional[str] = None, 
                      limit: Optional[int] = None) -> pd.DataFrame:
        """
        按股票代码查询交易记录
        
        Args:
            stock_code: 股票代码
            user_id: 可选的用户ID过滤
            limit: 限制返回记录数
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE stock_code = ?
        """
        params = [stock_code]
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += " ORDER BY trade_date DESC, trade_time DESC"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def query_by_trade_type(self, trade_type: int, user_id: Optional[str] = None, 
                           limit: Optional[int] = None) -> pd.DataFrame:
        """
        按交易类型查询交易记录
        
        Args:
            trade_type: 交易类型 (23-买入, 24-卖出)
            user_id: 可选的用户ID过滤
            limit: 限制返回记录数
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE trade_type = ?
        """
        params = [trade_type]
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += " ORDER BY trade_date DESC, trade_time DESC"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def get_daily_summary(self, target_date: str, user_id: Optional[str] = None) -> pd.DataFrame:
        """
        获取指定日期的交易汇总
        
        Args:
            target_date: 目标日期，格式：YYYY-MM-DD
            user_id: 可选的用户ID过滤
        
        Returns:
            DataFrame: 汇总结果
        """
        sql = """
        SELECT 
            user_id,
            stock_code,
            trade_type,
            CASE 
                WHEN trade_type = 23 THEN '买入'
                WHEN trade_type = 24 THEN '卖出'
                ELSE '未知'
            END as trade_type_name,
            COUNT(*) as trade_count,
            SUM(quantity) as total_quantity,
            SUM(amount) as total_amount,
            SUM(commission) as total_commission,
            SUM(stamp_tax) as total_stamp_tax,
            SUM(other_fees) as total_other_fees,
            SUM(net_amount) as total_net_amount
        FROM user_transactions 
        WHERE trade_date = ?
        """
        params = [target_date]
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += """
        GROUP BY user_id, stock_code, trade_type
        ORDER BY user_id, stock_code, trade_type
        """
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def get_user_summary(self, user_id: str, start_date: Optional[str] = None, 
                        end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取用户交易汇总统计
        
        Args:
            user_id: 用户ID
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        
        Returns:
            DataFrame: 汇总结果
        """
        sql = """
        SELECT 
            stock_code,
            COUNT(*) as trade_count,
            SUM(CASE WHEN trade_type = 23 THEN quantity ELSE 0 END) as buy_quantity,
            SUM(CASE WHEN trade_type = 24 THEN quantity ELSE 0 END) as sell_quantity,
            SUM(CASE WHEN trade_type = 23 THEN amount ELSE 0 END) as buy_amount,
            SUM(CASE WHEN trade_type = 24 THEN amount ELSE 0 END) as sell_amount,
            SUM(commission + stamp_tax + other_fees) as total_fees,
            MIN(trade_date) as first_trade_date,
            MAX(trade_date) as last_trade_date
        FROM user_transactions 
        WHERE user_id = ?
        """
        params = [user_id]
        
        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)
        
        sql += """
        GROUP BY stock_code
        ORDER BY trade_count DESC
        """
        
        return pd.read_sql_query(sql, self.conn, params=params)
    
    def get_recent_trades(self, days: int = 7, user_id: Optional[str] = None, 
                         limit: int = 100) -> pd.DataFrame:
        """
        获取最近N天的交易记录
        
        Args:
            days: 天数
            user_id: 可选的用户ID过滤
            limit: 限制返回记录数
        
        Returns:
            DataFrame: 查询结果
        """
        sql = """
        SELECT * FROM user_transactions 
        WHERE trade_date >= CURRENT_DATE - INTERVAL '{} days'
        """.format(days)
        params = []
        
        if user_id:
            sql += " AND user_id = ?"
            params.append(user_id)
        
        sql += f" ORDER BY trade_date DESC, trade_time DESC LIMIT {limit}"
        
        return pd.read_sql_query(sql, self.conn, params=params)


def main():
    """主函数 - 命令行接口"""
    parser = argparse.ArgumentParser(description='用户交易记录查询工具')
    
    # 查询类型
    parser.add_argument('--type', choices=['date', 'date-range', 'user', 'stock', 'trade-type', 
                                          'daily-summary', 'user-summary', 'recent'],
                       required=True, help='查询类型')
    
    # 通用参数
    parser.add_argument('--user-id', help='用户ID')
    parser.add_argument('--limit', type=int, help='限制返回记录数')
    parser.add_argument('--output', help='输出文件路径（CSV格式）')
    
    # 日期相关参数
    parser.add_argument('--date', help='查询日期 (YYYY-MM-DD)')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='最近天数（用于recent查询）')
    
    # 其他参数
    parser.add_argument('--stock-code', help='股票代码')
    parser.add_argument('--trade-type', type=int, choices=[23, 24], 
                       help='交易类型 (23-买入, 24-卖出)')
    
    args = parser.parse_args()
    
    try:
        query = UserTransactionQuery()
        df = None
        
        # 根据查询类型执行相应查询
        if args.type == 'date':
            if not args.date:
                print("错误：按日期查询需要指定 --date 参数")
                return 1
            df = query.query_by_date(args.date, args.user_id)
            
        elif args.type == 'date-range':
            if not args.start_date or not args.end_date:
                print("错误：按日期范围查询需要指定 --start-date 和 --end-date 参数")
                return 1
            df = query.query_by_date_range(args.start_date, args.end_date, args.user_id)
            
        elif args.type == 'user':
            if not args.user_id:
                print("错误：按用户查询需要指定 --user-id 参数")
                return 1
            df = query.query_by_user(args.user_id, args.limit)
            
        elif args.type == 'stock':
            if not args.stock_code:
                print("错误：按股票查询需要指定 --stock-code 参数")
                return 1
            df = query.query_by_stock(args.stock_code, args.user_id, args.limit)
            
        elif args.type == 'trade-type':
            if args.trade_type is None:
                print("错误：按交易类型查询需要指定 --trade-type 参数")
                return 1
            df = query.query_by_trade_type(args.trade_type, args.user_id, args.limit)
            
        elif args.type == 'daily-summary':
            if not args.date:
                print("错误：日汇总查询需要指定 --date 参数")
                return 1
            df = query.get_daily_summary(args.date, args.user_id)
            
        elif args.type == 'user-summary':
            if not args.user_id:
                print("错误：用户汇总查询需要指定 --user-id 参数")
                return 1
            df = query.get_user_summary(args.user_id, args.start_date, args.end_date)
            
        elif args.type == 'recent':
            df = query.get_recent_trades(args.days, args.user_id, args.limit or 100)
        
        # 输出结果
        if df is not None and not df.empty:
            print(f"\n查询结果：共 {len(df)} 条记录\n")
            
            # 如果指定了输出文件，保存为CSV
            if args.output:
                df.to_csv(args.output, index=False, encoding='utf-8-sig')
                print(f"结果已保存到：{args.output}")
            else:
                # 控制台输出，限制显示列数和行数
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                pd.set_option('display.max_colwidth', 20)
                
                if len(df) > 20:
                    print("前20条记录：")
                    print(df.head(20).to_string(index=False))
                    print(f"\n... 还有 {len(df) - 20} 条记录，使用 --output 参数保存完整结果")
                else:
                    print(df.to_string(index=False))
        else:
            print("\n未找到匹配的记录")
        
        return 0
        
    except Exception as e:
        print(f"查询失败：{e}")
        return 1


if __name__ == '__main__':
    exit(main())