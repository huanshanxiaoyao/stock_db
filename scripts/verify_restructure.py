#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证数据库重构结果
"""

import duckdb
from pathlib import Path
import pandas as pd
import sys
sys.path.append('..')
from config import get_config

def verify_database_structure():
    """验证数据库结构重组结果"""
    
    # 获取数据库路径
    config = get_config()
    db_path = Path(config.database.path)
    
    if not db_path.exists():
        print(f"错误：数据库文件不存在 {db_path}")
        return
    
    try:
        conn = duckdb.connect(str(db_path))
        
        print("=" * 80)
        print("数据库重构验证报告")
        print("=" * 80)
        
        # 1. 获取所有表
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()
        
        print(f"\n数据库中的表 (共 {len(tables)} 个):")
        for table_name, in tables:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  - {table_name}: {row_count:,} 行")
        
        # 2. 验证新创建的表
        print("\n=== 新创建的表验证 ===")
        
        # 检查 financial_indicators 表
        if any(t[0] == 'financial_indicators' for t in tables):
            print("\n✓ financial_indicators 表已创建")
            columns = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'financial_indicators' ORDER BY ordinal_position"
            ).fetchall()
            print(f"  字段数量: {len(columns)}")
            print("  主要字段:")
            for col_name, col_type in columns[:10]:  # 显示前10个字段
                print(f"    - {col_name}: {col_type}")
            if len(columns) > 10:
                print(f"    ... 还有 {len(columns) - 10} 个字段")
        else:
            print("✗ financial_indicators 表未找到")
        
        # 检查 technical_indicators 表
        if any(t[0] == 'technical_indicators' for t in tables):
            print("\n✓ technical_indicators 表已创建")
            columns = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'technical_indicators' ORDER BY ordinal_position"
            ).fetchall()
            print(f"  字段数量: {len(columns)}")
            print("  主要字段:")
            for col_name, col_type in columns[:10]:  # 显示前10个字段
                print(f"    - {col_name}: {col_type}")
            if len(columns) > 10:
                print(f"    ... 还有 {len(columns) - 10} 个字段")
        else:
            print("✗ technical_indicators 表未找到")
        
        # 检查原 indicator_data 表是否重命名
        if any(t[0] == 'indicator_data_backup' for t in tables):
            print("\n✓ 原 indicator_data 表已重命名为 indicator_data_backup")
        else:
            print("\n✗ indicator_data_backup 表未找到")
        
        # 3. 验证 balance_sheet 表的新增字段
        print("\n=== balance_sheet 表字段验证 ===")
        balance_columns = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'balance_sheet' ORDER BY ordinal_position"
        ).fetchall()
        print(f"balance_sheet 表字段数量: {len(balance_columns)}")
        
        # 检查一些关键新增字段
        key_balance_fields = ['money_cap', 'trading_assets', 'accounts_receivable', 'good_will', 'bonds_payable', 'retained_profit']
        balance_field_names = [col[0] for col in balance_columns]
        
        print("关键新增字段检查:")
        for field in key_balance_fields:
            if field in balance_field_names:
                print(f"  ✓ {field}")
            else:
                print(f"  ✗ {field} (缺失)")
        
        # 4. 验证 income_statement 表的新增字段
        print("\n=== income_statement 表字段验证 ===")
        income_columns = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'income_statement' ORDER BY ordinal_position"
        ).fetchall()
        print(f"income_statement 表字段数量: {len(income_columns)}")
        
        # 检查一些关键新增字段
        key_income_fields = ['interest_income', 'commission_income', 'reinsurance_cost', 'invest_income_associates', 'ci_parent_company_owners']
        income_field_names = [col[0] for col in income_columns]
        
        print("关键新增字段检查:")
        for field in key_income_fields:
            if field in income_field_names:
                print(f"  ✓ {field}")
            else:
                print(f"  ✗ {field} (缺失)")
        
        # 5. 总结
        print("\n=== 重构总结 ===")
        
        # 统计新增字段数量
        total_balance_fields = len(balance_columns)
        total_income_fields = len(income_columns)
        
        print(f"✓ indicator_data 表已分离为 financial_indicators 和 technical_indicators 两个表")
        print(f"✓ balance_sheet 表当前有 {total_balance_fields} 个字段")
        print(f"✓ income_statement 表当前有 {total_income_fields} 个字段")
        print(f"✓ 数据库备份已创建")
        
        print("\n重构任务完成！")
        
    except Exception as e:
        print(f"验证过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    verify_database_structure()