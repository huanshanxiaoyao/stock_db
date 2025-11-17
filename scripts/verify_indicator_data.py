#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证indicator_data表结构
"""

import duckdb

def verify_indicator_data_structure():
    """验证indicator_data表的字段结构"""
    try:
        # 连接数据库
        conn = duckdb.connect('data/stock_data_new.duckdb')
        
        # 获取表结构信息
        result = conn.execute('PRAGMA table_info(indicator_data)').fetchall()
        
        print('indicator_data表结构验证:')
        print(f'字段数量: {len(result)}')
        print('\n字段列表:')
        print('-' * 50)
        
        for i, row in enumerate(result, 1):
            field_name = row[1]
            field_type = row[2]
            is_nullable = 'NULL' if row[3] == 0 else 'NOT NULL'
            print(f'{i:2d}. {field_name:<30} {field_type:<15} {is_nullable}')
        
        # 验证关键字段是否存在
        field_names = [row[1] for row in result]
        
        key_fields = [
            'code', 'day', 'eps', 'roe', 'roa', 'roic',
            'gross_profit_margin', 'net_profit_margin', 'operating_profit_margin',
            'current_ratio', 'quick_ratio', 'debt_to_assets', 'debt_to_equity',
            'inventory_turnover', 'receivable_turnover', 'total_assets_turnover',
            'inc_revenue_year_on_year', 'inc_profit_year_on_year',
            'operating_cash_flow_per_share', 'cash_flow_per_share',
            'book_to_market_ratio', 'earnings_yield', 'capitalization_ratio',
            'du_return_on_equity', 'du_equity_multiplier'
        ]
        
        print('\n关键字段验证:')
        print('-' * 50)
        missing_fields = []
        for field in key_fields:
            if field in field_names:
                print(f'✓ {field}')
            else:
                print(f'✗ {field} (缺失)')
                missing_fields.append(field)
        
        if missing_fields:
            print(f'\n⚠️  发现 {len(missing_fields)} 个缺失字段')
        else:
            print('\n🎉 所有关键字段验证通过!')
        
        conn.close()
        return len(result), missing_fields
        
    except Exception as e:
        print(f'验证过程中出现错误: {e}')
        return 0, []

if __name__ == '__main__':
    verify_indicator_data_structure()