#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：修改price_data表中的北交所股票代码
将旧的北交所股票代码更新为新代码

使用方法:
python scripts/update_bj_codes_in_price_data.py [--dry-run] [--batch-size 1000]

参数说明:
--dry-run: 仅预览要修改的代码，不实际执行更新
--batch-size: 批量处理大小，默认1000条记录
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from duckdb_impl import DuckDBDatabase
from common.codes_utils import load_bj_code_mapping, convert2new_bj_code
from config import Config


def get_bj_stocks_in_price_data(db):
    """获取price_data表中所有的北交所股票代码"""
    sql = """
    SELECT DISTINCT code 
    FROM price_data 
    WHERE code LIKE '%.BJ'
    ORDER BY code
    """
    
    result = db.conn.execute(sql).fetchall()
    return [row[0] for row in result]


def get_code_update_mapping(bj_codes, mapping):
    """生成需要更新的代码映射"""
    update_mapping = {}
    
    for old_code in bj_codes:
        new_code = convert2new_bj_code(old_code, mapping)
        if new_code != old_code:
            update_mapping[old_code] = new_code
    
    return update_mapping


def count_records_by_code(db, code):
    """统计指定股票代码的记录数"""
    sql = "SELECT COUNT(*) FROM price_data WHERE code = ?"
    result = db.conn.execute(sql, [code]).fetchone()
    return result[0] if result else 0


def update_price_data_codes(db, update_mapping, batch_size=1000, dry_run=False):
    """批量更新price_data表中的股票代码"""
    
    total_updated = 0
    
    for old_code, new_code in update_mapping.items():
        print(f"\n处理股票代码: {old_code} -> {new_code}")
        
        # 统计该代码的记录数
        record_count = count_records_by_code(db, old_code)
        print(f"  找到 {record_count} 条记录")
        
        if record_count == 0:
            print(f"  跳过：没有找到代码 {old_code} 的记录")
            continue
        
        if dry_run:
            print(f"  [预览模式] 将更新 {record_count} 条记录")
            total_updated += record_count
            continue
        
        # 检查新代码是否已存在
        new_code_count = count_records_by_code(db, new_code)
        if new_code_count > 0:
            print(f"  警告：新代码 {new_code} 已存在 {new_code_count} 条记录")
            user_input = input(f"  是否继续更新？这可能导致数据重复 (y/N): ")
            if user_input.lower() != 'y':
                print(f"  跳过更新 {old_code}")
                continue
        
        # 执行更新
        try:
            update_sql = "UPDATE price_data SET code = ? WHERE code = ?"
            db.conn.execute(update_sql, [new_code, old_code])
            
            # 验证更新结果
            updated_count = count_records_by_code(db, new_code)
            remaining_count = count_records_by_code(db, old_code)
            
            print(f"  ✓ 更新完成：{new_code} 现有 {updated_count} 条记录")
            if remaining_count > 0:
                print(f"  警告：{old_code} 仍有 {remaining_count} 条记录未更新")
            
            total_updated += record_count
            
        except Exception as e:
            print(f"  ✗ 更新失败：{e}")
    
    return total_updated


def main():
    parser = argparse.ArgumentParser(description='更新price_data表中的北交所股票代码')
    parser.add_argument('--dry-run', action='store_true', 
                       help='仅预览要修改的代码，不实际执行更新')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='批量处理大小，默认1000条记录')
    
    args = parser.parse_args()
    
    print("=== 北交所股票代码更新脚本 ===")
    print(f"执行模式: {'预览模式' if args.dry_run else '实际更新'}")
    print(f"批量大小: {args.batch_size}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 初始化数据库连接
        config = Config()
        db = DuckDBDatabase(config.database.path)
        db.connect()
        print(f"✓ 数据库连接成功: {config.database.path}")
        
        # 加载北交所代码映射
        print("\n1. 加载北交所代码映射...")
        mapping = load_bj_code_mapping()
        if not mapping:
            print("✗ 未找到北交所代码映射文件或映射为空")
            print("请确保 D:\\Data\\shared\\BJ_old2new.csv 文件存在且格式正确")
            return 1
        
        print(f"✓ 加载了 {len(mapping)} 个代码映射关系")
        
        # 获取price_data表中的北交所股票
        print("\n2. 查找price_data表中的北交所股票...")
        bj_codes = get_bj_stocks_in_price_data(db)
        print(f"✓ 找到 {len(bj_codes)} 个北交所股票代码")
        
        if not bj_codes:
            print("没有找到需要更新的北交所股票代码")
            return 0
        
        # 生成更新映射
        print("\n3. 生成代码更新映射...")
        update_mapping = get_code_update_mapping(bj_codes, mapping)
        
        if not update_mapping:
            print("没有找到需要更新的代码映射")
            return 0
        
        print(f"✓ 找到 {len(update_mapping)} 个需要更新的代码:")
        for old_code, new_code in update_mapping.items():
            print(f"  {old_code} -> {new_code}")
        
        # 执行更新
        print(f"\n4. {'预览' if args.dry_run else '执行'}代码更新...")
        
        if not args.dry_run:
            confirm = input("确认要执行更新操作吗？(y/N): ")
            if confirm.lower() != 'y':
                print("操作已取消")
                return 0
        
        total_updated = update_price_data_codes(
            db, update_mapping, args.batch_size, args.dry_run
        )
        
        print(f"\n=== 更新完成 ===")
        print(f"{'预计' if args.dry_run else '实际'}更新记录数: {total_updated}")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not args.dry_run:
            print("\n建议执行以下验证步骤:")
            print("1. 检查更新后的数据完整性")
            print("2. 验证新代码的数据是否正确")
            print("3. 运行数据质量检查脚本")
        
    except Exception as e:
        print(f"✗ 脚本执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        if 'db' in locals():
            db.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())