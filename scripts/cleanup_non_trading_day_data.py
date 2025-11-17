#!/usr/bin/env python3
"""
非交易日数据清理脚本

此脚本用于安全地删除非交易日的用户数据，包括：
1. user_account_info 表中的非交易日数据
2. user_positions 表中的非交易日数据

注意：user_transactions 不需要清理，因为交易本来就不会每天都有

使用方法:
    python scripts/cleanup_non_trading_day_data.py --dry-run     # 预览模式，不实际删除
    python scripts/cleanup_non_trading_day_data.py --execute    # 执行删除
"""

import argparse
import logging
from datetime import datetime
import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from duckdb_impl import DuckDBDatabase
from config import get_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_non_trading_day_data(db):
    """查询非交易日数据"""
    logger.info("查询非交易日数据...")

    # 查询 user_account_info 中的非交易日数据
    account_sql = """
    SELECT ua.user_id, ua.info_date, 'user_account_info' as table_name
    FROM user_account_info ua
    LEFT JOIN (SELECT DISTINCT day FROM price_data) pd ON ua.info_date = pd.day
    WHERE pd.day IS NULL AND ua.info_date >= '2025-05-12'
    ORDER BY ua.info_date, ua.user_id
    """

    # 查询 user_positions 中的非交易日数据
    position_sql = """
    SELECT up.user_id, up.position_date, up.stock_code, 'user_positions' as table_name
    FROM user_positions up
    LEFT JOIN (SELECT DISTINCT day FROM price_data) pd ON up.position_date = pd.day
    WHERE pd.day IS NULL AND up.position_date >= '2025-05-12'
    ORDER BY up.position_date, up.user_id, up.stock_code
    """

    account_data = db.query_data(account_sql)
    position_data = db.query_data(position_sql)

    logger.info(f"发现 user_account_info 非交易日记录: {len(account_data)} 条")
    logger.info(f"发现 user_positions 非交易日记录: {len(position_data)} 条")

    return account_data, position_data

def preview_cleanup(account_data, position_data):
    """预览将要删除的数据"""
    logger.info("\n" + "="*60)
    logger.info("📋 预览将要删除的数据")
    logger.info("="*60)

    if len(account_data) > 0:
        logger.info(f"\n📊 user_account_info 表 ({len(account_data)} 条记录):")
        logger.info("日期范围:")
        dates = sorted(set([row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]) for row in account_data]))
        logger.info(f"  - 首个日期: {dates[0]}")
        logger.info(f"  - 最后日期: {dates[-1]}")
        logger.info(f"  - 涉及日期: {len(dates)} 天")

        users = set([str(row[0]) for row in account_data])
        logger.info(f"  - 涉及用户: {len(users)} 个")
        logger.info(f"  - 用户列表: {', '.join(list(users)[:5])}{'...' if len(users) > 5 else ''}")

        # 显示前几条记录
        logger.info("前5条记录:")
        for i, row in enumerate(account_data[:5]):
            date_str = row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1])
            logger.info(f"  {i+1}. 用户: {row[0]}, 日期: {date_str}")

    if len(position_data) > 0:
        logger.info(f"\n📈 user_positions 表 ({len(position_data)} 条记录):")
        dates = sorted(set([row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]) for row in position_data]))
        logger.info(f"  - 首个日期: {dates[0]}")
        logger.info(f"  - 最后日期: {dates[-1]}")
        logger.info(f"  - 涉及日期: {len(dates)} 天")

        users = set([str(row[0]) for row in position_data])
        stocks = set([str(row[2]) for row in position_data])
        logger.info(f"  - 涉及用户: {len(users)} 个")
        logger.info(f"  - 涉及股票: {len(stocks)} 只")

        # 显示前几条记录
        logger.info("前5条记录:")
        for i, row in enumerate(position_data[:5]):
            date_str = row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1])
            logger.info(f"  {i+1}. 用户: {row[0]}, 日期: {date_str}, 股票: {row[2]}")

def execute_cleanup(db, account_data, position_data):
    """执行数据清理"""
    logger.info("\n" + "="*60)
    logger.info("🗑️  开始执行数据清理")
    logger.info("="*60)

    total_deleted = 0

    try:
        # 清理 user_account_info
        if len(account_data) > 0:
            logger.info(f"\n🔄 清理 user_account_info 表...")
            delete_account_sql = """
            DELETE FROM user_account_info
            WHERE info_date NOT IN (SELECT DISTINCT day FROM price_data)
              AND info_date >= '2025-05-12'
            """

            db.execute(delete_account_sql)
            logger.info(f"✅ 已删除 user_account_info 中 {len(account_data)} 条非交易日记录")
            total_deleted += len(account_data)

        # 清理 user_positions
        if len(position_data) > 0:
            logger.info(f"\n🔄 清理 user_positions 表...")
            delete_position_sql = """
            DELETE FROM user_positions
            WHERE position_date NOT IN (SELECT DISTINCT day FROM price_data)
              AND position_date >= '2025-05-12'
            """

            db.execute(delete_position_sql)
            logger.info(f"✅ 已删除 user_positions 中 {len(position_data)} 条非交易日记录")
            total_deleted += len(position_data)

        logger.info(f"\n🎉 清理完成! 总共删除了 {total_deleted} 条记录")
        logger.info(f"清理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        logger.error(f"❌ 删除过程中发生错误: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="清理用户表中的非交易日数据")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="预览模式，只显示将要删除的数据，不实际执行")
    group.add_argument("--execute", action="store_true", help="执行删除操作")

    args = parser.parse_args()

    logger.info("🚀 启动非交易日数据清理脚本")
    logger.info(f"模式: {'预览' if args.dry_run else '执行删除'}")

    try:
        # 获取数据库连接
        config = get_config()
        db_path = config.database.path
        db = DuckDBDatabase(db_path)
        db.connect()
        logger.info(f"✅ 已连接到数据库: {db_path}")

        # 查询非交易日数据
        account_data, position_data = get_non_trading_day_data(db)

        if len(account_data) == 0 and len(position_data) == 0:
            logger.info("✨ 未发现需要清理的非交易日数据")
            return

        # 预览数据
        preview_cleanup(account_data, position_data)

        if args.dry_run:
            logger.info("\n🔍 这是预览模式，没有实际删除任何数据")
            logger.info("💡 要执行删除，请使用 --execute 参数")
        else:
            # 执行确认
            total_records = len(account_data) + len(position_data)
            logger.info(f"\n⚠️  确认删除 {total_records} 条非交易日数据？")
            logger.info("此操作不可撤销!")

            confirm = input("输入 'YES' 确认执行删除: ")
            if confirm != 'YES':
                logger.info("❌ 操作已取消")
                return

            # 执行清理
            execute_cleanup(db, account_data, position_data)

    except Exception as e:
        logger.error(f"❌ 脚本执行失败: {e}")
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()
            logger.info("📝 数据库连接已关闭")

if __name__ == "__main__":
    main()