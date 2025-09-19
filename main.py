#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据存储平台主程序

专注于数据存储、查询、更新和维护功能

使用方法:
1. 初始化数据库:
   python main.py init

2. 每日数据更新:
   python main.py daily

3. 历史数据更新:
   python main.py update-history --tables stock_list,price_data

4. 更新指定数据表:
   python main.py update-table --table price_data

5. 查看数据库信息:
   python main.py info

6. 执行SQL查询:
   python main.py query --sql "SELECT * FROM stock_list LIMIT 10"
   python main.py query --sql "SELECT * FROM user_transactions WHERE trade_date = '2025-09-15'"

7. 更新股票列表:
   python main.py update-stock-list

8. 数据质量检查:
   python main.py check-data                                    # 快速检查（默认）
   python main.py check-data --level standard                   # 标准检查
   python main.py check-data --tables price_data,user_positions # 检查指定表
   python main.py check-data --output-report logs/quality.json  # 生成详细报告

9. 导入持仓记录:
   python main.py import-positions --directory data/account
   python main.py import-positions --file data/account/123456789/account_positions/20250901.json
   python main.py import-positions --directory data/account --overwrite

10. 导入交易记录:
    python main.py import-trades --directory data/account
    python main.py import-trades --file data/account/123456789/trades.csv

11. 查询持仓汇总:
    python main.py positions-summary --user-id 123456789

12. 按天增量导入账户数据:
    python main.py import-account-data-daily --dates 20250903
    python main.py import-account-data-daily --dates 20250903 20250904 20250905 --account-ids 123456789 test_account
    python main.py import-account-data-daily --dates 20250903 --overwrite
    python main.py import-account-data-daily --dates 20250901 20250902 20250903 --data-path /path/to/data/account
"""

# 模块顶部导入区域（添加 logger 定义）
import argparse
import sys
import os
from datetime import datetime, timedelta
import logging
import pandas as pd
logger = logging.getLogger(__name__)

from api import StockDataAPI, create_api
from config import get_config
from data_source import DataType
from services.update_service import UpdateService
from services.position_service import PositionService
from date_utils import get_trading_days, get_last_trading_day

# 导入从其他文件移动过来的函数
from user_actions import action_import_trades, action_import_positions, action_positions_summary, action_import_account_data_daily
from stock_actions import action_daily, action_update_history, action_update_table, action_update_stock_list, action_check_data

# 支持的数据表列表（从DataType类获取）
SUPPORTED_TABLES = [
    DataType.STOCK_LIST,
    DataType.PRICE_DATA,
    DataType.BALANCE_SHEET,
    DataType.INCOME_STATEMENT,
    DataType.CASHFLOW_STATEMENT,
    DataType.INDICATOR_DATA,
    DataType.VALUATION_DATA
]

def setup_logging(log_level='INFO'):
    """
    设置日志配置
    """
    # 确保日志目录存在，避免 FileHandler 在目录不存在时抛错
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler('logs/stock_data_main.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def action_init(args):
    """初始化数据库"""
    logger.info("开始初始化数据库...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        api.initialize()  # initialize()内部会处理数据源注册
        logger.info("数据库初始化完成")
        
        # 更新股票列表
        #logger.info("正在更新股票列表...")
        #if api.update_stock_list(force_update=True):
        #    logger.info("股票列表更新完成")
        #else:
        #    logger.warning("警告: 股票列表更新失败")
        
        logger.info("\n初始化完成!")
        
    except Exception as e:
        logger.error(f"初始化失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()


def action_info(args):
    """
    显示数据库信息
    """
    logger.info("获取数据库信息...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        # 获取数据库基本信息
        db_info = api.get_database_info()
        
        logger.info("\n=== 数据库信息 ===")
        logger.info(f"数据库文件: {args.db_path}")
        
        if db_info:
            logger.info("\n=== 表信息 ===")
            if 'tables' in db_info:
                for table_info in db_info['tables']:
                    if isinstance(table_info, dict) and 'table_name' in table_info:
                        logger.info(f"{table_info['table_name']}:")
                        if 'record_count' in table_info:
                            logger.info(f"  记录数: {table_info['record_count']:,}")
                        if 'columns' in table_info:
                            logger.info(f"  列数: {len(table_info['columns'])}")
                        logger.info("")  # 空行
                
                if 'total_records' in db_info:
                    logger.info(f"数据库总记录数: {db_info['total_records']:,}")
            else:
                logger.info("未找到表信息")
            
            # 获取股票列表
            try:
                stocks = api.get_stock_list()
                logger.info(f"\n=== 股票信息 ===")
                logger.info(f"总股票数: {len(stocks)}")
                
                # 显示前5个股票代码作为示例
                if stocks:
                    logger.info(f"示例股票代码: {', '.join(stocks[:5])}")
                    
                # 获取市场统计信息
                market_stats = api.get_market_statistics()
                if market_stats:
                    logger.info("\n=== 市场分布 ===")
                    for market, count in market_stats.items():
                        logger.info(f"  {market}: {count} 只")
                        
            except Exception as e:
                logger.error(f"获取股票信息失败: {e}")
        else:
            logger.info("未能获取到数据库信息")
        
    except Exception as e:
        logger.error(f"获取数据库信息失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_query(args):
    """
    执行SQL查询
    """
    if not args.sql:
        logger.error("错误: 请提供SQL查询语句")
        return
    
    # 检查SQL语句是否为只读查询
    sql_upper = args.sql.strip().upper()
    if not sql_upper.startswith('SELECT'):
        logger.error("错误: 只允许执行SELECT查询语句")
        return
    
    # 检查是否包含危险的SQL关键词
    dangerous_keywords = ['DELETE', 'UPDATE', 'INSERT', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'REPLACE']
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            logger.error(f"错误: 查询语句不能包含 {keyword} 操作")
            return
    
    api = create_api(db_path=args.db_path)
    
    try:
        # 使用API的query方法执行查询
        api.initialize()
        result = api.query(args.sql)
        
        logger.info("\n=== 查询结果 ===")
        if len(result) > 0:
            logger.info(result.to_string(index=False))
            logger.info(f"\n共 {len(result)} 行记录")
        else:
            logger.info("查询结果为空")
        
        if args.output:
            result.to_csv(args.output, index=False)
            logger.info(f"\n结果已保存到: {args.output}")
            
    except Exception as e:
        logger.error(f"查询失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_check_data(args):
    """执行数据质量检查"""
    try:
        from data_quality import run_quality_check

        # 解析表列表
        tables = None
        if args.tables:
            tables = [t.strip() for t in args.tables.split(',')]

        # 执行质量检查
        report = run_quality_check(
            level=args.level,
            tables=tables,
            output_report=args.output_report
        )

        # 根据问题严重程度设置退出码
        if report.critical_issues > 0:
            return 1  # 有严重问题
        elif report.warning_issues > 0:
            return 2  # 只有警告问题
        else:
            return 0  # 无问题

    except Exception as e:
        logger.error(f"数据质量检查失败: {e}")
        logging.exception("详细错误信息:")
        return 3  # 检查执行失败

def main():
    """
    主函数
    """
    # 获取默认数据库路径
    config = get_config()
    default_db_path = config.database.path
    
    parser = argparse.ArgumentParser(description='股票数据存储平台')
    parser.add_argument('--db-path', default=default_db_path, help='数据库文件路径')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    
    subparsers = parser.add_subparsers(dest='action', help='可用命令')
    
    # init命令
    init_parser = subparsers.add_parser('init', help='初始化数据库')
    
    # daily命令
    daily_parser = subparsers.add_parser('daily', help='每日数据更新')
    daily_parser.add_argument('--tables', help='要更新的表名，用逗号分隔，如: price_data,financial 或 market,financial')
    daily_parser.add_argument('--bj-stocks', action='store_true', help='只更新北交所股票数据')
    
    # update-history命令
    update_history_parser = subparsers.add_parser('update-history', help='历史数据更新')
    update_history_parser.add_argument('--tables', help='要更新的表名，用逗号分隔，如: stock_list,price_data')
    update_history_parser.add_argument('--bj-stocks', action='store_true', help='更新北交所股票数据（默认使用tushare数据源）')
    
    # update-table命令
    update_table_parser = subparsers.add_parser('update-table', help='更新指定数据表')
    update_table_parser.add_argument('--table', required=True, choices=SUPPORTED_TABLES, help='要更新的表名')
    
    # info命令
    info_parser = subparsers.add_parser('info', help='显示数据库信息')
    
    # query命令
    query_parser = subparsers.add_parser('query', help='执行SQL查询')
    query_parser.add_argument('--sql', required=True, help='SQL查询语句')
    query_parser.add_argument('--output', help='输出文件路径')
    
    # update-stock-list命令
    update_stock_list_parser = subparsers.add_parser('update-stock-list', help='更新股票列表')
    
    # check-data命令
    check_data_parser = subparsers.add_parser('check-data', help='数据质量检查')
    check_data_parser.add_argument('--level', choices=['quick', 'standard'], default='quick',
                                   help='检查级别: quick(快速检查,默认) 或 standard(标准检查)')
    check_data_parser.add_argument('--tables', help='要检查的表名，用逗号分隔，如: price_data,user_transactions')
    check_data_parser.add_argument('--output-report', help='输出详细报告文件路径（JSON格式）')
    
    # import-trades命令
    import_trades_parser = subparsers.add_parser('import-trades', help='导入交易记录')
    import_trades_group = import_trades_parser.add_mutually_exclusive_group(required=True)
    import_trades_group.add_argument('--file', help='导入单个交易文件')
    import_trades_group.add_argument('--directory', help='导入目录下所有交易文件')
    
    # import-positions命令
    import_positions_parser = subparsers.add_parser('import-positions', help='导入持仓数据')
    import_positions_group = import_positions_parser.add_mutually_exclusive_group(required=True)
    import_positions_group.add_argument('--file', help='导入单个持仓文件')
    import_positions_group.add_argument('--directory', help='导入目录下所有持仓文件')
    import_positions_parser.add_argument('--overwrite', action='store_true', help='覆盖已存在的数据')
    
    # positions-summary命令
    positions_parser = subparsers.add_parser('positions-summary', help='查询持仓汇总')
    positions_parser.add_argument('--user-id', required=True, help='用户ID')
    positions_parser.add_argument('--end-date', help='截止日期 (YYYY-MM-DD)，默认为最新')
    positions_parser.add_argument('--output', help='输出文件路径')
    
    # import-account-data-daily命令
    account_daily_parser = subparsers.add_parser('import-account-data-daily', help='按天增量导入账户交易和持仓数据')
    account_daily_parser.add_argument('--dates', nargs='+', required=True, help='指定要导入的日期列表，格式为YYYYMMDD，如：20250903 20250904')
    account_daily_parser.add_argument('--account-ids', nargs='+', help='指定账户ID列表，不指定则导入所有账户')
    account_daily_parser.add_argument('--data-path', help='数据源路径，默认为 D:/Users/Jack/myqmt_admin/data/account')
    account_daily_parser.add_argument('--overwrite', action='store_true', help='覆盖已存在的数据')
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.log_level)
    
    # 确保logs目录存在（setup_logging中已保证，这里保留也不影响）
    os.makedirs('logs', exist_ok=True)
    
    if not args.action:
        parser.print_help()
        return
    
    # 执行对应的操作
    action_map = {
        'init': action_init,
        'daily': action_daily,
        'update-history': action_update_history,
        'update-table': action_update_table,
        'info': action_info,
        'query': action_query,
        'update-stock-list': action_update_stock_list,
        'check-data': action_check_data,
        'import-trades': action_import_trades,
        'import-positions': action_import_positions,
        'positions-summary': action_positions_summary,
        'import-account-data-daily': action_import_account_data_daily,
    }
    
    action_func = action_map.get(args.action)
    if action_func:
        action_func(args)
    else:
        logger.error(f"未知命令: {args.action}")
        parser.print_help()

if __name__ == '__main__':
    main()