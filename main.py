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

7. 更新股票列表:
   python main.py update-stock-list

8. 数据质量检查:
   python main.py check-data
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
from date_utils import get_trading_days, get_last_trading_day

# 支持的数据表列表（从DataType类获取）
SUPPORTED_TABLES = [
    DataType.STOCK_LIST,
    DataType.PRICE_DATA,
    DataType.BALANCE_SHEET,
    DataType.INCOME_STATEMENT,
    DataType.CASHFLOW_STATEMENT,
    DataType.INDICATOR_DATA,
    DataType.FUNDAMENTAL_DATA
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
        logger.info("正在更新股票列表...")
        if api.update_stock_list(force_update=True):
            logger.info("股票列表更新完成")
        else:
            logger.warning("警告: 股票列表更新失败")
        
        logger.info("\n初始化完成!")
        
    except Exception as e:
        logger.error(f"初始化失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_daily(args):
    """
    每日数据更新
    """
    logger.info("开始每日数据更新...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        api.initialize()
        
        # 获取股票列表
        if getattr(args, 'bj_stocks', False):
            # 获取北交所股票列表
            all_stock_codes = api.get_stock_list()
            if not all_stock_codes:
                logger.error("错误: 数据库中没有股票列表，请先运行 init 命令")
                return
            # 过滤出北交所股票
            stock_codes = [code for code in all_stock_codes if code.endswith('.BJ')]
            if not stock_codes:
                logger.error("错误: 数据库中没有北交所股票")
                return
            logger.info(f"找到 {len(stock_codes)} 只北交所股票")
        else:
            # 获取非北交所股票列表（原有逻辑）
            stock_codes = api.get_stock_list()
            if not stock_codes:
                logger.error("错误: 数据库中没有股票列表，请先运行 init 命令")
                return
        
        logger.info(f"准备进行 {len(stock_codes)} 只股票的每日更新")
        
        # 创建UpdateService实例并执行每日更新
        from services.update_service import UpdateService
        update_service = UpdateService(api.db, api.data_sources)
        result = update_service.daily_update()
        
        if result:
            logger.info("每日更新完成!")
            logger.info(f"更新时间: {result.get('update_time')}")
            if result.get('market_update'):
                logger.info(f"市场数据更新: {result['market_update'].get('message', '完成')}")
            if result.get('financial_update'):
                logger.info(f"财务数据更新: {result['financial_update'].get('message', '完成')}")
        else:
            logger.error("每日更新失败")
        
    except Exception as e:
        logger.error(f"每日更新失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_update_history(args):
    """
    历史数据更新
    """
    logger.info("开始历史数据更新...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        api.initialize()
        
        # 获取股票列表
        all_stock_codes = api.get_stock_list()
        if not all_stock_codes:
            logger.error("错误: 数据库中没有股票列表，请先运行 init 命令")
            return
        
        # 根据--bj-stocks选项过滤股票
        if getattr(args, 'bj_stocks', False):
            # 只更新北交所股票
            stock_codes = [code for code in all_stock_codes if code.endswith('.BJ')]
            logger.info(f"找到 {len(stock_codes)} 只北交所股票")
            if not stock_codes:
                logger.warning("没有找到北交所股票")
                return
        else:
            # 更新所有股票（排除北交所）
            stock_codes = all_stock_codes
        
        # 解析要更新的表
        tables_to_update = []
        if args.tables:
            tables_to_update = [t.strip() for t in args.tables.split(',')]
        else:
            tables_to_update = SUPPORTED_TABLES
        
        # 验证表名
        invalid_tables = [t for t in tables_to_update if t not in SUPPORTED_TABLES]
        if invalid_tables:
            logger.error(f"错误: 不支持的表名: {invalid_tables}")
            logger.info(f"支持的表名: {SUPPORTED_TABLES}")
            return
        
        logger.info(f"准备更新表: {tables_to_update}")
        logger.info(f"股票数量: {len(stock_codes)}")
        
        # 批量更新数据
        for table_name in tables_to_update:
            logger.info(f"\n正在更新 {table_name} 数据...")
            
            if table_name == DataType.STOCK_LIST:
                result = api.update_stock_list(force_update=True)
            elif table_name == DataType.PRICE_DATA:
                # 历史补齐：强制全量
                if getattr(args, 'bj_stocks', False):
                    # 北交所股票使用tushare数据源更新
                    result = api.update_bj_stocks_data(stock_codes, data_types=[DataType.PRICE_DATA], force_full_update=True)
                else:
                    # 非北交所股票使用默认数据源更新
                    result = api.update_data(stock_codes, data_types=[DataType.PRICE_DATA], force_full_update=True)
            elif table_name in ['balance_sheet', 'income_statement', 'cashflow_statement', 'financial_indicators']:
                # 历史补齐：强制全量（财务类）
                result = api.update_data(stock_codes, data_types=['financial'], force_full_update=True)
            else:
                logger.warning(f"警告: 暂不支持更新表 {table_name}")
                continue
            
            if isinstance(result, dict) and result.get('success'):
                logger.info(f"{table_name} 数据更新完成")
            elif result:
                logger.info(f"{table_name} 数据更新完成")
            else:
                logger.error(f"{table_name} 数据更新失败")
        
        logger.info("\n历史数据更新完成!")
        
    except Exception as e:
        logger.error(f"历史数据更新失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_update_table(args):
    """
    更新指定数据表
    """
    if not args.table:
        logger.error("错误: 请指定要更新的表名")
        return
    
    if args.table not in SUPPORTED_TABLES:
        logger.error(f"错误: 不支持的表名: {args.table}")
        logger.info(f"支持的表名: {SUPPORTED_TABLES}")
        return
    
    logger.info(f"开始更新表: {args.table}")
    
    api = create_api(db_path=args.db_path)
    
    try:
        api.initialize()
        
        if args.table == DataType.STOCK_LIST:
            result = api.update_stock_list(force_update=True)
            if result:
                logger.info("股票列表更新完成")
            else:
                logger.error("股票列表更新失败")
        else:
            # 获取股票列表
            stock_codes = api.get_stock_list()
            if not stock_codes:
                logger.error("错误: 数据库中没有股票列表，请先运行 init 命令")
                return
            
            logger.info(f"准备更新 {len(stock_codes)} 只股票的 {args.table} 数据")
            
            # 计算“日级别补齐”的截止日期：15点后为今天，否则为昨天
            now = datetime.now()
            end_date_cutoff = now.date() if now.hour >= 15 else (now.date() - timedelta(days=1))
            
            if args.table == DataType.PRICE_DATA:
                # 日级别增量补齐：从库里最新日期+1到 end_date_cutoff
                result = api.update_data(stock_codes, data_types=[DataType.PRICE_DATA], end_date=end_date_cutoff)
            elif args.table in ['balance_sheet', 'income_statement', 'cashflow_statement', 'financial_indicators']:
                # 财务类也按同一截止日策略（不强制全量）
                result = api.update_data(stock_codes, data_types=['financial'], end_date=end_date_cutoff)
            else:
                logger.warning(f"警告: 暂不支持更新表 {args.table}")
                return
            
            if isinstance(result, dict) and result.get('success'):
                logger.info(f"{args.table} 数据更新完成")
            elif result:
                logger.info(f"{args.table} 数据更新完成")
            else:
                logger.error(f"{args.table} 数据更新失败")
        
    except Exception as e:
        logger.error(f"更新表失败: {e}")
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
    
    api = create_api(db_path=args.db_path)
    
    try:
        # 直接使用数据库管理器执行查询
        result = api.db_manager.execute_query(args.sql)
        
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

def action_update_stock_list(args):
    """
    更新股票列表
    """
    logger.info("开始更新股票列表...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        api.initialize()
        
        result = api.update_stock_list(force_update=True)
        if result:
            logger.info("股票列表更新完成")
            
            # 显示更新后的统计信息
            stocks = api.get_stock_list()
            logger.info(f"当前股票总数: {len(stocks)}")
            
            market_stats = api.get_market_statistics()
            if market_stats:
                logger.info("市场分布:")
                for market, count in market_stats.items():
                    logger.info(f"  {market}: {count} 只")
        else:
            logger.error("股票列表更新失败")
        
    except Exception as e:
        logger.error(f"更新股票列表失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_check_data(args):
    """
    数据质量检查
    """
    logger.info("开始数据质量检查...")
    
    api = create_api(db_path=args.db_path)
    
    try:
        # 获取数据库信息
        db_info = api.get_database_info()
        
        logger.info("\n=== 数据完整性检查 ===")
        
        if db_info and 'tables' in db_info:
            for table_info in db_info['tables']:
                if isinstance(table_info, dict) and 'table_name' in table_info:
                    table_name = table_info['table_name']
                    record_count = table_info.get('record_count', 0)
                    
                    logger.info(f"{table_name}: {record_count:,} 条记录")
                    
                    # 检查是否有数据
                    if record_count == 0:
                        logger.warning(f"  警告: {table_name} 表为空")
                    
                    # 对于价格数据表，抽查200只股票从默认开始日期到最近交易日的数据完整性
                    if table_name == DataType.PRICE_DATA and record_count > 0:
                        try:
                            # 计算最近一个交易日（含当天为交易日的情况）
                            today_str = datetime.now().strftime("%Y%m%d")
                            last_trade_str = get_last_trading_day(today_str, n=0)
                            
                            # 获取默认历史起始日期（从配置/更新服务），并格式化为YYYYMMDD
                            default_start_date = None
                            try:
                                if getattr(api, "update_service", None) and getattr(api.update_service, "default_history_start_date", None):
                                    d = api.update_service.default_history_start_date
                                    default_start_date = d.strftime("%Y%m%d")
                            except Exception:
                                pass
                            if not default_start_date:
                                default_start_date = "20190101"
                            
                            # 从 stock_list 表随机抽取200只活跃股票（尽量带上上市日期用于校正）
                            sample_sql = """
                                SELECT code, start_date, end_date
                                FROM stock_list
                                WHERE (end_date IS NULL OR end_date > CURRENT_DATE)
                                ORDER BY RANDOM()
                                LIMIT 200
                            """
                            try:
                                sample_df = api.query(sample_sql)
                            except Exception:
                                sample_df = pd.DataFrame()
                            
                            # 兜底：如果 stock_list 不可用，则从 price_data 中取200个代码
                            if sample_df is None or sample_df.empty:
                                fallback_sql = "SELECT DISTINCT code FROM price_data ORDER BY code LIMIT 200"
                                sample_df = api.query(fallback_sql)
                                # 填充缺失的日期列
                                if not sample_df.empty:
                                    sample_df["start_date"] = None
                                    sample_df["end_date"] = None
                            
                            if sample_df is None or sample_df.empty:
                                logger.warning("  无法获取用于抽查的股票列表")
                                continue
                            
                            # 预先准备交易日缓存，减少重复获取
                            expected_days_cache = {}
                            
                            total_checked = 0
                            complete_count = 0
                            incomplete_count = 0
                            
                            for _, row in sample_df.iterrows():
                                code = row["code"]
                                
                                # 计算本次检查的起始日期：默认开始日期与上市日期取较晚者，避免上市前的日期被误判为缺失
                                stock_start = None
                                try:
                                    val = row.get("start_date")
                                    if pd.notna(val) and val:
                                        if hasattr(val, "strftime"):  # date/datetime
                                            stock_start = val.strftime("%Y%m%d")
                                        elif isinstance(val, str):
                                            # 支持多种常见格式
                                            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
                                                try:
                                                    stock_start = datetime.strptime(val, fmt).strftime("%Y%m%d")
                                                    break
                                                except Exception:
                                                    continue
                                except Exception:
                                    stock_start = None
                                
                                start_str = default_start_date
                                if stock_start and stock_start > start_str:
                                    start_str = stock_start
                                
                                # 快速跳过：若 start_str > last_trade_str 则无需检查
                                if start_str > last_trade_str:
                                    continue
                                
                                # 获取该区间的预期交易日
                                if start_str in expected_days_cache:
                                    expected_days = expected_days_cache[start_str]
                                else:
                                    expected_days = get_trading_days(start_str, last_trade_str)
                                    expected_days_cache[start_str] = expected_days
                                
                                expected_set = set(expected_days)
                                if not expected_set:
                                    continue
                                
                                # 查询已有价格数据的交易日（转换为YYYYMMDD方便比对）
                                start_sql_date = f"{start_str[:4]}-{start_str[4:6]}-{start_str[6:8]}"
                                end_sql_date = f"{last_trade_str[:4]}-{last_trade_str[4:6]}-{last_trade_str[6:8]}"
                                data_sql = """
                                    SELECT STRFTIME(day, '%Y%m%d') AS d
                                    FROM price_data
                                    WHERE code = ? AND day >= ? AND day <= ?
                                """
                                data_df = api.query(data_sql, [code, start_sql_date, end_sql_date])
                                existing_set = set(data_df["d"].tolist()) if data_df is not None and not data_df.empty else set()
                                
                                missing = expected_set - existing_set
                                total_checked += 1
                                if not missing:
                                    complete_count += 1
                                else:
                                    incomplete_count += 1
                                    # 仅打印前若干个缺失日期，避免日志过长
                                    some_missing = sorted(list(missing))[:5]
                                    logger.warning(f"  {code} 缺失 {len(missing)} 个交易日，例如: {', '.join(some_missing)} ...")
                            
                            logger.info(f"  PRICE_DATA 抽查完成: 共检查 {total_checked} 只，完整 {complete_count}，不完整 {incomplete_count}")
                        except Exception as e:
                            logger.error(f"  抽查价格数据完整性失败: {e}")
        
        logger.info("\n数据质量检查完成")
        
    except Exception as e:
        logger.error(f"数据质量检查失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='股票数据存储平台')
    parser.add_argument('--db-path', default='data/stock_data.duckdb', help='数据库文件路径')
    parser.add_argument('--log-level', default='DEBUG', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    
    subparsers = parser.add_subparsers(dest='action', help='可用命令')
    
    # init命令
    init_parser = subparsers.add_parser('init', help='初始化数据库')
    
    # daily命令
    daily_parser = subparsers.add_parser('daily', help='每日数据更新')
    
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
    }
    
    action_func = action_map.get(args.action)
    if action_func:
        action_func(args)
    else:
        logger.error(f"未知命令: {args.action}")
        parser.print_help()

if __name__ == '__main__':
    main()