#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户相关操作函数
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
import os
from api import create_api
from services.position_service import PositionService
from services.trade_import_service import TradeImportService
from date_utils import get_trading_days

logger = logging.getLogger(__name__)

def _import_account_data_files(service, file_dir, data_type, account_id, trading_days, overwrite):
    """导入账户数据文件的通用函数（交易/持仓）"""
    imported_count = 0
    error_count = 0

    if not os.path.exists(file_dir):
        return imported_count, error_count

    for trade_date in trading_days:
        date_str = trade_date.strftime('%Y%m%d')
        data_file = os.path.join(file_dir, f"{date_str}.json")

        if os.path.exists(data_file):
            try:
                if data_type == "交易" and overwrite:
                    # 交易数据覆盖导入：先删除旧数据，再按指定文件导入，避免使用硬编码路径
                    try:
                        service.database.delete_user_transactions(account_id, trade_date)
                        logger.debug(f"  已删除账户 {account_id} 在 {date_str} 的旧交易数据")
                    except Exception as e:
                        logger.error(f"  删除旧交易数据失败: 账户 {account_id}, 日期 {date_str}: {e}")
                        error_count += 1
                        # 删除失败时跳过本次导入以避免重复数据
                        continue

                if data_type == "交易":
                    result = service.import_trade_file(data_file)
                    success = result.get('success', False)
                    imported_num = result.get('imported_count', 0)

                    if success:
                        logger.info(f"  {data_type}数据导入成功: {date_str} ({imported_num} 条记录)")
                        imported_count += imported_num
                    else:
                        logger.error(f"  {data_type}数据导入失败: {date_str}")
                        for error in result.get('errors', []):
                            logger.error(f"    - {error}")
                        error_count += 1
                else:
                    # 持仓结果结构与交易不同：没有 success/imported_count 字段
                    result = service.import_position_file(data_file, overwrite=overwrite)
                    success_positions = result.get('success_positions', 0)
                    failed_positions = result.get('failed_positions', 0)
                    total_positions = result.get('total_positions', success_positions + failed_positions)

                    if failed_positions == 0:
                        logger.info(f"  {data_type}数据导入成功: {date_str} (成功 {success_positions} / 共 {total_positions})")
                        imported_count += success_positions
                    else:
                        logger.error(f"  {data_type}数据导入存在失败: {date_str} (成功 {success_positions} / 失败 {failed_positions} / 共 {total_positions})")
                        # 兼容原有按文件计数的错误统计方式
                        error_count += 1
            except Exception as e:
                logger.error(f"  导入{data_type}文件失败 {data_file}: {e}")
                error_count += 1
        else:
            logger.debug(f"  {data_type}文件不存在: {data_file}")

    return imported_count, error_count

def action_import_trades(args):
    """导入交易记录"""
    api = create_api(args.db_path)
    try:
        api.initialize()
        
        if args.file:
            # 导入单个文件
            logger.info(f"开始导入交易文件: {args.file}")
            result = api.import_trade_file(args.file)
            
            if result['success']:
                logger.info(f"文件导入成功: {args.file}")
                logger.info(f"账户ID: {result['account_id']}, 交易日期: {result['trade_date']}")
                logger.info(f"总交易数: {result['total_trades']}, 导入成功: {result['imported_count']}")
                if result['error_count'] > 0:
                    logger.warning(f"导入过程中有 {result['error_count']} 个错误:")
                    for error in result['errors']:
                        logger.warning(f"  - {error}")
            else:
                logger.error(f"文件导入失败: {args.file}")
                for error in result['errors']:
                    logger.error(f"  - {error}")
                    
        elif args.directory:
            # 导入目录下所有文件
            logger.info(f"开始导入目录下的交易文件: {args.directory}")
            result = api.import_trade_directory(args.directory)
            
            if result['success']:
                logger.info(f"目录导入完成: {args.directory}")
                logger.info(f"处理文件数: {result['processed_files']}/{result['total_files']}")
                logger.info(f"总交易数: {result['total_trades']}, 导入成功: {result['imported_count']}")
                
                if result['error_count'] > 0:
                    logger.warning(f"导入过程中有 {result['error_count']} 个错误")
                    
                # 显示每个文件的详细结果
                for file_result in result['file_results']:
                    if file_result['success']:
                        logger.info(f"  ✓ {file_result['file_path']}: {file_result['imported_count']} 条记录")
                    else:
                        logger.error(f"  ✗ {file_result['file_path']}: 导入失败")
                        for error in file_result['errors']:
                            logger.error(f"    - {error}")
            else:
                logger.error(f"目录导入失败: {args.directory}")
                
        else:
            logger.error("请指定要导入的文件或目录")
            
    except Exception as e:
        logger.error(f"交易记录导入失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_query_trades(args):
    """查询交易记录"""
    api = create_api(args.db_path)
    try:
        api.initialize()
        
        # 解析日期参数
        trade_date = None
        start_date = None
        end_date = None
        
        if args.trade_date:
            trade_date = datetime.strptime(args.trade_date, '%Y-%m-%d').date()
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        # 查询交易记录
        df = api.get_user_transactions(
            user_id=args.user_id,
            stock_code=args.stock_code,
            trade_date=trade_date,
            strategy_id=args.strategy_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.info("未找到符合条件的交易记录")
        else:
            logger.info(f"找到 {len(df)} 条交易记录:")
            print(df.to_string(index=False))
            
            if args.output:
                df.to_csv(args.output, index=False, encoding='utf-8-sig')
                logger.info(f"结果已保存到: {args.output}")
                
    except Exception as e:
        logger.error(f"查询交易记录失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_import_positions(args):
    """导入持仓数据"""
    api = create_api(args.db_path)
    try:
        api.initialize()
        
        if args.file:
            # 导入单个文件
            logger.info(f"开始导入持仓文件: {args.file}")
            result = api.import_position_file(args.file, overwrite=getattr(args, 'overwrite', False))
            
            logger.info(f"文件导入完成: {args.file}")
            logger.info(f"总持仓数: {result['total_positions']}, 导入成功: {result['success_positions']}, 失败: {result['failed_positions']}")
            
            if result['failed_positions'] > 0:
                logger.warning("导入过程中有失败的记录，请检查日志")
            success = result['failed_positions'] == 0
            logger.info(f"导入结果: 成功{result['success_positions']}, 失败{result['failed_positions']}")
            
        elif args.directory:
            # 导入目录下所有文件
            logger.info(f"导入目录: {args.directory}")

            
            
            directory_path = Path(args.directory)
            if not directory_path.exists():
                logger.error(f"目录不存在: {args.directory}")
                return
            
            # 遍历目录结构，查找持仓文件
            imported_count = 0
            failed_count = 0
            
            # 支持的文件模式: account_id/account_positions/YYYYMMDD.json
            for account_dir in directory_path.iterdir():
                if not account_dir.is_dir():
                    continue
                    
                positions_dir = account_dir / "account_positions"
                if not positions_dir.exists():
                    continue
                    
                logger.info(f"处理账户目录: {account_dir.name}")
                
                for json_file in positions_dir.glob("*.json"):
                     try:
                         logger.info(f"导入文件: {json_file}")
                         result = position_service.import_position_file(str(json_file), overwrite=args.overwrite)
                         if result['failed_positions'] == 0:
                             imported_count += 1
                             logger.info(f"成功导入: {json_file.name}, 持仓记录: {result['success_positions']}")
                         else:
                             failed_count += 1
                             logger.warning(f"导入失败: {json_file.name}, 失败记录: {result['failed_positions']}")
                     except Exception as e:
                         failed_count += 1
                         logger.error(f"导入文件 {json_file} 失败: {e}")
            
            logger.info(f"导入完成 - 成功: {imported_count}, 失败: {failed_count}")
            success = failed_count == 0
        
        if success:
            logger.info("持仓数据导入完成")
        else:
            logger.error("持仓数据导入过程中出现错误")
            
    except Exception as e:
        logger.error(f"导入持仓数据失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()


def action_positions_summary(args):
    """查询持仓汇总"""
    api = create_api(args.db_path)
    try:
        api.initialize()
        
        # 解析日期参数
        end_date = None
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        # 查询持仓汇总
        df = api.get_user_positions_summary(args.user_id, end_date)
        
        if df.empty:
            logger.info(f"用户 {args.user_id} 没有持仓记录")
        else:
            logger.info(f"用户 {args.user_id} 的持仓汇总 ({len(df)} 只股票):")
            print(df.to_string(index=False))
            
            if args.output:
                df.to_csv(args.output, index=False, encoding='utf-8-sig')
                logger.info(f"结果已保存到: {args.output}")
                
    except Exception as e:
        logger.error(f"查询持仓汇总失败: {e}")
        logging.exception("详细错误信息:")
    finally:
        api.close()

def action_import_account_data_daily(args):
    """按天增量导入账户交易和持仓数据"""
    api = create_api(args.db_path)
    try:
        api.initialize()
        
        # 设置数据路径
        data_path = args.data_path or "D:/Users/Jack/myqmt_admin/data/account"
        if not os.path.exists(data_path):
            logger.error(f"数据路径不存在: {data_path}")
            return
        
        # 获取要导入的日期列表
        if not args.dates:
            logger.error("必须指定要导入的日期")
            return
        
        # 验证日期格式
        import_dates = []
        for date_str in args.dates:
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d').date()
                import_dates.append(date_obj)
            except ValueError:
                logger.error(f"日期格式错误: {date_str}，应为YYYYMMDD格式")
                return
        
        # 获取账户列表
        account_dirs = []
        if args.account_ids:
            # 指定账户
            for account_id in args.account_ids:
                account_dir = os.path.join(data_path, account_id)
                if os.path.exists(account_dir):
                    account_dirs.append((account_id, account_dir))
                else:
                    logger.warning(f"账户目录不存在: {account_dir}")
        else:
            # 所有账户
            for item in os.listdir(data_path):
                account_dir = os.path.join(data_path, item)
                if os.path.isdir(account_dir):
                    account_dirs.append((item, account_dir))
        
        if not account_dirs:
            logger.error("没有找到可用的账户目录")
            return
        
        logger.info(f"开始增量导入，处理 {len(account_dirs)} 个账户，指定日期: {[d.strftime('%Y%m%d') for d in import_dates]}")
        
        # 批量获取交易日（优化性能）
        if import_dates:
            min_date = min(import_dates)
            max_date = max(import_dates)
            # 获取日期范围内的所有交易日
            all_trading_days_str = get_trading_days(min_date.strftime('%Y%m%d'), max_date.strftime('%Y%m%d'))
            all_trading_days_set = set(all_trading_days_str)
            
            # 过滤出指定日期中的交易日
            trading_days = []
            for date_obj in import_dates:
                date_str = date_obj.strftime('%Y%m%d')
                if date_str in all_trading_days_set:
                    trading_days.append(date_obj)
                else:
                    logger.warning(f"跳过非交易日: {date_str}")
        else:
            trading_days = []
        
        if not trading_days:
            logger.info("指定日期中没有交易日")
            return
        
        logger.info(f"需要处理的交易日: {[d.strftime('%Y%m%d') for d in trading_days]}")
        
        # 创建服务实例（避免在循环中重复创建）
        trade_service = TradeImportService(api.db.db)
        position_service = PositionService(api.db.db)
        
        total_imported = 0
        total_errors = 0
        
        # 处理每个账户
        for account_id, account_dir in account_dirs:
            logger.info(f"\n处理账户: {account_id}")
            
            # 导入交易数据
            trades_dir = os.path.join(account_dir, "trades_orders")
            trade_imported, trade_errors = _import_account_data_files(
                trade_service, trades_dir, "交易",
                account_id, trading_days, args.overwrite
            )
            total_imported += trade_imported
            total_errors += trade_errors
            
            # 导入持仓数据
            positions_dir = os.path.join(account_dir, "account_positions")
            position_imported, position_errors = _import_account_data_files(
                position_service, positions_dir, "持仓",
                account_id, trading_days, args.overwrite
            )
            total_imported += position_imported
            total_errors += position_errors
        
        logger.info(f"\n增量导入完成:")
        logger.info(f"  总导入记录数: {total_imported}")
        logger.info(f"  错误数: {total_errors}")
        
    except Exception as e:
        logger.error(f"增量导入失败: {e}")
    finally:
        api.close()