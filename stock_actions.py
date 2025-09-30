#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据相关操作函数
"""

import logging
from datetime import datetime, timedelta
from api import create_api
from data_source import DataType
from services.update_service import UpdateService
from common.date_utils import get_trading_days, get_last_trading_day

logger = logging.getLogger(__name__)

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

def action_daily(args):
    """每日数据更新"""
    logger = logging.getLogger(__name__)
    logger.info("开始每日数据更新")

    try:
        # 使用 create_api 创建API实例（推荐方式）
        api = create_api(db_path=args.db_path)
        api.initialize()

        # 解析目标日期参数
        target_date = None
        if args.target_date:
            try:
                from datetime import datetime
                target_date = datetime.strptime(args.target_date, '%Y%m%d').date()
                logger.info(f"指定更新日期: {target_date}")
            except ValueError:
                logger.error(f"日期格式错误: {args.target_date}，应为YYYYMMDD格式，如：20250630")
                return

        # 解析表类型参数
        data_types = None
        if args.tables:
            # 解析用户指定的表类型
            table_list = [t.strip() for t in args.tables.split(',')]
            data_types = []

            for table in table_list:
                if table not in SUPPORTED_TABLES:
                    logger.warning(f"不支持的表类型: {table}")
                    continue

                # 将具体表名映射到数据类型
                if table in ['price_data', 'valuation_data', 'indicator_data', 'mtss_data']:
                    data_types.append(table)

            if not data_types:
                logger.error("没有有效的表类型，使用默认的市场数据更新")
                return

        # 确定交易所类型
        exchange = 'bj' if args.bj_stocks else 'all'

        if target_date:
            logger.info(f"准备进行{'北交所' if args.bj_stocks else '所有'}股票在 {target_date} 的数据更新")
        else:
            logger.info(f"准备进行{'北交所' if args.bj_stocks else '所有'}股票的每日增量更新")

        if data_types:
            logger.info(f"更新数据类型: {', '.join(data_types)}")

        # 通过API层调用更新
        if target_date:
            result = api.daily_update(exchange=exchange, data_types=data_types, target_date=target_date)
        else:
            result = api.daily_update(exchange=exchange, data_types=data_types)
        
        # 处理返回结果
        if result.get('success'):
            update_result = result.get('result', {})
            logger.info("每日更新完成!")
            logger.info(f"更新时间: {update_result.get('update_time')}")
        else:
            logger.error(f"每日更新失败: {result.get('message')}")
        
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

    api.initialize()
    
    # 获取股票列表
    all_stock_codes = api.get_stock_list(exchange='BSE' if args.bj_stocks else None)
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
        elif table_name == DataType.INDICATOR_DATA:
            # 历史补齐：强制全量（技术指标数据）
            if getattr(args, 'bj_stocks', False):
                # 北交所股票使用tushare数据源更新
                result = api.update_bj_stocks_data(stock_codes, data_types=[DataType.INDICATOR_DATA], force_full_update=True)
            else:
                # 非北交所股票使用默认数据源更新
                result = api.update_data(stock_codes, data_types=[DataType.INDICATOR_DATA], force_full_update=True)
        elif table_name == DataType.FUNDAMENTAL_DATA:
            # 历史补齐：强制全量（基本面数据）
            if getattr(args, 'bj_stocks', False):
                # 北交所股票使用tushare数据源更新
                result = api.update_bj_stocks_data(stock_codes, data_types=[DataType.FUNDAMENTAL_DATA], force_full_update=True)
            else:
                # 非北交所股票使用默认数据源更新
                result = api.update_data(stock_codes, data_types=[DataType.FUNDAMENTAL_DATA], force_full_update=True)
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

def send_feishu_quality_report(report, check_type="数据质量检查"):
    """
    发送数据质量报告到飞书

    Args:
        report: QualityReport对象
        check_type: 检查类型描述
    """
    try:
        from common.feishu_client import FeishuWebhookBot
        import os
        from datetime import datetime

        # 获取飞书配置
        webhook_url = os.getenv('FEISHU_WEBHOOK_URL')
        secret = os.getenv('FEISHU_SECRET')

        if not webhook_url:
            logger.warning("未配置飞书Webhook URL，跳过飞书通知")
            return False

        # 创建飞书客户端
        feishu_bot = FeishuWebhookBot(webhook_url, secret)

        # 构建富文本消息内容
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 确定消息颜色和状态
        if report.critical_issues > 0:
            status_emoji = "🔴"
            status_text = "发现严重问题"
        elif report.warning_issues > 0:
            status_emoji = "🟡"
            status_text = "发现警告问题"
        else:
            status_emoji = "🟢"
            status_text = "检查通过"

        # 构建富文本内容 - 简化格式
        content = [
            # 标题行
            [
                {"tag": "text", "text": f"{status_emoji} {check_type}报告"}
            ],
            # 基本信息合并到一行
            [
                {"tag": "text", "text": f"📅 检查时间: {current_time}\n⏱️ 检查时长: {report.summary.get('check_duration_seconds', 0):.1f}秒\n📊 检查表数: {len(report.tables_checked)}\n🔍 总问题数: {report.total_issues}"}
            ]
        ]

        # 如果有问题，添加详细统计
        if report.total_issues > 0:
            problem_text = f"\n❌ 严重问题: {report.critical_issues}个\n⚠️ 警告问题: {report.warning_issues}个"

            # 按表统计问题
            table_stats = report.summary.get('table_stats', {})
            if table_stats:
                problem_text += "\n\n📋 问题分布:"
                for table, stats in table_stats.items():
                    total_table_issues = stats.get('critical', 0) + stats.get('warning', 0) + stats.get('info', 0)
                    if total_table_issues > 0:
                        problem_text += f"\n• {table}: {total_table_issues}个 (严重:{stats.get('critical', 0)}, 警告:{stats.get('warning', 0)})"

            # 显示前3个最重要的问题
            critical_issues = [issue for issue in report.issues if issue.severity == 'critical']
            warning_issues = [issue for issue in report.issues if issue.severity == 'warning']
            important_issues = critical_issues[:2] + warning_issues[:1]  # 最多显示3个问题

            if important_issues:
                problem_text += "\n\n🚨 主要问题:"
                for issue in important_issues:
                    severity_emoji = "❌" if issue.severity == 'critical' else "⚠️"
                    problem_text += f"\n{severity_emoji} [{issue.table}] {issue.description}"

            content.append([
                {"tag": "text", "text": problem_text}
            ])
        else:
            content.append([
                {"tag": "text", "text": "\n✅ 未发现数据质量问题，数据状态良好！"}
            ])

        # 发送消息
        title = f"股票数据库{check_type}报告 - {status_text}"
        result = feishu_bot.send_rich_text(content, title)

        if result.get('code') == 0:
            logger.info("飞书通知发送成功")
            return True
        else:
            logger.error(f"飞书通知发送失败: {result.get('msg', '未知错误')}")
            return False

    except Exception as e:
        logger.error(f"发送飞书通知时出错: {e}")
        return False


def action_check_data(args):
    """
    数据质量检查
    """
    from services.data_quality_service import DataQualityService
    import json
    import os

    # 获取检查参数
    level = getattr(args, 'level', 'quick')
    tables = getattr(args, 'tables', None)
    output_report = getattr(args, 'output_report', None)
    daily_routine = getattr(args, 'daily_routine', False)
    recent_days = getattr(args, 'recent_days', 3)
    historical_sample_days = getattr(args, 'historical_sample_days', 30)
    no_feishu = getattr(args, 'no_feishu', False)

    if tables:
        tables = [t.strip() for t in tables.split(',')]

    # 确定检查类型
    if daily_routine:
        logger.info(f"开始日常例行数据质量检查...")
        logger.info(f"- 最近{recent_days}个交易日全量检查")
        logger.info(f"- 历史{historical_sample_days}个交易日抽样检查")
    else:
        logger.info(f"开始{level}级别数据质量检查...")

    api = create_api(db_path=args.db_path)

    try:
        api.initialize()

        # 创建数据质量服务
        quality_service = DataQualityService(api)

        # 执行检查
        if daily_routine:
            report = quality_service.daily_routine_check(
                recent_days=recent_days,
                historical_sample_days=historical_sample_days
            )
        else:
            report = quality_service.check_data_quality(level=level, tables=tables)

        # 输出检查结果摘要
        logger.info(f"\n=== 数据质量检查报告 ===")
        if daily_routine:
            logger.info(f"检查类型: 日常例行检查")
            logger.info(f"最近交易日: {report.summary.get('recent_days_checked', recent_days)}天")
            logger.info(f"历史抽样: {report.summary.get('historical_sample_days', historical_sample_days)}天")
        else:
            logger.info(f"检查级别: {report.check_level}")
        logger.info(f"检查时长: {report.summary.get('check_duration_seconds', 0):.1f}秒")
        logger.info(f"检查表数: {len(report.tables_checked)}")
        logger.info(f"发现问题: {report.total_issues}个")

        if report.total_issues > 0:
            logger.info(f"  严重问题: {report.critical_issues}个")
            logger.info(f"  警告问题: {report.warning_issues}个")

            # 按表统计问题
            table_stats = report.summary.get('table_stats', {})
            if table_stats:
                logger.info(f"\n问题分布:")
                for table, stats in table_stats.items():
                    total_table_issues = stats.get('critical', 0) + stats.get('warning', 0) + stats.get('info', 0)
                    logger.info(f"  {table}: {total_table_issues}个问题 "
                              f"(严重:{stats.get('critical', 0)}, 警告:{stats.get('warning', 0)})")

            # 显示问题明细
            logger.info(f"\n问题明细:")
            for issue in report.issues:
                severity_prefix = "❌" if issue.severity == 'critical' else "⚠️" if issue.severity == 'warning' else "ℹ️"
                logger.info(f"  {severity_prefix} [{issue.table}] {issue.description}")
                if issue.samples:
                    samples_str = ", ".join(issue.samples[:3])  # 显示前3个样本
                    sample_count = len(issue.samples)
                    if sample_count > 3:
                        samples_str += f" (共{sample_count}个样本)"
                    logger.info(f"     样本: {samples_str}")

            # 按类别统计问题
            category_stats = report.summary.get('category_stats', {})
            if category_stats:
                logger.info(f"\n问题类别:")
                logger.info(f"  完整性: {category_stats.get('completeness', 0)}个")
                logger.info(f"  唯一性: {category_stats.get('uniqueness', 0)}个")
                logger.info(f"  准确性: {category_stats.get('accuracy', 0)}个")
        else:
            logger.info("未发现数据质量问题")

        # 保存详细报告
        if output_report:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_report), exist_ok=True)

            with open(output_report, 'w', encoding='utf-8') as f:
                json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)

            logger.info(f"\n详细报告已保存到: {output_report}")
        else:
            # 如果没有指定输出文件，生成默认报告文件
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_report_path = f"logs/data_quality_report_{timestamp}.json"

            # 确保输出目录存在
            os.makedirs(os.path.dirname(default_report_path), exist_ok=True)

            with open(default_report_path, 'w', encoding='utf-8') as f:
                json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)

            logger.info(f"\n详细报告已自动保存到: {default_report_path}")

        # 发送飞书通知（如果未禁用）
        if not no_feishu:
            check_type = "日常例行检查" if daily_routine else f"{level}级别数据质量检查"
            logger.info("正在发送飞书通知...")
            feishu_success = send_feishu_quality_report(report, check_type)
            if not feishu_success:
                logger.warning("飞书通知发送失败，但不影响检查结果")

        # 如果有严重问题，提醒用户注意
        if report.critical_issues > 0:
            logger.error(f"\n发现{report.critical_issues}个严重数据质量问题，建议立即处理！")
            return False
        elif report.warning_issues > 0:
            logger.warning(f"\n发现{report.warning_issues}个警告级别问题，建议关注。")

        return True

    except Exception as e:
        logger.error(f"数据质量检查失败: {e}")
        logging.exception("详细错误信息:")
        return False
    finally:
        api.close()