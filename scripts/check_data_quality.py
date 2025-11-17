#!/usr/bin/env python3
"""
数据质量检查命令行工具

用法:
    python scripts/check_data_quality.py                    # 检查最近3天数据 (默认)
    python scripts/check_data_quality.py --recent-days 14   # 检查最近14天数据
    python scripts/check_data_quality.py --recent-days 14 --historical-days 60  # 自定义历史抽样天数
    python scripts/check_data_quality.py --output-json report.json  # 将报告保存为JSON
"""

import sys
import os
import argparse
import json
import logging
from datetime import datetime

# 添加项目根目录到路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api import create_api
from services.data_quality_service import DataQualityService


def setup_logging(verbose: bool = False):
    """设置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def print_report(report):
    """打印质量检查报告到控制台"""
    print("\n" + "="*80)
    print("数据质量检查报告")
    print("="*80)
    print(f"检查级别: {report.check_level}")
    print(f"检查时间: {report.check_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"检查耗时: {report.summary.get('check_duration_seconds', 0):.2f} 秒")
    print(f"检查表数量: {len(report.tables_checked)}")
    print(f"检查的表: {', '.join(report.tables_checked)}")
    print("\n" + "-"*80)
    print(f"总问题数: {report.total_issues}")
    print(f"严重问题: {report.critical_issues}")
    print(f"警告问题: {report.warning_issues}")
    print("-"*80)

    if report.summary.get('check_type') == 'daily_routine':
        print(f"最近检查天数: {report.summary.get('recent_days_checked', 0)}")
        print(f"历史抽样天数: {report.summary.get('historical_sample_days', 0)}")
        print("-"*80)

    # 按表分组显示问题
    if report.issues:
        print("\n问题详情 (按表分组):\n")

        # 按表分组
        issues_by_table = {}
        for issue in report.issues:
            if issue.table not in issues_by_table:
                issues_by_table[issue.table] = []
            issues_by_table[issue.table].append(issue)

        # 显示每个表的问题
        for table, issues in sorted(issues_by_table.items()):
            print(f"\n【{table}】")
            for issue in issues:
                severity_icon = "🔴" if issue.severity == 'critical' else "🟡"
                print(f"  {severity_icon} [{issue.severity.upper()}] [{issue.category}]")
                print(f"     {issue.description}")
                if issue.samples:
                    print(f"     样本: {', '.join(issue.samples[:3])}")
    else:
        print("\n✅ 未发现数据质量问题！")

    print("\n" + "="*80 + "\n")


def save_json_report(report, output_path: str):
    """将报告保存为JSON文件"""
    report_dict = report.to_dict()
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report_dict, f, ensure_ascii=False, indent=2)
    print(f"报告已保存到: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='数据质量检查工具 - 检查indicator_data和valuation_data的完整性（排除BJ股票），同时检查price_data（包含BJ股票）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 检查最近3天数据（默认）
  python scripts/check_data_quality.py

  # 检查最近14天数据（约2周）
  python scripts/check_data_quality.py --recent-days 14

  # 检查最近14天数据，并增加历史抽样到60天
  python scripts/check_data_quality.py --recent-days 14 --historical-days 60

  # 将报告保存为JSON
  python scripts/check_data_quality.py --recent-days 14 --output-json report.json

  # 详细日志输出
  python scripts/check_data_quality.py --verbose

说明:
  - price_data: 检查所有股票（包括BJ股票）
  - valuation_data: 仅检查非BJ股票
  - indicator_data: 仅检查非BJ股票
        """
    )

    parser.add_argument(
        '--recent-days',
        type=int,
        default=3,
        help='检查最近N个交易日的数据（全量股票检查）默认: 3天'
    )

    parser.add_argument(
        '--historical-days',
        type=int,
        default=30,
        help='历史数据抽样检查的天数，默认: 30天'
    )

    parser.add_argument(
        '--output-json',
        type=str,
        help='将报告保存为JSON文件到指定路径'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='显示详细日志'
    )

    args = parser.parse_args()

    # 设置日志
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        # 创建API实例
        logger.info("初始化数据库连接...")
        api = create_api()

        # 创建数据质量服务
        logger.info("创建数据质量服务...")
        quality_service = DataQualityService(api)

        # 执行日常检查
        logger.info(f"开始数据质量检查: 最近{args.recent_days}天, 历史抽样{args.historical_days}天")
        report = quality_service.daily_routine_check(
            recent_days=args.recent_days,
            historical_sample_days=args.historical_days
        )

        # 打印报告
        print_report(report)

        # 保存JSON报告（如果指定）
        if args.output_json:
            save_json_report(report, args.output_json)

        # 根据问题严重性返回退出码
        if report.critical_issues > 0:
            logger.error(f"发现{report.critical_issues}个严重问题")
            sys.exit(2)
        elif report.warning_issues > 0:
            logger.warning(f"发现{report.warning_issues}个警告")
            sys.exit(1)
        else:
            logger.info("数据质量检查通过")
            sys.exit(0)

    except Exception as e:
        logger.error(f"数据质量检查失败: {e}", exc_info=True)
        sys.exit(3)


if __name__ == '__main__':
    main()
