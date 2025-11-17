#!/usr/bin/env python3
"""
北交所股票复权因子数据补全脚本

功能：
1. 检查所有北交所股票的adj_factor数据完整性
2. 补全缺失的复权因子数据
3. 支持指定时间范围和股票范围

使用示例：
    # 检查所有北交所股票缺失情况（不实际更新）
    python scripts/backfill_bj_adj_factors.py --dry-run

    # 补全所有缺失数据
    python scripts/backfill_bj_adj_factors.py

    # 补全指定时间范围
    python scripts/backfill_bj_adj_factors.py --start-date 2024-01-01 --end-date 2024-12-31

    # 补全指定股票
    python scripts/backfill_bj_adj_factors.py --codes 430047.BJ,832039.BJ
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载环境变量
from dotenv import load_dotenv
load_dotenv()

import argparse
from datetime import date, datetime
import pandas as pd
import logging
from typing import List, Optional, Tuple

from duckdb_impl import DuckDBDatabase
from providers.tushare import TushareDataSource
from config import get_config


class BJAdjustmentFactorBackfill:
    """北交所复权因子数据补全工具"""

    def __init__(self):
        self.config = get_config()
        self.db = DuckDBDatabase(self.config.database.path)
        # 初始化Tushare数据源
        import os
        from dotenv import load_dotenv
        load_dotenv()

        tushare_config = {}
        if 'TUSHARE_TOKEN' in os.environ:
            tushare_config['token'] = os.environ['TUSHARE_TOKEN']
        self.tushare = TushareDataSource(tushare_config)
        self.logger = logging.getLogger(__name__)

        # 初始化数据库连接
        self.db.connect()

        # 认证Tushare数据源
        token = os.environ.get('TUSHARE_TOKEN', '')
        if not self.tushare.authenticate(token=token):
            raise RuntimeError("Tushare数据源认证失败")

        self.logger.info("初始化完成")

    def find_missing_adj_factors(self, start_date: Optional[date] = None,
                               end_date: Optional[date] = None,
                               codes: Optional[List[str]] = None) -> pd.DataFrame:
        """查找缺失复权因子的北交所股票数据

        Args:
            start_date: 开始日期，为None时不限制
            end_date: 结束日期，为None时不限制
            codes: 指定股票代码列表，为None时检查所有北交所股票

        Returns:
            pd.DataFrame: 缺失数据记录，包含code、day列
        """

        # 构建查询条件
        where_conditions = ["code LIKE '%.BJ'", "(adj_factor IS NULL OR factor IS NULL)"]

        if start_date:
            where_conditions.append(f"day >= '{start_date}'")
        if end_date:
            where_conditions.append(f"day <= '{end_date}'")
        if codes:
            code_list = "', '".join(codes)
            where_conditions.append(f"code IN ('{code_list}')")

        where_clause = " AND ".join(where_conditions)

        query = f"""
        SELECT code, day, adj_factor, factor
        FROM price_data
        WHERE {where_clause}
        ORDER BY code, day
        """

        self.logger.info(f"执行查询: {query}")
        missing_data = self.db.query_data(query)

        if missing_data is not None and len(missing_data) > 0:
            self.logger.info(f"找到 {len(missing_data)} 条缺失复权因子的记录")
            # 按股票分组显示统计
            stock_counts = missing_data.groupby('code').size()
            self.logger.info("各股票缺失记录数:")
            for code, count in stock_counts.items():
                self.logger.info(f"  {code}: {count} 条")
        else:
            self.logger.info("未找到缺失复权因子的记录")

        return missing_data

    def backfill_adj_factors(self, missing_data: pd.DataFrame, dry_run: bool = False) -> int:
        """补全缺失的复权因子数据

        Args:
            missing_data: 缺失数据的DataFrame
            dry_run: 是否为试运行（不实际更新数据库）

        Returns:
            int: 成功更新的记录数
        """

        if missing_data is None or len(missing_data) == 0:
            self.logger.info("没有需要补全的数据")
            return 0

        # 按股票分组处理
        success_count = 0
        stock_groups = missing_data.groupby('code')

        for code, group in stock_groups:
            try:
                # 获取该股票的日期范围
                min_date = group['day'].min()
                max_date = group['day'].max()

                self.logger.info(f"处理股票 {code}，日期范围: {min_date} 到 {max_date}")

                # 调用Tushare获取复权因子数据
                self.tushare._wait_for_rate_limit()
                adj_df = self.tushare.pro.adj_factor(
                    ts_code=code,
                    start_date=min_date.strftime('%Y%m%d'),
                    end_date=max_date.strftime('%Y%m%d')
                )

                print(f"获取到 {code} 的复权因子数据，共 {len(adj_df)} 条")

                if adj_df is not None and len(adj_df) > 0:
                    # 转换日期格式
                    adj_df['day'] = pd.to_datetime(adj_df['trade_date'], format='%Y%m%d').dt.date
                    
                    # 确保group中的day字段也转换为date类型
                    group_copy = group.copy()
                    group_copy['day'] = pd.to_datetime(group['day']).dt.date

                    # 准备更新数据
                    updates = []
                    for _, adj_row in adj_df.iterrows():
                        # 检查是否在缺失列表中
                        #print(f"check type: {type(adj_row['day'])}  vs {type(group_copy['day'].iloc[0])}")
                        match_mask = (group_copy['day'] == adj_row['day'])
                        if match_mask.any():
                            updates.append({
                                'code': code,
                                'day': adj_row['day'],
                                'adj_factor': adj_row['adj_factor']
                            })

                    if updates and not dry_run:
                        # 批量更新数据库
                        for update in updates:
                            self.db.update_data(
                                'price_data',
                                {
                                    'adj_factor': update['adj_factor'],
                                    'factor': update['adj_factor']
                                },
                                {
                                    'code': update['code'],
                                    'day': update['day']
                                }
                            )

                        success_count += len(updates)
                        self.logger.info(f"股票 {code} 更新了 {len(updates)} 条记录")

                    elif updates and dry_run:
                        success_count += len(updates)
                        self.logger.info(f"[试运行] 股票 {code} 将更新 {len(updates)} 条记录")

                    else:
                        self.logger.warning(f"股票 {code} 未找到匹配的复权因子数据")

                else:
                    self.logger.warning(f"股票 {code} 未获取到复权因子数据")

            except Exception as e:
                self.logger.error(f"处理股票 {code} 失败: {e}")
                continue

        return success_count

    def run(self, start_date: Optional[date] = None, end_date: Optional[date] = None,
            codes: Optional[List[str]] = None, dry_run: bool = False) -> None:
        """执行补全任务

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定股票代码列表
            dry_run: 是否为试运行
        """

        self.logger.info("="*60)
        self.logger.info("开始北交所股票复权因子数据补全任务")
        self.logger.info(f"参数: start_date={start_date}, end_date={end_date}, codes={codes}, dry_run={dry_run}")
        self.logger.info("="*60)

        try:
            # 查找缺失数据
            missing_data = self.find_missing_adj_factors(start_date, end_date, codes)

            if missing_data is None or len(missing_data) == 0:
                self.logger.info("任务完成：没有需要补全的数据")
                return

            # 补全数据
            updated_count = self.backfill_adj_factors(missing_data, dry_run)

            if dry_run:
                self.logger.info(f"[试运行] 将更新 {updated_count} 条记录")
            else:
                self.logger.info(f"成功更新 {updated_count} 条记录")

            self.logger.info("任务完成")

        except Exception as e:
            self.logger.error(f"任务执行失败: {e}")
            raise

        finally:
            # 清理资源
            self.tushare.close()
            self.db.close()


def parse_date(date_str: str) -> date:
    """解析日期字符串"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"无效的日期格式: {date_str}，请使用 YYYY-MM-DD 格式")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="北交所股票复权因子数据补全脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--start-date', type=parse_date,
        help='开始日期 (YYYY-MM-DD格式)'
    )

    parser.add_argument(
        '--end-date', type=parse_date,
        help='结束日期 (YYYY-MM-DD格式)'
    )

    parser.add_argument(
        '--codes',
        help='指定股票代码，多个代码用逗号分隔 (例如: 430047.BJ,832039.BJ)'
    )

    parser.add_argument(
        '--dry-run', action='store_true',
        help='试运行模式，只检查不实际更新数据'
    )

    parser.add_argument(
        '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO', help='日志级别'
    )

    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 解析股票代码
    codes = None
    if args.codes:
        codes = [code.strip() for code in args.codes.split(',')]
        # 验证股票代码格式
        for code in codes:
            if not code.endswith('.BJ'):
                parser.error(f"股票代码 {code} 不是北交所股票代码（应以.BJ结尾）")

    # 验证日期范围
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("开始日期不能晚于结束日期")

    try:
        # 执行补全任务
        backfill = BJAdjustmentFactorBackfill()
        backfill.run(
            start_date=args.start_date,
            end_date=args.end_date,
            codes=codes,
            dry_run=args.dry_run
        )

    except Exception as e:
        logging.error(f"脚本执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()