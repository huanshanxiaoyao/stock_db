"""数据质量检查服务类"""

from typing import List, Dict, Any, Optional, Set
from datetime import date, datetime, timedelta
from dataclasses import dataclass
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import random

from data_source import DataType
from common.date_utils import get_trading_days, get_last_trading_day
from config import get_config


@dataclass
class QualityIssue:
    """数据质量问题记录"""
    severity: str  # 'critical', 'warning', 'info'
    category: str  # 'completeness', 'uniqueness', 'accuracy'
    table: str
    description: str
    count: int = 0
    samples: List[str] = None

    def __post_init__(self):
        if self.samples is None:
            self.samples = []


@dataclass
class QualityReport:
    """数据质量检查报告"""
    check_level: str
    check_time: datetime
    total_issues: int
    critical_issues: int
    warning_issues: int
    tables_checked: List[str]
    issues: List[QualityIssue]
    summary: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'check_level': self.check_level,
            'check_time': self.check_time.isoformat(),
            'total_issues': int(self.total_issues),
            'critical_issues': int(self.critical_issues),
            'warning_issues': int(self.warning_issues),
            'tables_checked': self.tables_checked,
            'issues': [
                {
                    'severity': issue.severity,
                    'category': issue.category,
                    'table': issue.table,
                    'description': issue.description,
                    'count': int(issue.count),
                    'samples': issue.samples[:5]  # 只保留前5个样本
                }
                for issue in self.issues
            ],
            'summary': self.summary
        }


class DataQualityService:
    """数据质量检查服务类"""

    def __init__(self, api):
        """初始化数据质量服务

        Args:
            api: StockDataAPI实例
        """
        self.api = api
        self.logger = logging.getLogger(__name__)
        self.issues = []

        # 加载配置
        self.config = get_config()
        self._load_config()

    def _load_config(self):
        """加载配置参数"""
        # 获取数据质量配置
        dq_config = getattr(self.config, 'data_quality', None)

        # 默认检查的数据表配置
        if dq_config and hasattr(dq_config, 'default_tables'):
            self.DEFAULT_TABLES = {
                'quick': getattr(dq_config.default_tables, 'quick', [
                    DataType.STOCK_LIST,
                    DataType.PRICE_DATA,
                    'user_transactions',
                    'user_positions'
                ]),
                'standard': getattr(dq_config.default_tables, 'standard', [
                    DataType.STOCK_LIST,
                    DataType.PRICE_DATA,
                    DataType.INDICATOR_DATA,
                    DataType.VALUATION_DATA,
                    'user_transactions',
                    'user_positions',
                    'user_account_info'
                ])
            }
        else:
            # 使用默认值
            self.DEFAULT_TABLES = {
                'quick': [
                    DataType.STOCK_LIST,
                    DataType.PRICE_DATA,
                    'user_transactions',
                    'user_positions'
                ],
                'standard': [
                    DataType.STOCK_LIST,
                    DataType.PRICE_DATA,
                    DataType.INDICATOR_DATA,
                    DataType.VALUATION_DATA,
                    'user_transactions',
                    'user_positions',
                    'user_account_info'
                ]
            }

        # 检查阈值配置
        if dq_config and hasattr(dq_config, 'thresholds'):
            thresholds = dq_config.thresholds
            self.THRESHOLDS = {
                'price_change_limit': getattr(thresholds, 'price_change_limit', 0.5),
                'volume_anomaly_ratio': getattr(thresholds, 'volume_anomaly_ratio', 10),
                'null_ratio_warning': getattr(thresholds, 'null_ratio_warning', 0.1),
                'null_ratio_critical': getattr(thresholds, 'null_ratio_critical', 0.3),
            }
        else:
            # 使用默认阈值
            self.THRESHOLDS = {
                'price_change_limit': 0.5,
                'volume_anomaly_ratio': 10,
                'null_ratio_warning': 0.1,
                'null_ratio_critical': 0.3,
            }

        # 抽样配置
        if dq_config and hasattr(dq_config, 'sampling'):
            sampling = dq_config.sampling
            self.THRESHOLDS.update({
                'sample_size_quick': getattr(sampling, 'sample_size_quick', 100),
                'sample_size_standard': getattr(sampling, 'sample_size_standard', 500),
            })
        else:
            # 使用默认抽样数量
            self.THRESHOLDS.update({
                'sample_size_quick': 100,
                'sample_size_standard': 500,
            })

    def check_data_quality(self, level: str = 'quick',
                          tables: Optional[List[str]] = None) -> QualityReport:
        """执行数据质量检查

        Args:
            level: 检查级别 'quick' 或 'standard'
            tables: 要检查的表列表，None则使用默认配置

        Returns:
            QualityReport: 检查报告
        """
        start_time = datetime.now()
        self.issues = []

        # 确定要检查的表
        if tables is None:
            tables = self.DEFAULT_TABLES.get(level, self.DEFAULT_TABLES['quick'])

        # 过滤存在的表
        existing_tables = self._get_existing_tables()
        tables_to_check = [t for t in tables if t in existing_tables]

        if not tables_to_check:
            self.logger.warning("没有找到可检查的数据表")
            return self._create_empty_report(level, start_time)

        self.logger.info(f"开始{level}级别数据质量检查，共检查{len(tables_to_check)}个表")

        try:
            # 执行各类检查
            self._check_completeness(tables_to_check, level)
            self._check_uniqueness(tables_to_check, level)
            self._check_accuracy(tables_to_check, level)

            # 生成报告
            report = self._generate_report(level, start_time, tables_to_check)

            self.logger.info(f"数据质量检查完成，发现{report.total_issues}个问题")
            return report

        except Exception as e:
            self.logger.error(f"数据质量检查失败: {e}")
            raise

    def _get_existing_tables(self) -> Set[str]:
        """获取数据库中存在的表"""
        try:
            db_info = self.api.get_database_info()
            if db_info and 'tables' in db_info:
                return {
                    table_info.get('table_name', '')
                    for table_info in db_info['tables']
                    if isinstance(table_info, dict) and table_info.get('table_name')
                }
            return set()
        except Exception as e:
            self.logger.warning(f"获取表信息失败: {e}")
            return set()

    def _check_completeness(self, tables: List[str], level: str):
        """检查数据完整性"""
        self.logger.info("正在检查数据完整性...")

        for table in tables:
            try:
                # 检查表是否为空
                self._check_table_empty(table)

                # 检查关键字段的NULL值
                self._check_null_values(table, level)

                # 对特定表进行时间序列完整性检查
                if table == DataType.PRICE_DATA:
                    self._check_price_data_completeness(level)
                elif table in ['user_transactions', 'user_positions']:
                    self._check_user_data_completeness(table, level)

            except Exception as e:
                self.logger.error(f"检查表{table}完整性时失败: {e}")
                self._add_issue('critical', 'completeness', table, f"检查失败: {e}")

    def _check_uniqueness(self, tables: List[str], level: str):
        """检查数据唯一性"""
        self.logger.info("正在检查数据唯一性...")

        # 主键重复检查配置
        primary_key_configs = {
            DataType.STOCK_LIST: ['code'],
            DataType.PRICE_DATA: ['code', 'day'],
            DataType.INDICATOR_DATA: ['code', 'stat_date'],
            DataType.VALUATION_DATA: ['code', 'day'],
            'user_transactions': ['trade_id'],
            'user_positions': ['user_id', 'position_date', 'stock_code'],
            'user_account_info': ['user_id', 'info_date']
        }

        for table in tables:
            if table in primary_key_configs:
                try:
                    self._check_primary_key_duplicates(table, primary_key_configs[table])
                except Exception as e:
                    self.logger.error(f"检查表{table}唯一性时失败: {e}")
                    self._add_issue('critical', 'uniqueness', table, f"检查失败: {e}")

    def _check_accuracy(self, tables: List[str], level: str):
        """检查数据准确性"""
        self.logger.info("正在检查数据准确性...")

        for table in tables:
            try:
                if table == DataType.PRICE_DATA:
                    self._check_price_data_accuracy(level)
                # 移除财务数据表的准确性检查
                elif table == 'user_transactions':
                    self._check_transaction_accuracy(level)
                elif table == 'user_positions':
                    self._check_position_accuracy(level)

            except Exception as e:
                self.logger.error(f"检查表{table}准确性时失败: {e}")
                self._add_issue('critical', 'accuracy', table, f"检查失败: {e}")

    def _check_table_empty(self, table: str):
        """检查表是否为空"""
        try:
            count_sql = f"SELECT COUNT(*) as count FROM {table}"
            result = self.api.query(count_sql)
            if result.empty or result.iloc[0]['count'] == 0:
                self._add_issue('critical', 'completeness', table, "表为空，没有任何数据")
        except Exception as e:
            self._add_issue('critical', 'completeness', table, f"无法查询表记录数: {e}")

    def _check_null_values(self, table: str, level: str):
        """检查关键字段的NULL值比例"""
        # 关键字段配置
        key_fields = {
            DataType.STOCK_LIST: ['code', 'display_name'],
            DataType.PRICE_DATA: ['code', 'day', 'open', 'high', 'low', 'close', 'volume'],
            DataType.INDICATOR_DATA: ['code', 'stat_date', 'eps', 'roe'],
            DataType.VALUATION_DATA: ['code', 'day', 'market_cap', 'pe_ratio'],
            'user_transactions': ['trade_id', 'user_id', 'stock_code', 'trade_date'],  # 使用trade_id
            'user_positions': ['user_id', 'position_date', 'stock_code'],
            'user_account_info': ['user_id', 'info_date', 'total_assets']
        }

        if table not in key_fields:
            return

        try:
            # 获取总记录数
            total_sql = f"SELECT COUNT(*) as total FROM {table}"
            total_result = self.api.query(total_sql)
            if total_result.empty:
                return
            total_count = total_result.iloc[0]['total']

            if total_count == 0:
                return

            # 检查每个关键字段的NULL值
            for field in key_fields[table]:
                null_sql = f"SELECT COUNT(*) as null_count FROM {table} WHERE {field} IS NULL"
                null_result = self.api.query(null_sql)
                if not null_result.empty:
                    null_count = null_result.iloc[0]['null_count']
                    null_ratio = null_count / total_count

                    if null_ratio > self.THRESHOLDS['null_ratio_critical']:
                        self._add_issue('critical', 'completeness', table,
                                      f"关键字段{field}有{null_count}条NULL值，比例{null_ratio:.2%}")
                    elif null_ratio > self.THRESHOLDS['null_ratio_warning']:
                        self._add_issue('warning', 'completeness', table,
                                      f"关键字段{field}有{null_count}条NULL值，比例{null_ratio:.2%}")

        except Exception as e:
            self._add_issue('warning', 'completeness', table, f"检查NULL值失败: {e}")

    def _check_price_data_completeness(self, level: str):
        """检查价格数据完整性"""
        sample_size = (self.THRESHOLDS['sample_size_standard'] if level == 'standard'
                      else self.THRESHOLDS['sample_size_quick'])

        try:
            # 获取活跃股票样本
            sample_sql = f"""
                SELECT code, start_date, end_date
                FROM stock_list
                WHERE (end_date IS NULL OR end_date > CURRENT_DATE)
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """
            sample_df = self.api.query(sample_sql)

            if sample_df.empty:
                self._add_issue('warning', 'completeness', DataType.PRICE_DATA,
                              "无法获取股票列表进行完整性检查")
                return

            # 计算检查时间范围
            today_str = datetime.now().strftime("%Y%m%d")
            last_trade_str = get_last_trading_day(today_str, n=0)
            default_start_str = "20190101"

            incomplete_count = 0
            checked_count = 0

            for _, row in sample_df.iterrows():
                code = row['code']

                # 确定检查起始日期
                stock_start = self._parse_date_to_str(row.get('start_date'))
                start_str = max(default_start_str, stock_start) if stock_start else default_start_str

                if start_str > last_trade_str:
                    continue

                # 获取预期交易日
                expected_days = get_trading_days(start_str, last_trade_str)
                if not expected_days:
                    continue

                # 查询实际数据
                start_date = f"{start_str[:4]}-{start_str[4:6]}-{start_str[6:8]}"
                end_date = f"{last_trade_str[:4]}-{last_trade_str[4:6]}-{last_trade_str[6:8]}"

                data_sql = """
                    SELECT COUNT(*) as count
                    FROM price_data
                    WHERE code = ? AND day >= ? AND day <= ?
                """
                data_result = self.api.query(data_sql, [code, start_date, end_date])

                if not data_result.empty:
                    actual_count = data_result.iloc[0]['count']
                    expected_count = len(expected_days)

                    checked_count += 1
                    if actual_count < expected_count * 0.95:  # 允许5%的缺失
                        incomplete_count += 1
                        missing_ratio = 1 - (actual_count / expected_count)
                        if missing_ratio > 0.2:  # 缺失超过20%为严重问题
                            self._add_issue('critical', 'completeness', DataType.PRICE_DATA,
                                          f"股票{code}价格数据严重缺失，缺失比例{missing_ratio:.2%}",
                                          samples=[code])
                        elif missing_ratio > 0.05:  # 缺失超过5%为警告
                            self._add_issue('warning', 'completeness', DataType.PRICE_DATA,
                                          f"股票{code}价格数据缺失，缺失比例{missing_ratio:.2%}",
                                          samples=[code])

            # 汇总报告
            if checked_count > 0:
                incomplete_ratio = incomplete_count / checked_count
                if incomplete_ratio > 0.1:  # 超过10%的股票数据不完整
                    self._add_issue('warning', 'completeness', DataType.PRICE_DATA,
                                  f"抽样检查发现{incomplete_count}/{checked_count}只股票价格数据不完整")

        except Exception as e:
            self._add_issue('critical', 'completeness', DataType.PRICE_DATA,
                          f"价格数据完整性检查失败: {e}")

    def _check_user_data_completeness(self, table: str, level: str):
        """检查用户数据完整性"""
        try:
            if table == 'user_transactions':
                # 检查是否有无效的股票代码
                invalid_codes_sql = """
                    SELECT COUNT(DISTINCT ut.stock_code) as count
                    FROM user_transactions ut
                    LEFT JOIN stock_list sl ON ut.stock_code = sl.code
                    WHERE sl.code IS NULL
                """
                result = self.api.query(invalid_codes_sql)
                if not result.empty and result.iloc[0]['count'] > 0:
                    self._add_issue('warning', 'completeness', table,
                                  f"发现{result.iloc[0]['count']}个不在股票列表中的股票代码")

            elif table == 'user_positions':
                # 检查持仓记录的日期连续性
                gap_sql = """
                    SELECT user_id, COUNT(*) as gap_count
                    FROM (
                        SELECT user_id, position_date,
                               LAG(position_date) OVER (PARTITION BY user_id ORDER BY position_date) as prev_date
                        FROM user_positions
                    ) t
                    WHERE prev_date IS NOT NULL
                    AND position_date > prev_date + INTERVAL '7 days'
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                """
                result = self.api.query(gap_sql)
                if not result.empty:
                    for _, row in result.iterrows():
                        self._add_issue('warning', 'completeness', table,
                                      f"用户{row['user_id']}的持仓记录存在{row['gap_count']}个时间间隔")

        except Exception as e:
            self._add_issue('warning', 'completeness', table, f"用户数据完整性检查失败: {e}")

    def _check_primary_key_duplicates(self, table: str, key_fields: List[str]):
        """检查主键重复"""
        try:
            key_fields_str = ', '.join(key_fields)
            duplicate_sql = f"""
                SELECT {key_fields_str}, COUNT(*) as count
                FROM {table}
                GROUP BY {key_fields_str}
                HAVING COUNT(*) > 1
                LIMIT 10
            """

            result = self.api.query(duplicate_sql)
            if not result.empty:
                samples = []
                total_duplicates = 0
                for _, row in result.iterrows():
                    count = row['count']
                    total_duplicates += count - 1  # 每组重复记录中，除了第一条，其他都是多余的
                    key_values = [str(row[field]) for field in key_fields]
                    samples.append(f"({', '.join(key_values)})")

                self._add_issue('critical', 'uniqueness', table,
                              f"发现{total_duplicates}条重复记录",
                              count=total_duplicates, samples=samples)

        except Exception as e:
            self._add_issue('warning', 'uniqueness', table, f"检查主键重复失败: {e}")

    def _check_price_data_accuracy(self, level: str):
        """检查价格数据准确性"""
        try:
            # 检查价格逻辑
            logic_sql = """
                SELECT code, day, open, high, low, close, volume
                FROM price_data
                WHERE high < low OR open < 0 OR close < 0 OR high < 0 OR low < 0 OR volume < 0
                LIMIT 20
            """
            result = self.api.query(logic_sql)
            if not result.empty:
                samples = [f"{row['code']}@{row['day']}" for _, row in result.iterrows()]
                self._add_issue('critical', 'accuracy', DataType.PRICE_DATA,
                              f"发现{len(result)}条价格逻辑错误记录（负价格或高价<低价）",
                              count=len(result), samples=samples)

            # 检查异常波动（仅在标准检查中进行）
            if level == 'standard':
                volatility_sql = f"""
                    SELECT code, day, close, prev_close,
                           ABS(close - prev_close) / prev_close as change_ratio
                    FROM (
                        SELECT code, day, close,
                               LAG(close) OVER (PARTITION BY code ORDER BY day) as prev_close
                        FROM price_data
                        WHERE day >= CURRENT_DATE - INTERVAL '30 days'
                    ) t
                    WHERE prev_close > 0
                    AND ABS(close - prev_close) / prev_close > {self.THRESHOLDS['price_change_limit']}
                    LIMIT 50
                """
                result = self.api.query(volatility_sql)
                if not result.empty:
                    samples = [f"{row['code']}@{row['day']}({row['change_ratio']:.2%})"
                              for _, row in result.iterrows()]
                    self._add_issue('warning', 'accuracy', DataType.PRICE_DATA,
                                  f"发现{len(result)}条异常波动记录（单日涨跌幅超过{self.THRESHOLDS['price_change_limit']:.0%}）",
                                  count=len(result), samples=samples[:10])

        except Exception as e:
            self._add_issue('warning', 'accuracy', DataType.PRICE_DATA, f"价格数据准确性检查失败: {e}")


    def _check_transaction_accuracy(self, level: str):
        """检查交易记录准确性"""
        try:
            # 检查交易金额计算 - 使用正确的字段名
            amount_sql = """
                SELECT trade_id, quantity, price, amount,
                       ABS(quantity * price - amount) as diff
                FROM user_transactions
                WHERE quantity > 0 AND price > 0 AND amount > 0
                AND ABS(quantity * price - amount) / amount > 0.01
                LIMIT 20
            """
            result = self.api.query(amount_sql)
            if not result.empty:
                samples = [row['trade_id'] for _, row in result.iterrows()]
                self._add_issue('warning', 'accuracy', 'user_transactions',
                              f"发现{len(result)}条交易金额计算错误记录",
                              count=len(result), samples=samples)

        except Exception as e:
            self._add_issue('warning', 'accuracy', 'user_transactions',
                          f"交易记录准确性检查失败: {e}")

    def _check_position_accuracy(self, level: str):
        """检查持仓记录准确性"""
        try:
            # 检查市值计算 - 字段名都是正确的，保持不变
            value_sql = """
                SELECT position_id, position_quantity, current_price, market_value,
                       ABS(position_quantity * current_price - market_value) as diff
                FROM user_positions
                WHERE position_quantity > 0 AND current_price > 0 AND market_value > 0
                AND ABS(position_quantity * current_price - market_value) / market_value > 0.01
                LIMIT 20
            """
            result = self.api.query(value_sql)
            if not result.empty:
                samples = [row['position_id'] for _, row in result.iterrows()]
                self._add_issue('warning', 'accuracy', 'user_positions',
                              f"发现{len(result)}条持仓市值计算错误记录",
                              count=len(result), samples=samples)

        except Exception as e:
            self._add_issue('warning', 'accuracy', 'user_positions',
                          f"持仓记录准确性检查失败: {e}")

    def _parse_date_to_str(self, date_value) -> Optional[str]:
        """解析日期值为YYYYMMDD字符串"""
        if pd.isna(date_value) or not date_value:
            return None

        try:
            if hasattr(date_value, 'strftime'):  # date/datetime对象
                return date_value.strftime("%Y%m%d")
            elif isinstance(date_value, str):
                # 尝试多种日期格式
                for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
                    try:
                        return datetime.strptime(date_value, fmt).strftime("%Y%m%d")
                    except:
                        continue
        except:
            pass

        return None

    def _add_issue(self, severity: str, category: str, table: str,
                   description: str, count: int = 1, samples: List[str] = None):
        """添加质量问题记录"""
        issue = QualityIssue(
            severity=severity,
            category=category,
            table=table,
            description=description,
            count=count,
            samples=samples or []
        )
        self.issues.append(issue)

        # 实时输出严重问题
        if severity == 'critical':
            self.logger.error(f"[{table}] {description}")
        elif severity == 'warning':
            self.logger.warning(f"[{table}] {description}")

    def _generate_report(self, level: str, start_time: datetime,
                        tables_checked: List[str]) -> QualityReport:
        """生成检查报告"""
        critical_count = sum(1 for issue in self.issues if issue.severity == 'critical')
        warning_count = sum(1 for issue in self.issues if issue.severity == 'warning')

        # 按表分组统计
        table_stats = {}
        for issue in self.issues:
            if issue.table not in table_stats:
                table_stats[issue.table] = {'critical': 0, 'warning': 0, 'info': 0}
            table_stats[issue.table][issue.severity] += 1

        summary = {
            'check_duration_seconds': (datetime.now() - start_time).total_seconds(),
            'tables_with_issues': len(table_stats),
            'table_stats': table_stats,
            'category_stats': {
                'completeness': sum(1 for i in self.issues if i.category == 'completeness'),
                'uniqueness': sum(1 for i in self.issues if i.category == 'uniqueness'),
                'accuracy': sum(1 for i in self.issues if i.category == 'accuracy')
            }
        }

        return QualityReport(
            check_level=level,
            check_time=start_time,
            total_issues=len(self.issues),
            critical_issues=critical_count,
            warning_issues=warning_count,
            tables_checked=tables_checked,
            issues=self.issues,
            summary=summary
        )

    def _check_financial_data_for_period(self, trading_day: str, full_stock_check: bool):
        """检查财务数据（季度数据）在指定时期的完整性"""
        try:
            # 财务数据是季度数据，需要检查最近的季度报告期
            year = int(trading_day[:4])
            month = int(trading_day[4:6])

            # 确定当前日期对应的最近报告期
            if month <= 3:
                latest_quarter = f"{year-1}1231"  # 上年年报
            elif month <= 4:
                latest_quarter = f"{year}0331"    # 一季报
            elif month <= 8:
                latest_quarter = f"{year}0630"    # 中报
            elif month <= 10:
                latest_quarter = f"{year}0930"    # 三季报
            else:
                latest_quarter = f"{year}1231"    # 年报

            self.logger.info(f"检查交易日{trading_day}对应的财务数据，最近报告期：{latest_quarter}")

            # 财务数据表列表（只检查indicator_data）
            financial_tables = {
                DataType.INDICATOR_DATA: 'indicator_data'
            }

            # 获取活跃股票列表
            day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"
            active_stocks_sql = """
                SELECT code FROM stock_list
                WHERE (end_date IS NULL OR end_date > ?)
                AND start_date <= ?
            """
            active_stocks = self.api.query(active_stocks_sql, [day_formatted, day_formatted])

            if active_stocks.empty:
                self.logger.warning(f"交易日{trading_day}无活跃股票信息")
                return

            expected_count = len(active_stocks)
            quarter_formatted = f"{latest_quarter[:4]}-{latest_quarter[4:6]}-{latest_quarter[6:8]}"

            # 检查每个财务数据表
            for data_type, table_name in financial_tables.items():
                try:
                    if full_stock_check:
                        # 全量检查
                        financial_sql = f"""
                            SELECT COUNT(DISTINCT code) as count
                            FROM {table_name}
                            WHERE stat_date = ? OR statDate = ?
                        """
                        result = self.api.query(financial_sql, [quarter_formatted, quarter_formatted])

                        if not result.empty:
                            actual_count = result.iloc[0]['count']
                            if actual_count < expected_count * 0.7:  # 财务数据70%覆盖率
                                missing_ratio = 1 - (actual_count / expected_count)
                                severity = 'critical' if missing_ratio > 0.5 else 'warning'
                                self._add_issue(severity, 'completeness', data_type,
                                              f"报告期{latest_quarter}的{table_name}数据缺失: {actual_count}/{expected_count}股票，缺失{missing_ratio:.2%}")
                        else:
                            self._add_issue('warning', 'completeness', data_type,
                                          f"报告期{latest_quarter}的{table_name}数据为空")
                    else:
                        # 抽样检查（历史数据）
                        sample_stocks = active_stocks.sample(n=min(50, len(active_stocks)))
                        missing_count = 0

                        for _, row in sample_stocks.iterrows():
                            code = row['code']
                            check_sql = f"""
                                SELECT COUNT(*) as count FROM {table_name}
                                WHERE code = ? AND (stat_date = ? OR statDate = ?)
                            """
                            check_result = self.api.query(check_sql, [code, quarter_formatted, quarter_formatted])

                            if check_result.empty or check_result.iloc[0]['count'] == 0:
                                missing_count += 1

                        if missing_count > len(sample_stocks) * 0.3:  # 超过30%缺失
                            self._add_issue('warning', 'completeness', data_type,
                                          f"报告期{latest_quarter}抽样发现{missing_count}/{len(sample_stocks)}股票{table_name}数据缺失")

                except Exception as e:
                    self._add_issue('warning', 'completeness', data_type,
                                  f"检查{table_name}数据失败: {e}")

        except Exception as e:
            self._add_issue('warning', 'completeness', 'financial_data',
                          f"财务数据检查失败: {e}")

    def _create_empty_report(self, level: str, start_time: datetime) -> QualityReport:
        """创建空的检查报告"""
        return QualityReport(
            check_level=level,
            check_time=start_time,
            total_issues=0,
            critical_issues=0,
            warning_issues=0,
            tables_checked=[],
            issues=[],
            summary={'check_duration_seconds': 0, 'error': 'No tables to check'}
        )

    def daily_routine_check(self, recent_days: int = 3,
                           historical_sample_days: int = 30) -> QualityReport:
        """执行日常例行数据质量检查

        Args:
            recent_days: 检查最近几个交易日的数据（全量股票检查）
            historical_sample_days: 历史数据抽样检查的天数

        Returns:
            QualityReport: 综合检查报告
        """
        start_time = datetime.now()
        self.issues = []

        self.logger.info(f"开始日常例行数据质量检查...")
        self.logger.info(f"- 最近{recent_days}个交易日全量检查")
        self.logger.info(f"- 历史{historical_sample_days}个交易日抽样检查")

        try:
            # 1. 检查最近几个交易日的数据（全量股票）
            self._check_recent_trading_days(recent_days)

            # 2. 历史数据抽样检查
            self._check_historical_sample(historical_sample_days)

            # 3. 检查季度财报数据（indicator_data）
            self._check_quarterly_indicator_data()

            # 4. 生成综合报告
            tables_checked = [
                DataType.STOCK_LIST,
                DataType.PRICE_DATA,
                DataType.VALUATION_DATA,
                DataType.INDICATOR_DATA,
                'user_transactions',
                'user_positions',
                'user_account_info'
            ]

            report = self._generate_routine_report('daily_routine', start_time,
                                                 tables_checked, recent_days,
                                                 historical_sample_days)

            self.logger.info(f"日常例行检查完成，发现{report.total_issues}个问题")
            return report

        except Exception as e:
            self.logger.error(f"日常例行检查失败: {e}")
            raise

    def _check_recent_trading_days(self, recent_days: int):
        """检查最近几个交易日的数据（全量股票检查）"""
        self.logger.info(f"正在检查最近{recent_days}个交易日的数据（排除今天）...")

        try:
            # 获取最近的交易日，从昨天开始（排除今天）
            today_str = datetime.now().strftime("%Y%m%d")

            # 获取最近N个交易日，从昨天开始（n=1开始，排除今天）
            recent_trading_days = []
            for i in range(1, recent_days + 1):  # 从1开始，跳过今天
                trading_day = get_last_trading_day(today_str, n=i)
                if trading_day:
                    recent_trading_days.append(trading_day)

            if not recent_trading_days:
                self._add_issue('critical', 'completeness', 'general',
                              "无法获取最近的交易日信息")
                return

            self.logger.info(f"检查交易日: {recent_trading_days}")

            # 检查股票列表的基本完整性
            self._check_stock_list_basic()

            # 对每个交易日进行全量股票检查
            for trading_day in recent_trading_days:
                self._check_trading_day_data(trading_day, full_stock_check=True)

        except Exception as e:
            self._add_issue('critical', 'completeness', 'recent_data',
                          f"最近交易日检查失败: {e}")

    def _check_historical_sample(self, sample_days: int):
        """历史数据抽样检查"""
        self.logger.info(f"正在进行历史{sample_days}天数据抽样检查...")

        try:
            # 获取历史交易日列表进行抽样
            historical_days = self._get_historical_trading_days(sample_days)

            if not historical_days:
                self._add_issue('warning', 'completeness', 'historical_data',
                              "无法获取历史交易日进行抽样检查")
                return

            self.logger.info(f"抽样检查{len(historical_days)}个历史交易日")

            # 对抽样的历史交易日进行检查
            for trading_day in historical_days:
                self._check_trading_day_data(trading_day, full_stock_check=False)

            # 检查用户数据在这些日期的完整性（全量检查）
            self._check_user_data_in_date_range(historical_days)

        except Exception as e:
            self._add_issue('warning', 'completeness', 'historical_data',
                          f"历史数据抽样检查失败: {e}")

    def _get_historical_trading_days(self, sample_days: int) -> List[str]:
        """获取历史交易日样本"""
        try:
            # 获取过去一年的交易日，排除最近一周（避免与recent_days_check重复）
            today_str = datetime.now().strftime("%Y%m%d")
            end_date_str = get_last_trading_day(today_str, n=7)  # 排除最近一周
            start_date_obj = datetime.strptime(end_date_str, "%Y%m%d") - timedelta(days=365)
            start_date_str = start_date_obj.strftime("%Y%m%d")

            # 从数据库获取实际存在数据的交易日
            sql = """
                SELECT DISTINCT day
                FROM price_data
                WHERE day >= ? AND day <= ?
                ORDER BY day DESC
            """

            start_formatted = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            end_formatted = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"

            result = self.api.query(sql, [start_formatted, end_formatted])

            if result.empty:
                return []

            # 转换为YYYYMMDD格式
            available_days = [
                row['day'].strftime("%Y%m%d") if hasattr(row['day'], 'strftime')
                else str(row['day']).replace('-', '')
                for _, row in result.iterrows()
            ]

            # 随机抽样
            sample_size = min(sample_days, len(available_days))
            return random.sample(available_days, sample_size)

        except Exception as e:
            self.logger.error(f"获取历史交易日失败: {e}")
            return []

    def _check_trading_day_data(self, trading_day: str, full_stock_check: bool = True):
        """检查特定交易日的数据"""
        day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"

        try:
            self.logger.info(f"检查交易日{trading_day}的数据（全量检查={full_stock_check}）")

            # 检查价格数据（日频数据）
            self._check_price_data_for_day(trading_day, full_stock_check)

            # 检查估值数据（日频数据）
            self._check_valuation_data_for_day(trading_day, full_stock_check)

            # 注意：indicator_data是季度财报数据，不需要按交易日检查

        except Exception as e:
            self._add_issue('warning', 'completeness', f'trading_day_{trading_day}',
                          f"交易日{trading_day}数据检查失败: {e}")

    def _check_price_data_for_day(self, trading_day: str, full_stock_check: bool):
        """检查特定交易日的价格数据"""
        day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"

        try:
            if full_stock_check:
                # 全量股票检查 - 获取所有活跃股票
                active_stocks_sql = """
                    SELECT code FROM stock_list
                    WHERE (end_date IS NULL OR end_date > ?)
                    AND start_date <= ?
                """
                active_stocks = self.api.query(active_stocks_sql, [day_formatted, day_formatted])

                if active_stocks.empty:
                    self._add_issue('warning', 'completeness', DataType.PRICE_DATA,
                                  f"交易日{trading_day}无活跃股票信息")
                    return

                # 检查有价格数据的股票数量
                price_data_sql = """
                    SELECT COUNT(DISTINCT code) as count FROM price_data WHERE day = ?
                """
                price_result = self.api.query(price_data_sql, [day_formatted])

                if not price_result.empty:
                    actual_count = price_result.iloc[0]['count']
                    expected_count = len(active_stocks)

                    if actual_count < expected_count * 0.95:  # 允许5%的缺失
                        missing_ratio = 1 - (actual_count / expected_count)
                        severity = 'critical' if missing_ratio > 0.2 else 'warning'

                        # 获取缺失数据的股票样本
                        missing_samples = []
                        missing_sql = """
                            SELECT sl.code
                            FROM stock_list sl
                            LEFT JOIN price_data pd ON sl.code = pd.code AND pd.day = ?
                            WHERE (sl.end_date IS NULL OR sl.end_date > ?)
                            AND sl.start_date <= ?
                            AND pd.code IS NULL
                            LIMIT 5
                        """
                        missing_result = self.api.query(missing_sql, [day_formatted, day_formatted, day_formatted])
                        if not missing_result.empty:
                            missing_samples = missing_result['code'].tolist()

                        self._add_issue(severity, 'completeness', DataType.PRICE_DATA,
                                      f"交易日{trading_day}价格数据缺失: {actual_count}/{expected_count}股票，缺失{missing_ratio:.2%}",
                                      count=expected_count-actual_count, samples=missing_samples)
            else:
                # 抽样检查 - 随机检查部分股票
                sample_sql = """
                    SELECT code FROM stock_list
                    WHERE (end_date IS NULL OR end_date > ?)
                    AND start_date <= ?
                    ORDER BY RANDOM() LIMIT 50
                """
                sample_stocks = self.api.query(sample_sql, [day_formatted, day_formatted])

                if not sample_stocks.empty:
                    missing_count = 0
                    for _, row in sample_stocks.iterrows():
                        code = row['code']
                        check_sql = "SELECT COUNT(*) as count FROM price_data WHERE code = ? AND day = ?"
                        check_result = self.api.query(check_sql, [code, day_formatted])

                        if check_result.empty or check_result.iloc[0]['count'] == 0:
                            missing_count += 1

                    if missing_count > len(sample_stocks) * 0.1:  # 超过10%缺失
                        self._add_issue('warning', 'completeness', DataType.PRICE_DATA,
                                      f"交易日{trading_day}抽样发现{missing_count}/{len(sample_stocks)}股票价格数据缺失")

            # 检查价格数据的逻辑正确性
            logic_sql = """
                SELECT COUNT(*) as invalid_count FROM price_data
                WHERE day = ? AND (high < low OR open < 0 OR close < 0 OR high < 0 OR low < 0 OR volume < 0)
            """
            logic_result = self.api.query(logic_sql, [day_formatted])

            if not logic_result.empty and logic_result.iloc[0]['invalid_count'] > 0:
                invalid_count = logic_result.iloc[0]['invalid_count']
                self._add_issue('critical', 'accuracy', DataType.PRICE_DATA,
                              f"交易日{trading_day}发现{invalid_count}条价格逻辑错误记录")

        except Exception as e:
            self._add_issue('warning', 'accuracy', DataType.PRICE_DATA,
                          f"交易日{trading_day}价格数据检查失败: {e}")

    def _check_valuation_data_for_day(self, trading_day: str, full_stock_check: bool):
        """检查特定交易日的估值数据（排除北交所股票）"""
        day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"

        try:
            # 检查估值数据与价格数据的一致性（排除BJ股票）
            missing_sql = """
                SELECT COUNT(*) as missing_count
                FROM price_data p
                LEFT JOIN valuation_data v ON p.code = v.code AND p.day = v.day
                WHERE p.day = ? AND v.code IS NULL
                AND p.code NOT LIKE '%.BJ'
            """
            missing_result = self.api.query(missing_sql, [day_formatted])

            if not missing_result.empty:
                missing_count = missing_result.iloc[0]['missing_count']
                if missing_count > 0:
                    # 获取价格数据总数作为基准（排除BJ股票）
                    total_sql = "SELECT COUNT(*) as total FROM price_data WHERE day = ? AND code NOT LIKE '%.BJ'"
                    total_result = self.api.query(total_sql, [day_formatted])

                    if not total_result.empty:
                        total_count = total_result.iloc[0]['total']
                        if total_count > 0:
                            missing_ratio = missing_count / total_count
                            if missing_ratio > 0.1:  # 超过10%缺失
                                severity = 'critical' if missing_ratio > 0.5 else 'warning'

                                # 获取缺失估值数据的股票样本（排除BJ股票）
                                valuation_samples = []
                                valuation_sample_sql = """
                                    SELECT p.code
                                    FROM price_data p
                                    LEFT JOIN valuation_data v ON p.code = v.code AND p.day = v.day
                                    WHERE p.day = ? AND v.code IS NULL
                                    AND p.code NOT LIKE '%.BJ'
                                    LIMIT 5
                                """
                                valuation_sample_result = self.api.query(valuation_sample_sql, [day_formatted])
                                if not valuation_sample_result.empty:
                                    valuation_samples = valuation_sample_result['code'].tolist()

                                self._add_issue(severity, 'completeness', DataType.VALUATION_DATA,
                                              f"交易日{trading_day}估值数据缺失{missing_count}/{total_count}，比例{missing_ratio:.2%}",
                                              count=missing_count, samples=valuation_samples)

        except Exception as e:
            self._add_issue('warning', 'completeness', DataType.VALUATION_DATA,
                          f"交易日{trading_day}估值数据检查失败: {e}")

    def _check_user_data_in_date_range(self, date_list: List[str]):
        """检查用户数据在指定日期范围内的质量"""
        self.logger.info(f"检查用户数据在{len(date_list)}个日期的质量...")

        # 转换日期格式
        formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in date_list]
        date_str = "','".join(formatted_dates)

        try:
            # 检查用户持仓数据
            self._check_user_positions_in_dates(formatted_dates)

            # 检查用户账户信息
            self._check_user_account_info_in_dates(formatted_dates)

            # 检查用户交易记录
            self._check_user_transactions_in_dates(formatted_dates)

        except Exception as e:
            self._add_issue('warning', 'completeness', 'user_data',
                          f"用户数据日期范围检查失败: {e}")

    def _check_user_positions_in_dates(self, formatted_dates: List[str]):
        """检查用户持仓数据在指定日期的质量"""
        try:
            # 检查是否有持仓数据
            dates_placeholder = ','.join(['?' for _ in formatted_dates])
            positions_sql = f"""
                SELECT position_date, COUNT(*) as position_count
                FROM user_positions
                WHERE position_date IN ({dates_placeholder})
                GROUP BY position_date
            """
            positions_result = self.api.query(positions_sql, formatted_dates)

            if positions_result.empty:
                self._add_issue('warning', 'completeness', 'user_positions',
                              f"在抽样的{len(formatted_dates)}个日期中未找到用户持仓数据")
            else:
                # 检查持仓数据的逻辑正确性
                logic_sql = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM user_positions
                    WHERE position_date IN ({dates_placeholder})
                    AND (position_quantity < 0 OR available_quantity < 0
                         OR frozen_quantity > position_quantity
                         OR available_quantity + frozen_quantity > position_quantity)
                """
                logic_result = self.api.query(logic_sql, formatted_dates)

                if not logic_result.empty and logic_result.iloc[0]['invalid_count'] > 0:
                    invalid_count = logic_result.iloc[0]['invalid_count']
                    self._add_issue('critical', 'accuracy', 'user_positions',
                                  f"在抽样日期中发现{invalid_count}条持仓数量逻辑错误记录")

        except Exception as e:
            self._add_issue('warning', 'completeness', 'user_positions',
                          f"用户持仓数据检查失败: {e}")

    def _check_user_account_info_in_dates(self, formatted_dates: List[str]):
        """检查用户账户信息在指定日期的质量"""
        try:
            dates_placeholder = ','.join(['?' for _ in formatted_dates])
            account_sql = f"""
                SELECT info_date, COUNT(*) as account_count
                FROM user_account_info
                WHERE info_date IN ({dates_placeholder})
                GROUP BY info_date
            """
            account_result = self.api.query(account_sql, formatted_dates)

            if account_result.empty:
                self._add_issue('warning', 'completeness', 'user_account_info',
                              f"在抽样的{len(formatted_dates)}个日期中未找到用户账户信息")
            else:
                # 检查资产平衡
                balance_sql = f"""
                    SELECT COUNT(*) as imbalanced_count
                    FROM user_account_info
                    WHERE info_date IN ({dates_placeholder})
                    AND total_assets > 0
                    AND ABS(total_assets - (position_market_value + available_cash + frozen_cash)) > 10
                """
                balance_result = self.api.query(balance_sql, formatted_dates)

                if not balance_result.empty and balance_result.iloc[0]['imbalanced_count'] > 0:
                    imbalanced_count = balance_result.iloc[0]['imbalanced_count']
                    self._add_issue('warning', 'accuracy', 'user_account_info',
                                  f"在抽样日期中发现{imbalanced_count}条资产平衡错误记录")

        except Exception as e:
            self._add_issue('warning', 'completeness', 'user_account_info',
                          f"用户账户信息检查失败: {e}")

    def _check_user_transactions_in_dates(self, formatted_dates: List[str]):
        """检查用户交易记录在指定日期的质量"""
        try:
            dates_placeholder = ','.join(['?' for _ in formatted_dates])
            trans_sql = f"""
                SELECT trade_date, COUNT(*) as trans_count
                FROM user_transactions
                WHERE trade_date IN ({dates_placeholder})
                GROUP BY trade_date
            """
            trans_result = self.api.query(trans_sql, formatted_dates)

            if not trans_result.empty:
                # 检查交易记录的准确性
                accuracy_sql = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM user_transactions
                    WHERE trade_date IN ({dates_placeholder})
                    AND (quantity <= 0 OR price <= 0 OR trade_type NOT IN (23, 24))
                """
                accuracy_result = self.api.query(accuracy_sql, formatted_dates)

                if not accuracy_result.empty and accuracy_result.iloc[0]['invalid_count'] > 0:
                    invalid_count = accuracy_result.iloc[0]['invalid_count']
                    self._add_issue('critical', 'accuracy', 'user_transactions',
                                  f"在抽样日期中发现{invalid_count}条交易记录逻辑错误")

        except Exception as e:
            self._add_issue('warning', 'completeness', 'user_transactions',
                          f"用户交易记录检查失败: {e}")

    def _check_stock_list_basic(self):
        """检查股票列表的基本质量"""
        try:
            # 检查表是否为空
            count_sql = "SELECT COUNT(*) as count FROM stock_list"
            count_result = self.api.query(count_sql)

            if count_result.empty or count_result.iloc[0]['count'] == 0:
                self._add_issue('critical', 'completeness', DataType.STOCK_LIST, "股票列表为空")
                return

            # 检查关键字段的NULL值
            null_sql = """
                SELECT
                    SUM(CASE WHEN code IS NULL THEN 1 ELSE 0 END) as null_code,
                    SUM(CASE WHEN display_name IS NULL THEN 1 ELSE 0 END) as null_name,
                    COUNT(*) as total
                FROM stock_list
            """
            null_result = self.api.query(null_sql)

            if not null_result.empty:
                row = null_result.iloc[0]
                total = row['total']

                if row['null_code'] > 0:
                    self._add_issue('critical', 'completeness', DataType.STOCK_LIST,
                                  f"发现{row['null_code']}条股票代码为NULL的记录")

                if row['null_name'] > 0:
                    ratio = row['null_name'] / total
                    severity = 'critical' if ratio > 0.1 else 'warning'
                    self._add_issue(severity, 'completeness', DataType.STOCK_LIST,
                                  f"发现{row['null_name']}条股票名称为NULL的记录，比例{ratio:.2%}")

        except Exception as e:
            self._add_issue('critical', 'completeness', DataType.STOCK_LIST,
                          f"股票列表基本检查失败: {e}")

    def _generate_routine_report(self, level: str, start_time: datetime,
                               tables_checked: List[str], recent_days: int,
                               historical_sample_days: int) -> QualityReport:
        """生成例行检查报告"""
        critical_count = sum(1 for issue in self.issues if issue.severity == 'critical')
        warning_count = sum(1 for issue in self.issues if issue.severity == 'warning')

        # 按表分组统计
        table_stats = {}
        for issue in self.issues:
            if issue.table not in table_stats:
                table_stats[issue.table] = {'critical': 0, 'warning': 0, 'info': 0}
            table_stats[issue.table][issue.severity] += 1

        summary = {
            'check_type': 'daily_routine',
            'recent_days_checked': recent_days,
            'historical_sample_days': historical_sample_days,
            'check_duration_seconds': (datetime.now() - start_time).total_seconds(),
            'tables_with_issues': len(table_stats),
            'table_stats': table_stats,
            'category_stats': {
                'completeness': sum(1 for i in self.issues if i.category == 'completeness'),
                'uniqueness': sum(1 for i in self.issues if i.category == 'uniqueness'),
                'accuracy': sum(1 for i in self.issues if i.category == 'accuracy')
            }
        }

        return QualityReport(
            check_level=level,
            check_time=start_time,
            total_issues=len(self.issues),
            critical_issues=critical_count,
            warning_issues=warning_count,
            tables_checked=tables_checked,
            issues=self.issues,
            summary=summary
        )

    def _check_financial_data_for_period(self, trading_day: str, full_stock_check: bool):
        """检查财务数据（季度数据）在指定时期的完整性"""
        try:
            # 财务数据是季度数据，需要检查最近的季度报告期
            year = int(trading_day[:4])
            month = int(trading_day[4:6])

            # 确定当前日期对应的最近报告期
            if month <= 3:
                latest_quarter = f"{year-1}1231"  # 上年年报
            elif month <= 4:
                latest_quarter = f"{year}0331"    # 一季报
            elif month <= 8:
                latest_quarter = f"{year}0630"    # 中报
            elif month <= 10:
                latest_quarter = f"{year}0930"    # 三季报
            else:
                latest_quarter = f"{year}1231"    # 年报

            self.logger.info(f"检查交易日{trading_day}对应的财务数据，最近报告期：{latest_quarter}")

            # 财务数据表列表（只检查indicator_data）
            financial_tables = {
                DataType.INDICATOR_DATA: 'indicator_data'
            }

            # 获取活跃股票列表
            day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"
            active_stocks_sql = """
                SELECT code FROM stock_list
                WHERE (end_date IS NULL OR end_date > ?)
                AND start_date <= ?
            """
            active_stocks = self.api.query(active_stocks_sql, [day_formatted, day_formatted])

            if active_stocks.empty:
                self.logger.warning(f"交易日{trading_day}无活跃股票信息")
                return

            expected_count = len(active_stocks)
            quarter_formatted = f"{latest_quarter[:4]}-{latest_quarter[4:6]}-{latest_quarter[6:8]}"

            # 检查每个财务数据表
            for data_type, table_name in financial_tables.items():
                try:
                    if full_stock_check:
                        # 全量检查
                        financial_sql = f"""
                            SELECT COUNT(DISTINCT code) as count
                            FROM {table_name}
                            WHERE stat_date = ? OR statDate = ?
                        """
                        result = self.api.query(financial_sql, [quarter_formatted, quarter_formatted])

                        if not result.empty:
                            actual_count = result.iloc[0]['count']
                            if actual_count < expected_count * 0.7:  # 财务数据70%覆盖率
                                missing_ratio = 1 - (actual_count / expected_count)
                                severity = 'critical' if missing_ratio > 0.5 else 'warning'
                                self._add_issue(severity, 'completeness', data_type,
                                              f"报告期{latest_quarter}的{table_name}数据缺失: {actual_count}/{expected_count}股票，缺失{missing_ratio:.2%}")
                        else:
                            self._add_issue('warning', 'completeness', data_type,
                                          f"报告期{latest_quarter}的{table_name}数据为空")
                    else:
                        # 抽样检查（历史数据）
                        sample_stocks = active_stocks.sample(n=min(50, len(active_stocks)))
                        missing_count = 0

                        for _, row in sample_stocks.iterrows():
                            code = row['code']
                            check_sql = f"""
                                SELECT COUNT(*) as count FROM {table_name}
                                WHERE code = ? AND (stat_date = ? OR statDate = ?)
                            """
                            check_result = self.api.query(check_sql, [code, quarter_formatted, quarter_formatted])

                            if check_result.empty or check_result.iloc[0]['count'] == 0:
                                missing_count += 1

                        if missing_count > len(sample_stocks) * 0.3:  # 超过30%缺失
                            self._add_issue('warning', 'completeness', data_type,
                                          f"报告期{latest_quarter}抽样发现{missing_count}/{len(sample_stocks)}股票{table_name}数据缺失")

                except Exception as e:
                    self._add_issue('warning', 'completeness', data_type,
                                  f"检查{table_name}数据失败: {e}")

        except Exception as e:
            self._add_issue('warning', 'completeness', 'financial_data',
                          f"财务数据检查失败: {e}")
    def _check_indicator_data_for_day(self, trading_day: str, full_stock_check: bool):
        """检查技术指标数据（日频数据）在特定交易日的完整性"""
        day_formatted = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"

        try:
            # 检查技术指标数据与价格数据的一致性
            missing_sql = """
                SELECT COUNT(*) as missing_count
                FROM price_data p
                LEFT JOIN indicator_data i ON p.code = i.code AND p.day = i.statDate
                WHERE p.day = ? AND i.code IS NULL
            """
            missing_result = self.api.query(missing_sql, [day_formatted])

            if not missing_result.empty:
                missing_count = missing_result.iloc[0]['missing_count']
                if missing_count > 0:
                    # 获取价格数据总数作为基准
                    total_sql = "SELECT COUNT(*) as total FROM price_data WHERE day = ?"
                    total_result = self.api.query(total_sql, [day_formatted])

                    if not total_result.empty:
                        total_count = total_result.iloc[0]['total']
                        if total_count > 0:
                            missing_ratio = missing_count / total_count
                            if missing_ratio > 0.1:  # 超过10%缺失
                                severity = 'critical' if missing_ratio > 0.5 else 'warning'
                                samples = []
                                # 获取一些缺失的股票代码作为样本
                                sample_sql = """
                                    SELECT p.code
                                    FROM price_data p
                                    LEFT JOIN indicator_data i ON p.code = i.code AND p.day = i.statDate
                                    WHERE p.day = ? AND i.code IS NULL
                                    LIMIT 5
                                """
                                sample_result = self.api.query(sample_sql, [day_formatted])
                                if not sample_result.empty:
                                    samples = sample_result['code'].tolist()

                                self._add_issue(severity, 'completeness', DataType.INDICATOR_DATA,
                                              f"交易日{trading_day}技术指标数据缺失{missing_count}/{total_count}股票，缺失比例{missing_ratio:.2%}",
                                              count=missing_count, samples=samples)

        except Exception as e:
            self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                          f"交易日{trading_day}技术指标数据检查失败: {e}")

    def _check_quarterly_indicator_data(self):
        """检查季度财报指标数据（indicator_data）的完整性和时效性（排除北交所股票）"""
        try:
            # 检查数据总量（排除BJ股票）
            total_sql = "SELECT COUNT(*) as total FROM indicator_data WHERE code NOT LIKE '%.BJ'"
            total_result = self.api.query(total_sql)

            if total_result.empty or total_result.iloc[0]['total'] == 0:
                self._add_issue('critical', 'completeness', DataType.INDICATOR_DATA,
                              "indicator_data表为空，无财报指标数据")
                return

            total_count = total_result.iloc[0]['total']

            # 检查最近的财报期分布（只查询季度末日期，排除BJ股票）
            recent_quarters_sql = """
                SELECT statDate, COUNT(*) as count
                FROM indicator_data
                WHERE statDate >= '2024-01-01'
                AND code NOT LIKE '%.BJ'
                AND (CAST(statDate AS VARCHAR) LIKE '%-03-31' OR
                     CAST(statDate AS VARCHAR) LIKE '%-06-30' OR
                     CAST(statDate AS VARCHAR) LIKE '%-09-30' OR
                     CAST(statDate AS VARCHAR) LIKE '%-12-31')
                GROUP BY statDate
                ORDER BY statDate DESC
            """
            recent_result = self.api.query(recent_quarters_sql)

            if recent_result.empty:
                self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                              "2024年以来无财报指标数据")
            else:
                # 检查最新财报期是否过时（超过6个月没有新数据）
                latest_date = recent_result.iloc[0]['statDate']
                from datetime import datetime, timedelta

                # 处理不同的日期格式
                if isinstance(latest_date, str):
                    latest_datetime = datetime.strptime(latest_date, '%Y-%m-%d')
                else:
                    latest_datetime = latest_date

                six_months_ago = datetime.now() - timedelta(days=180)

                if latest_datetime < six_months_ago:
                    self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                                  f"最新财报数据过期，最新期间：{latest_date}")

                # 检查主要财报期的数据覆盖
                expected_quarters = ['2025-06-30', '2025-03-31', '2024-12-31', '2024-09-30']
                missing_quarters = []

                for quarter in expected_quarters:
                    quarter_data = recent_result[recent_result['statDate'] == quarter]
                    if quarter_data.empty:
                        missing_quarters.append(quarter)
                    else:
                        count = quarter_data.iloc[0]['count']
                        if count < 3000:  # 预期至少3000只股票有数据
                            self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                                          f"财报期{quarter}数据不足，仅{count}只股票有数据")

                if missing_quarters:
                    self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                                  f"缺失财报期数据: {', '.join(missing_quarters)}")

            # 检查数据完整性（是否有空值过多的情况，排除BJ股票）
            null_check_sql = """
                SELECT
                    SUM(CASE WHEN eps IS NULL THEN 1 ELSE 0 END) as null_eps,
                    SUM(CASE WHEN roe IS NULL THEN 1 ELSE 0 END) as null_roe,
                    COUNT(*) as total
                FROM indicator_data
                WHERE statDate >= '2024-01-01'
                AND code NOT LIKE '%.BJ'
            """
            null_result = self.api.query(null_check_sql)

            if not null_result.empty:
                row = null_result.iloc[0]
                if row['total'] > 0:
                    eps_null_ratio = row['null_eps'] / row['total']
                    roe_null_ratio = row['null_roe'] / row['total']

                    if eps_null_ratio > 0.5:
                        self._add_issue('warning', 'accuracy', DataType.INDICATOR_DATA,
                                      f"EPS字段空值比例过高: {eps_null_ratio:.2%}")

                    if roe_null_ratio > 0.5:
                        self._add_issue('warning', 'accuracy', DataType.INDICATOR_DATA,
                                      f"ROE字段空值比例过高: {roe_null_ratio:.2%}")

        except Exception as e:
            self._add_issue('warning', 'completeness', DataType.INDICATOR_DATA,
                          f"季度财报数据检查失败: {e}")
