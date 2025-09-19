"""数据质量检查服务类"""

from typing import List, Dict, Any, Optional, Set
from datetime import date, datetime, timedelta
from dataclasses import dataclass
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
import json

from data_source import DataType
from date_utils import get_trading_days, get_last_trading_day
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
            'total_issues': self.total_issues,
            'critical_issues': self.critical_issues,
            'warning_issues': self.warning_issues,
            'tables_checked': self.tables_checked,
            'issues': [
                {
                    'severity': issue.severity,
                    'category': issue.category,
                    'table': issue.table,
                    'description': issue.description,
                    'count': issue.count,
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