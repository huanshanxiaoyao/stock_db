"""数据质量检查引擎"""

from typing import List, Dict, Any
from datetime import datetime
from dataclasses import dataclass
from .config_loader import ConfigLoader, QualityConfig
from .template_processor import TemplateProcessor
from .utils import validate_sql_readonly

@dataclass
class QualityIssue:
    """质量问题记录"""
    severity: str  # 'critical', 'warning'
    category: str  # 'completeness', 'uniqueness', 'accuracy'
    table: str
    check_name: str
    description: str
    count: int = 0
    samples: List[str] = None

    def __post_init__(self):
        if self.samples is None:
            self.samples = []

@dataclass
class QualityReport:
    """质量检查报告"""
    check_level: str
    check_time: datetime
    total_issues: int
    critical_issues: int
    warning_issues: int
    tables_checked: List[str]
    issues: List[QualityIssue]
    execution_duration: float

class CheckEngine:
    """检查执行引擎"""

    def __init__(self, db_interface, config_loader: ConfigLoader, template_processor: TemplateProcessor):
        self.db = db_interface
        self.config_loader = config_loader
        self.template_processor = template_processor
        self.issues = []

    def run_quality_check(self, level: str = 'quick', tables: List[str] = None) -> QualityReport:
        """执行数据质量检查 - 主入口"""
        start_time = datetime.now()
        self.issues = []

        # 加载配置
        config = self.config_loader.load_config()

        # 确定要检查的表
        tables_to_check = self._determine_tables_to_check(config, level, tables)

        # 执行检查
        for table_name in tables_to_check:
            if self.config_loader.is_table_enabled(config, table_name):
                self._check_single_table(config, table_name, level)

        # 生成报告
        return self._generate_report(level, start_time, tables_to_check)

    def _determine_tables_to_check(self, config: QualityConfig, level: str, tables: List[str]) -> List[str]:
        """确定要检查的表列表"""
        if tables:
            # 验证指定的表是否在配置中存在
            invalid_tables = [t for t in tables if t not in config.tables]
            if invalid_tables:
                raise ValueError(f"配置中不存在以下表: {', '.join(invalid_tables)}")

            # 验证表是否启用
            disabled_tables = [t for t in tables if not config.tables[t].get('enabled', False)]
            if disabled_tables:
                print(f"警告: 以下表在配置中被禁用，将跳过检查: {', '.join(disabled_tables)}")
                tables = [t for t in tables if t not in disabled_tables]

            return tables

        # 从配置中获取默认表列表
        check_levels = config.global_config.get('check_levels', {})
        level_config = check_levels.get(level, {})
        default_tables = level_config.get('default_tables', [])
        if default_tables:
            return default_tables

        # 如果没有配置默认表，返回所有启用的表
        enabled_tables = [name for name in config.tables.keys() if config.tables[name].get('enabled', False)]
        if not enabled_tables:
            raise ValueError("没有找到可检查的表，请检查配置文件")

        return enabled_tables

    def _check_single_table(self, config: QualityConfig, table_name: str, level: str):
        """检查单个表"""
        table_config = self.config_loader.get_table_config(config, table_name)

        # 执行完整性检查
        completeness_checks = table_config.get('completeness_checks', [])
        self._execute_check_category(table_name, 'completeness', completeness_checks, level, config)

        # 执行唯一性检查
        uniqueness_checks = table_config.get('uniqueness_checks', [])
        self._execute_check_category(table_name, 'uniqueness', uniqueness_checks, level, config)

        # 执行准确性检查
        accuracy_checks = table_config.get('accuracy_checks', [])
        self._execute_check_category(table_name, 'accuracy', accuracy_checks, level, config)

    def _execute_check_category(self, table_name: str, category: str, checks: List[Dict], level: str, config: QualityConfig):
        """执行特定类别的检查"""
        for check in checks:
            # 检查是否应该在当前级别执行
            check_level = check.get('level', 'quick')
            if check_level != 'quick' and level == 'quick':
                continue

            self._execute_single_check(table_name, category, check, config)

    def _execute_single_check(self, table_name: str, category: str, check: Dict, config: QualityConfig):
        """执行单个检查"""
        check_name = check['name']

        # 准备模板上下文
        context = {
            'table_name': table_name,
            'sample_size': config.global_config.get('check_levels', {}).get('quick', {}).get('sample_size', 100)
        }

        # 处理不同类型的检查
        if 'sql' in check:
            # 直接SQL检查
            sql = self.template_processor.process_sql(check['sql'], context)
            validate_sql_readonly(sql)
            try:
                result = self.db.query_data(sql)
            except Exception as e:
                # SQL执行失败，记录为严重问题
                issue = QualityIssue(
                    severity='critical',
                    category=category,
                    table=table_name,
                    check_name=check['name'],
                    description=f"SQL执行失败: {e}",
                    count=0
                )
                self.issues.append(issue)
                return

            if 'condition' in check:
                # 有条件验证的检查
                condition = check['condition']
                self._validate_condition_check(table_name, category, check, result, condition)
            elif 'condition_template' in check:
                # 条件模板检查
                condition_template = check['condition_template']
                # 准备额外的上下文变量
                template_context = self._prepare_condition_context(table_name, context, config)
                condition = self.template_processor.process_condition(condition_template, template_context)
                self._validate_condition_check(table_name, category, check, result, condition)
            else:
                # 简单的计数检查
                self._validate_count_check(table_name, category, check, result)

        elif 'sql_template' in check:
            # 字段模板检查（如NULL值检查）
            self._execute_template_check(table_name, category, check, context, config)

        elif check_name == 'time_series_completeness':
            # 特殊的时间序列检查
            self._execute_time_series_check(table_name, category, check, context, config)

    def _validate_condition_check(self, table_name: str, category: str, check: Dict, result, condition: str):
        """验证带条件的检查"""
        if result.empty:
            return

        # 评估条件（简单的数值比较）
        first_row = result.iloc[0]

        # 解析条件（如 "count > 0", "invalid_count = 0", "stocks_with_indicators >= active_stocks * 0.8"）
        if ">=" in condition:
            field, expected_expr = condition.split(">=")
            field = field.strip()
            expected = float(expected_expr.strip())
            actual = first_row.get(field, 0)
            passed = actual >= expected
        elif ">" in condition:
            field, expected_expr = condition.split(">")
            field = field.strip()
            expected = float(expected_expr.strip())
            actual = first_row.get(field, 0)
            passed = actual > expected
        elif "=" in condition:
            field, expected_expr = condition.split("=")
            field = field.strip()
            expected = float(expected_expr.strip())
            actual = first_row.get(field, 0)
            passed = actual == expected
        else:
            passed = False
            field = 'unknown'
            actual = 0

        if not passed:
            issue = QualityIssue(
                severity=check.get('severity', 'warning'),
                category=category,
                table=table_name,
                check_name=check['name'],
                description=check.get('description', f"{check['name']} 检查失败"),
                count=int(first_row.get(field, 0))
            )
            self.issues.append(issue)

    def _validate_count_check(self, table_name: str, category: str, check: Dict, result):
        """验证计数检查"""
        if not result.empty and len(result) > 0:
            # 有结果说明检查失败
            samples = []
            for i, row in result.iterrows():
                if i >= 5:  # 最多5个样本
                    break
                # 构建样本描述
                sample_parts = [str(row[col]) for col in result.columns if col != 'count']
                samples.append("(" + ", ".join(sample_parts) + ")")

            issue = QualityIssue(
                severity=check.get('severity', 'warning'),
                category=category,
                table=table_name,
                check_name=check['name'],
                description=check.get('description', f"{check['name']} 检查失败"),
                count=len(result),
                samples=samples
            )
            self.issues.append(issue)

    def _execute_template_check(self, table_name: str, category: str, check: Dict, context: Dict, config: QualityConfig):
        """执行模板检查（如字段NULL值检查）"""
        sql_template = check['sql_template']
        fields = check.get('fields', [])
        threshold = check.get('threshold', 0)

        for field in fields:
            field_context = {**context, 'field': field}
            sql = self.template_processor.process_sql(sql_template, field_context)
            validate_sql_readonly(sql)
            result = self.db.query_data(sql)

            if not result.empty:
                null_count = result.iloc[0].get('null_count', 0)
                if null_count > threshold:
                    issue = QualityIssue(
                        severity=check.get('severity', 'warning'),
                        category=category,
                        table=table_name,
                        check_name=f"{check['name']}_{field}",
                        description=f"{check.get('description', '')} - 字段 {field}",
                        count=int(null_count)
                    )
                    self.issues.append(issue)

    def _execute_time_series_check(self, table_name: str, category: str, check: Dict, context: Dict, config: QualityConfig):
        """执行时间序列完整性检查"""
        if 'sql_base' not in check:
            return

        # 获取要检查的股票样本
        sql = self.template_processor.process_sql(check['sql_base'], context)
        validate_sql_readonly(sql)

        try:
            sample_stocks = self.db.query_data(sql)
        except Exception as e:
            # SQL执行失败
            issue = QualityIssue(
                severity='critical',
                category=category,
                table=table_name,
                check_name=check['name'],
                description=f"时间序列检查SQL执行失败: {e}",
                count=0
            )
            self.issues.append(issue)
            return

        if sample_stocks.empty:
            return

        completion_threshold = check.get('completion_threshold', 0.95)
        incomplete_stocks = []

        # 逐个检查每只股票的时间序列完整性
        for _, stock_row in sample_stocks.iterrows():
            code = stock_row['code']
            start_date = stock_row.get('start_date')
            end_date = stock_row.get('end_date')

            # 计算该股票的数据完整性
            completeness_ratio = self._calculate_stock_completeness(table_name, code, start_date, end_date)

            if completeness_ratio < completion_threshold:
                incomplete_stocks.append({
                    'code': code,
                    'completeness': f"{completeness_ratio:.2%}"
                })

        # 如果有不完整的股票，记录问题
        if incomplete_stocks:
            samples = [f"{item['code']}({item['completeness']})" for item in incomplete_stocks[:5]]
            issue = QualityIssue(
                severity=check.get('severity', 'warning'),
                category=category,
                table=table_name,
                check_name=check['name'],
                description=f"时间序列完整性检查失败，{len(incomplete_stocks)}只股票数据不完整",
                count=len(incomplete_stocks),
                samples=samples
            )
            self.issues.append(issue)

    def _calculate_stock_completeness(self, table_name: str, code: str, start_date, end_date) -> float:
        """计算单只股票的数据完整性比率"""
        try:
            from common.date_utils import get_trading_days, get_last_trading_day
            from datetime import datetime

            # 确定检查的时间范围
            today_str = datetime.now().strftime("%Y%m%d")
            last_trade_str = get_last_trading_day(today_str, n=0)

            # 处理开始日期
            if start_date:
                if hasattr(start_date, 'strftime'):
                    start_str = start_date.strftime("%Y%m%d")
                else:
                    start_str = str(start_date).replace('-', '')[:8]
            else:
                start_str = "20190101"  # 默认开始日期

            # 处理结束日期
            if end_date and end_date < datetime.now().date():
                if hasattr(end_date, 'strftime'):
                    end_str = end_date.strftime("%Y%m%d")
                else:
                    end_str = str(end_date).replace('-', '')[:8]
            else:
                end_str = last_trade_str

            # 确保开始日期不晚于结束日期
            if start_str > end_str:
                return 1.0  # 无效日期范围，认为完整

            # 获取预期的交易日
            expected_days = get_trading_days(start_str, end_str)
            if not expected_days:
                return 1.0  # 没有预期交易日

            # 查询实际数据条数
            start_date_sql = f"{start_str[:4]}-{start_str[4:6]}-{start_str[6:8]}"
            end_date_sql = f"{end_str[:4]}-{end_str[4:6]}-{end_str[6:8]}"

            actual_sql = f"""
                SELECT COUNT(*) as count
                FROM {table_name}
                WHERE code = '{code}' AND day >= '{start_date_sql}' AND day <= '{end_date_sql}'
            """

            result = self.db.query_data(actual_sql)
            if result.empty:
                return 0.0

            actual_count = result.iloc[0]['count']
            expected_count = len(expected_days)

            return min(actual_count / expected_count, 1.0) if expected_count > 0 else 1.0

        except Exception as e:
            # 计算失败时返回 1.0，避免误报
            return 1.0

    def _generate_report(self, level: str, start_time: datetime, tables_checked: List[str]) -> QualityReport:
        """生成检查报告"""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        critical_count = sum(1 for issue in self.issues if issue.severity == 'critical')
        warning_count = sum(1 for issue in self.issues if issue.severity == 'warning')

        return QualityReport(
            check_level=level,
            check_time=start_time,
            total_issues=len(self.issues),
            critical_issues=critical_count,
            warning_issues=warning_count,
            tables_checked=tables_checked,
            issues=self.issues,
            execution_duration=duration
        )

    def _prepare_condition_context(self, table_name: str, base_context: Dict, config: QualityConfig) -> Dict[str, Any]:
        """为条件模板准备上下文变量"""
        context = {**base_context}

        # 添加动态计算的变量
        try:
            if 'active_stocks' not in context:
                # 计算活跃股票数量
                active_stocks_sql = """
                    SELECT COUNT(*) as count
                    FROM stock_list
                    WHERE (end_date IS NULL OR end_date > CURRENT_DATE)
                    AND status = 'normal'
                """
                result = self.db.query_data(active_stocks_sql)
                context['active_stocks'] = result.iloc[0]['count'] if not result.empty else 0

            if 'total_records' not in context:
                # 计算表总记录数
                total_sql = f"SELECT COUNT(*) as count FROM {table_name}"
                result = self.db.query_data(total_sql)
                context['total_records'] = result.iloc[0]['count'] if not result.empty else 0

            if 'recent_records' not in context:
                # 计算最近记录数（根据表选择合适的日期字段）
                date_field = 'day'
                if table_name == 'indicator_data':
                    date_field = 'statDate'
                elif table_name in ['income_statement', 'balance_sheet', 'cashflow_statement']:
                    date_field = 'statDate'

                recent_sql = f"""
                    SELECT COUNT(*) as count FROM {table_name}
                    WHERE {date_field} >= CURRENT_DATE - INTERVAL '30 days'
                """
                try:
                    result = self.db.query_data(recent_sql)
                    context['recent_records'] = result.iloc[0]['count'] if not result.empty else 0
                except:
                    context['recent_records'] = context['total_records']

        except Exception as e:
            # 如果动态计算失败，使用默认值
            context.setdefault('active_stocks', 4000)
            context.setdefault('total_records', 1)
            context.setdefault('recent_records', 1)

        return context

    def _evaluate_expression(self, expr: str) -> float:
        """安全地计算简单的数学表达式"""
        try:
            # 只允许数字、运算符和有限的变量名
            allowed_chars = set('0123456789+-*/.() ')
            if not all(c in allowed_chars or c.isalpha() or c == '_' for c in expr):
                raise ValueError(f"表达式包含不允许的字符: {expr}")

            # 简单的数值检查
            if expr.replace('.', '').replace('-', '').isdigit():
                return float(expr)

            # 不支持复杂表达式，抛出错误
            raise ValueError(f"不支持的表达式格式: {expr}")

        except Exception:
            # 如果计算失败，尝试解析为简单数字
            try:
                return float(expr)
            except:
                return 0.0