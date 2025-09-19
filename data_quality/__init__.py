"""数据质量检查子系统

对外提供数据质量检查功能的独立子模块
"""

from .check_engine import CheckEngine, QualityReport
from .config_loader import ConfigLoader
from .template_processor import TemplateProcessor
from .reporters.console_reporter import ConsoleReporter
from .reporters.json_reporter import JSONReporter

def run_quality_check(db_path: str = None, level: str = 'quick', tables: list = None,
                     output_report: str = None) -> QualityReport:
    """执行数据质量检查 - 对外主接口"""
    # 如果没有指定数据库路径，从配置中读取
    if db_path is None:
        from config import get_config
        config_obj = get_config()
        db_path = config_obj.database.path

    # 创建数据库连接（只读模式，不初始化）
    from duckdb_impl import DuckDBDatabase
    db = DuckDBDatabase(db_path)
    db.connect()  # 只连接，不创建表

    # 初始化组件
    config_loader = ConfigLoader()
    template_processor = TemplateProcessor(config_loader.load_config().thresholds)
    check_engine = CheckEngine(db, config_loader, template_processor)

    # 执行检查
    report = check_engine.run_quality_check(level, tables)

    # 输出报告
    console_reporter = ConsoleReporter()
    console_reporter.print_report(report)

    # 保存JSON报告
    if output_report:
        json_reporter = JSONReporter()
        json_reporter.save_report(report, output_report)
        print(f"详细报告已保存到: {output_report}")

    return report