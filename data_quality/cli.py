"""数据质量检查CLI接口"""

import argparse
import sys
from . import run_quality_check

def main():
    """CLI主入口"""
    parser = argparse.ArgumentParser(description="数据质量检查工具")
    parser.add_argument("--level", choices=["quick", "standard"], default="quick",
                       help="检查级别")
    parser.add_argument("--tables", nargs="*", help="要检查的表列表")
    parser.add_argument("--db-path", default=None, help="数据库路径（默认从配置文件读取）")
    parser.add_argument("--output-report", help="JSON报告输出路径")

    args = parser.parse_args()

    try:
        report = run_quality_check(
            db_path=args.db_path,
            level=args.level,
            tables=args.tables,
            output_report=args.output_report
        )

        # 设置退出码
        if report.critical_issues > 0:
            sys.exit(1)  # 有严重问题时退出码为1
        elif report.warning_issues > 0:
            sys.exit(2)  # 只有警告问题时退出码为2
        else:
            sys.exit(0)  # 无问题时退出码为0

    except FileNotFoundError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(4)
    except ValueError as e:
        print(f"配置错误: {e}", file=sys.stderr)
        sys.exit(5)
    except Exception as e:
        print(f"执行失败: {e}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()