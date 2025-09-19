"""控制台报告器"""

class ConsoleReporter:
    """控制台报告器"""

    def print_report(self, report):
        """打印完整报告"""
        self._print_header(report)
        self._print_summary(report)
        self._print_issues(report)
        self._print_footer(report)

    def _print_header(self, report):
        """打印报告头部"""
        print(f"\n{'='*60}")
        print(f"数据质量检查报告 - {report.check_level.upper()}级别")
        print(f"{'='*60}")
        print(f"检查时间: {report.check_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"执行耗时: {report.execution_duration:.2f}秒")

    def _print_summary(self, report):
        """打印摘要信息"""
        print(f"\n📊 检查摘要:")
        print(f"   检查表数: {len(report.tables_checked)}")
        print(f"   总问题数: {report.total_issues}")
        print(f"   严重问题: {report.critical_issues}")
        print(f"   警告问题: {report.warning_issues}")

        if report.total_issues == 0:
            print("   🎉 恭喜！未发现任何数据质量问题")

    def _print_issues(self, report):
        """打印问题详情"""
        if not report.issues:
            return

        print(f"\n📋 问题详情:")

        # 按表分组显示
        issues_by_table = {}
        for issue in report.issues:
            if issue.table not in issues_by_table:
                issues_by_table[issue.table] = []
            issues_by_table[issue.table].append(issue)

        for table_name, table_issues in issues_by_table.items():
            print(f"\n  📄 {table_name}:")
            for issue in table_issues:
                severity_icon = "🔴" if issue.severity == 'critical' else "🟡"
                print(f"    {severity_icon} [{issue.category.upper()}] {issue.description}")
                if issue.count > 0:
                    print(f"        影响记录数: {issue.count}")
                if issue.samples:
                    samples_text = ", ".join(issue.samples[:3])
                    print(f"        样本: {samples_text}")

    def _print_footer(self, report):
        """打印报告尾部"""
        print(f"\n{'='*60}")
        if report.critical_issues > 0:
            print("⚠️  发现严重问题，建议立即处理")
        elif report.warning_issues > 0:
            print("⚠️  发现警告问题，建议关注并计划处理")
        print()