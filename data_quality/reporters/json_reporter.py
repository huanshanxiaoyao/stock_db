"""JSON报告器"""

import json
from typing import Dict, Any

class JSONReporter:
    """JSON报告器"""

    def generate_json_report(self, report) -> Dict[str, Any]:
        """生成JSON格式报告"""
        return {
            "metadata": {
                "check_level": report.check_level,
                "check_time": report.check_time.isoformat(),
                "execution_duration_seconds": report.execution_duration
            },
            "summary": {
                "tables_checked": report.tables_checked,
                "total_issues": report.total_issues,
                "critical_issues": report.critical_issues,
                "warning_issues": report.warning_issues
            },
            "issues": [
                {
                    "severity": issue.severity,
                    "category": issue.category,
                    "table": issue.table,
                    "check_name": issue.check_name,
                    "description": issue.description,
                    "count": issue.count,
                    "samples": issue.samples
                }
                for issue in report.issues
            ]
        }

    def save_report(self, report, file_path: str):
        """保存JSON报告到文件"""
        json_data = self.generate_json_report(report)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)