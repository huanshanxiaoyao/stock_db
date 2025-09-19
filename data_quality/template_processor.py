"""SQL模板处理器"""

from typing import Dict, Any

class TemplateProcessor:
    """SQL模板处理器"""

    def __init__(self, thresholds: Dict[str, Any]):
        self.thresholds = thresholds

    def process_sql(self, sql_template: str, context: Dict[str, Any] = None) -> str:
        """处理SQL模板中的占位符"""
        template_context = {**self.thresholds}
        if context:
            template_context.update(context)

        # 添加常用的上下文变量
        if 'sample_size' not in template_context:
            template_context['sample_size'] = 100

        return sql_template.format(**template_context)

    def process_condition(self, condition_template: str, context: Dict[str, Any] = None) -> str:
        """处理条件模板"""
        return self.process_sql(condition_template, context)