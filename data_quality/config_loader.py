"""数据质量配置加载器"""

from dataclasses import dataclass
from typing import Dict, List, Any
import yaml

@dataclass
class QualityConfig:
    """数据质量配置"""
    global_config: Dict[str, Any]
    tables: Dict[str, Dict[str, Any]]
    thresholds: Dict[str, float]

class ConfigLoader:
    """配置加载器"""

    def __init__(self, config_path: str = "data_quality_checks.yaml"):
        self.config_path = config_path

    def load_config(self) -> QualityConfig:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"数据质量配置文件不存在: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {e}")
        except Exception as e:
            raise RuntimeError(f"读取配置文件失败: {e}")

        if not raw_config:
            raise ValueError("配置文件为空")

        return QualityConfig(
            global_config=raw_config.get('global_config', {}),
            tables=raw_config.get('tables', {}),
            thresholds=raw_config.get('global_config', {}).get('thresholds', {})
        )

    def get_table_config(self, config: QualityConfig, table_name: str) -> Dict[str, Any]:
        """获取表配置"""
        return config.tables.get(table_name, {})

    def is_table_enabled(self, config: QualityConfig, table_name: str) -> bool:
        """检查表是否启用检查"""
        table_config = self.get_table_config(config, table_name)
        return table_config.get('enabled', False)