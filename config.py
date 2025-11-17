"""配置管理模块"""

import os
import yaml
import json
from typing import Dict, Any, Optional, Union
from pathlib import Path
import logging
from dataclasses import dataclass, field
import platform

# 获取项目根目录的绝对路径
PROJECT_ROOT = Path(__file__).parent.resolve()

def _get_default_data_root() -> Path:
    """获取平台特定的默认数据根目录

    Returns:
        Path: 默认数据根目录路径
    """
    system = platform.system()
    if system == "Windows":
        return Path("D:/Data/stock_db")
    elif system == "Darwin":  # macOS
        return Path.home() / "Data" / "stock_db"
    else:  # Linux
        return Path.home() / "data" / "stock_db"

# 默认数据根目录（平台自适应）
DEFAULT_DATA_ROOT = _get_default_data_root()

def get_data_root() -> Path:
    """获取数据根目录

    优先级：
    1. 环境变量 STOCK_DATA_ROOT
    2. 配置文件中的设置
    3. 平台默认路径:
       - Windows: D:/Data/stock_db
       - macOS: ~/Data/stock_db
       - Linux: ~/data/stock_db
    """
    # 从环境变量获取
    env_path = os.getenv("STOCK_DATA_ROOT")
    if env_path:
        return Path(env_path).resolve()
    
    # 使用默认路径
    return DEFAULT_DATA_ROOT

def resolve_data_path(relative_path: str) -> str:
    """解析数据相关路径，基于数据根目录
    
    Args:
        relative_path: 相对于数据根目录的路径
        
    Returns:
        str: 绝对路径
    """
    data_root = get_data_root()
    return str(data_root / relative_path)

def resolve_project_path(relative_path: str) -> str:
    """解析项目相关路径，基于项目根目录
    
    Args:
        relative_path: 相对于项目根目录的路径
        
    Returns:
        str: 绝对路径
    """
    return str(PROJECT_ROOT / relative_path)


@dataclass
class DatabaseConfig:
    """数据库配置"""
    type: str = "duckdb"
    path: str = resolve_data_path("stock_data.duckdb")
    memory_mode: bool = False
    pool_size: int = 5
    query_timeout: int = 300


@dataclass
class DataSourceConfig:
    """数据源配置"""
    enabled: bool = True
    credentials: Dict[str, str] = field(default_factory=dict)
    api_config: Dict[str, Any] = field(default_factory=lambda: {
        "request_interval": 0.1,
        "max_retries": 3,
        "timeout": 30,
        "batch_size": 100
    })


@dataclass
class UpdateConfig:
    """更新配置"""
    default_data_types: list = field(default_factory=lambda: ["financial", "market"])
    max_workers: int = 4
    incremental_update: bool = True
    data_retention_days: int = 0
    # 默认历史起始日期（字符串，格式 YYYY-MM-DD）
    default_history_start_date: str = "2019-01-01"





@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Dict[str, Any] = field(default_factory=lambda: {
        "enabled": True,
        "path": resolve_project_path("logs/stock_data.log"),
        "max_size": 10,
        "backup_count": 5
    })
    console: Dict[str, Any] = field(default_factory=lambda: {
        "enabled": True,
        "level": "INFO"
    })


@dataclass
class CacheConfig:
    """缓存配置"""
    enabled: bool = True
    type: str = "memory"
    ttl: int = 3600
    max_size: int = 1000
    redis: Dict[str, Any] = field(default_factory=lambda: {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": None
    })




@dataclass
class PerformanceConfig:
    """性能配置"""
    query_cache_size: int = 100
    preload_data: bool = False
    parallel_queries: bool = True
    memory_limit: int = 2048


@dataclass
class SecurityConfig:
    """安全配置"""
    encrypt_credentials: bool = True
    encrypt_database: bool = False
    access_logging: bool = True


@dataclass
class DevelopmentConfig:
    """开发配置"""
    debug_mode: bool = False
    profiling: bool = False
    test_database: str = resolve_data_path("test_stock_data.duckdb")
    mock_data: bool = False


@dataclass
class AnalysisConfig:
    """分析配置"""
    default_periods: Dict[str, int] = field(default_factory=lambda: {
        "financial": 8,
        "market": 252
    })
    scoring_weights: Dict[str, float] = field(default_factory=lambda: {
        "financial_health": 0.25,
        "profitability": 0.25,
        "growth": 0.20,
        "valuation": 0.20,
        "technical": 0.10
    })
    benchmarks: Dict[str, float] = field(default_factory=lambda: {
        "industry_pe": 15.0,
        "industry_pb": 1.5,
        "risk_free_rate": 0.03
    })


class Config:
    """主配置类"""
    
    def __init__(self, config_path: Optional[str] = None):
        """初始化配置
        
        Args:
            config_path: 配置文件路径
        """
        self.logger = logging.getLogger(__name__)
        
        # 初始化默认配置
        self.database = DatabaseConfig()
        self.data_sources = {
            "jqdata": DataSourceConfig(),
            "tushare": DataSourceConfig()
        }
        self.update = UpdateConfig()
        self.logging = LoggingConfig()
        self.cache = CacheConfig()
        self.performance = PerformanceConfig()
        self.security = SecurityConfig()
        self.development = DevelopmentConfig()
        # 新增：分析配置
        self.analysis = AnalysisConfig()
        
        # 加载配置文件
        if config_path:
            self.load_config(config_path)
        else:
            # 尝试加载默认配置文件
            self._load_default_config()
        
        # 从环境变量加载敏感信息
        self._load_from_env()
    
    def load_config(self, config_path: str) -> None:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            self.logger.warning(f"配置文件不存在: {config_path}")
            return
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                if config_file.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                elif config_file.suffix.lower() == '.json':
                    config_data = json.load(f)
                else:
                    self.logger.error(f"不支持的配置文件格式: {config_file.suffix}")
                    return
            
            self._update_config(config_data)
            self.logger.info(f"成功加载配置文件: {config_path}")
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
    
    def _load_default_config(self) -> None:
        """加载默认配置文件"""
        # 获取项目根目录（config.py所在目录）
        project_dir = Path(__file__).parent.resolve()
        
        default_paths = [
            "config.yaml",
            "config.yml", 
            "config.json",
            "stock_data_config.yaml",
            "stock_data_config.yml"
        ]
        
        # 只在项目目录查找配置文件
        for path in default_paths:
            config_path = project_dir / path
            if config_path.exists():
                self.load_config(str(config_path))
                self.logger.info(f"加载项目配置文件: {config_path}")
                return
        
        # 如果项目目录没有配置文件，记录警告但继续使用默认配置
        self.logger.warning(f"项目目录未找到配置文件: {project_dir}，使用默认配置")
    
    def _load_from_env(self) -> None:
        """从环境变量加载配置"""
        # 数据库配置
        if os.getenv("STOCK_DB_PATH"):
            db_path = os.getenv("STOCK_DB_PATH")
            # 如果是绝对路径直接使用，否则基于数据根目录
            if Path(db_path).is_absolute():
                self.database.path = db_path
            else:
                self.database.path = resolve_data_path(db_path)
        
        # 聚宽配置
        jq_username = os.getenv("JQ_USERNAME")
        jq_password = os.getenv("JQ_PASSWORD")
        if jq_username and jq_password:
            if "jqdata" not in self.data_sources:
                self.data_sources["jqdata"] = DataSourceConfig()
            self.data_sources["jqdata"].credentials.update({
                "username": jq_username,
                "password": jq_password
            })
        
        # Tushare配置
        tushare_token = os.getenv("TUSHARE_TOKEN")
        if tushare_token:
            if "tushare" not in self.data_sources:
                self.data_sources["tushare"] = DataSourceConfig()
            self.data_sources["tushare"].credentials.update({
                "token": tushare_token
            })
        
        # 日志级别
        if os.getenv("LOG_LEVEL"):
            self.logging.level = os.getenv("LOG_LEVEL")
        
        # 开发模式
        if os.getenv("DEBUG_MODE"):
            self.development.debug_mode = os.getenv("DEBUG_MODE").lower() == "true"
    
    def _update_config(self, config_data: Dict[str, Any]) -> None:
        """更新配置数据
        
        Args:
            config_data: 配置数据字典
        """
        # 更新数据库配置
        if "database" in config_data:
            db_config = config_data["database"]
            for key, value in db_config.items():
                if hasattr(self.database, key):
                    # 特殊处理数据库路径，需要解析为绝对路径
                    if key == "path" and not Path(value).is_absolute():
                        value = resolve_data_path(value)
                    setattr(self.database, key, value)
        
        # 更新数据源配置
        if "data_sources" in config_data:
            for source_name, source_config in config_data["data_sources"].items():
                if source_name not in self.data_sources:
                    self.data_sources[source_name] = DataSourceConfig()
                
                ds_config = self.data_sources[source_name]
                for key, value in source_config.items():
                    if hasattr(ds_config, key):
                        setattr(ds_config, key, value)
        
        # 更新其他配置
        config_mappings = {
            "update": self.update,
            "logging": self.logging,
            "cache": self.cache,
            "performance": self.performance,
            "security": self.security,
            "development": self.development,
            # 新增：分析配置
            "analysis": self.analysis
        }
        
        for config_key, config_obj in config_mappings.items():
            if config_key in config_data:
                self._update_dataclass(config_obj, config_data[config_key])
    
    def _update_dataclass(self, obj: Any, data: Dict[str, Any]) -> None:
        """更新数据类对象
        
        Args:
            obj: 数据类对象
            data: 更新数据
        """
        for key, value in data.items():
            if hasattr(obj, key):
                current_value = getattr(obj, key)
                if isinstance(current_value, dict) and isinstance(value, dict):
                    current_value.update(value)
                else:
                    setattr(obj, key, value)
    
    def get_data_source_config(self, source_name: str) -> Optional[DataSourceConfig]:
        """获取数据源配置
        
        Args:
            source_name: 数据源名称
            
        Returns:
            DataSourceConfig: 数据源配置
        """
        return self.data_sources.get(source_name)
    
    def save_config(self, config_path: str) -> None:
        """保存配置到文件
        
        Args:
            config_path: 配置文件路径
        """
        config_data = {
            "database": self._dataclass_to_dict(self.database),
            "data_sources": {name: self._dataclass_to_dict(config) 
                           for name, config in self.data_sources.items()},
            "update": self._dataclass_to_dict(self.update),
            "logging": self._dataclass_to_dict(self.logging),
            "cache": self._dataclass_to_dict(self.cache),
            "performance": self._dataclass_to_dict(self.performance),
            "security": self._dataclass_to_dict(self.security),
            "development": self._dataclass_to_dict(self.development),
            # 新增：分析配置
            "analysis": self._dataclass_to_dict(self.analysis)
        }
        
        config_file = Path(config_path)
        
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                if config_file.suffix.lower() in ['.yaml', '.yml']:
                    yaml.dump(config_data, f, default_flow_style=False, 
                             allow_unicode=True, indent=2)
                elif config_file.suffix.lower() == '.json':
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                else:
                    raise ValueError(f"不支持的配置文件格式: {config_file.suffix}")
            
            self.logger.info(f"配置已保存到: {config_path}")
            
        except Exception as e:
            self.logger.error(f"保存配置文件失败: {e}")
    
    def _dataclass_to_dict(self, obj: Any) -> Dict[str, Any]:
        """将数据类转换为字典
        
        Args:
            obj: 数据类对象
            
        Returns:
            Dict[str, Any]: 字典数据
        """
        if hasattr(obj, '__dataclass_fields__'):
            return {field: getattr(obj, field) for field in obj.__dataclass_fields__}
        else:
            return obj.__dict__
    
    def validate_config(self) -> bool:
        """验证配置的有效性
        
        Returns:
            bool: 配置是否有效
        """
        try:
            # 验证数据库配置
            if not self.database.path:
                self.logger.error("数据库路径不能为空")
                return False
            
            # 验证数据源配置
            for source_name, source_config in self.data_sources.items():
                if source_config.enabled and not source_config.credentials:
                    self.logger.error(f"数据源 {source_name} 已启用但缺少认证信息")
                    return False
            
            # 验证分析权重
            total_weight = sum(self.analysis.scoring_weights.values())
            if abs(total_weight - 1.0) > 0.01:
                self.logger.warning(f"分析权重总和不等于1.0: {total_weight}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"配置验证失败: {e}")
            return False


# 全局配置实例
_global_config: Optional[Config] = None


def get_config(config_path: Optional[str] = None) -> Config:
    """获取全局配置实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Config: 配置实例
    """
    global _global_config
    
    if _global_config is None:
        _global_config = Config(config_path)
    
    return _global_config


def set_config(config: Config) -> None:
    """设置全局配置实例
    
    Args:
        config: 配置实例
    """
    global _global_config
    _global_config = config


def reset_config() -> None:
    """重置全局配置实例"""
    global _global_config
    _global_config = None