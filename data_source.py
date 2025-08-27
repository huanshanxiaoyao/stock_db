"""数据源抽象基类"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime
import pandas as pd
from models.base import BaseModel


class DataSourceInterface(ABC):
    """数据源接口抽象类"""
    
    @abstractmethod
    def authenticate(self, **credentials) -> bool:
        """认证登录"""
        pass
    
    @abstractmethod
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        pass
    
    @abstractmethod
    def get_stock_list(self, **kwargs) -> List[str]:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def get_financial_data(self, codes: List[str], start_date: date, end_date: date, 
                          data_type: str) -> pd.DataFrame:
        """获取财务数据"""
        pass
    
    @abstractmethod
    def get_market_data(self, codes: List[str], start_date: date, end_date: date, 
                       data_type: str) -> pd.DataFrame:
        """获取市场数据"""
        pass
    
    @abstractmethod
    def get_fundamental_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取基本面数据"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """关闭连接"""
        pass


class BaseDataSource(DataSourceInterface):
    """数据源基类，提供通用功能"""
    
    def __init__(self, name: str):
        self.name = name
        self._authenticated = False
        self._credentials = {}
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return self._authenticated
    
    def _validate_date_range(self, start_date: date, end_date: date) -> bool:
        """验证日期范围"""
        if start_date > end_date:
            raise ValueError(f"开始日期 {start_date} 不能晚于结束日期 {end_date}")
        
        if end_date > date.today():
            raise ValueError(f"结束日期 {end_date} 不能晚于今天")
        
        return True
    
    def _validate_codes(self, codes: List[str]) -> bool:
        """验证股票代码"""
        if not codes:
            raise ValueError("股票代码列表不能为空")
        
        for code in codes:
            if not isinstance(code, str) or not code.strip():
                raise ValueError(f"无效的股票代码: {code}")
        
        return True
    
    def _convert_to_models(self, df: pd.DataFrame, model_class) -> List[BaseModel]:
        """将DataFrame转换为数据模型列表"""
        models = []
        for _, row in df.iterrows():
            try:
                model = model_class.from_dict(row.to_dict())
                if model.validate():
                    models.append(model)
                else:
                    print(f"数据验证失败，跳过记录: {row.to_dict()}")
            except Exception as e:
                print(f"转换数据模型失败: {e}, 数据: {row.to_dict()}")
        
        return models
    
    def _batch_process(self, codes: List[str], batch_size: int = 50) -> List[List[str]]:
        """将股票代码列表分批处理"""
        batches = []
        for i in range(0, len(codes), batch_size):
            batches.append(codes[i:i + batch_size])
        return batches
    
    def _handle_api_error(self, error: Exception, operation: str) -> None:
        """处理API错误"""
        error_msg = f"{self.name} {operation} 失败: {str(error)}"
        print(error_msg)
        raise RuntimeError(error_msg) from error
    
    def get_data_with_retry(self, func, *args, max_retries: int = 3, **kwargs):
        """带重试机制的数据获取"""
        import time
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                
                wait_time = 2 ** attempt  # 指数退避
                print(f"第 {attempt + 1} 次尝试失败，{wait_time} 秒后重试: {str(e)}")
                time.sleep(wait_time)
        
        return None


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self):
        self._sources: Dict[str, DataSourceInterface] = {}
        self._default_source: Optional[str] = None
    
    @property
    def sources(self) -> Dict[str, DataSourceInterface]:
        """获取所有数据源的字典"""
        return self._sources
    
    def register_source(self, name: str, source: DataSourceInterface, 
                       set_as_default: bool = False) -> None:
        """注册数据源"""
        self._sources[name] = source
        
        if set_as_default or not self._default_source:
            self._default_source = name
    
    def get_source(self, name: Optional[str] = None) -> DataSourceInterface:
        """获取数据源"""
        source_name = name or self._default_source
        
        if not source_name:
            raise ValueError("没有可用的数据源")
        
        if source_name not in self._sources:
            raise ValueError(f"数据源 '{source_name}' 未注册")
        
        return self._sources[source_name]
    
    def list_sources(self) -> List[str]:
        """列出所有已注册的数据源"""
        return list(self._sources.keys())
    
    def get_default_source(self) -> Optional[str]:
        """获取默认数据源名称"""
        return self._default_source
    
    def set_default_source(self, name: str) -> None:
        """设置默认数据源"""
        if name not in self._sources:
            raise ValueError(f"数据源 '{name}' 未注册")
        
        self._default_source = name
    
    def get_preferred_source_for_stock(self, code: str) -> Optional[str]:
        """根据股票代码获取首选数据源"""
        # 北交所股票优先使用tushare数据源
        if code.endswith('.BJ'):
            if 'tushare' in self._sources:
                return 'tushare'
        
        # 其他股票使用默认数据源
        return self._default_source
    
    def get_source_for_stock(self, code: str) -> DataSourceInterface:
        """根据股票代码获取合适的数据源实例"""
        preferred_source = self.get_preferred_source_for_stock(code)
        return self.get_source(preferred_source)
    
    def authenticate_all(self, credentials_map: Dict[str, Dict[str, Any]]) -> Dict[str, bool]:
        """批量认证所有数据源"""
        results = {}
        
        for name, source in self._sources.items():
            if name in credentials_map:
                try:
                    success = source.authenticate(**credentials_map[name])
                    results[name] = success
                    if success:
                        # 确保认证状态被正确设置
                        source._authenticated = True
                        print(f"{name}数据源认证成功")
                    else:
                        print(f"{name}数据源认证失败")
                except Exception as e:
                    print(f"数据源 {name} 认证失败: {e}")
                    results[name] = False
            else:
                results[name] = False
        
        return results
    
    def close_all(self) -> None:
        """关闭所有数据源连接"""
        for source in self._sources.values():
            try:
                source.close()
            except Exception as e:
                print(f"关闭数据源连接失败: {e}")


class DataSourceConfig:
    """数据源配置类"""
    
    def __init__(self, name: str, source_type: str, **config):
        self.name = name
        self.source_type = source_type
        self.config = config
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'source_type': self.source_type,
            **self.config
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourceConfig':
        """从字典创建配置"""
        name = data.pop('name')
        source_type = data.pop('source_type')
        return cls(name, source_type, **data)


# 数据表常量
class DataType:
    """数据表类型常量"""
    
    # 基础数据表
    STOCK_LIST = 'stock_list'
    
    # 财务数据表
    INCOME_STATEMENT = 'income_statement'
    CASHFLOW_STATEMENT = 'cashflow_statement'
    BALANCE_SHEET = 'balance_sheet'
    
    # 市场数据表
    FUNDAMENTAL_DATA = 'fundamental_data'
    INDICATOR_DATA = 'indicator_data'
    MTSS_DATA = 'mtss_data'
    PRICE_DATA = 'price_data'
    
    # 其他数据类型（暂未使用）
    STOCK_INFO = 'stock_info'
    TRADING_DATA = 'trading_data'
    
    @classmethod
    def get_financial_types(cls) -> List[str]:
        """获取财务数据类型"""
        return [cls.INCOME_STATEMENT, cls.CASHFLOW_STATEMENT, cls.BALANCE_SHEET]
    
    @classmethod
    def get_market_types(cls) -> List[str]:
        """获取市场数据类型"""
        return [cls.FUNDAMENTAL_DATA, cls.INDICATOR_DATA, cls.MTSS_DATA, cls.PRICE_DATA]
    
    @classmethod
    def get_core_tables(cls) -> List[str]:
        """获取核心数据表（用于main.py的SUPPORTED_TABLES）"""
        return [cls.STOCK_LIST] + cls.get_financial_types() + cls.get_market_types()[:-1]  # 排除PRICE_DATA，因为它在main.py中单独处理
    
    @classmethod
    def get_all_tables(cls) -> List[str]:
        """获取所有数据表"""
        return [cls.STOCK_LIST] + cls.get_financial_types() + cls.get_market_types()
    
    @classmethod
    def get_all_types(cls) -> List[str]:
        """获取所有数据类型（包括未使用的）"""
        return cls.get_all_tables() + [cls.STOCK_INFO, cls.TRADING_DATA]