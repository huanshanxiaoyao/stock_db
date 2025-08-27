"""基础数据模型"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import date
from typing import Optional, Dict, Any
import pandas as pd


@dataclass
class BaseModel(ABC):
    """所有数据模型的基类"""
    
    code: str  # 股票代码
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return asdict(self)
    
    def to_series(self) -> pd.Series:
        """转换为pandas Series"""
        return pd.Series(self.to_dict())
    
    @abstractmethod
    def validate(self) -> bool:
        """数据验证"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """从字典创建对象"""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        pass