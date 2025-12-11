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
        import numpy as np
        data = asdict(self)

        # 处理不能JSON序列化的类型
        for key, value in data.items():
            # 先检查 NaT/NaN，因为它们可能通过 isinstance(value, date) 检查
            if pd.isna(value):
                # 处理 pandas NaT 和 NaN 值
                data[key] = None
            elif isinstance(value, date):
                data[key] = value.strftime('%Y-%m-%d')
            elif isinstance(value, (bool, np.bool_)):
                data[key] = bool(value)  # 确保是Python原生bool类型
            elif value is None:
                data[key] = None

        return data
    
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