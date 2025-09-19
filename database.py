"""数据库抽象层"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import date
import pandas as pd
from models.base import BaseModel
from data_source import DataType


class DatabaseInterface(ABC):
    """数据库接口抽象类"""
    
    @abstractmethod
    def connect(self) -> None:
        """连接数据库"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """关闭数据库连接"""
        pass
    
    @abstractmethod
    def create_tables(self) -> None:
        """创建所有数据表"""
        pass
    
    @abstractmethod
    def insert_data(self, model: BaseModel) -> bool:
        """插入单条数据"""
        pass
    
    @abstractmethod
    def insert_batch(self, models: List[BaseModel]) -> bool:
        """批量插入数据"""
        pass
    
    @abstractmethod
    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """插入DataFrame数据"""
        pass
    
    @abstractmethod
    def query_data(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """执行SQL查询"""
        pass
    
    @abstractmethod
    def get_latest_date(self, table_name: str, code: Optional[str] = None) -> Optional[date]:
        """获取最新数据日期"""
        pass

    @abstractmethod
    def get_latest_dates_batch(self, table_name: str, codes: List[str]) -> Dict[str, Optional[date]]:
        """批量获取多只股票的最新数据日期

        Args:
            table_name: 表名
            codes: 股票代码列表

        Returns:
            Dict[str, Optional[date]]: 股票代码到最新日期的映射，如果股票没有数据则值为None
        """
        pass
    
    @abstractmethod
    def get_existing_codes(self, table_name: str) -> List[str]:
        """获取已存在的股票代码"""
        pass
    
    @abstractmethod
    def delete_data(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """删除数据"""
        pass
    
    @abstractmethod
    def update_data(self, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """更新数据"""
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        pass
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        pass


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, database: DatabaseInterface):
        self.db = database
        self._connected = False
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def connect(self) -> None:
        """连接数据库"""
        if not self._connected:
            self.db.connect()
            self._connected = True
    
    def close(self) -> None:
        """关闭数据库连接"""
        if self._connected:
            self.db.close()
            self._connected = False
    
    def initialize(self) -> None:
        """初始化数据库（创建表等）"""
        self.connect()
        self.db.create_tables()
    
    def save_model(self, model: BaseModel) -> bool:
        """保存数据模型"""
        if not model.validate():
            raise ValueError(f"数据验证失败: {model}")
        
        return self.db.insert_data(model)
    
    def save_models(self, models: List[BaseModel]) -> bool:
        """批量保存数据模型"""
        # 验证所有模型
        for model in models:
            if not model.validate():
                raise ValueError(f"数据验证失败: {model}")
        
        return self.db.insert_batch(models)
    
    def save_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """保存DataFrame数据"""
        return self.db.insert_dataframe(df, table_name)
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """执行查询"""
        return self.db.query_data(sql, params)
    
    def get_latest_date(self, table_name: str, code: Optional[str] = None) -> Optional[date]:
        """获取最新数据日期"""
        return self.db.get_latest_date(table_name, code)

    def get_latest_dates_batch(self, table_name: str, codes: List[str]) -> Dict[str, Optional[date]]:
        """批量获取多只股票的最新数据日期"""
        return self.db.get_latest_dates_batch(table_name, codes)
    
    def get_existing_codes(self, table_name: str) -> List[str]:
        """获取已存在的股票代码"""
        return self.db.get_existing_codes(table_name)
    
    def delete_data(self, table_name: str, **conditions) -> bool:
        """删除数据"""
        return self.db.delete_data(table_name, conditions)
    
    def update_data(self, table_name: str, data: Dict[str, Any], **conditions) -> bool:
        """更新数据"""
        return self.db.update_data(table_name, data, conditions)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return self.db.table_exists(table_name)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        return self.db.get_table_info(table_name)
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        info = {
            'tables': [],
            'total_records': 0
        }
        
        # 获取所有表的信息
        table_names = DataType.get_all_tables()
        
        for table_name in table_names:
            if self.table_exists(table_name):
                table_info = self.get_table_info(table_name)
                info['tables'].append(table_info)
                if 'record_count' in table_info:
                    info['total_records'] += table_info['record_count']
        
        return info
    
    # 便捷查询方法
    def get_financial_data(self, code: str, start_date: Optional[date] = None, 
                          end_date: Optional[date] = None) -> Dict[str, pd.DataFrame]:
        """获取财务数据"""
        result = {}
        
        # 构建查询条件
        where_clause = "WHERE code = ?"
        params = [code]
        
        if start_date:
            where_clause += " AND stat_date >= ?"
            params.append(start_date)
        
        if end_date:
            where_clause += " AND stat_date <= ?"
            params.append(end_date)
        
        # 查询各类财务数据
        financial_tables = {
            'income_statement': '利润表',
            'cashflow_statement': '现金流量表', 
            'balance_sheet': '资产负债表'
        }
        
        for table_name, table_desc in financial_tables.items():
            if self.table_exists(table_name):
                sql = f"SELECT * FROM {table_name} {where_clause} ORDER BY stat_date"
                df = self.query(sql, dict(zip(['code'] + [f'param_{i}' for i in range(len(params)-1)], params)))
                if not df.empty:
                    result[table_desc] = df
        
        return result
    
    def get_market_data(self, code: str, start_date: Optional[date] = None, 
                       end_date: Optional[date] = None) -> Dict[str, pd.DataFrame]:
        """获取市场数据"""
        result = {}
        
        # 构建查询条件
        where_clause = "WHERE code = ?"
        params = [code]
        
        if start_date:
            where_clause += " AND day >= ?"
            params.append(start_date)
        
        if end_date:
            where_clause += " AND day <= ?"
            params.append(end_date)
        
        # 查询各类市场数据
        market_tables = {
            'valuation_data': '估值数据',
            'indicator_data': '技术指标数据',
            'mtss_data': '融资融券数据'
        }
        
        for table_name, table_desc in market_tables.items():
            if self.table_exists(table_name):
                sql = f"SELECT * FROM {table_name} {where_clause} ORDER BY day"
                df = self.query(sql, dict(zip(['code'] + [f'param_{i}' for i in range(len(params)-1)], params)))
                if not df.empty:
                    result[table_desc] = df
        
        return result