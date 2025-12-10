"""股票列表数据模型"""

from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any
from .base import BaseModel


@dataclass
class StockInfo(BaseModel):
    """股票基本信息模型"""
    
    # 基本信息
    code: str  # 股票代码
    display_name: str  # 股票名称
    name: str  # 股票简称
    start_date: date  # 上市日期
    end_date: Optional[date] = None  # 退市日期
    
    # 市场信息
    exchange: Optional[str] = None  # 交易所代码 (XSHG/XSHE/BSE)
    market: Optional[str] = None  # 市场类型 (main/gem/star/bse)
    security_type: str = 'stock'  # 证券类型 ('stock' or 'index')

    # 行业分类（后续补充）
    industry_code: Optional[str] = None  # 行业代码
    industry_name: Optional[str] = None  # 行业名称
    sector_code: Optional[str] = None  # 板块代码
    sector_name: Optional[str] = None  # 板块名称
    
    # 状态信息
    status: str = 'normal'  # 状态: normal/suspended/delisted
    is_st: bool = False  # 是否ST股票
    
    # 更新时间
    update_date: Optional[date] = None  # 最后更新日期
    
    def validate(self) -> bool:
        """数据验证"""
        if not self.code or not self.display_name or not self.start_date:
            return False
        
        # 检查股票代码格式
        if not (self.code.endswith('.SH') or self.code.endswith('.SZ') or 
                self.code.endswith('.BJ') or self.code.endswith('.XSHG') or 
                self.code.endswith('.XSHE') or self.code.endswith('.BSE')):
            return False
        
        # 检查日期逻辑
        if self.end_date and self.end_date < self.start_date:
            return False
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StockInfo':
        """从字典创建对象"""
        return cls(**data)
    
    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        return 'stock_list'
    
    @property
    def is_active(self) -> bool:
        """是否为活跃股票（未退市）"""
        return self.end_date is None and self.status == 'normal'

    @property
    def is_index(self) -> bool:
        """是否为指数"""
        return self.security_type == 'index'

    @property
    def is_stock(self) -> bool:
        """是否为股票"""
        return self.security_type == 'stock'

    @property
    def exchange_name(self) -> str:
        """交易所名称"""
        exchange_map = {
            'XSHG': '上海证券交易所',
            'XSHE': '深圳证券交易所', 
            'BSE': '北京证券交易所'
        }
        return exchange_map.get(self.exchange, self.exchange or '未知')
    
    @property
    def market_name(self) -> str:
        """市场名称"""
        market_map = {
            'main': '主板',
            'gem': '创业板',
            'star': '科创板',
            'bse': '北交所'
        }
        return market_map.get(self.market, self.market or '未知')
    
    def to_jq_code(self) -> str:
        """转换为聚宽格式代码"""
        if self.code.endswith('.SZ'):
            return self.code.replace('.SZ', '.XSHE')
        elif self.code.endswith('.SH'):
            return self.code.replace('.SH', '.XSHG')
        elif self.code.endswith('.BJ'):
            return self.code.replace('.BJ', '.BSE')
        return self.code
    
    @classmethod
    def from_jq_code(cls, jq_code: str, **kwargs) -> str:
        """从聚宽格式代码转换为标准格式"""
        if jq_code.endswith('.XSHE'):
            return jq_code.replace('.XSHE', '.SZ')
        elif jq_code.endswith('.XSHG'):
            return jq_code.replace('.XSHG', '.SH')
        elif jq_code.endswith('.BSE'):
            return jq_code.replace('.BSE', '.BJ')
        return jq_code
    
    def get_market_info(self) -> Dict[str, str]:
        """获取市场信息摘要"""
        return {
            'code': self.code,
            'name': self.display_name,
            'exchange': self.exchange_name,
            'market': self.market_name,
            'status': self.status,
            'listing_date': self.start_date.strftime('%Y-%m-%d'),
            'delisting_date': self.end_date.strftime('%Y-%m-%d') if self.end_date else None
        }