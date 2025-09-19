"""用户持仓记录数据模型"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any, List
from decimal import Decimal
import logging
import pandas as pd


@dataclass
class UserPosition:
    """用户持仓记录模型"""
    
    # 基本信息 - 字段顺序必须与数据库表结构匹配
    position_id: str  # 持仓记录唯一ID (主键)
    user_id: str  # 用户ID
    position_date: date  # 持仓日期
    stock_code: str  # 股票代码
    
    # 持仓数量信息
    position_quantity: int  # 持仓数量
    available_quantity: int  # 可用数量
    frozen_quantity: int = 0  # 冻结数量
    transit_shares: int = 0  # 在途股份
    yesterday_quantity: int = 0  # 昨夜持股
    
    # 价格和市值信息
    open_price: Decimal = Decimal('0.0000')  # 开仓价格（成本价）
    market_value: Decimal = Decimal('0.0000')  # 持仓市值
    current_price: Optional[Decimal] = None  # 当前价格
    
    # 盈亏信息
    unrealized_pnl: Optional[Decimal] = None  # 未实现盈亏
    unrealized_pnl_ratio: Optional[Decimal] = None  # 未实现盈亏比例
    
    # 其他信息
    remark: Optional[str] = None  # 备注信息
    
    # 系统信息
    timestamp: Optional[datetime] = None  # 数据时间戳
    created_at: Optional[datetime] = None  # 记录创建时间
    updated_at: Optional[datetime] = None  # 记录更新时间
    
    def validate(self) -> bool:
        """数据验证"""
        # 检查必填字段
        if not all([self.position_id, self.user_id, self.stock_code, self.position_date]):
            return False
        
        # 检查数量字段
        if self.position_quantity < 0 or self.available_quantity < 0:
            return False
        
        # 检查冻结数量不能超过持仓数量
        if self.frozen_quantity > self.position_quantity:
            return False
        
        # 检查可用数量 + 冻结数量 <= 持仓数量
        if self.available_quantity + self.frozen_quantity > self.position_quantity:
            return False
        
        # 检查股票代码格式
        if not (self.stock_code.endswith('.SH') or self.stock_code.endswith('.SZ') or 
                self.stock_code.endswith('.BJ')):
            return False
        
        # 检查价格字段
        if self.open_price < 0 or self.market_value < 0:
            return False
        
        if self.current_price is not None and self.current_price < 0:
            return False
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserPosition':
        """从字典创建对象"""
        # 处理日期字段
        if 'position_date' in data and isinstance(data['position_date'], str):
            data['position_date'] = datetime.strptime(data['position_date'], '%Y-%m-%d').date()
        
        # 处理时间字段
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.strptime(data['created_at'], '%Y-%m-%d %H:%M:%S')
        
        if 'updated_at' in data and isinstance(data['updated_at'], str):
            data['updated_at'] = datetime.strptime(data['updated_at'], '%Y-%m-%d %H:%M:%S')
        
        # 处理Decimal字段
        decimal_fields = ['open_price', 'market_value', 'current_price', 
                         'unrealized_pnl', 'unrealized_pnl_ratio']
        for field in decimal_fields:
            if field in data and data[field] is not None and not isinstance(data[field], Decimal):
                data[field] = Decimal(str(data[field]))
        
        return cls(**data)
    
    @classmethod
    def from_json_position(cls, user_id: str, position_data: Dict[str, Any], 
                          position_date: date, timestamp: Optional[datetime] = None) -> 'UserPosition':
        """从JSON持仓数据创建UserPosition对象
        
        Args:
            user_id: 用户ID
            position_data: 持仓数据字典
            position_date: 持仓日期
            timestamp: 数据时间戳
            
        Returns:
            UserPosition对象
        """
        try:
            # 生成持仓记录ID
            position_id = f"POS_{user_id}_{position_date.strftime('%Y%m%d')}_{position_data['证券代码']}"
            
            # 计算当前价格（从市值和数量计算）
            current_price = None
            if position_data['持仓数量'] > 0 and position_data['持仓市值'] > 0:
                current_price = Decimal(str(position_data['持仓市值'])) / position_data['持仓数量']
            
            # 计算未实现盈亏
            unrealized_pnl = None
            unrealized_pnl_ratio = None
            if current_price and position_data['开仓价格'] > 0:
                cost_value = Decimal(str(position_data['开仓价格'])) * position_data['持仓数量']
                market_value = Decimal(str(position_data['持仓市值']))
                unrealized_pnl = market_value - cost_value
                if cost_value > 0:
                    unrealized_pnl_ratio = unrealized_pnl / cost_value
            
            return cls(
                position_id=position_id,
                user_id=user_id,
                position_date=position_date,
                stock_code=position_data['证券代码'],
                position_quantity=position_data['持仓数量'],
                available_quantity=position_data['可用数量'],
                frozen_quantity=position_data.get('冻结数量', 0),
                transit_shares=position_data.get('在途股份', 0),
                yesterday_quantity=position_data.get('昨夜持股', 0),
                open_price=Decimal(str(position_data['开仓价格'])),
                market_value=Decimal(str(position_data['持仓市值'])),
                current_price=current_price,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_ratio=unrealized_pnl_ratio,
                timestamp=timestamp or datetime.now(),
                created_at=timestamp or datetime.now(),
                updated_at=timestamp or datetime.now()
            )
        
        except Exception as e:
            logging.error(f"创建UserPosition对象失败: {e}, 数据: {position_data}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            'position_id': self.position_id,
            'user_id': self.user_id,
            'position_date': self.position_date.strftime('%Y-%m-%d') if self.position_date else None,
            'stock_code': self.stock_code,
            'position_quantity': self.position_quantity,
            'available_quantity': self.available_quantity,
            'frozen_quantity': self.frozen_quantity,
            'transit_shares': self.transit_shares,
            'yesterday_quantity': self.yesterday_quantity,
            'open_price': float(self.open_price) if self.open_price else None,
            'market_value': float(self.market_value) if self.market_value else None,
            'current_price': float(self.current_price) if self.current_price else None,
            'unrealized_pnl': float(self.unrealized_pnl) if self.unrealized_pnl else None,
            'unrealized_pnl_ratio': float(self.unrealized_pnl_ratio) if self.unrealized_pnl_ratio else None,
            'remark': self.remark,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }
        return result
    
    def to_series(self) -> pd.Series:
        """转换为pandas Series"""
        return pd.Series(self.to_dict())
    
    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        return 'user_positions'
    
    def calculate_profit_loss(self, current_market_price: Optional[Decimal] = None) -> Dict[str, Decimal]:
        """计算盈亏信息
        
        Args:
            current_market_price: 当前市场价格，如果不提供则使用对象中的current_price
            
        Returns:
            包含盈亏信息的字典
        """
        if current_market_price:
            self.current_price = current_market_price
        
        if not self.current_price or self.position_quantity <= 0:
            return {
                'unrealized_pnl': Decimal('0'),
                'unrealized_pnl_ratio': Decimal('0'),
                'current_market_value': Decimal('0')
            }
        
        # 计算当前市值
        current_market_value = self.current_price * self.position_quantity
        
        # 计算成本价值
        cost_value = self.open_price * self.position_quantity
        
        # 计算未实现盈亏
        unrealized_pnl = current_market_value - cost_value
        
        # 计算盈亏比例
        unrealized_pnl_ratio = Decimal('0')
        if cost_value > 0:
            unrealized_pnl_ratio = unrealized_pnl / cost_value
        
        # 更新对象属性
        self.unrealized_pnl = unrealized_pnl
        self.unrealized_pnl_ratio = unrealized_pnl_ratio
        self.market_value = current_market_value
        
        return {
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_ratio': unrealized_pnl_ratio,
            'current_market_value': current_market_value
        }


@dataclass
class UserAccountInfo:
    """用户账户信息模型"""
    
    # 基本信息 (联合主键: user_id + info_date)
    user_id: str  # 用户ID (主键组成部分)
    info_date: date  # 信息日期 (主键组成部分)
    
    # 资产信息
    total_assets: Decimal = Decimal('0.00')  # 总资产
    position_market_value: Decimal = Decimal('0.00')  # 持仓市值
    available_cash: Decimal = Decimal('0.00')  # 可用资金
    frozen_cash: Decimal = Decimal('0.00')  # 冻结资金
    
    # 盈亏信息
    total_profit_loss: Optional[Decimal] = None  # 总盈亏
    total_profit_loss_ratio: Optional[Decimal] = None  # 总盈亏比例
    
    # 其他信息
    timestamp: Optional[datetime] = None  # 数据时间戳
    
    # 系统信息
    created_at: Optional[datetime] = None  # 记录创建时间
    updated_at: Optional[datetime] = None  # 记录更新时间
    
    def validate(self) -> bool:
        """数据验证"""
        # 检查必填字段
        if not all([self.user_id, self.info_date]):
            return False
        
        # 检查金额字段
        if (self.total_assets < 0 or self.position_market_value < 0 or 
            self.available_cash < 0 or self.frozen_cash < 0):
            return False
        
        # 检查资产逻辑：总资产应该等于持仓市值 + 可用资金 + 冻结资金
        calculated_total = self.position_market_value + self.available_cash + self.frozen_cash
        if abs(self.total_assets - calculated_total) > Decimal('10'):  # 允许1分钱的误差
            logging.warning(f"总资产不匹配: 声明={self.total_assets}, 计算={calculated_total}")
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserAccountInfo':
        """从字典创建对象"""
        # 处理日期字段
        if 'info_date' in data and isinstance(data['info_date'], str):
            data['info_date'] = datetime.strptime(data['info_date'], '%Y-%m-%d').date()
        
        # 处理时间字段
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.strptime(data['created_at'], '%Y-%m-%d %H:%M:%S')
        
        if 'updated_at' in data and isinstance(data['updated_at'], str):
            data['updated_at'] = datetime.strptime(data['updated_at'], '%Y-%m-%d %H:%M:%S')
        
        # 处理Decimal字段
        decimal_fields = ['total_assets', 'position_market_value', 'available_cash', 
                         'frozen_cash', 'total_profit_loss', 'total_profit_loss_ratio']
        for field in decimal_fields:
            if field in data and data[field] is not None and not isinstance(data[field], Decimal):
                data[field] = Decimal(str(data[field]))
        
        return cls(**data)
    
    @classmethod
    def from_json_account_info(cls, user_id: str, account_data: Dict[str, Any], 
                              info_date: date, timestamp: Optional[datetime] = None) -> 'UserAccountInfo':
        """从JSON账户数据创建UserAccountInfo对象
        
        Args:
            user_id: 用户ID
            account_data: 账户数据字典
            info_date: 信息日期
            timestamp: 数据时间戳
            
        Returns:
            UserAccountInfo对象
        """
        try:
            return cls(
                user_id=user_id,
                info_date=info_date,
                total_assets=Decimal(str(account_data['总资产'])),
                position_market_value=Decimal(str(account_data['持仓市值'])),
                available_cash=Decimal(str(account_data['可用资金'])),
                frozen_cash=Decimal(str(account_data.get('冻结资金', 0.0))),
                timestamp=timestamp,
                created_at=timestamp or datetime.now(),
                updated_at=timestamp or datetime.now()
            )
        
        except Exception as e:
            logging.error(f"创建UserAccountInfo对象失败: {e}, 数据: {account_data}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            'user_id': self.user_id,
            'info_date': self.info_date.strftime('%Y-%m-%d') if self.info_date else None,
            'total_assets': float(self.total_assets) if self.total_assets else None,
            'position_market_value': float(self.position_market_value) if self.position_market_value else None,
            'available_cash': float(self.available_cash) if self.available_cash else None,
            'frozen_cash': float(self.frozen_cash) if self.frozen_cash else None,
            'total_profit_loss': float(self.total_profit_loss) if self.total_profit_loss else None,
            'total_profit_loss_ratio': float(self.total_profit_loss_ratio) if self.total_profit_loss_ratio else None,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S') if self.timestamp else None,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }
        return result
    
    def to_series(self) -> pd.Series:
        """转换为pandas Series"""
        return pd.Series(self.to_dict())
    
    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        return 'user_account_info'