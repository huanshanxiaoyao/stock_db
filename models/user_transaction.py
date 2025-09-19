"""用户交易记录数据模型"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any
from decimal import Decimal
import logging
import pandas as pd


@dataclass
class UserTransaction:
    """用户交易记录模型"""
    
    # 基本信息 - 字段顺序必须与数据库表结构匹配
    trade_id: str  # 交易唯一ID (主键)
    user_id: str  # 用户ID
    stock_code: str  # 股票代码 (对应数据库stock_code列)
    
    # 时间信息
    trade_date: date  # 交易日期
    trade_time: datetime  # 交易时间 (对应数据库trade_time列)
    
    # 交易信息
    trade_type: int  # 交易类型：23-买入，24-卖出
    strategy_id: Optional[str] = None  # 策略ID
    quantity: int = 0  # 交易数量 (对应数据库quantity列)
    price: Decimal = Decimal('0.0000')  # 交易价格
    amount: Decimal = Decimal('0.0000')  # 交易总金额 (对应数据库amount列)
    
    # 费用信息
    commission: Decimal = Decimal('0.0000')  # 佣金
    stamp_tax: Decimal = Decimal('0.0000')  # 印花税 (对应数据库stamp_tax列)
    other_fees: Decimal = Decimal('0.0000')  # 其他费用
    net_amount: Decimal = Decimal('0.0000')  # 净交易金额
    
    # 原始信息
    order_id: Optional[str] = None  # 订单ID
    remark: Optional[str] = None  # 备注信息
    
    # 系统信息
    created_at: Optional[datetime] = None  # 记录创建时间
    
    def validate(self) -> bool:
        """数据验证"""
        # 检查必填字段
        if not all([self.trade_id, self.user_id, self.stock_code, 
                   self.trade_date, self.trade_time]):
            return False
        
        # 检查交易类型
        if self.trade_type not in [23, 24]:  # 23-买入，24-卖出
            return False
        
        # 检查数量和价格
        if self.quantity <= 0 or self.price <= 0:
            return False
        
        # 检查股票代码格式
        if not (self.stock_code.endswith('.SH') or self.stock_code.endswith('.SZ') or 
                self.stock_code.endswith('.BJ')):
            return False
        
        # 检查日期逻辑
        if self.trade_time.date() != self.trade_date:
            return False
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserTransaction':
        """从字典创建对象"""
        # 处理日期时间字段
        if 'trade_date' in data and isinstance(data['trade_date'], str):
            data['trade_date'] = datetime.strptime(data['trade_date'], '%Y-%m-%d').date()
        
        if 'trade_time' in data and isinstance(data['trade_time'], str):
            data['trade_time'] = datetime.strptime(data['trade_time'], '%Y-%m-%d %H:%M:%S')
        
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.strptime(data['created_at'], '%Y-%m-%d %H:%M:%S')
        
        # 处理Decimal字段
        decimal_fields = ['price', 'amount', 'commission', 'stamp_tax', 'other_fees', 'net_amount']
        for field in decimal_fields:
            if field in data and not isinstance(data[field], Decimal):
                data[field] = Decimal(str(data[field]))
        
        return cls(**data)
    
    @classmethod
    def from_json_trade(cls, user_id: str, trade_data: Dict[str, Any], file_path: str = '') -> 'UserTransaction':
        """从JSON交易数据创建UserTransaction对象
        
        Args:
            user_id: 用户ID (纯账户ID，如6681802088)
            trade_data: 交易数据字典
            file_path: JSON文件路径，用于提取日期信息
            
        Returns:
            UserTransaction对象
        """
        try:
            # 提取基本信息
            trade_date_str = trade_data.get('TradeDate', '')
            trade_time_str = trade_data.get('TradeTime', '')
            trade_id = trade_data.get('TradeId', '')  # 使用TradeId而不是TradeID
            
            # 如果没有TradeId，生成一个简单的ID
            if not trade_id:
                import time
                trade_id = f"{user_id}_{int(time.time() * 1000000)}"
            
            # 解析交易日期和时间
            if trade_date_str:
                trade_date = datetime.strptime(trade_date_str, '%Y%m%d').date()
            else:
                # 如果TradeDate为空，从文件路径中提取日期
                import os
                filename = os.path.basename(file_path)
                date_str = filename.replace('.json', '')
                try:
                    trade_date = datetime.strptime(date_str, '%Y%m%d').date()
                except ValueError:
                    # 如果无法解析，使用当前日期
                    trade_date = datetime.now().date()
            
            # 构建完整的交易时间
            if trade_time_str:
                # 如果有具体时间，组合日期和时间
                trade_datetime_str = f"{trade_date.strftime('%Y%m%d')} {trade_time_str}"
                trade_time = datetime.strptime(trade_datetime_str, '%Y%m%d %H:%M:%S')
            else:
                # 如果没有具体时间，使用日期的开始时间
                trade_time = datetime.combine(trade_date, datetime.min.time())
            
            # 从备注中提取策略ID
            remark = trade_data.get('Remark', '')
            strategy_id = None
            if remark and 'strategy_id:' in remark:
                try:
                    strategy_id = remark.split('strategy_id:')[1].strip()
                except (IndexError, AttributeError):
                    strategy_id = None
            
            # 创建UserTransaction对象 - 支持中文和英文字段名
            return cls(
                trade_id=trade_id,
                user_id=user_id,  # 只存储纯用户ID，如6681802088
                stock_code=trade_data.get('Code', trade_data.get('证券代码', trade_data.get('StockCode', ''))),
                trade_date=trade_date,
                trade_time=trade_time,
                trade_type=int(trade_data.get('TradeType', 23)),  # 23-买入，24-卖出
                strategy_id=strategy_id,
                quantity=int(trade_data.get('Volume', trade_data.get('Quantity', trade_data.get('成交数量', 0)))),
                price=Decimal(str(trade_data.get('Price', trade_data.get('TradePrice', trade_data.get('成交均价', '0.0000'))))),
                amount=Decimal(str(trade_data.get('Value', trade_data.get('Amount', trade_data.get('成交金额', '0.0000'))))),
                commission=Decimal(str(trade_data.get('Commission', '0.0000'))),
                stamp_tax=Decimal(str(trade_data.get('Tax', '0.0000'))),
                other_fees=Decimal(str(trade_data.get('OtherFees', '0.0000'))),
                net_amount=Decimal(str(trade_data.get('NetAmount', trade_data.get('成交金额', '0.0000')))),
                order_id=str(trade_data.get('OrderID', trade_data.get('OrderId', trade_data.get('订单编号', '')))),
                remark=remark,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"创建UserTransaction对象失败: {e}")
            logger.error(f"用户ID: {user_id}")
            logger.error(f"交易数据: {trade_data}")
            logger.error(f"Code字段值: {trade_data.get('Code', trade_data.get('证券代码', 'NOT_FOUND'))}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        from dataclasses import asdict
        return asdict(self)
    
    def to_series(self) -> pd.Series:
        """转换为pandas Series"""
        return pd.Series(self.to_dict())
    
    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        return 'user_transactions'
    
    @property
    def is_buy(self) -> bool:
        """是否为买入交易"""
        return self.trade_type == 23
    
    @property
    def is_sell(self) -> bool:
        """是否为卖出交易"""
        return self.trade_type == 24
    
    @property
    def trade_type_name(self) -> str:
        """交易类型名称"""
        return '买入' if self.is_buy else '卖出'
    
    def calculate_net_amount(self) -> Decimal:
        """计算净交易金额"""
        if self.is_buy:
            # 买入：交易金额 + 佣金 + 税费 + 其他费用
            return self.value + self.commission + self.tax + self.other_fees
        else:
            # 卖出：交易金额 - 佣金 - 税费 - 其他费用
            return self.value - self.commission - self.tax - self.other_fees