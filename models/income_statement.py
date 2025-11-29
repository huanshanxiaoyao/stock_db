"""利润表数据模型"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Dict, Any
from models.base import BaseModel


@dataclass
class IncomeStatement(BaseModel):
    """利润表数据模型 - 对齐聚宽API finance.STK_INCOME_STATEMENT"""

    # 基础字段
    code: str  # 股票代码
    pub_date: Optional[date] = None  # 公告日期
    stat_date: Optional[date] = None  # 统计截止日期/报告期

    # 收入类
    total_operating_revenue: Optional[float] = None  # 营业总收入(元)
    operating_revenue: Optional[float] = None  # 营业收入(元)
    interest_income: Optional[float] = None  # 利息收入(元)
    premiums_earned: Optional[float] = None  # 已赚保费(元)
    commission_income: Optional[float] = None  # 手续费及佣金收入(元)

    # 成本费用类
    total_operating_cost: Optional[float] = None  # 营业总成本(元)
    operating_cost: Optional[float] = None  # 营业成本(元)
    interest_expense: Optional[float] = None  # 利息支出(元)
    commission_expense: Optional[float] = None  # 手续费及佣金支出(元)
    refunded_premiums: Optional[float] = None  # 退保金(元)
    net_pay_insurance_claims: Optional[float] = None  # 赔付支出净额(元)
    withdraw_insurance_contract_reserve: Optional[float] = None  # 提取保险合同准备金净额(元)
    policy_dividend_payout: Optional[float] = None  # 保单红利支出(元)
    reinsurance_cost: Optional[float] = None  # 分保费用(元)
    operating_tax_surcharges: Optional[float] = None  # 营业税金及附加(元)
    sale_expense: Optional[float] = None  # 销售费用(元)
    administration_expense: Optional[float] = None  # 管理费用(元)
    exploration_expense: Optional[float] = None  # 勘探费用(元)
    financial_expense: Optional[float] = None  # 财务费用(元)
    asset_impairment_loss: Optional[float] = None  # 资产减值损失(元)

    # 投资收益类
    fair_value_variable_income: Optional[float] = None  # 公允价值变动收益(元)
    investment_income: Optional[float] = None  # 投资收益(元)
    invest_income_associates: Optional[float] = None  # 对联营企业和合营企业的投资收益(元)
    exchange_income: Optional[float] = None  # 汇兑收益(元)
    other_items_influenced_income: Optional[float] = None  # 其他项目影响的收益(元)

    # 利润类
    operating_profit: Optional[float] = None  # 营业利润(元)
    subsidy_income: Optional[float] = None  # 补贴收入(元)
    non_operating_revenue: Optional[float] = None  # 营业外收入(元)
    non_operating_expense: Optional[float] = None  # 营业外支出(元)
    disposal_loss_non_current_liability: Optional[float] = None  # 处置非流动资产损失(元)
    total_profit: Optional[float] = None  # 利润总额(元)
    income_tax: Optional[float] = None  # 所得税费用(元)
    net_profit: Optional[float] = None  # 净利润(元)
    np_parent_company_owners: Optional[float] = None  # 归属于母公司所有者的净利润(元)
    minority_profit: Optional[float] = None  # 少数股东损益(元)

    # 每股收益
    eps: Optional[float] = None  # 每股收益(元)
    basic_eps: Optional[float] = None  # 基本每股收益(元)
    diluted_eps: Optional[float] = None  # 稀释每股收益(元)

    # 综合收益
    other_composite_income: Optional[float] = None  # 其他综合收益(元)
    total_composite_income: Optional[float] = None  # 综合收益总额(元)
    ci_parent_company_owners: Optional[float] = None  # 归属于母公司所有者的综合收益总额(元)
    ci_minority_owners: Optional[float] = None  # 归属于少数股东的综合收益总额(元)

    def validate(self) -> bool:
        """数据验证"""
        # 基本字段验证
        if not self.code or not isinstance(self.code, str):
            return False

        # stat_date是必需的（报告期）
        if not self.stat_date:
            return False

        return True

    def get_table_name(self) -> str:
        """获取对应的数据库表名"""
        return 'income_statement'

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IncomeStatement':
        """从字典创建对象"""
        # 处理日期字段
        for date_field in ['pub_date', 'stat_date']:
            if date_field in data and data[date_field]:
                if isinstance(data[date_field], str):
                    from datetime import datetime
                    try:
                        data[date_field] = datetime.strptime(data[date_field], '%Y-%m-%d').date()
                    except ValueError:
                        data[date_field] = None
                elif isinstance(data[date_field], date):
                    pass  # 已经是date类型
                else:
                    data[date_field] = None

        # 创建实例，只使用类中定义的字段
        valid_fields = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**valid_fields)
