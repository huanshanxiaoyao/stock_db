"""资产负债表数据模型"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Dict, Any
from models.base import BaseModel


@dataclass
class BalanceSheet(BaseModel):
    """资产负债表数据模型 - 对齐聚宽API finance.STK_BALANCE_SHEET"""

    # 基础字段
    code: str  # 股票代码
    pub_date: Optional[date] = None  # 公告日期
    stat_date: Optional[date] = None  # 统计截止日期/报告期

    # 流动资产
    cash_equivalents: Optional[float] = None  # 货币资金(元)
    settlement_provi: Optional[float] = None  # 结算备付金(元)
    lend_capital: Optional[float] = None  # 拆出资金(元)
    trading_assets: Optional[float] = None  # 交易性金融资产(元)
    bill_receivable: Optional[float] = None  # 应收票据(元)
    account_receivable: Optional[float] = None  # 应收账款(元)
    advance_payment: Optional[float] = None  # 预付款项(元)
    insurance_receivables: Optional[float] = None  # 应收保费(元)
    reinsurance_receivables: Optional[float] = None  # 应收分保账款(元)
    reinsurance_contract_reserves_receivable: Optional[float] = None  # 应收分保合同准备金(元)
    interest_receivable: Optional[float] = None  # 应收利息(元)
    dividend_receivable: Optional[float] = None  # 应收股利(元)
    other_receivable: Optional[float] = None  # 其他应收款(元)
    bought_sellback_assets: Optional[float] = None  # 买入返售金融资产(元)
    inventories: Optional[float] = None  # 存货(元)
    expendable_biological_asset: Optional[float] = None  # 消耗性生物资产(元)
    non_current_asset_in_one_year: Optional[float] = None  # 一年内到期的非流动资产(元)
    other_current_assets: Optional[float] = None  # 其他流动资产(元)
    total_current_assets: Optional[float] = None  # 流动资产合计(元)

    # 非流动资产
    loan_and_advance_current: Optional[float] = None  # 发放贷款及垫款(元)
    available_for_sale_assets: Optional[float] = None  # 可供出售金融资产(元)
    held_to_maturity_investments: Optional[float] = None  # 持有至到期投资(元)
    long_term_receivable: Optional[float] = None  # 长期应收款(元)
    long_term_equity_invest: Optional[float] = None  # 长期股权投资(元)
    investment_real_estate: Optional[float] = None  # 投资性房地产(元)
    fixed_assets: Optional[float] = None  # 固定资产(元)
    constru_in_process: Optional[float] = None  # 在建工程(元)
    construction_materials: Optional[float] = None  # 工程物资(元)
    fixed_assets_liquidation: Optional[float] = None  # 固定资产清理(元)
    biological_assets: Optional[float] = None  # 生产性生物资产(元)
    oil_and_gas_assets: Optional[float] = None  # 油气资产(元)
    intangible_assets: Optional[float] = None  # 无形资产(元)
    development_expenditure: Optional[float] = None  # 开发支出(元)
    good_will: Optional[float] = None  # 商誉(元)
    long_deferred_expense: Optional[float] = None  # 长期待摊费用(元)
    deferred_tax_assets: Optional[float] = None  # 递延所得税资产(元)
    other_non_current_assets: Optional[float] = None  # 其他非流动资产(元)
    total_non_current_assets: Optional[float] = None  # 非流动资产合计(元)
    total_assets: Optional[float] = None  # 资产总计(元)

    # 流动负债
    short_term_loan: Optional[float] = None  # 短期借款(元)
    borrowing_from_central_bank: Optional[float] = None  # 向中央银行借款(元)
    lend_capital_liability: Optional[float] = None  # 拆入资金(元)
    trading_liability: Optional[float] = None  # 交易性金融负债(元)
    notes_payable: Optional[float] = None  # 应付票据(元)
    accounts_payable: Optional[float] = None  # 应付账款(元)
    advance_peceipts: Optional[float] = None  # 预收款项(元)
    sold_buyback_secu_proceeds: Optional[float] = None  # 卖出回购金融资产款(元)
    commission_payable: Optional[float] = None  # 应付手续费及佣金(元)
    salaries_payable: Optional[float] = None  # 应付职工薪酬(元)
    taxs_payable: Optional[float] = None  # 应交税费(元)
    interest_payable: Optional[float] = None  # 应付利息(元)
    dividend_payable: Optional[float] = None  # 应付股利(元)
    other_payable: Optional[float] = None  # 其他应付款(元)
    reinsurance_payables: Optional[float] = None  # 应付分保账款(元)
    insurance_contract_reserves: Optional[float] = None  # 保险合同准备金(元)
    proxy_secu_proceeds: Optional[float] = None  # 代理买卖证券款(元)
    receivings_from_vicariously_sold_securities: Optional[float] = None  # 代理承销证券款(元)
    non_current_liability_in_one_year: Optional[float] = None  # 一年内到期的非流动负债(元)
    other_current_liability: Optional[float] = None  # 其他流动负债(元)
    total_current_liability: Optional[float] = None  # 流动负债合计(元)

    # 非流动负债
    long_term_loan: Optional[float] = None  # 长期借款(元)
    bonds_payable: Optional[float] = None  # 应付债券(元)
    long_term_account_payable: Optional[float] = None  # 长期应付款(元)
    specific_account_payable: Optional[float] = None  # 专项应付款(元)
    estimated_liability: Optional[float] = None  # 预计负债(元)
    deferred_tax_liability: Optional[float] = None  # 递延所得税负债(元)
    other_non_current_liability: Optional[float] = None  # 其他非流动负债(元)
    total_non_current_liability: Optional[float] = None  # 非流动负债合计(元)
    total_liability: Optional[float] = None  # 负债合计(元)

    # 所有者权益
    paidin_capital: Optional[float] = None  # 实收资本(或股本)(元)
    other_equity_tools: Optional[float] = None  # 其他权益工具(元)
    other_equity_tools_PRE_STOCK: Optional[float] = None  # 其他权益工具:优先股(元)
    other_equity_tools_PERPETUAL_DEBT: Optional[float] = None  # 其他权益工具:永续债(元)
    other_equity_tools_OTHER: Optional[float] = None  # 其他权益工具:其他(元)
    capital_reserve_fund: Optional[float] = None  # 资本公积(元)
    treasury_stock: Optional[float] = None  # 库存股(元)
    other_comprehensive_income: Optional[float] = None  # 其他综合收益(元)
    specific_reserves: Optional[float] = None  # 专项储备(元)
    earned_surplus: Optional[float] = None  # 盈余公积(元)
    general_risk_preparation: Optional[float] = None  # 一般风险准备(元)
    undistributed_profit: Optional[float] = None  # 未分配利润(元)
    foreign_exchange_gain: Optional[float] = None  # 外币报表折算差额(元)
    total_owner_equities: Optional[float] = None  # 归属于母公司所有者权益合计(元)
    minority_interests: Optional[float] = None  # 少数股东权益(元)
    total_sheet_owner_equities: Optional[float] = None  # 所有者权益合计(元)
    total_liability_equity: Optional[float] = None  # 负债和所有者权益总计(元)

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
        return 'balance_sheet'

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BalanceSheet':
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
