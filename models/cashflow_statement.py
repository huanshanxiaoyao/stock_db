"""现金流量表数据模型"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Dict, Any
from models.base import BaseModel


@dataclass
class CashflowStatement(BaseModel):
    """现金流量表数据模型 - 对齐聚宽API finance.STK_CASHFLOW_STATEMENT"""

    # 基础字段
    code: str  # 股票代码
    pub_date: Optional[date] = None  # 公告日期
    stat_date: Optional[date] = None  # 统计截止日期/报告期

    # 经营活动现金流入
    goods_sale_and_service_render_cash: Optional[float] = None  # 销售商品、提供劳务收到的现金(元)
    net_deposit_increase: Optional[float] = None  # 客户存款和同业存放款项净增加额(元)
    net_borrowing_from_central_bank: Optional[float] = None  # 向中央银行借款净增加额(元)
    net_borrowing_from_finance_co: Optional[float] = None  # 向其他金融机构拆入资金净增加额(元)
    net_original_insurance_cash: Optional[float] = None  # 收到原保险合同保费取得的现金(元)
    net_cash_received_from_reinsurance_business: Optional[float] = None  # 收到再保险业务现金净额(元)
    net_insurer_deposit_investment: Optional[float] = None  # 保户储金及投资款净增加额(元)
    net_deal_trading_assets: Optional[float] = None  # 处置交易性金融资产净增加额(元)
    interest_and_commission_cashin: Optional[float] = None  # 收取利息、手续费及佣金的现金(元)
    net_increase_in_placements: Optional[float] = None  # 拆入资金净增加额(元)
    net_buyback: Optional[float] = None  # 回购业务资金净增加额(元)
    tax_levy_refund: Optional[float] = None  # 收到的税费返还(元)
    other_cashin_related_operate: Optional[float] = None  # 收到其他与经营活动有关的现金(元)
    subtotal_operate_cash_inflow: Optional[float] = None  # 经营活动现金流入小计(元)

    # 经营活动现金流出
    goods_and_services_cash_paid: Optional[float] = None  # 购买商品、接受劳务支付的现金(元)
    net_loan_and_advance_increase: Optional[float] = None  # 客户贷款及垫款净增加额(元)
    net_deposit_in_cb_and_ib: Optional[float] = None  # 存放中央银行和同业款项净增加额(元)
    original_compensation_paid: Optional[float] = None  # 支付原保险合同赔付款项的现金(元)
    handling_charges_and_commission: Optional[float] = None  # 支付利息、手续费及佣金的现金(元)
    policy_dividend_cash_paid: Optional[float] = None  # 支付保单红利的现金(元)
    staff_behalf_paid: Optional[float] = None  # 支付给职工以及为职工支付的现金(元)
    tax_payments: Optional[float] = None  # 支付的各项税费(元)
    other_cash_paid_related_operate: Optional[float] = None  # 支付其他与经营活动有关的现金(元)
    subtotal_operate_cash_outflow: Optional[float] = None  # 经营活动现金流出小计(元)
    net_operate_cash_flow: Optional[float] = None  # 经营活动产生的现金流量净额(元)

    # 投资活动现金流入
    invest_withdrawal_cash: Optional[float] = None  # 收回投资收到的现金(元)
    invest_proceeds: Optional[float] = None  # 取得投资收益收到的现金(元)
    fix_intan_other_asset_dispo_cash: Optional[float] = None  # 处置固定资产、无形资产和其他长期资产收回的现金净额(元)
    net_cash_deal_subcompany: Optional[float] = None  # 处置子公司及其他营业单位收到的现金净额(元)
    other_cash_from_invest_act: Optional[float] = None  # 收到其他与投资活动有关的现金(元)
    subtotal_invest_cash_inflow: Optional[float] = None  # 投资活动现金流入小计(元)

    # 投资活动现金流出
    fix_intan_other_asset_acqui_cash: Optional[float] = None  # 购建固定资产、无形资产和其他长期资产支付的现金(元)
    invest_cash_paid: Optional[float] = None  # 投资支付的现金(元)
    impawned_loan_net_increase: Optional[float] = None  # 质押贷款净增加额(元)
    net_cash_from_sub_company: Optional[float] = None  # 取得子公司及其他营业单位支付的现金净额(元)
    other_cash_to_invest_act: Optional[float] = None  # 支付其他与投资活动有关的现金(元)
    subtotal_invest_cash_outflow: Optional[float] = None  # 投资活动现金流出小计(元)
    net_invest_cash_flow: Optional[float] = None  # 投资活动产生的现金流量净额(元)

    # 筹资活动现金流入
    cash_from_invest: Optional[float] = None  # 吸收投资收到的现金(元)
    cash_from_borrowing: Optional[float] = None  # 取得借款收到的现金(元)
    cash_from_bonds_issue: Optional[float] = None  # 发行债券收到的现金(元)
    other_cash_from_finance_act: Optional[float] = None  # 收到其他与筹资活动有关的现金(元)
    subtotal_finance_cash_inflow: Optional[float] = None  # 筹资活动现金流入小计(元)

    # 筹资活动现金流出
    borrowing_repayment: Optional[float] = None  # 偿还债务支付的现金(元)
    dividend_interest_payment: Optional[float] = None  # 分配股利、利润或偿付利息支付的现金(元)
    other_cash_to_finance_act: Optional[float] = None  # 支付其他与筹资活动有关的现金(元)
    subtotal_finance_cash_outflow: Optional[float] = None  # 筹资活动现金流出小计(元)
    net_finance_cash_flow: Optional[float] = None  # 筹资活动产生的现金流量净额(元)

    # 汇率变动及现金净增加
    exchange_rate_change_effect: Optional[float] = None  # 汇率变动对现金及现金等价物的影响(元)
    cash_equivalent_increase: Optional[float] = None  # 现金及现金等价物净增加额(元)
    cash_equivalents_at_beginning: Optional[float] = None  # 期初现金及现金等价物余额(元)
    cash_and_equivalents_at_end: Optional[float] = None  # 期末现金及现金等价物余额(元)

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
        return 'cashflow_statement'

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CashflowStatement':
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
