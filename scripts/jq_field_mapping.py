#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JoinQuant API字段映射配置
自动生成于: 2025-08-11 17:15:17
"""

# JoinQuant API字段到本地数据库字段的映射
JQ_TO_LOCAL_FIELD_MAPPING = {
    'code': 'code',
    'pubDate': 'pub_date',
    'statDate': 'stat_date',
    'eps': 'eps',
    'adjusted_profit': 'adjusted_profit',
    'operating_profit': 'operating_profit',
    'value_change_profit': 'value_change_profit',
    'roe': 'roe',
    'inc_return': 'inc_return',
    'roa': 'roa',
    'net_profit_margin': 'net_profit_margin',
    'gross_profit_margin': 'gross_profit_margin',
    'expense_to_total_revenue': 'expense_to_total_revenue',
    'operation_profit_to_total_revenue': 'operation_profit_to_total_revenue',
    'net_profit_to_total_revenue': 'net_profit_to_total_revenue',
    'operating_expense_to_total_revenue': 'operating_expense_to_total_revenue',
    'ga_expense_to_total_revenue': 'ga_expense_to_total_revenue',
    'financing_expense_to_total_revenue': 'financing_expense_to_total_revenue',
    'operating_profit_to_profit': 'operating_profit_to_profit',
    'invesment_profit_to_profit': 'invesment_profit_to_profit',
    'adjusted_profit_to_profit': 'adjusted_profit_to_profit',
    'goods_sale_and_service_to_revenue': 'goods_sale_and_service_to_revenue',
    'ocf_to_revenue': 'ocf_to_revenue',
    'ocf_to_operating_profit': 'ocf_to_operating_profit',
    'inc_total_revenue_year_on_year': 'inc_total_revenue_year_on_year',
    'inc_total_revenue_annual': 'inc_total_revenue_annual',
    'inc_revenue_year_on_year': 'inc_revenue_year_on_year',
}

# 本地数据库字段到JoinQuant API字段的反向映射
LOCAL_TO_JQ_FIELD_MAPPING = {v: k for k, v in JQ_TO_LOCAL_FIELD_MAPPING.items()}

# 字段映射函数
def map_jq_to_local(jq_data: dict) -> dict:
    """将JoinQuant API数据映射到本地字段"""
    local_data = {}
    for jq_field, value in jq_data.items():
        local_field = JQ_TO_LOCAL_FIELD_MAPPING.get(jq_field)
        if local_field:
            local_data[local_field] = value
        else:
            # 未映射的字段保持原名
            local_data[jq_field] = value
    return local_data

def map_local_to_jq(local_data: dict) -> dict:
    """将本地数据映射到JoinQuant API字段"""
    jq_data = {}
    for local_field, value in local_data.items():
        jq_field = LOCAL_TO_JQ_FIELD_MAPPING.get(local_field, local_field)
        jq_data[jq_field] = value
    return jq_data

# 支持的JoinQuant API字段列表
SUPPORTED_JQ_FIELDS = list(JQ_TO_LOCAL_FIELD_MAPPING.keys())

# 本地扩展字段(JoinQuant API中没有的字段)
LOCAL_EXTENDED_FIELDS = [
    'roic',  # 投入资本回报率
    'operating_profit_margin',  # 营业利润率
    'inc_profit_year_on_year',  # 净利润同比增长率
    'inc_net_profit_year_on_year',  # 净利润同比增长率(年化)
    'inc_net_profit_to_shareholders_year_on_year',  # 归母净利润同比增长率
    'debt_to_assets',  # 资产负债率
    'debt_to_equity',  # 产权比率
    'current_ratio',  # 流动比率
    'quick_ratio',  # 速动比率
    'inventory_turnover',  # 存货周转率
    'receivable_turnover',  # 应收账款周转率
    'accounts_payable_turnover',  # 应付账款周转率
    'current_assets_turnover',  # 流动资产周转率
    'fixed_assets_turnover',  # 固定资产周转率
    'total_assets_turnover',  # 总资产周转率
    'operating_cash_flow_per_share',  # 每股经营现金流
    'cash_flow_per_share',  # 每股现金流量净额
    'book_to_market_ratio',  # 净资产与市价比率
    'earnings_yield',  # 盈利收益率
    'capitalization_ratio',  # 股本报酬率
    'du_return_on_equity',  # 杜邦分析净资产收益率
    'du_equity_multiplier'  # 杜邦分析权益乘数
]
