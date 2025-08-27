"""DuckDB数据库实现"""

import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import date
import logging
from pathlib import Path
import threading
import shutil

from database import DatabaseInterface
from models.base import BaseModel


class DuckDBDatabase(DatabaseInterface):
    """DuckDB数据库实现"""
    
    def __init__(self, db_path: str = "data/stock_data.duckdb", snapshot_path: Optional[str] = None):
        self.db_path = Path(db_path)
        # 快照文件路径，默认与数据库同目录，文件名为 bak_<db_name>
        self.snapshot_path = Path(snapshot_path) if snapshot_path else (self.db_path.parent / f"bak_{self.db_path.name}")
        self.conn = None
        self.logger = logging.getLogger(__name__)
        self._lock = threading.RLock()
        # 脏标记：仅当本次运行发生写入操作时为 True
        self._dirty = False
    
    def connect(self) -> None:
        """连接数据库"""
        try:
            # 确保数据库目录存在
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.conn = duckdb.connect(str(self.db_path))
            # 新的连接会话开始，重置脏标记
            self._dirty = False
            self.logger.info(f"已连接到DuckDB数据库: {self.db_path}")
        except Exception as e:
            self.logger.error(f"连接数据库失败: {e}")
            raise
    
    def close(self) -> None:
        """关闭数据库连接并在需要时进行快照"""
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.logger.info("数据库连接已关闭")
            # 在连接关闭后，如果有写入发生则做一次快照
            if self._dirty:
                try:
                    # 确保快照目录存在
                    self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(self.db_path, self.snapshot_path)
                    self.logger.info(f"检测到本次运行有数据库更新，已创建快照: {self.snapshot_path}")
                except Exception as e:
                    self.logger.error(f"创建数据库快照失败: {e}")
            else:
                self.logger.info("本次运行无数据库更新，跳过快照")
    
    def create_tables(self) -> None:
        """创建所有数据表"""
        if not self.conn:
            raise RuntimeError("数据库未连接")
        
        # 利润表 - 基于聚宽API利润表字段对齐
        income_statement_sql = """
        CREATE TABLE IF NOT EXISTS income_statement (
            code VARCHAR,
            pub_date DATE,
            stat_date DATE,
            -- 收入类
            total_operating_revenue DOUBLE,  -- 营业总收入(元)
            operating_revenue DOUBLE,        -- 营业收入(元)
            interest_income DOUBLE,          -- 利息收入(元)
            premiums_earned DOUBLE,          -- 已赚保费(元)
            commission_income DOUBLE,        -- 手续费及佣金收入(元)
            -- 成本费用类
            total_operating_cost DOUBLE,     -- 营业总成本(元)
            operating_cost DOUBLE,           -- 营业成本(元)
            interest_expense DOUBLE,         -- 利息支出(元)
            commission_expense DOUBLE,       -- 手续费及佣金支出(元)
            refunded_premiums DOUBLE,        -- 退保金(元)
            net_pay_insurance_claims DOUBLE, -- 赔付支出净额(元)
            withdraw_insurance_contract_reserve DOUBLE, -- 提取保险合同准备金净额(元)
            policy_dividend_payout DOUBLE,  -- 保单红利支出(元)
            reinsurance_cost DOUBLE,         -- 分保费用(元)
            operating_tax_surcharges DOUBLE, -- 营业税金及附加(元)
            sale_expense DOUBLE,             -- 销售费用(元)
            administration_expense DOUBLE,   -- 管理费用(元)
            exploration_expense DOUBLE,      -- 勘探费用(元)
            financial_expense DOUBLE,        -- 财务费用(元)
            asset_impairment_loss DOUBLE,    -- 资产减值损失(元)
            -- 投资收益类
            fair_value_variable_income DOUBLE, -- 公允价值变动收益(元)
            investment_income DOUBLE,        -- 投资收益(元)
            invest_income_associates DOUBLE, -- 对联营企业和合营企业的投资收益(元)
            exchange_income DOUBLE,          -- 汇兑收益(元)
            other_items_influenced_income DOUBLE, -- 其他项目影响的收益(元)
            -- 利润类
            operating_profit DOUBLE,         -- 营业利润(元)
            subsidy_income DOUBLE,           -- 补贴收入(元)
            non_operating_revenue DOUBLE,    -- 营业外收入(元)
            non_operating_expense DOUBLE,    -- 营业外支出(元)
            disposal_loss_non_current_liability DOUBLE, -- 处置非流动资产损失(元)
            total_profit DOUBLE,             -- 利润总额(元)
            income_tax DOUBLE,               -- 所得税费用(元)
            net_profit DOUBLE,               -- 净利润(元)
            np_parent_company_owners DOUBLE, -- 归属于母公司所有者的净利润(元)
            minority_profit DOUBLE,          -- 少数股东损益(元)
            -- 每股收益
            eps DOUBLE,                      -- 每股收益(元)
            basic_eps DOUBLE,                -- 基本每股收益(元)
            diluted_eps DOUBLE,              -- 稀释每股收益(元)
            -- 综合收益
            other_composite_income DOUBLE,   -- 其他综合收益(元)
            total_composite_income DOUBLE,   -- 综合收益总额(元)
            ci_parent_company_owners DOUBLE, -- 归属于母公司所有者的综合收益总额(元)
            ci_minority_owners DOUBLE,       -- 归属于少数股东的综合收益总额(元)
            PRIMARY KEY (code, stat_date)
        )
        """
        
        # 现金流量表 - 基于聚宽API现金流量表字段对齐
        cashflow_statement_sql = """
        CREATE TABLE IF NOT EXISTS cashflow_statement (
            code VARCHAR,
            pub_date DATE,
            stat_date DATE,
            -- 经营活动现金流入
            goods_sale_and_service_render_cash DOUBLE, -- 销售商品、提供劳务收到的现金(元)
            net_deposit_increase DOUBLE,               -- 客户存款和同业存放款项净增加额(元)
            net_borrowing_from_central_bank DOUBLE,    -- 向中央银行借款净增加额(元)
            net_borrowing_from_finance_co DOUBLE,      -- 向其他金融机构拆入资金净增加额(元)
            net_original_insurance_cash DOUBLE,        -- 收到原保险合同保费取得的现金(元)
            net_cash_received_from_reinsurance_business DOUBLE, -- 收到再保险业务现金净额(元)
            net_insurer_deposit_investment DOUBLE,     -- 保户储金及投资款净增加额(元)
            net_deal_trading_assets DOUBLE,            -- 处置交易性金融资产净增加额(元)
            interest_and_commission_cashin DOUBLE,     -- 收取利息、手续费及佣金的现金(元)
            net_increase_in_placements DOUBLE,         -- 拆入资金净增加额(元)
            net_buyback DOUBLE,                        -- 回购业务资金净增加额(元)
            tax_levy_refund DOUBLE,                    -- 收到的税费返还(元)
            other_cashin_related_operate DOUBLE,       -- 收到其他与经营活动有关的现金(元)
            subtotal_operate_cash_inflow DOUBLE,       -- 经营活动现金流入小计(元)
            -- 经营活动现金流出
            goods_and_services_cash_paid DOUBLE,       -- 购买商品、接受劳务支付的现金(元)
            net_loan_and_advance_increase DOUBLE,      -- 客户贷款及垫款净增加额(元)
            net_deposit_in_cb_and_ib DOUBLE,           -- 存放中央银行和同业款项净增加额(元)
            original_compensation_paid DOUBLE,         -- 支付原保险合同赔付款项的现金(元)
            handling_charges_and_commission DOUBLE,    -- 支付利息、手续费及佣金的现金(元)
            policy_dividend_cash_paid DOUBLE,          -- 支付保单红利的现金(元)
            staff_behalf_paid DOUBLE,                  -- 支付给职工以及为职工支付的现金(元)
            tax_payments DOUBLE,                       -- 支付的各项税费(元)
            other_cash_paid_related_operate DOUBLE,    -- 支付其他与经营活动有关的现金(元)
            subtotal_operate_cash_outflow DOUBLE,      -- 经营活动现金流出小计(元)
            net_operate_cash_flow DOUBLE,              -- 经营活动产生的现金流量净额(元)
            -- 投资活动现金流入
            invest_withdrawal_cash DOUBLE,             -- 收回投资收到的现金(元)
            invest_proceeds DOUBLE,                    -- 取得投资收益收到的现金(元)
            fix_intan_other_asset_dispo_cash DOUBLE,   -- 处置固定资产、无形资产和其他长期资产收回的现金净额(元)
            net_cash_deal_subcompany DOUBLE,           -- 处置子公司及其他营业单位收到的现金净额(元)
            other_cash_from_invest_act DOUBLE,         -- 收到其他与投资活动有关的现金(元)
            subtotal_invest_cash_inflow DOUBLE,        -- 投资活动现金流入小计(元)
            -- 投资活动现金流出
            fix_intan_other_asset_acqui_cash DOUBLE,   -- 购建固定资产、无形资产和其他长期资产支付的现金(元)
            invest_cash_paid DOUBLE,                   -- 投资支付的现金(元)
            impawned_loan_net_increase DOUBLE,         -- 质押贷款净增加额(元)
            net_cash_from_sub_company DOUBLE,          -- 取得子公司及其他营业单位支付的现金净额(元)
            other_cash_to_invest_act DOUBLE,           -- 支付其他与投资活动有关的现金(元)
            subtotal_invest_cash_outflow DOUBLE,       -- 投资活动现金流出小计(元)
            net_invest_cash_flow DOUBLE,               -- 投资活动产生的现金流量净额(元)
            -- 筹资活动现金流入
            cash_from_invest DOUBLE,                   -- 吸收投资收到的现金(元)
            cash_from_borrowing DOUBLE,                -- 取得借款收到的现金(元)
            cash_from_bonds_issue DOUBLE,              -- 发行债券收到的现金(元)
            other_cash_from_finance_act DOUBLE,        -- 收到其他与筹资活动有关的现金(元)
            subtotal_finance_cash_inflow DOUBLE,       -- 筹资活动现金流入小计(元)
            -- 筹资活动现金流出
            borrowing_repayment DOUBLE,                -- 偿还债务支付的现金(元)
            dividend_interest_payment DOUBLE,          -- 分配股利、利润或偿付利息支付的现金(元)
            other_cash_to_finance_act DOUBLE,          -- 支付其他与筹资活动有关的现金(元)
            subtotal_finance_cash_outflow DOUBLE,      -- 筹资活动现金流出小计(元)
            net_finance_cash_flow DOUBLE,              -- 筹资活动产生的现金流量净额(元)
            -- 汇率变动及现金净增加
            exchange_rate_change_effect DOUBLE,        -- 汇率变动对现金及现金等价物的影响(元)
            cash_equivalent_increase DOUBLE,           -- 现金及现金等价物净增加额(元)
            cash_equivalents_at_beginning DOUBLE,      -- 期初现金及现金等价物余额(元)
            cash_and_equivalents_at_end DOUBLE,        -- 期末现金及现金等价物余额(元)
            PRIMARY KEY (code, stat_date)
        )
        """
        
        # 资产负债表 - 基于聚宽API资产负债表字段对齐
        balance_sheet_sql = """
        CREATE TABLE IF NOT EXISTS balance_sheet (
            code VARCHAR,
            pub_date DATE,
            stat_date DATE,
            -- 流动资产
            cash_equivalents DOUBLE,                   -- 货币资金(元)
            settlement_provi DOUBLE,                   -- 结算备付金(元)
            lend_capital DOUBLE,                       -- 拆出资金(元)
            trading_assets DOUBLE,                     -- 交易性金融资产(元)
            bill_receivable DOUBLE,                    -- 应收票据(元)
            account_receivable DOUBLE,                 -- 应收账款(元)
            advance_payment DOUBLE,                    -- 预付款项(元)
            insurance_receivables DOUBLE,              -- 应收保费(元)
            reinsurance_receivables DOUBLE,            -- 应收分保账款(元)
            reinsurance_contract_reserves_receivable DOUBLE, -- 应收分保合同准备金(元)
            interest_receivable DOUBLE,                -- 应收利息(元)
            dividend_receivable DOUBLE,                -- 应收股利(元)
            other_receivable DOUBLE,                   -- 其他应收款(元)
            bought_sellback_assets DOUBLE,             -- 买入返售金融资产(元)
            inventories DOUBLE,                        -- 存货(元)
            expendable_biological_asset DOUBLE,        -- 消耗性生物资产(元)
            non_current_asset_in_one_year DOUBLE,      -- 一年内到期的非流动资产(元)
            other_current_assets DOUBLE,               -- 其他流动资产(元)
            total_current_assets DOUBLE,               -- 流动资产合计(元)
            -- 非流动资产
            loan_and_advance_current DOUBLE,           -- 发放贷款及垫款(元)
            available_for_sale_assets DOUBLE,          -- 可供出售金融资产(元)
            held_to_maturity_investments DOUBLE,       -- 持有至到期投资(元)
            long_term_receivable DOUBLE,               -- 长期应收款(元)
            long_term_equity_invest DOUBLE,            -- 长期股权投资(元)
            investment_real_estate DOUBLE,             -- 投资性房地产(元)
            fixed_assets DOUBLE,                       -- 固定资产(元)
            constru_in_process DOUBLE,                 -- 在建工程(元)
            construction_materials DOUBLE,             -- 工程物资(元)
            fixed_assets_liquidation DOUBLE,           -- 固定资产清理(元)
            biological_assets DOUBLE,                  -- 生产性生物资产(元)
            oil_and_gas_assets DOUBLE,                 -- 油气资产(元)
            intangible_assets DOUBLE,                  -- 无形资产(元)
            development_expenditure DOUBLE,            -- 开发支出(元)
            good_will DOUBLE,                          -- 商誉(元)
            long_deferred_expense DOUBLE,              -- 长期待摊费用(元)
            deferred_tax_assets DOUBLE,                -- 递延所得税资产(元)
            other_non_current_assets DOUBLE,           -- 其他非流动资产(元)
            total_non_current_assets DOUBLE,           -- 非流动资产合计(元)
            total_assets DOUBLE,                       -- 资产总计(元)
            -- 流动负债
            short_term_loan DOUBLE,                    -- 短期借款(元)
            borrowing_from_central_bank DOUBLE,        -- 向中央银行借款(元)
            lend_capital_liability DOUBLE,             -- 拆入资金(元)
            trading_liability DOUBLE,                  -- 交易性金融负债(元)
            notes_payable DOUBLE,                      -- 应付票据(元)
            accounts_payable DOUBLE,                   -- 应付账款(元)
            advance_peceipts DOUBLE,                   -- 预收款项(元)
            sold_buyback_secu_proceeds DOUBLE,         -- 卖出回购金融资产款(元)
            commission_payable DOUBLE,                 -- 应付手续费及佣金(元)
            salaries_payable DOUBLE,                   -- 应付职工薪酬(元)
            taxs_payable DOUBLE,                       -- 应交税费(元)
            interest_payable DOUBLE,                   -- 应付利息(元)
            dividend_payable DOUBLE,                   -- 应付股利(元)
            other_payable DOUBLE,                      -- 其他应付款(元)
            reinsurance_payables DOUBLE,               -- 应付分保账款(元)
            insurance_contract_reserves DOUBLE,        -- 保险合同准备金(元)
            proxy_secu_proceeds DOUBLE,                -- 代理买卖证券款(元)
            receivings_from_vicariously_sold_securities DOUBLE, -- 代理承销证券款(元)
            non_current_liability_in_one_year DOUBLE,  -- 一年内到期的非流动负债(元)
            other_current_liability DOUBLE,            -- 其他流动负债(元)
            total_current_liability DOUBLE,            -- 流动负债合计(元)
            -- 非流动负债
            long_term_loan DOUBLE,                     -- 长期借款(元)
            bonds_payable DOUBLE,                      -- 应付债券(元)
            long_term_account_payable DOUBLE,          -- 长期应付款(元)
            specific_account_payable DOUBLE,           -- 专项应付款(元)
            estimated_liability DOUBLE,                -- 预计负债(元)
            deferred_tax_liability DOUBLE,             -- 递延所得税负债(元)
            other_non_current_liability DOUBLE,        -- 其他非流动负债(元)
            total_non_current_liability DOUBLE,        -- 非流动负债合计(元)
            total_liability DOUBLE,                    -- 负债合计(元)
            -- 所有者权益
            paidin_capital DOUBLE,                     -- 实收资本(或股本)(元)
            other_equity_tools DOUBLE,                 -- 其他权益工具(元)
            other_equity_tools_PRE_STOCK DOUBLE,       -- 其他权益工具:优先股(元)
            other_equity_tools_PERPETUAL_DEBT DOUBLE,  -- 其他权益工具:永续债(元)
            other_equity_tools_OTHER DOUBLE,           -- 其他权益工具:其他(元)
            capital_reserve_fund DOUBLE,               -- 资本公积(元)
            treasury_stock DOUBLE,                     -- 库存股(元)
            other_comprehensive_income DOUBLE,         -- 其他综合收益(元)
            specific_reserves DOUBLE,                  -- 专项储备(元)
            earned_surplus DOUBLE,                     -- 盈余公积(元)
            general_risk_preparation DOUBLE,           -- 一般风险准备(元)
            undistributed_profit DOUBLE,               -- 未分配利润(元)
            foreign_exchange_gain DOUBLE,              -- 外币报表折算差额(元)
            total_owner_equities DOUBLE,               -- 归属于母公司所有者权益合计(元)
            minority_interests DOUBLE,                 -- 少数股东权益(元)
            total_sheet_owner_equities DOUBLE,         -- 所有者权益合计(元)
            total_liability_equity DOUBLE,             -- 负债和所有者权益总计(元)
            PRIMARY KEY (code, stat_date)
        )
        """
        
        # 基本面数据表 - 基于聚宽API valuation估值数据表字段
        fundamental_data_sql = """
        CREATE TABLE IF NOT EXISTS fundamental_data (
            code VARCHAR,
            day DATE,
            capitalization DOUBLE,           -- 总股本(万股)
            circulating_cap DOUBLE,          -- 流通股本(万股)
            market_cap DOUBLE,               -- 总市值(亿元)
            circulating_market_cap DOUBLE,   -- 流通市值(亿元)
            turnover_ratio DOUBLE,           -- 换手率(%)
            pe_ratio DOUBLE,                 -- 市盈率(PE, TTM)
            pe_ratio_lyr DOUBLE,             -- 市盈率(PE)
            pb_ratio DOUBLE,                 -- 市净率(PB)
            ps_ratio DOUBLE,                 -- 市销率(PS, TTM)
            pcf_ratio DOUBLE,                -- 市现率(PCF, 现金净流量TTM)
            pcf_ratio2 DOUBLE,               -- 市现率(PCF,经营活动现金流TTM)
            dividend_ratio DOUBLE,           -- 股息率(TTM) %
            free_cap DOUBLE,                 -- 自由流通股本(万股)
            free_market_cap DOUBLE,          -- 自由流通市值(亿元)
            a_cap DOUBLE,                    -- A股总股本(万股)
            a_market_cap DOUBLE,             -- A股总市值(亿元)
            PRIMARY KEY (code, day)
        )
        """
        
        # 技术指标数据表
        indicator_data_sql = """
        CREATE TABLE IF NOT EXISTS indicator_data (
            code VARCHAR,
            day DATE,
            
            -- 基础盈利指标
            eps DOUBLE,  -- 每股收益EPS(元)
            adjusted_profit DOUBLE,  -- 扣除非经常损益后的净利润(元)
            operating_profit DOUBLE,  -- 经营活动净收益(元)
            value_change_profit DOUBLE,  -- 价值变动净收益(元)
            
            -- 盈利能力指标
            roe DOUBLE,  -- 净资产收益率ROE(%)
            roa DOUBLE,  -- 总资产收益率ROA(%)
            roic DOUBLE,  -- 投入资本回报率ROIC(%)
            inc_return DOUBLE,  -- 净资产收益率(扣除非经常损益)(%)
            
            -- 盈利质量指标
            gross_profit_margin DOUBLE,  -- 毛利率(%)
            net_profit_margin DOUBLE,  -- 净利率(%)
            operating_profit_margin DOUBLE,  -- 营业利润率(%)
            
            -- 偿债能力指标
            debt_to_assets DOUBLE,  -- 资产负债率(%)
            debt_to_equity DOUBLE,  -- 产权比率(%)
            current_ratio DOUBLE,  -- 流动比率
            quick_ratio DOUBLE,  -- 速动比率
            equity_ratio DOUBLE,  -- 股东权益比率(%)
            
            -- 运营能力指标
            inventory_turnover DOUBLE,  -- 存货周转率(次)
            total_asset_turnover DOUBLE,  -- 总资产周转率(次)
            receivable_turnover DOUBLE,  -- 应收账款周转率(次)
            current_asset_turnover DOUBLE,  -- 流动资产周转率(次)
            
            -- 成长能力指标
            inc_revenue_year_on_year DOUBLE,  -- 营业收入同比增长率(%)
            inc_profit_year_on_year DOUBLE,  -- 净利润同比增长率(%)
            inc_net_profit_year_on_year DOUBLE,  -- 净利润同比增长率(%)
            inc_total_revenue_year_on_year DOUBLE,  -- 营业总收入同比增长率(%)
            
            -- 现金流指标
            ocf_to_revenue DOUBLE,  -- 经营活动产生的现金流量净额/营业收入(%)
            ocf_to_operating_profit DOUBLE,  -- 经营活动产生的现金流量净额/经营活动净收益(%)
            
            -- 杜邦分析相关
            du_return_on_equity DOUBLE,  -- 净资产收益率(杜邦分析)
            du_asset_turnover DOUBLE,  -- 总资产周转率(杜邦分析)
            du_equity_multiplier DOUBLE,  -- 权益乘数(杜邦分析)
            
            -- 其他重要指标
            book_to_market_ratio DOUBLE,  -- 账面市值比
            earnings_yield DOUBLE,  -- 盈利收益率
            capitalization_ratio DOUBLE,  -- 资本化比率
            
            PRIMARY KEY (code, day)
        )
        """
        
        # 融资融券数据表
        mtss_data_sql = """
        CREATE TABLE IF NOT EXISTS mtss_data (
            code VARCHAR,
            day DATE,
            fin_value DOUBLE,
            fin_buy_value DOUBLE,
            fin_refund_value DOUBLE,
            sec_value DOUBLE,
            sec_sell_value DOUBLE,
            sec_refund_value DOUBLE,
            sec_sell_vol DOUBLE,
            sec_refund_vol DOUBLE,
            fin_sec_value DOUBLE,
            PRIMARY KEY (code, day)
        )
        """
        
        # 价格数据表 - 基于聚宽get_price接口字段
        price_data_sql = """
        CREATE TABLE IF NOT EXISTS price_data (
            code VARCHAR,
            day DATE,
            -- 基础价格数据 (聚宽get_price核心字段)
            open DOUBLE,
            close DOUBLE,
            high DOUBLE,
            low DOUBLE,
            pre_close DOUBLE,
            -- 成交数据
            volume DOUBLE,
            money DOUBLE,
            -- 复权相关
            factor DOUBLE,
            -- 涨跌停价格
            high_limit DOUBLE,
            low_limit DOUBLE,
            -- 其他价格指标
            avg DOUBLE,
            -- 交易状态
            paused INTEGER,
            -- 兼容字段 (保持向后兼容)
            adj_close DOUBLE,
            adj_factor DOUBLE,
            PRIMARY KEY (code, day)
        )
        """
        
        # 股票列表表
        stock_list_sql = """
        CREATE TABLE IF NOT EXISTS stock_list (
            code VARCHAR PRIMARY KEY,
            display_name VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            exchange VARCHAR,
            market VARCHAR,
            industry_code VARCHAR,
            industry_name VARCHAR,
            sector_code VARCHAR,
            sector_name VARCHAR,
            status VARCHAR DEFAULT 'normal',
            is_st BOOLEAN DEFAULT FALSE,
            update_date DATE
        )
        """
        
        # 执行创建表的SQL
        tables = {
            'income_statement': income_statement_sql,
            'cashflow_statement': cashflow_statement_sql,
            'balance_sheet': balance_sheet_sql,
            'fundamental_data': fundamental_data_sql,
            'indicator_data': indicator_data_sql,
            'mtss_data': mtss_data_sql,
            'price_data': price_data_sql,
            'stock_list': stock_list_sql
        }
        
        for table_name, sql in tables.items():
            try:
                with self._lock:
                    self.conn.execute(sql)
                self.logger.info(f"表 {table_name} 创建成功")
            except Exception as e:
                self.logger.error(f"创建表 {table_name} 失败: {e}")
                raise
    
    def insert_data(self, model: BaseModel) -> bool:
        """插入单条数据"""
        try:
            data_dict = model.to_dict()
            table_name = model.get_table_name()
            
            # 构建插入SQL
            columns = list(data_dict.keys())
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            values = [data_dict[col] for col in columns]
            with self._lock:
                self.conn.execute(sql, values)
            # 标记本次会话发生了写入
            self._dirty = True
            
            return True
        except Exception as e:
            self.logger.error(f"插入数据失败: {e}")
            return False
    
    def insert_batch(self, models: List[BaseModel]) -> bool:
        """批量插入数据"""
        if not models:
            return True
        
        try:
            # 按表名分组
            tables_data = {}
            for model in models:
                table_name = model.get_table_name()
                if table_name not in tables_data:
                    tables_data[table_name] = []
                tables_data[table_name].append(model.to_dict())
            
            # 批量插入每个表
            for table_name, data_list in tables_data.items():
                df = pd.DataFrame(data_list)
                self.insert_dataframe(df, table_name)
            
            return True
        except Exception as e:
            self.logger.error(f"批量插入数据失败: {e}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """插入DataFrame数据"""
        if df.empty:
            return True
        
        try:
            # 显式注册 DataFrame，避免多线程下隐式变量解析问题
            with self._lock:
                self.conn.register('tmp_df', df)
                self.conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM tmp_df")
                try:
                    self.conn.unregister('tmp_df')
                except Exception:
                    # 某些版本可能没有 unregister，忽略异常
                    pass
            self.logger.info(f"成功插入 {len(df)} 条记录到表 {table_name}")
            # 标记本次会话发生了写入
            self._dirty = True
            return True
        except Exception as e:
            self.logger.error(f"插入DataFrame到表 {table_name} 失败: {e}")
            return False
    
    def query_data(self, sql: str, params: Optional[Any] = None) -> pd.DataFrame:
        """执行SQL查询"""
        try:
            if params is not None:
                if isinstance(params, dict):
                    exec_params = list(params.values())
                elif isinstance(params, (list, tuple)):
                    exec_params = list(params)
                else:
                    # 单值参数
                    exec_params = [params]
                with self._lock:
                    result = self.conn.execute(sql, exec_params).fetchdf()
            else:
                with self._lock:
                    result = self.conn.execute(sql).fetchdf()
            return result
        except Exception as e:
            self.logger.error(f"查询数据失败: {e}")
            return pd.DataFrame()
    
    def get_latest_date(self, table_name: str, code: Optional[str] = None) -> Optional[date]:
        """获取最新数据日期"""
        try:
            # 根据表类型选择日期字段
            date_field = 'day' if table_name in ['fundamental_data', 'indicator_data', 'mtss_data', 'price_data'] else 'stat_date'
            
            if code:
                sql = f"SELECT MAX({date_field}) as max_date FROM {table_name} WHERE code = ?"
                with self._lock:
                    result = self.conn.execute(sql, [code]).fetchone()
            else:
                sql = f"SELECT MAX({date_field}) as max_date FROM {table_name}"
                with self._lock:
                    result = self.conn.execute(sql).fetchone()
            
            if result and result[0]:
                return result[0] if isinstance(result[0], date) else date.fromisoformat(str(result[0]))
            return None
        except Exception as e:
            self.logger.error(f"获取最新日期失败: {e}")
            return None
    
    def get_existing_codes(self, table_name: str) -> List[str]:
        """获取已存在的股票代码"""
        try:
            sql = f"SELECT DISTINCT code FROM {table_name} ORDER BY code"
            with self._lock:
                result = self.conn.execute(sql).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            self.logger.error(f"获取股票代码失败: {e}")
            return []
    
    def delete_data(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """删除数据"""
        try:
            where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
            sql = f"DELETE FROM {table_name} WHERE {where_clause}"
            with self._lock:
                self.conn.execute(sql, list(conditions.values()))
            # 标记写入（删除也是数据变更）
            self._dirty = True
            return True
        except Exception as e:
            self.logger.error(f"删除数据失败: {e}")
            return False
    
    def update_data(self, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """更新数据"""
        try:
            set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
            where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            
            values = list(data.values()) + list(conditions.values())
            with self._lock:
                self.conn.execute(sql, values)
            # 标记写入（更新属于数据变更）
            self._dirty = True
            return True
        except Exception as e:
            self.logger.error(f"更新数据失败: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            # 使用DuckDB的SHOW TABLES语法
            with self._lock:
                result = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in result]
            return table_name in table_names
        except Exception as e:
            self.logger.error(f"检查表存在性失败: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        try:
            # 获取记录数
            count_sql = f"SELECT COUNT(*) FROM {table_name}"
            with self._lock:
                count_result = self.conn.execute(count_sql).fetchone()
            record_count = count_result[0] if count_result else 0
            
            # 获取表结构
            schema_sql = f"PRAGMA table_info({table_name})"
            with self._lock:
                schema_result = self.conn.execute(schema_sql).fetchall()
            
            columns = []
            for row in schema_result:
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'not_null': bool(row[3]),
                    'primary_key': bool(row[5])
                })
            
            return {
                'table_name': table_name,
                'record_count': record_count,
                'columns': columns
            }
        except Exception as e:
            self.logger.error(f"获取表信息失败: {e}")
            return {'table_name': table_name, 'record_count': 0, 'columns': []}