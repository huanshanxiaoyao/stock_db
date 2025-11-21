"""DuckDB数据库实现"""

import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
import threading
import shutil
import glob
import os

from database import DatabaseInterface
from models.base import BaseModel


class DuckDBDatabase(DatabaseInterface):
    """DuckDB数据库实现"""
    
    def __init__(self, db_path: Optional[str] = None, snapshot_path: Optional[str] = None, is_replica: bool = False):
        # 如果没有指定db_path，使用配置中的路径
        if db_path is None:
            from config import get_config
            config = get_config()
            db_path = config.database.path
        self.db_path = Path(db_path)
        # 快照文件路径，默认与数据库同目录，文件名为 bak_<db_name>
        self.snapshot_path = Path(snapshot_path) if snapshot_path else (self.db_path.parent / f"bak_{self.db_path.name}")
        self.conn = None
        self.logger = logging.getLogger(__name__)
        self._lock = threading.RLock()
        # 脏标记：仅当本次运行发生写入操作时为 True
        self._dirty = False
        # 副本模式标记：副本数据库不需要备份（副本本身就是备份）
        self._is_replica = is_replica
    
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
        """关闭数据库连接并进行定时备份"""
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.logger.info("数据库连接已关闭")
            
            # 执行定时备份逻辑
            self._perform_timed_backup()
    
    def _perform_timed_backup(self) -> None:
        """执行定时备份逻辑"""
        # 副本模式下不需要备份（副本本身就是备份）
        if self._is_replica:
            self.logger.debug("副本模式下跳过备份")
            return

        try:
            # 确保备份目录存在
            backup_dir = self.db_path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 检查是否需要创建新备份
            if self._should_create_backup(backup_dir):
                # 创建带时间戳的备份文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_filename = f"{self.db_path.stem}_backup_{timestamp}.duckdb"
                backup_path = backup_dir / backup_filename
                
                # 创建备份
                shutil.copy2(self.db_path, backup_path)
                self.logger.info(f"已创建定时备份: {backup_path}")
                
                # 清理48小时前的旧备份
                self._cleanup_old_backups(backup_dir)
            else:
                self.logger.debug("距离上次备份不足1小时，跳过备份")
                
        except Exception as e:
            self.logger.error(f"定时备份失败: {e}")
    
    def _should_create_backup(self, backup_dir: Path) -> bool:
        """检查是否应该创建新备份"""
        try:
            # 查找最新的备份文件
            backup_pattern = f"{self.db_path.stem}_backup_*.duckdb"
            backup_files = list(backup_dir.glob(backup_pattern))
            
            if not backup_files:
                # 没有备份文件，需要创建
                return True
            
            # 找到最新的备份文件
            latest_backup = max(backup_files, key=lambda f: f.stat().st_mtime)
            latest_backup_time = datetime.fromtimestamp(latest_backup.stat().st_mtime)
            
            # 检查是否超过1小时
            time_diff = datetime.now() - latest_backup_time
            return time_diff >= timedelta(hours=1)
            
        except Exception as e:
            self.logger.error(f"检查备份时间失败: {e}")
            return True  # 出错时默认创建备份
    
    def _cleanup_old_backups(self, backup_dir: Path) -> None:
        """清理48小时前的旧备份文件"""
        try:
            # 计算48小时前的时间
            cutoff_time = datetime.now() - timedelta(hours=48)
            
            # 查找所有备份文件
            backup_pattern = f"{self.db_path.stem}_backup_*.duckdb"
            backup_files = list(backup_dir.glob(backup_pattern))
            
            deleted_count = 0
            for backup_file in backup_files:
                file_time = datetime.fromtimestamp(backup_file.stat().st_mtime)
                if file_time < cutoff_time:
                    try:
                        backup_file.unlink()
                        deleted_count += 1
                        self.logger.debug(f"已删除旧备份: {backup_file}")
                    except Exception as e:
                        self.logger.warning(f"删除旧备份失败 {backup_file}: {e}")
            
            if deleted_count > 0:
                self.logger.info(f"已清理 {deleted_count} 个48小时前的旧备份文件")
                
        except Exception as e:
            self.logger.error(f"清理旧备份失败: {e}")
    
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
        
        # 估值数据表 - 基于聚宽API valuation估值数据表字段
        valuation_data_sql = """
        CREATE TABLE IF NOT EXISTS valuation_data (
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
        
        # 财务指标数据表 - 严格基于聚宽API indicator财务指标表
        indicator_data_sql = """
        CREATE TABLE IF NOT EXISTS indicator_data (
            -- 基础字段
            code VARCHAR,  -- 股票代码 带后缀.XSHE/.XSHG
            pubDate DATE,  -- 公司发布财报日期
            statDate DATE,  -- 财报统计的季度的最后一天
            
            -- 盈利能力指标
            eps DOUBLE,  -- 每股收益EPS(元)
            adjusted_profit DOUBLE,  -- 扣除非经常损益后的净利润(元)
            operating_profit DOUBLE,  -- 经营活动净收益(元)
            value_change_profit DOUBLE,  -- 价值变动净收益(元)
            roe DOUBLE,  -- 净资产收益率ROE(%)
            inc_return DOUBLE,  -- 净资产收益率(扣除非经常损益)(%)
            roa DOUBLE,  -- 总资产净利率ROA(%)
            net_profit_margin DOUBLE,  -- 销售净利率(%)
            gross_profit_margin DOUBLE,  -- 销售毛利率(%)
            
            -- 成本费用指标
            expense_to_total_revenue DOUBLE,  -- 营业总成本/营业总收入(%)
            operation_profit_to_total_revenue DOUBLE,  -- 营业利润/营业总收入(%)
            net_profit_to_total_revenue DOUBLE,  -- 净利润/营业总收入(%)
            operating_expense_to_total_revenue DOUBLE,  -- 营业费用/营业总收入(%)
            ga_expense_to_total_revenue DOUBLE,  -- 管理费用/营业总收入(%)
            financing_expense_to_total_revenue DOUBLE,  -- 财务费用/营业总收入(%)
            
            -- 盈利质量指标
            operating_profit_to_profit DOUBLE,  -- 经营活动净收益/利润总额(%)
            invesment_profit_to_profit DOUBLE,  -- 价值变动净收益/利润总额(%)
            adjusted_profit_to_profit DOUBLE,  -- 扣除非经常损益后的净利润/归属于母公司所有者的净利润(%)
            
            -- 现金流指标
            goods_sale_and_service_to_revenue DOUBLE,  -- 销售商品提供劳务收到的现金/营业收入(%)
            ocf_to_revenue DOUBLE,  -- 经营活动产生的现金流量净额/营业收入(%)
            ocf_to_operating_profit DOUBLE,  -- 经营活动产生的现金流量净额/经营活动净收益(%)
            
            -- 成长能力指标
            inc_total_revenue_year_on_year DOUBLE,  -- 营业总收入同比增长率(%)
            inc_total_revenue_annual DOUBLE,  -- 营业总收入环比增长率(%)
            inc_revenue_year_on_year DOUBLE,  -- 营业收入同比增长率(%)
            inc_revenue_annual DOUBLE,  -- 营业收入环比增长率(%)
            inc_operation_profit_year_on_year DOUBLE,  -- 营业利润同比增长率(%)
            inc_operation_profit_annual DOUBLE,  -- 营业利润环比增长率(%)
            inc_net_profit_year_on_year DOUBLE,  -- 净利润同比增长率(%)
            inc_net_profit_annual DOUBLE,  -- 净利润环比增长率(%)
            inc_net_profit_to_shareholders_year_on_year DOUBLE,  -- 归母净利润同比增长率(%)
            inc_net_profit_to_shareholders_annual DOUBLE,  -- 归母净利润环比增长率(%)
            
            PRIMARY KEY (code, pubDate, statDate)
        )
        """
        
        # 融资融券和资金流向数据表 (MTSS + Money Flow)
        mtss_data_sql = """
        CREATE TABLE IF NOT EXISTS mtss_data (
            code VARCHAR,
            day DATE,
            -- 融资融券数据 (Source2: get_mtss from JQData - 9 fields)
            fin_value DOUBLE,              -- 融资余额(元)
            fin_buy_value DOUBLE,          -- 融资买入额(元)
            fin_refund_value DOUBLE,       -- 融资偿还额(元)
            sec_value DOUBLE,              -- 融券余额(元)
            sec_sell_value DOUBLE,         -- 融券卖出额(元)
            sec_refund_value DOUBLE,       -- 融券偿还额(元)
            fin_sec_value DOUBLE,          -- 融资融券余额(元)
            -- 资金流向数据 (Source1: get_money_flow_pro from JQData - 12 fields)
            inflow_xl DOUBLE,              -- 超大单流入
            inflow_l DOUBLE,               -- 大单流入
            inflow_m DOUBLE,               -- 中单流入
            inflow_s DOUBLE,               -- 小单流入
            outflow_xl DOUBLE,             -- 超大单流出
            outflow_l DOUBLE,              -- 大单流出
            outflow_m DOUBLE,              -- 中单流出
            outflow_s DOUBLE,              -- 小单流出
            netflow_xl DOUBLE,             -- 超大单净流入
            netflow_l DOUBLE,              -- 大单净流入
            netflow_m DOUBLE,              -- 中单净流入
            netflow_s DOUBLE,              -- 小单净流入
            PRIMARY KEY (code, day)
        )
        """
        
        # 每日基本指标数据表 - 基于Tushare daily_basic接口
        daily_basic_sql = """
        CREATE TABLE IF NOT EXISTS daily_basic (
            code VARCHAR,
            day DATE,
            -- 价格数据
            close DOUBLE,                  -- 当日收盘价
            -- 换手率
            turnover_rate DOUBLE,          -- 换手率(%)
            turnover_rate_f DOUBLE,        -- 换手率(自由流通股)(%)
            volume_ratio DOUBLE,           -- 量比
            -- 估值指标
            pe DOUBLE,                     -- 市盈率(总市值/净利润, 亏损的PE为空)
            pe_ttm DOUBLE,                 -- 市盈率(TTM)
            pb DOUBLE,                     -- 市净率(总市值/净资产)
            ps DOUBLE,                     -- 市销率
            ps_ttm DOUBLE,                 -- 市销率(TTM)
            dv_ratio DOUBLE,               -- 股息率(%)
            dv_ttm DOUBLE,                 -- 股息率(TTM)(%)
            -- 股本数据
            total_share DOUBLE,            -- 总股本(万股)
            float_share DOUBLE,            -- 流通股本(万股)
            free_share DOUBLE,             -- 自由流通股本(万股)
            -- 市值数据
            total_mv DOUBLE,               -- 总市值(万元)
            circ_mv DOUBLE,                -- 流通市值(万元)
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
        
        # 用户交易记录表
        user_transactions_sql = """
        CREATE TABLE IF NOT EXISTS user_transactions (
            trade_id VARCHAR PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            stock_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            trade_type INTEGER NOT NULL,
            strategy_id VARCHAR,
            quantity DOUBLE NOT NULL,
            price DOUBLE NOT NULL,
            amount DOUBLE NOT NULL,
            commission DOUBLE DEFAULT 0,
            stamp_tax DOUBLE DEFAULT 0,
            other_fees DOUBLE DEFAULT 0,
            net_amount DOUBLE NOT NULL,
            order_id VARCHAR,
            remark VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # 用户持仓记录表
        user_positions_sql = """
        CREATE TABLE IF NOT EXISTS user_positions (
            position_id VARCHAR PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            position_date DATE NOT NULL,
            stock_code VARCHAR NOT NULL,
            position_quantity INTEGER NOT NULL,
            available_quantity INTEGER NOT NULL,
            frozen_quantity INTEGER DEFAULT 0,
            transit_shares INTEGER DEFAULT 0,
            yesterday_quantity INTEGER DEFAULT 0,
            open_price DOUBLE NOT NULL,
            market_value DOUBLE NOT NULL,
            current_price DOUBLE,
            unrealized_pnl DOUBLE,
            unrealized_pnl_ratio DOUBLE,
            remark VARCHAR,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # 用户账户信息表
        user_account_info_sql = """
        CREATE TABLE IF NOT EXISTS user_account_info (
            user_id VARCHAR NOT NULL,
            info_date DATE NOT NULL,
            total_assets DOUBLE NOT NULL,
            position_market_value DOUBLE NOT NULL,
            available_cash DOUBLE NOT NULL,
            frozen_cash DOUBLE DEFAULT 0,
            total_profit_loss DOUBLE,
            total_profit_loss_ratio DOUBLE,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, info_date)
        )
        """
        
        # 执行创建表的SQL
        tables = {
            'income_statement': income_statement_sql,
            'cashflow_statement': cashflow_statement_sql,
            'balance_sheet': balance_sheet_sql,
            'valuation_data': valuation_data_sql,
            'indicator_data': indicator_data_sql,
            'mtss_data': mtss_data_sql,
            'daily_basic': daily_basic_sql,
            'price_data': price_data_sql,
            'stock_list': stock_list_sql,
            'user_transactions': user_transactions_sql,
            'user_positions': user_positions_sql,
            'user_account_info': user_account_info_sql
        }
        
        for table_name, sql in tables.items():
            try:
                with self._lock:
                    self.conn.execute(sql)
                self.logger.info(f"表 {table_name} 创建成功")
            except Exception as e:
                self.logger.error(f"创建表 {table_name} 失败: {e}")
                raise
        
        # 创建user_transactions表的索引
        try:
            with self._lock:
                # 用户ID索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_transactions_user_id ON user_transactions(user_id)")
                # 股票代码索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_transactions_stock_code ON user_transactions(stock_code)")
                # 交易日期索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_transactions_trade_date ON user_transactions(trade_date)")
                # 策略ID索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_transactions_strategy_id ON user_transactions(strategy_id)")
                # 复合索引：用户ID + 股票代码 + 交易日期
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_transactions_composite ON user_transactions(user_id, stock_code, trade_date)")
            self.logger.info("user_transactions表索引创建成功")
        except Exception as e:
            self.logger.error(f"创建user_transactions表索引失败: {e}")
            raise
        
        # 创建user_positions表的索引
        try:
            with self._lock:
                # 用户ID索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_positions_user_id ON user_positions(user_id)")
                # 股票代码索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_positions_stock_code ON user_positions(stock_code)")
                # 持仓日期索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_positions_date ON user_positions(position_date)")
                # 复合索引：用户ID + 持仓日期
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_positions_user_date ON user_positions(user_id, position_date)")
                # 复合索引：用户ID + 股票代码 + 持仓日期（唯一约束）
                self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_positions_unique ON user_positions(user_id, stock_code, position_date)")
            self.logger.info("user_positions表索引创建成功")
        except Exception as e:
            self.logger.error(f"创建user_positions表索引失败: {e}")
            raise
        
        # 创建user_account_info表的索引
        try:
            with self._lock:
                # 用户ID索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_account_info_user_id ON user_account_info(user_id)")
                # 信息日期索引
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_account_info_date ON user_account_info(info_date)")
                # 复合索引：用户ID + 信息日期（唯一约束）
                self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_account_info_unique ON user_account_info(user_id, info_date)")
            self.logger.info("user_account_info表索引创建成功")
        except Exception as e:
            self.logger.error(f"创建user_account_info表索引失败: {e}")
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
        """插入DataFrame数据（显式列对齐，忽略多余列，补齐缺失列）"""
        if df.empty:
            return True

        # 表存在性与结构
        table_exists = self.table_exists(table_name)
        if not table_exists:
            self.logger.error(f"目标表不存在: {table_name}")

        # 统一列名类型为字符串，消除重复列（保留首个）
        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        if df.columns.duplicated().any():
            dup_cols = df.columns[df.columns.duplicated()].unique().tolist()
            self.logger.warning(f"[{table_name}] DataFrame重复的列(保留第一个): {dup_cols}")
            df = df.loc[:, ~df.columns.duplicated()]

        df_cols = df.columns.tolist()
        df_shape = df.shape
        df_dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}

        table_info = self.get_table_info(table_name) if table_exists else {'columns': []}
        table_cols = [col['name'] for col in table_info.get('columns', [])]
        table_types = {col['name']: col['type'] for col in table_info.get('columns', [])}

        # 诊断列差异
        missing_in_df = [c for c in table_cols if c not in df_cols]      # 表需要但DF缺失
        extra_in_df = [c for c in df_cols if c not in table_cols]        # DF多余

        if not table_exists:
            self.logger.warning(f"[{table_name}] 目标表不存在，后续插入必然失败")
        #self.logger.info(f"[{table_name}] DataFrame形状: {df_shape}, 列数量: {len(df_cols)}")
        #self.logger.info(f"[{table_name}] 表列数量: {len(table_cols)}")
        if len(df_cols) != len(table_cols):
            self.logger.warning(f"[{table_name}] 列数量不匹配: DataFrame={len(df_cols)} 列, 表={len(table_cols)} 列")
        if missing_in_df:
            self.logger.warning(f"[{table_name}] DataFrame缺失但表中存在的列: {missing_in_df}")
        if extra_in_df:
            self.logger.warning(f"[{table_name}] DataFrame多余的列(将被忽略): {extra_in_df}")

        #self.logger.debug(f"[{table_name}] DataFrame列: {df_cols}")
        #self.logger.debug(f"[{table_name}] 表列: {table_cols}")
        #self.logger.debug(f"[{table_name}] DataFrame dtypes: {df_dtypes}")
        #self.logger.debug(f"[{table_name}] 表列类型: {table_types}")

        # 生成与表结构对齐的DataFrame
        for col in missing_in_df:
            df[col] = pd.NA
        aligned_cols = table_cols if table_cols else df.columns.tolist()
        aligned_df = df.reindex(columns=aligned_cols)

        # 数据类型修复：日期字段转为date，code转str
        date_fields = [c for c in ['day', 'pubDate', 'statDate'] if c in aligned_df.columns]
        for c in date_fields:
            try:
                aligned_df[c] = pd.to_datetime(aligned_df[c], errors='coerce').dt.date
            except Exception:
                pass
        if 'code' in aligned_df.columns:
            aligned_df['code'] = aligned_df['code'].astype(str)

        # 预览一行样本
        if not aligned_df.empty:
            preview_cols = aligned_df.columns[:min(10, len(aligned_df.columns))]
            sample_row_preview = {k: aligned_df.iloc[0].get(k) for k in preview_cols}
            self.logger.debug(f"[{table_name}] 对齐后DataFrame示例行(前{len(preview_cols)}列): {sample_row_preview}")

        # 显式注册并按表列顺序插入
        with self._lock:
            self.conn.register('tmp_df', aligned_df)
            try:
                if table_cols:
                    cols_escaped = ', '.join(table_cols)
                    self.conn.execute(f"INSERT OR REPLACE INTO {table_name} ({cols_escaped}) SELECT {cols_escaped} FROM tmp_df")
                else:
                    # 回退：当无法获取表结构时，仍尝试按当前列顺序插入
                    self.conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM tmp_df")
            except Exception as e:
                self.logger.error(f"[{table_name}] 执行插入失败: {e}")
                self.logger.error(f"[{table_name}] 再次确认: 对齐后DataFrame列数={len(aligned_df.columns)}, 表列数={len(table_cols) if table_cols else '未知(获取失败)'}")
                if extra_in_df:
                    self.logger.error(f"[{table_name}] 失败时检测到的DataFrame多余列: {extra_in_df}")
                if missing_in_df:
                    self.logger.error(f"[{table_name}] 失败时检测到的DataFrame缺失列: {missing_in_df}")
                raise
            finally:
                try:
                    self.conn.unregister('tmp_df')
                except Exception:
                    pass
        self.logger.info(f"成功插入 {len(aligned_df)} 条记录到表 {table_name}")
        self._dirty = True
        return True

    
    def query_data(self, sql: str, params: Optional[Any] = None) -> pd.DataFrame:
        """执行SQL查询"""
        try:
            # 检查是否为写入操作（DELETE、UPDATE、INSERT、CREATE、DROP、ALTER等）
            sql_upper = sql.strip().upper()
            is_write_operation = any(sql_upper.startswith(op) for op in 
                                   ['DELETE', 'UPDATE', 'INSERT', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE'])
            
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
            
            # 如果是写入操作，标记为脏数据
            if is_write_operation:
                self._dirty = True
                #self.logger.debug(f"检测到写入操作，已标记数据库为脏状态: {sql[:50]}...")
            
            return result
        except Exception as e:
            self.logger.error(f"查询数据失败: {e}， sql:{sql}")
            return pd.DataFrame()
    
    def get_latest_date(self, table_name: str, code: Optional[str] = None) -> Optional[date]:
        """获取最新数据日期"""
        try:
            # 根据表类型选择日期字段
            if table_name == 'indicator_data':
                date_field = 'pubDate'  # indicator_data表使用pubDate作为主要日期字段
            elif table_name in ['valuation_data', 'mtss_data', 'daily_basic', 'price_data']:
                date_field = 'day'
            else:
                date_field = 'stat_date'

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

    def get_latest_dates_batch(self, table_name: str, codes: List[str]) -> Dict[str, Optional[date]]:
        """批量获取多只股票的最新数据日期"""
        try:
            if not codes:
                return {}

            # 根据表类型选择日期字段
            if table_name == 'indicator_data':
                date_field = 'pubDate'  # indicator_data表使用pubDate作为主要日期字段
            elif table_name in ['valuation_data', 'mtss_data', 'daily_basic', 'price_data']:
                date_field = 'day'
            else:
                date_field = 'stat_date'

            # 构建 IN 子句的占位符
            placeholders = ','.join(['?' for _ in codes])
            sql = f"""
                SELECT code, MAX({date_field}) as max_date
                FROM {table_name}
                WHERE code IN ({placeholders})
                GROUP BY code
            """

            with self._lock:
                result = self.conn.execute(sql, codes).fetchall()

            # 构建结果字典，确保所有股票都有结果
            result_dict = {}

            # 首先初始化所有股票为None
            for code in codes:
                result_dict[code] = None

            # 然后填入查询到的日期
            for row in result:
                code, max_date = row
                if max_date:
                    result_dict[code] = max_date if isinstance(max_date, date) else date.fromisoformat(str(max_date))

            self.logger.debug(f"批量查询 {table_name} 最新日期: {len(codes)} 只股票, {len([d for d in result_dict.values() if d])} 只有数据")
            return result_dict

        except Exception as e:
            self.logger.error(f"批量获取最新日期失败: {e}")
            # 返回所有股票都为None的字典
            return {code: None for code in codes}
    
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
        # 使用DuckDB的SHOW TABLES语法
        with self._lock:
            result = self.conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in result]
        return table_name in table_names
    
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
    
    def get_user_transactions(self, user_id: str = None, stock_code: str = None, 
                             trade_date: date = None, strategy_id: str = None,
                             start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """查询用户交易记录"""
        conditions = []
        params = []
        
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if stock_code:
            conditions.append("stock_code = ?")
            params.append(stock_code)
        if trade_date:
            conditions.append("trade_date = ?")
            params.append(trade_date)
        if strategy_id:
            conditions.append("strategy_id = ?")
            params.append(strategy_id)
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        sql = f"SELECT * FROM user_transactions{where_clause} ORDER BY trade_time DESC"
        
        return self.query_data(sql, params)
    
    def insert_user_transactions(self, transactions: List['UserTransaction']) -> bool:
        """批量插入用户交易记录，检查重复并记录错误日志"""
        if not transactions:
            return True
        
        try:
            # 检查重复记录
            duplicates = []
            valid_transactions = []
            
            for transaction in transactions:
                trade_id = transaction.trade_id
                
                # 检查数据库中是否已存在该trade_id（交易唯一ID）
                check_sql = "SELECT COUNT(*) as count FROM user_transactions WHERE trade_id = ?"
                result = self.query_data(check_sql, [trade_id])
                
                if not result.empty and result.iloc[0]['count'] > 0:
                    # 记录重复的成交
                    duplicates.append({
                        'trade_id': trade_id,
                        'order_id': transaction.order_id,
                        'user_id': transaction.user_id,
                        'stock_code': transaction.stock_code,
                        'trade_date': transaction.trade_date,
                        'trade_time': transaction.trade_time
                    })
                    self.logger.error(f"检测到重复成交，跳过插入: trade_id={trade_id}, order_id={transaction.order_id}, user_id={transaction.user_id}, stock_code={transaction.stock_code}, trade_date={transaction.trade_date}")
                else:
                    valid_transactions.append(transaction)
            
            # 插入有效的交易记录
            if valid_transactions:
                success = self.insert_batch(valid_transactions)
                if success:
                    self.logger.info(f"成功插入{len(valid_transactions)}条交易记录，跳过{len(duplicates)}条重复记录")
                return success
            else:
                self.logger.warning("所有交易记录都是重复的，未插入任何数据")
                return True  # 虽然没有插入，但不算失败
                
        except Exception as e:
            self.logger.error(f"插入用户交易记录时发生错误: {e}")
            return False
    
    def delete_user_transactions(self, user_id: str, trade_date: date = None) -> bool:
        """删除用户交易记录"""
        conditions = {'user_id': user_id}
        if trade_date:
            conditions['trade_date'] = trade_date
        return self.delete_data('user_transactions', conditions)
    
    def get_user_positions_summary(self, user_id: str, end_date: date = None) -> pd.DataFrame:
        """获取用户持仓汇总"""
        date_condition = f" AND trade_date <= '{end_date}'" if end_date else ""
        sql = f"""
        SELECT 
            stock_code,
            SUM(CASE WHEN trade_type = 23 THEN quantity ELSE -quantity END) as total_quantity,
            SUM(CASE WHEN trade_type = 23 THEN net_amount ELSE -net_amount END) as total_amount,
            COUNT(*) as transaction_count
        FROM user_transactions 
        WHERE user_id = ?{date_condition}
        GROUP BY stock_code
        HAVING total_quantity > 0
        ORDER BY stock_code
        """
        return self.query_data(sql, [user_id])