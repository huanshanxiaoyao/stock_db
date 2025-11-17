#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库结构重构脚本
1. 重新设计 indicator_data 表，分离技术指标和财务指标
2. 补充 balance_sheet 和 income_statement 表的核心财务字段
"""

import duckdb
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def backup_database(db_path):
    """备份数据库"""
    from datetime import datetime
    backup_path = db_path.parent / f"stock_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    # 使用 DuckDB 的 EXPORT/IMPORT 功能进行备份
    conn = duckdb.connect(str(db_path))
    try:
        # 导出所有数据到临时目录
        temp_dir = db_path.parent / "temp_backup"
        temp_dir.mkdir(exist_ok=True)
        
        # 获取所有表
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        
        # 创建备份数据库
        backup_conn = duckdb.connect(str(backup_path))
        
        for table_name, in tables:
            # 获取表结构
            create_sql = conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
            if create_sql:
                backup_conn.execute(create_sql[0])
            
            # 复制数据
            data = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            if data:
                columns = [desc[0] for desc in conn.description]
                placeholders = ','.join(['?' for _ in columns])
                backup_conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", data)
        
        backup_conn.close()
        logger.info(f"数据库备份完成: {backup_path}")
        
    except Exception as e:
        logger.error(f"备份失败: {e}")
        raise
    finally:
        conn.close()
    
    return backup_path

def restructure_indicator_data(conn):
    """重构 indicator_data 表，分离技术指标和财务指标"""
    logger.info("开始重构 indicator_data 表...")
    
    try:
        # 1. 创建新的财务指标表 (financial_indicators)
        financial_indicators_sql = """
        CREATE TABLE IF NOT EXISTS financial_indicators (
            code VARCHAR NOT NULL,
            day DATE NOT NULL,
            -- 盈利能力指标
            eps DOUBLE,                                    -- 每股收益
            adjusted_profit DOUBLE,                        -- 扣除非经常损益后的净利润
            operating_profit DOUBLE,                       -- 经营活动净收益
            value_change_profit DOUBLE,                    -- 价值变动净收益
            roe DOUBLE,                                    -- 净资产收益率
            roa DOUBLE,                                    -- 总资产收益率
            roic DOUBLE,                                   -- 投入资本回报率
            inc_return DOUBLE,                             -- 净资产收益率(增长率)
            gross_profit_margin DOUBLE,                    -- 毛利率
            net_profit_margin DOUBLE,                      -- 净利率
            operating_profit_margin DOUBLE,                -- 营业利润率
            
            -- 成长能力指标
            inc_revenue_year_on_year DOUBLE,               -- 营业收入同比增长率
            inc_profit_year_on_year DOUBLE,                -- 净利润同比增长率
            inc_net_profit_year_on_year DOUBLE,            -- 净利润同比增长率(年化)
            inc_net_profit_to_shareholders_year_on_year DOUBLE, -- 归母净利润同比增长率
            
            -- 偿债能力指标
            debt_to_assets DOUBLE,                         -- 资产负债率
            debt_to_equity DOUBLE,                         -- 产权比率
            current_ratio DOUBLE,                          -- 流动比率
            quick_ratio DOUBLE,                            -- 速动比率
            
            -- 营运能力指标
            inventory_turnover DOUBLE,                     -- 存货周转率
            receivable_turnover DOUBLE,                    -- 应收账款周转率
            accounts_payable_turnover DOUBLE,              -- 应付账款周转率
            current_assets_turnover DOUBLE,                -- 流动资产周转率
            fixed_assets_turnover DOUBLE,                  -- 固定资产周转率
            total_assets_turnover DOUBLE,                  -- 总资产周转率
            
            -- 现金流指标
            operating_cash_flow_per_share DOUBLE,          -- 每股经营现金流
            cash_flow_per_share DOUBLE,                    -- 每股现金流量净额
            
            -- 估值指标
            book_to_market_ratio DOUBLE,                   -- 净资产与市价比率
            earnings_yield DOUBLE,                         -- 盈利收益率
            capitalization_ratio DOUBLE,                   -- 股本报酬率
            
            -- 杜邦分析
            du_return_on_equity DOUBLE,                    -- 杜邦分析净资产收益率
            du_equity_multiplier DOUBLE,                   -- 杜邦分析权益乘数
            
            PRIMARY KEY (code, day)
        )
        """
        
        # 2. 创建新的技术指标表 (technical_indicators)
        technical_indicators_sql = """
        CREATE TABLE IF NOT EXISTS technical_indicators (
            code VARCHAR NOT NULL,
            day DATE NOT NULL,
            -- 价格数据
            open DOUBLE,                                   -- 开盘价
            close DOUBLE,                                  -- 收盘价
            high DOUBLE,                                   -- 最高价
            low DOUBLE,                                    -- 最低价
            
            -- 成交量数据
            volume DOUBLE,                                 -- 成交量
            money DOUBLE,                                  -- 成交额
            volume_ratio DOUBLE,                           -- 量比
            turnover_rate DOUBLE,                          -- 换手率
            
            -- 移动平均线
            ma5 DOUBLE,                                    -- 5日移动平均线
            ma10 DOUBLE,                                   -- 10日移动平均线
            ma20 DOUBLE,                                   -- 20日移动平均线
            ma60 DOUBLE,                                   -- 60日移动平均线
            
            -- 技术指标
            atr DOUBLE,                                    -- 平均真实波幅
            volatility DOUBLE,                             -- 波动率
            rsi DOUBLE,                                    -- RSI指标
            macd DOUBLE,                                   -- MACD指标
            macd_signal DOUBLE,                            -- MACD信号线
            macd_hist DOUBLE,                              -- MACD柱状图
            
            PRIMARY KEY (code, day)
        )
        """
        
        # 执行创建表语句
        conn.execute(financial_indicators_sql)
        conn.execute(technical_indicators_sql)
        
        # 3. 从原 indicator_data 表迁移数据
        # 检查原表是否存在数据
        count_result = conn.execute("SELECT COUNT(*) FROM indicator_data").fetchone()
        if count_result[0] > 0:
            logger.info(f"原 indicator_data 表有 {count_result[0]} 条记录，开始迁移数据...")
            
            # 迁移技术指标数据
            migrate_technical_sql = """
            INSERT INTO technical_indicators 
            SELECT 
                code, day, open, close, high, low, volume, money, 
                volume_ratio, turnover_rate, ma5, ma10, ma20, ma60,
                atr, volatility, rsi, macd, macd_signal, macd_hist
            FROM indicator_data
            """
            conn.execute(migrate_technical_sql)
            logger.info("技术指标数据迁移完成")
        
        # 4. 重命名原表为备份表
        conn.execute("ALTER TABLE indicator_data RENAME TO indicator_data_backup")
        
        logger.info("indicator_data 表重构完成")
        
    except Exception as e:
        logger.error(f"重构 indicator_data 表失败: {e}")
        raise

def enhance_balance_sheet(conn):
    """补充 balance_sheet 表的核心财务字段"""
    logger.info("开始补充 balance_sheet 表字段...")
    
    try:
        # 获取当前表结构
        current_columns = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'balance_sheet'"
        ).fetchall()
        current_column_names = {col[0] for col in current_columns}
        
        # 需要添加的字段列表
        new_columns = [
            # 流动资产
            ('money_cap', 'DOUBLE', '货币资金'),
            ('settlement_provi', 'DOUBLE', '结算备付金'),
            ('lend_capital', 'DOUBLE', '拆出资金'),
            ('trading_assets', 'DOUBLE', '交易性金融资产'),
            ('notes_receivable', 'DOUBLE', '应收票据'),
            ('accounts_receivable', 'DOUBLE', '应收账款'),
            ('advance_payment', 'DOUBLE', '预付款项'),
            ('insurance_receivables', 'DOUBLE', '应收保费'),
            ('other_receivable', 'DOUBLE', '其他应收款'),
            
            # 非流动资产
            ('available_for_sale_assets', 'DOUBLE', '可供出售金融资产'),
            ('held_to_maturity_investments', 'DOUBLE', '持有至到期投资'),
            ('long_term_equity_invest', 'DOUBLE', '长期股权投资'),
            ('investment_real_estate', 'DOUBLE', '投资性房地产'),
            ('constru_in_process', 'DOUBLE', '在建工程'),
            ('good_will', 'DOUBLE', '商誉'),
            ('deferred_tax_assets', 'DOUBLE', '递延所得税资产'),
            
            # 流动负债
            ('notes_payable', 'DOUBLE', '应付票据'),
            ('salaries_payable', 'DOUBLE', '应付职工薪酬'),
            ('tax_payable', 'DOUBLE', '应交税费'),
            
            # 非流动负债
            ('bonds_payable', 'DOUBLE', '应付债券'),
            
            # 所有者权益
            ('paid_capital', 'DOUBLE', '实收资本(或股本)'),
            ('surplus_reserve_fund', 'DOUBLE', '盈余公积'),
            ('retained_profit', 'DOUBLE', '未分配利润'),
            ('total_owner_equity', 'DOUBLE', '归属于母公司所有者权益合计'),
            ('minority_equity', 'DOUBLE', '少数股东权益'),
            ('total_equity', 'DOUBLE', '所有者权益合计')
        ]
        
        # 添加缺失的字段
        added_count = 0
        for col_name, col_type, col_comment in new_columns:
            if col_name not in current_column_names:
                alter_sql = f"ALTER TABLE balance_sheet ADD COLUMN {col_name} {col_type}"
                conn.execute(alter_sql)
                added_count += 1
                logger.info(f"添加字段: {col_name} ({col_comment})")
        
        logger.info(f"balance_sheet 表补充完成，新增 {added_count} 个字段")
        
    except Exception as e:
        logger.error(f"补充 balance_sheet 表失败: {e}")
        raise

def enhance_income_statement(conn):
    """补充 income_statement 表的核心财务字段"""
    logger.info("开始补充 income_statement 表字段...")
    
    try:
        # 获取当前表结构
        current_columns = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'income_statement'"
        ).fetchall()
        current_column_names = {col[0] for col in current_columns}
        
        # 需要添加的字段列表
        new_columns = [
            # 收入项目
            ('interest_income', 'DOUBLE', '利息收入'),
            ('premiums_earned', 'DOUBLE', '已赚保费'),
            ('commission_income', 'DOUBLE', '手续费及佣金收入'),
            
            # 成本费用项目
            ('interest_expense', 'DOUBLE', '利息支出'),
            ('commission_expense', 'DOUBLE', '手续费及佣金支出'),
            ('refunded_premiums', 'DOUBLE', '退保金'),
            ('net_pay_insurance_claims', 'DOUBLE', '赔付支出净额'),
            ('policy_dividend_payout', 'DOUBLE', '保单红利支出'),
            ('reinsurance_cost', 'DOUBLE', '分保费用'),
            ('operating_tax_surcharges', 'DOUBLE', '营业税金及附加'),
            ('finance_expense', 'DOUBLE', '财务费用'),
            ('asset_impairment_loss', 'DOUBLE', '资产减值损失'),
            
            # 投资收益项目
            ('fair_value_variable_income', 'DOUBLE', '公允价值变动收益'),
            ('invest_income', 'DOUBLE', '投资收益'),
            ('invest_income_associates', 'DOUBLE', '对联营企业和合营企业的投资收益'),
            ('exchange_income', 'DOUBLE', '汇兑收益'),
            
            # 利润项目
            ('non_operating_revenue', 'DOUBLE', '营业外收入'),
            ('non_operating_expense', 'DOUBLE', '营业外支出'),
            ('minority_profit', 'DOUBLE', '少数股东损益'),
            
            # 综合收益项目
            ('other_composite_income', 'DOUBLE', '其他综合收益'),
            ('total_composite_income', 'DOUBLE', '综合收益总额'),
            ('ci_parent_company_owners', 'DOUBLE', '归属于母公司所有者的综合收益总额'),
            ('ci_minority_owners', 'DOUBLE', '归属于少数股东的综合收益总额')
        ]
        
        # 添加缺失的字段
        added_count = 0
        for col_name, col_type, col_comment in new_columns:
            if col_name not in current_column_names:
                alter_sql = f"ALTER TABLE income_statement ADD COLUMN {col_name} {col_type}"
                conn.execute(alter_sql)
                added_count += 1
                logger.info(f"添加字段: {col_name} ({col_comment})")
        
        logger.info(f"income_statement 表补充完成，新增 {added_count} 个字段")
        
    except Exception as e:
        logger.error(f"补充 income_statement 表失败: {e}")
        raise

def main():
    """主函数"""
    # 获取数据库路径
    import sys
    sys.path.append('..')
    from config import get_config
    config = get_config()
    db_path = Path(config.database.path)
    
    if not db_path.exists():
        logger.error(f"数据库文件不存在: {db_path}")
        return
    
    try:
        # 1. 备份数据库
        logger.info("开始备份数据库...")
        backup_path = backup_database(db_path)
        
        # 2. 连接数据库进行重构
        conn = duckdb.connect(str(db_path))
        
        try:
            # 3. 重构 indicator_data 表
            restructure_indicator_data(conn)
            
            # 4. 补充 balance_sheet 表
            enhance_balance_sheet(conn)
            
            # 5. 补充 income_statement 表
            enhance_income_statement(conn)
            
            logger.info("数据库重构完成！")
            logger.info(f"备份文件位置: {backup_path}")
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"数据库重构失败: {e}")
        raise

if __name__ == "__main__":
    main()