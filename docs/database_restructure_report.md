# 数据库重构报告

## 概述

本次数据库重构主要完成了两个核心任务：
1. **重新设计 indicator_data 表**：将技术指标和财务指标分离到不同的表中
2. **补充核心财务字段**：为 balance_sheet 和 income_statement 表添加缺失的重要财务字段

重构时间：2025-08-11 16:57:02  
备份文件：`data/stock_data_backup_20250811_165702.duckdb`

## 重构详情

### 1. indicator_data 表重构

#### 原表结构问题
- 原 `indicator_data` 表混合了技术指标和财务指标
- 字段定义不清晰，难以维护和扩展
- 与 README.md 文档描述不符

#### 重构方案
将原 `indicator_data` 表分离为两个专门的表：

##### 1.1 新建 financial_indicators 表（财务指标表）
- **字段数量**：34个
- **主键**：(code, day)
- **主要指标分类**：
  - **盈利能力指标**：eps（每股收益）、roe（净资产收益率）、roa（总资产收益率）、各种利润率等
  - **成长能力指标**：营业收入同比增长率、净利润同比增长率等
  - **偿债能力指标**：资产负债率、流动比率、速动比率等
  - **营运能力指标**：各种周转率指标
  - **现金流指标**：每股经营现金流、每股现金流量净额等
  - **估值指标**：净资产与市价比率、盈利收益率等
  - **杜邦分析**：杜邦分析相关指标

##### 1.2 新建 technical_indicators 表（技术指标表）
- **字段数量**：20个
- **主键**：(code, day)
- **主要指标分类**：
  - **价格数据**：开盘价、收盘价、最高价、最低价
  - **成交量数据**：成交量、成交额、量比、换手率
  - **移动平均线**：MA5、MA10、MA20、MA60
  - **技术指标**：ATR、波动率、RSI、MACD系列指标

##### 1.3 数据迁移
- 原 `indicator_data` 表重命名为 `indicator_data_backup`
- 技术指标数据已迁移到 `technical_indicators` 表
- 财务指标数据结构已准备就绪，等待数据填充

### 2. balance_sheet 表字段补充

#### 补充前状态
- 原有字段数量：24个
- 缺失多个核心资产负债表字段

#### 补充后状态
- **当前字段数量**：51个
- **新增字段数量**：27个

#### 新增字段详情

##### 流动资产类
- `money_cap`：货币资金
- `settlement_provi`：结算备付金
- `lend_capital`：拆出资金
- `trading_assets`：交易性金融资产
- `notes_receivable`：应收票据
- `accounts_receivable`：应收账款
- `advance_payment`：预付款项
- `insurance_receivables`：应收保费
- `other_receivable`：其他应收款

##### 非流动资产类
- `available_for_sale_assets`：可供出售金融资产
- `held_to_maturity_investments`：持有至到期投资
- `long_term_equity_invest`：长期股权投资
- `investment_real_estate`：投资性房地产
- `constru_in_process`：在建工程
- `good_will`：商誉
- `deferred_tax_assets`：递延所得税资产

##### 负债类
- `notes_payable`：应付票据
- `salaries_payable`：应付职工薪酬
- `tax_payable`：应交税费
- `bonds_payable`：应付债券

##### 所有者权益类
- `paid_capital`：实收资本(或股本)
- `surplus_reserve_fund`：盈余公积
- `retained_profit`：未分配利润
- `total_owner_equity`：归属于母公司所有者权益合计
- `minority_equity`：少数股东权益
- `total_equity`：所有者权益合计

### 3. income_statement 表字段补充

#### 补充前状态
- 原有字段数量：19个
- 缺失多个核心利润表字段

#### 补充后状态
- **当前字段数量**：42个
- **新增字段数量**：23个

#### 新增字段详情

##### 收入项目
- `interest_income`：利息收入
- `premiums_earned`：已赚保费
- `commission_income`：手续费及佣金收入

##### 成本费用项目
- `interest_expense`：利息支出
- `commission_expense`：手续费及佣金支出
- `refunded_premiums`：退保金
- `net_pay_insurance_claims`：赔付支出净额
- `policy_dividend_payout`：保单红利支出
- `reinsurance_cost`：分保费用
- `operating_tax_surcharges`：营业税金及附加
- `finance_expense`：财务费用
- `asset_impairment_loss`：资产减值损失

##### 投资收益项目
- `fair_value_variable_income`：公允价值变动收益
- `invest_income`：投资收益
- `invest_income_associates`：对联营企业和合营企业的投资收益
- `exchange_income`：汇兑收益

##### 利润项目
- `non_operating_revenue`：营业外收入
- `non_operating_expense`：营业外支出
- `minority_profit`：少数股东损益

##### 综合收益项目
- `other_composite_income`：其他综合收益
- `total_composite_income`：综合收益总额
- `ci_parent_company_owners`：归属于母公司所有者的综合收益总额
- `ci_minority_owners`：归属于少数股东的综合收益总额

## 数据库当前状态

### 表结构概览
| 表名 | 行数 | 状态 | 说明 |
|------|------|------|------|
| balance_sheet | 0 | ✅ 已增强 | 补充了27个核心财务字段 |
| cashflow_statement | 0 | 🔄 待处理 | 未在本次重构范围内 |
| financial_indicators | 0 | ✅ 新建 | 财务指标专用表 |
| fundamental_data | 0 | 🔄 待处理 | 未在本次重构范围内 |
| income_statement | 0 | ✅ 已增强 | 补充了23个核心财务字段 |
| indicator_data_backup | 0 | 📦 备份 | 原indicator_data表备份 |
| mtss_data | 0 | 🔄 待处理 | 未在本次重构范围内 |
| price_data | 217,480 | ✅ 正常 | 价格数据完整 |
| price_data_test | 0 | 📦 测试表 | 测试用途 |
| stock_list | 5,437 | ✅ 正常 | 股票列表完整 |
| stock_list_test | 5,437 | 📦 测试表 | 测试用途 |
| technical_indicators | 0 | ✅ 新建 | 技术指标专用表 |

## 重构效果

### ✅ 已完成的改进
1. **数据结构清晰化**：技术指标和财务指标完全分离，便于维护和查询
2. **字段完整性提升**：
   - balance_sheet 表字段从24个增加到51个
   - income_statement 表字段从19个增加到42个
3. **符合会计准则**：新增字段覆盖了标准财务报表的主要科目
4. **数据安全性**：完整备份确保数据安全

### 📈 预期收益
1. **查询性能优化**：分离后的表结构更适合不同类型的分析需求
2. **数据维护便利**：清晰的表结构便于数据更新和维护
3. **扩展性增强**：为未来添加新的指标类型提供了良好的基础
4. **兼容性提升**：更好地符合README.md文档规范

## 后续建议

### 🔄 待完成任务
1. **数据填充**：为新建的 financial_indicators 表填充历史财务指标数据
2. **cashflow_statement 表优化**：根据需要补充现金流量表的缺失字段
3. **fundamental_data 表整理**：处理基本面数据表的结构差异
4. **数据验证**：建立数据质量检查机制

### 🛠️ 维护建议
1. **定期备份**：建议在重要数据更新前进行备份
2. **索引优化**：为新表创建适当的索引以提升查询性能
3. **文档更新**：更新README.md以反映新的表结构
4. **测试验证**：在生产环境使用前进行充分测试

## 技术细节

### 使用的工具和技术
- **数据库**：DuckDB
- **编程语言**：Python 3
- **备份策略**：完整数据库文件备份
- **迁移方式**：DDL语句 + 数据迁移

### 关键脚本文件
- `restructure_database.py`：主要重构脚本
- `verify_restructure.py`：重构结果验证脚本
- `database_structure_differences_report.md`：原始结构差异分析报告

---

**重构完成时间**：2025-08-11 16:59:45  
**重构状态**：✅ 成功完成  
**数据安全性**：✅ 已备份保护