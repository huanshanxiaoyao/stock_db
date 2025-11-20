# 基本面数据表字段对齐总结

## 概述

本次对齐工作基于聚宽API文档中的valuation估值数据表字段规范，对`fundamental_data`表进行了字段标准化，确保与聚宽API接口完全兼容。

**参考文档**: https://www.joinquant.com/help/api/doc?name=JQDatadoc&id=9884

## 字段对齐详情

### 新增字段

以下字段已成功添加到`fundamental_data`表中：

| 字段名 | 类型 | 说明 | 聚宽API对应字段 |
|--------|------|------|----------------|
| pcf_ratio2 | DOUBLE | 市现率(PCF,经营活动现金流TTM) | pcf_ratio2 |
| dividend_ratio | DOUBLE | 股息率(TTM) % | dividend_ratio |
| free_cap | DOUBLE | 自由流通股本(万股) | free_cap |
| free_market_cap | DOUBLE | 自由流通市值(亿元) | free_market_cap |
| a_cap | DOUBLE | A股总股本(万股) | a_cap |
| a_market_cap | DOUBLE | A股总市值(亿元) | a_market_cap |
| turnover_ratio | DOUBLE | 换手率(%) | turnover_ratio |

### 保留字段

以下字段在对齐前后保持不变：

| 字段名 | 类型 | 说明 | 聚宽API对应字段 |
|--------|------|------|----------------|
| code | VARCHAR | 股票代码 | code |
| day | DATE | 数据日期 | day |
| capitalization | DOUBLE | 总股本(万股) | capitalization |
| circulating_cap | DOUBLE | 流通股本(万股) | circulating_cap |
| market_cap | DOUBLE | 总市值(亿元) | market_cap |
| circulating_market_cap | DOUBLE | 流通市值(亿元) | circulating_market_cap |
| pe_ratio | DOUBLE | 市盈率(PE, TTM) | pe_ratio |
| pe_ratio_lyr | DOUBLE | 市盈率(PE) | pe_ratio_lyr |
| pb_ratio | DOUBLE | 市净率(PB) | pb_ratio |
| ps_ratio | DOUBLE | 市销率(PS, TTM) | ps_ratio |
| pcf_ratio | DOUBLE | 市现率(PCF, 现金净流量TTM) | pcf_ratio |

### 移除的字段

以下字段不属于聚宽valuation表，已从表结构定义中移除（但数据库中的现有数据保留）：

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| roe | DOUBLE | 净资产收益率 | 属于财务指标，不属于估值数据 |
| roa | DOUBLE | 总资产收益率 | 属于财务指标，不属于估值数据 |
| roic | DOUBLE | 投入资本回报率 | 属于财务指标，不属于估值数据 |
| gross_profit_margin | DOUBLE | 毛利率 | 属于财务指标，不属于估值数据 |
| net_profit_margin | DOUBLE | 净利率 | 属于财务指标，不属于估值数据 |
| operating_profit_margin | DOUBLE | 营业利润率 | 属于财务指标，不属于估值数据 |
| revenue_growth_rate | DOUBLE | 营收增长率 | 属于财务指标，不属于估值数据 |
| net_profit_growth_rate | DOUBLE | 净利润增长率 | 属于财务指标，不属于估值数据 |
| debt_to_equity_ratio | DOUBLE | 资产负债率 | 属于财务指标，不属于估值数据 |
| current_ratio | DOUBLE | 流动比率 | 属于财务指标，不属于估值数据 |
| quick_ratio | DOUBLE | 速动比率 | 属于财务指标，不属于估值数据 |

## 聚宽API valuation表完整字段对照

| 聚宽字段名 | 数据库字段名 | 类型 | 说明 | 状态 |
|------------|--------------|------|------|------|
| code | code | VARCHAR | 股票代码 | ✅ 已对齐 |
| day | day | DATE | 日期 | ✅ 已对齐 |
| capitalization | capitalization | DOUBLE | 总股本(万股) | ✅ 已对齐 |
| circulating_cap | circulating_cap | DOUBLE | 流通股本(万股) | ✅ 已对齐 |
| market_cap | market_cap | DOUBLE | 总市值(亿元) | ✅ 已对齐 |
| circulating_market_cap | circulating_market_cap | DOUBLE | 流通市值(亿元) | ✅ 已对齐 |
| turnover_ratio | turnover_ratio | DOUBLE | 换手率(%) | ✅ 已对齐 |
| pe_ratio | pe_ratio | DOUBLE | 市盈率(PE, TTM) | ✅ 已对齐 |
| pe_ratio_lyr | pe_ratio_lyr | DOUBLE | 市盈率(PE) | ✅ 已对齐 |
| pb_ratio | pb_ratio | DOUBLE | 市净率(PB) | ✅ 已对齐 |
| ps_ratio | ps_ratio | DOUBLE | 市销率(PS, TTM) | ✅ 已对齐 |
| pcf_ratio | pcf_ratio | DOUBLE | 市现率(PCF, 现金净流量TTM) | ✅ 已对齐 |
| pcf_ratio2 | pcf_ratio2 | DOUBLE | 市现率(PCF,经营活动现金流TTM) | ✅ 已对齐 |
| dividend_ratio | dividend_ratio | DOUBLE | 股息率(TTM) % | ✅ 已对齐 |
| free_cap | free_cap | DOUBLE | 自由流通股本(万股) | ✅ 已对齐 |
| free_market_cap | free_market_cap | DOUBLE | 自由流通市值(亿元) | ✅ 已对齐 |
| a_cap | a_cap | DOUBLE | A股总股本(万股) | ✅ 已对齐 |
| a_market_cap | a_market_cap | DOUBLE | A股总市值(亿元) | ✅ 已对齐 |

## 数据库变更记录

### 执行的操作

1. **数据备份**: 原表数据已备份为 `fundamental_data_backup_20250811_101018`
2. **字段添加**: 成功添加7个新字段
3. **表结构更新**: 更新了以下文件中的表结构定义：
   - `duckdb_impl.py`
   - `README.md`

### 影响的数据库文件

- **主要数据库**: `data/stock_data_new.duckdb`
- **备份表**: `fundamental_data_backup_20250811_101018`

## 使用建议

### 数据获取

现在可以使用聚宽API的完整valuation字段集合：

```python
# 获取估值数据
from jqdata import *
df = get_fundamentals(query(valuation), date='2023-12-29')

# 所有字段都可以直接存储到fundamental_data表中
```

### 数据查询

```sql
-- 查询完整的估值数据
SELECT * FROM fundamental_data 
WHERE code = '000001.XSHE' 
AND day >= '2023-01-01'
ORDER BY day DESC;

-- 查询特定估值指标
SELECT code, day, pe_ratio, pb_ratio, ps_ratio, 
       market_cap, circulating_market_cap, dividend_ratio
FROM fundamental_data 
WHERE day = '2023-12-29'
ORDER BY market_cap DESC;
```

## 验证结果

✅ **字段对齐验证通过**
- 所有聚宽API valuation表字段已成功添加
- 表结构定义已在相关文件中更新
- 数据备份已完成
- 文档已同步更新

## 注意事项

1. **数据兼容性**: 新增字段初始值为NULL，需要重新获取数据填充
2. **代码更新**: 使用fundamental_data表的代码可能需要适配新字段
3. **数据源**: 确保数据获取代码使用聚宽API的valuation表获取完整字段
4. **备份恢复**: 如需回滚，可使用备份表 `fundamental_data_backup_20250811_101018`

## 后续工作建议

1. **数据填充**: 运行数据获取程序，填充新增字段的历史数据
2. **代码适配**: 更新相关的数据处理和分析代码
3. **测试验证**: 验证数据获取和存储功能正常工作
4. **文档完善**: 更新API文档和使用示例

---

**对齐完成时间**: 2025-08-11 10:10:18  
**执行工具**: migrate_fundamental_data.py  
**数据库版本**: stock_data_new.duckdb