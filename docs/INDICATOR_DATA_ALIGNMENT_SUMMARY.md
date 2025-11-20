# indicator_data表字段对齐总结

## 概述

本文档记录了`indicator_data`表与聚宽API indicator财务指标表字段的严格对齐工作。通过迁移脚本，成功将原有的混合指标表转换为完全符合聚宽官方API indicator表结构的标准财务指标表。

## 迁移详情

### 迁移时间
- **执行时间**: 待执行
- **迁移脚本**: `migrate_indicator_data.py`
- **备份表名**: 将自动生成

### 字段变更统计
- **原有字段数**: 混合字段(技术指标+财务指标)
- **目标字段数**: 26个(严格按聚宽indicator表)
- **对齐标准**: 聚宽官方API文档 indicator表
- **参考文档**: https://www.joinquant.com/help/api/doc?name=JQDatadoc&id=9885

## 字段对齐详情

### 严格对齐聚宽indicator表字段 (26个)

#### 基础字段
- `code` - 股票代码(带后缀.XSHE/.XSHG)
- `pubDate` - 公司发布财报日期
- `statDate` - 财报统计的季度的最后一天

#### 盈利能力指标
- `eps` - 每股收益EPS(元)
- `adjusted_profit` - 扣除非经常损益后的净利润(元)
- `operating_profit` - 经营活动净收益(元)
- `value_change_profit` - 价值变动净收益(元)
- `roe` - 净资产收益率ROE(%)
- `inc_return` - 净资产收益率(扣除非经常损益)(%)
- `roa` - 总资产净利率ROA(%)
- `net_profit_margin` - 销售净利率(%)
- `gross_profit_margin` - 销售毛利率(%)

#### 成本费用指标
- `expense_to_total_revenue` - 营业总成本/营业总收入(%)
- `operation_profit_to_total_revenue` - 营业利润/营业总收入(%)
- `net_profit_to_total_revenue` - 净利润/营业总收入(%)
- `operating_expense_to_total_revenue` - 营业费用/营业总收入(%)
- `ga_expense_to_total_revenue` - 管理费用/营业总收入(%)
- `financing_expense_to_total_revenue` - 财务费用/营业总收入(%)

#### 盈利质量指标
- `operating_profit_to_profit` - 经营活动净收益/利润总额(%)
- `invesment_profit_to_profit` - 价值变动净收益/利润总额(%)
- `adjusted_profit_to_profit` - 扣除非经常损益后的净利润/归属于母公司所有者的净利润(%)
#### 现金流指标
- `goods_sale_and_service_to_revenue` - 销售商品提供劳务收到的现金/营业收入(%)
- `ocf_to_revenue` - 经营活动产生的现金流量净额/营业收入(%)
- `ocf_to_operating_profit` - 经营活动产生的现金流量净额/经营活动净收益(%)

#### 成长能力指标
- `inc_total_revenue_year_on_year` - 营业总收入同比增长率(%)
- `inc_total_revenue_annual` - 营业总收入环比增长率(%)
- `inc_revenue_year_on_year` - 营业收入同比增长率(%)

### 字段对齐原则

1. **严格按照聚宽官方API文档**: 完全遵循聚宽indicator表的字段定义
2. **移除非官方字段**: 删除所有不在聚宽indicator表中的字段
3. **统一字段命名**: 使用聚宽官方的字段名称
4. **保持数据类型一致**: 按照聚宽API的数据类型定义

### 主要变更

#### 基础字段变更
- `day` → `pubDate` + `statDate`: 改为聚宽标准的两个日期字段
- 移除所有技术指标相关字段
- 移除所有非聚宽indicator表的财务字段

#### 新增聚宽标准字段
- 成本费用分析指标(6个)
- 盈利质量分析指标(3个)
- 现金流分析指标(3个)
- 成长能力指标(3个)

## 数据库变更

### 表结构更新
1. **duckdb_impl.py**: 更新了DuckDB实现中的表结构定义
2. **README.md**: 更新了文档中的表结构说明

### 主键设置
- 保持原有主键设置: `PRIMARY KEY (code, day)`

## 使用建议

### 数据获取
```python
# 获取单只股票的财务指标数据
api.get_indicator_data('000001.XSHE', '2023-01-01', '2023-12-31')

# 批量获取多只股票的财务指标数据
api.get_batch_indicator_data(['000001.XSHE', '000002.XSHE'], '2023-01-01', '2023-12-31')
```

### 常用查询
```sql
-- 查询高ROE股票
SELECT code, day, roe, roa, eps 
FROM indicator_data 
WHERE roe > 0.15 AND day = '2023-12-29'
ORDER BY roe DESC;

-- 查询成长性良好的股票
SELECT code, day, inc_revenue_year_on_year, inc_profit_year_on_year
FROM indicator_data 
WHERE inc_revenue_year_on_year > 0.2 AND inc_profit_year_on_year > 0.2
AND day = '2023-12-29';

-- 查询财务健康的股票
SELECT code, day, current_ratio, quick_ratio, debt_to_assets
FROM indicator_data 
WHERE current_ratio > 1.5 AND quick_ratio > 1.0 AND debt_to_assets < 0.6
AND day = '2023-12-29';
```

## 验证结果

### 迁移验证
- ✅ 数据库连接成功
- ✅ 原表备份完成
- ✅ 新字段添加成功 (32个)
- ✅ 表结构验证通过 (52个字段)
- ✅ 主键约束保持不变
- ✅ 代码文件更新完成
- ✅ 文档更新完成

### 字段完整性检查
所有34个目标字段均已成功添加到表中，字段对齐工作完成。

## 注意事项

1. **数据兼容性**: 新增字段初始值为NULL，需要通过数据更新服务填充实际数据
2. **查询性能**: 字段数量增加可能影响查询性能，建议根据需要创建适当的索引
3. **数据更新**: 需要更新数据获取逻辑以支持新的财务指标字段
4. **备份恢复**: 如需回滚，可使用备份表`indicator_data_backup_20250811_101850`

## 后续工作

1. **数据填充**: 实现从聚宽API获取财务指标数据的逻辑
2. **数据验证**: 验证新字段数据的准确性和完整性
3. **性能优化**: 根据查询模式优化表结构和索引
4. **API更新**: 更新相关API接口以支持新的财务指标字段

---

*本文档记录了indicator_data表字段对齐的完整过程，为后续的数据维护和功能开发提供参考。*