# 数据质量检查功能Bug修复总结

## 问题描述

在执行 `python main.py check-data --tables price_data,user_transactions --level standard` 时遇到数据库字段名错误：

1. **transaction_id字段不存在**: 实际表中使用的是`trade_id`
2. **volume字段不存在**: 实际表中使用的是`quantity`
3. **value字段不存在**: 实际表中使用的是`amount`

## 错误信息

```
Binder Error: Referenced column "transaction_id" not found in FROM clause!
Candidate bindings: "trade_id", "strategy_id", "created_at", "trade_date", "trade_time"

Binder Error: Referenced column "volume" not found in FROM clause!
Candidate bindings: "amount", "order_id", "stock_code", "commission", "other_fees"
```

## 修复内容

### 1. 主键配置修复 (`services/data_quality_service.py:260`)

```python
# 修复前
'user_transactions': ['transaction_id'],

# 修复后
'user_transactions': ['trade_id'],  # 使用trade_id而不是transaction_id
```

### 2. 关键字段配置修复 (`services/data_quality_service.py:314`)

```python
# 修复前
'user_transactions': ['transaction_id', 'user_id', 'stock_code', 'trade_date'],

# 修复后
'user_transactions': ['trade_id', 'user_id', 'stock_code', 'trade_date'],  # 使用trade_id
```

### 3. 交易记录准确性检查SQL修复 (`services/data_quality_service.py:572-579`)

```python
# 修复前
amount_sql = """
    SELECT transaction_id, volume, price, value,
           ABS(volume * price - value) as diff
    FROM user_transactions
    WHERE volume > 0 AND price > 0 AND value > 0
    AND ABS(volume * price - value) / value > 0.01
    LIMIT 20
"""

# 修复后
amount_sql = """
    SELECT trade_id, quantity, price, amount,
           ABS(quantity * price - amount) as diff
    FROM user_transactions
    WHERE quantity > 0 AND price > 0 AND amount > 0
    AND ABS(quantity * price - amount) / amount > 0.01
    LIMIT 20
"""
```

## 实际表结构对照

### user_transactions表字段

| 数据模型字段 | 数据库字段 | 类型 | 说明 |
|-------------|-----------|------|------|
| trade_id | trade_id | VARCHAR | 交易ID (主键) |
| quantity | quantity | DOUBLE | 交易数量 |
| price | price | DOUBLE | 交易价格 |
| amount | amount | DOUBLE | 交易金额 |

## 修复验证

修复后执行测试命令均正常运行：

```bash
# 单表检查 - 正常
python main.py check-data --tables user_transactions --level quick

# 多表检查 - 正常
python main.py check-data --tables price_data,user_transactions --level standard

# 生成详细报告 - 正常
python main.py check-data --tables price_data,user_transactions --level standard --output-report logs/test_quality_report.json
```

## 输出示例

修复后的正常输出：
```
=== 数据质量检查报告 ===
检查级别: standard
检查时长: 0.1秒
检查表数: 2
发现问题: 3个
  严重问题: 2个
  警告问题: 1个

问题分布:
  price_data: 2个问题 (严重:1, 警告:1)
  user_transactions: 1个问题 (严重:1, 警告:0)

问题类别:
  完整性: 3个
  唯一性: 0个
  准确性: 0个

详细报告已保存到: logs/test_quality_report.json
```

## 根本原因

数据质量检查服务中硬编码的字段名与实际数据模型定义不一致。应该参考实际的数据模型类 (`models/user_transaction.py`) 和数据库表结构来定义检查逻辑。

## 防范措施

建议后续开发时：
1. 在数据模型类中定义字段映射常量
2. 使用动态表结构查询而不是硬编码字段名
3. 增加更完善的测试覆盖，包括实际数据库表结构测试