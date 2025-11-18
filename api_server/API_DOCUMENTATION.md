# 股票数据平台 REST API 文档

本文档描述了股票数据平台所有可用的REST API接口，包括对7个核心数据表的支持情况。

## 服务器启动

```bash
# 开发环境启动
python api_server/start.py --host 127.0.0.1 --port 5005

# 直接启动（绕过副本模式）
python api_server/server.py --host 127.0.0.1 --port 5005 --no-replica
```

## 基础端点

### 健康检查
```http
GET /health
```

**响应示例:**
```json
{
  "status": "healthy",
  "timestamp": "2025-09-29T15:23:23.427616",
  "version": "1.0.0"
}
```

### API信息
```http
GET /api/v1/info
```

## 1. stock_list 表 ✅ 完全支持

### 获取股票列表
```http
GET /api/v1/stocks?market={market}&exchange={exchange}&active_only={bool}&limit={int}&offset={int}
```

**参数:**
- `market`: 市场类型（可选）
- `exchange`: 交易所（可选）
- `active_only`: 只显示活跃股票（默认true）
- `limit`: 返回数量限制
- `offset`: 分页偏移

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks?limit=3"
```

**响应示例:**
```json
{
  "success": true,
  "data": [
    {"code": "000001.SZ", "name": "平安银行", "display_name": "000001.SZ 平安银行"},
    {"code": "000002.SZ", "name": "万科A", "display_name": "000002.SZ 万科A"},
    {"code": "000004.SZ", "name": "国华网安", "display_name": "000004.SZ 国华网安"}
  ],
  "pagination": {
    "total": 5152,
    "limit": 3,
    "offset": 0,
    "count": 3
  }
}
```

### 获取单个股票信息
```http
GET /api/v1/stocks/{code}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks/000001.SZ"
```

## 2. price_data 表 ✅ 完全支持

### 获取股票价格数据（支持股票代码+日期范围）
```http
GET /api/v1/stocks/{code}/price?start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&fields={fields}
```

**参数:**
- `start_date`: 开始日期（YYYY-MM-DD格式）
- `end_date`: 结束日期（YYYY-MM-DD格式）
- `fields`: 字段列表（逗号分隔）

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks/000001.SZ/price?start_date=2025-09-20&end_date=2025-09-27"
```

**响应示例:**
```json
{
  "success": true,
  "count": 5,
  "data": [
    {
      "trade_date": "2025-09-26T00:00:00",
      "open": 16.39,
      "high": 16.59,
      "low": 16.22,
      "close": 16.33,
      "volume": 525548.0,
      "money": 856917687.88,
      "factor": 1.43324313
    }
  ]
}
```

### 批量获取价格数据
```http
POST /api/v1/stocks/batch/prices
Content-Type: application/json

{
  "codes": ["000001.SZ", "000002.SZ"],
  "start_date": "2025-09-20",
  "end_date": "2025-09-27",
  "fields": ["open", "high", "low", "close", "volume"]
}
```

**请求示例:**
```bash
curl -X POST "http://127.0.0.1:5005/api/v1/stocks/batch/prices" \
  -H "Content-Type: application/json" \
  -d '{
    "codes": ["000001.SZ", "000002.SZ"],
    "start_date": "2025-09-20",
    "end_date": "2025-09-27"
  }'
```

## 3. valuation_data 表 ✅ 完全支持

### 获取估值数据（支持股票代码+日期范围）
```http
GET /api/v1/stocks/{code}/financial?type=valuation&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks/000001.SZ/financial?type=valuation&start_date=2025-09-20&end_date=2025-09-27"
```

**响应示例:**
```json
{
  "success": true,
  "data": {
    "valuation_data": [
      {
        "code": "000001.SZ",
        "day": "2025-09-26T00:00:00",
        "pe_ratio": 5.0858,
        "pb_ratio": 0.5027,
        "market_cap": 2212.2747,
        "turnover_ratio": 0.3985
      }
    ]
  }
}
```

## 4. indicator_data 表 ✅ 完全支持

### 获取指标数据（支持股票代码+日期范围）
```http
GET /api/v1/stocks/{code}/financial?type=indicator&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks/000001.SZ/financial?type=indicator&start_date=2025-06-01&end_date=2025-09-30"
```

**响应示例:**
```json
{
  "success": true,
  "data": {
    "indicator_data": [
      {
        "code": "000001.SZ",
        "statDate": "2025-09-10T00:00:00",
        "pubDate": "2025-09-10T00:00:00",
        "eps": 0.5552,
        "roe": 2.12,
        "roa": 0.18,
        "gross_profit_margin": null,
        "net_profit_margin": 30.2
      }
    ]
  }
}
```

## 5. user_transactions 表 ✅ 完全支持

### 获取用户交易记录（支持user_id+日期范围 或 股票代码+日期范围）
```http
GET /api/v1/transactions?user_id={user_id}&stock_code={code}&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&trade_type={int}&limit={int}&offset={int}
```

**参数:**
- `user_id`: 用户ID
- `stock_code`: 股票代码
- `start_date`: 开始日期
- `end_date`: 结束日期
- `trade_type`: 交易类型（1=买入，2=卖出）
- `limit`: 返回数量限制（默认100）
- `offset`: 分页偏移

**请求示例:**
```bash
# 查询指定用户的交易记录
curl "http://127.0.0.1:5005/api/v1/transactions?user_id=test_user&start_date=2025-09-01&end_date=2025-09-30&limit=5"

# 查询指定股票的所有交易记录
curl "http://127.0.0.1:5005/api/v1/transactions?stock_code=000001.SZ&start_date=2025-09-01&end_date=2025-09-30"

# 查询指定用户指定股票的交易记录
curl "http://127.0.0.1:5005/api/v1/transactions?user_id=test_user&stock_code=000001.SZ&start_date=2025-09-01&end_date=2025-09-30"
```

### 获取用户最近N天交易记录
```http
GET /api/v1/transactions/recent?user_id={user_id}&days={int}&stock_code={code}&trade_type={int}&limit={int}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/transactions/recent?user_id=test_user&days=7&limit=10"
```

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "user_id": "test_user",
      "stock_code": "000001.SZ",
      "trade_date": "2025-09-25",
      "trade_time": "09:30:00",
      "trade_type": 1,
      "price": 16.35,
      "quantity": 1000,
      "amount": 16350.0,
      "commission": 5.0,
      "stamp_tax": 0.0
    }
  ],
  "pagination": {
    "total": 1,
    "limit": 10,
    "offset": 0,
    "count": 1
  }
}
```

## 6. user_positions 表 ✅ 完全支持

### 获取用户持仓记录（支持user_id+日期范围 或 股票代码+日期范围）
```http
GET /api/v1/positions?user_id={user_id}&stock_code={code}&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&limit={int}&offset={int}
```

**请求示例:**
```bash
# 查询指定用户的持仓记录
curl "http://127.0.0.1:5005/api/v1/positions?user_id=test_user&start_date=2025-09-01&end_date=2025-09-30"

# 查询指定股票的所有持仓记录
curl "http://127.0.0.1:5005/api/v1/positions?stock_code=000001.SZ&start_date=2025-09-01&end_date=2025-09-30"

# 查询指定用户指定股票的持仓记录
curl "http://127.0.0.1:5005/api/v1/positions?user_id=test_user&stock_code=000001.SZ&position_date=2025-09-26"
```

### 获取用户持仓汇总
```http
GET /api/v1/positions/summary?user_id={user_id}&position_date={YYYY-MM-DD}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/positions/summary?user_id=test_user"
```

**响应示例:**
```json
{
  "success": true,
  "data": {
    "user_id": "test_user",
    "position_date": "2025-09-26",
    "summary": {
      "total_positions": 5,
      "unique_stocks": 5,
      "total_quantity": 10000,
      "total_market_value": 163500.0,
      "total_unrealized_pnl": 1500.0,
      "avg_pnl_ratio": 0.92
    },
    "top_positions": [
      {
        "stock_code": "000001.SZ",
        "stock_name": "平安银行",
        "current_quantity": 2000,
        "market_value": 32700.0,
        "unrealized_pnl": 300.0,
        "unrealized_pnl_ratio": 0.93
      }
    ]
  }
}
```

## 7. user_account_info 表 ✅ 完全支持

### 获取用户账户信息（支持user_id+日期范围）
```http
GET /api/v1/accounts?user_id={user_id}&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}&limit={int}&offset={int}
```

**请求示例:**
```bash
# 查询指定用户的账户信息
curl "http://127.0.0.1:5005/api/v1/accounts?user_id=test_user&start_date=2025-09-01&end_date=2025-09-30"

# 查询指定日期的账户信息
curl "http://127.0.0.1:5005/api/v1/accounts?user_id=test_user&info_date=2025-09-26"
```

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "user_id": "test_user",
      "info_date": "2025-09-26",
      "total_asset": 200000.0,
      "available_cash": 36500.0,
      "market_value": 163500.0,
      "frozen_cash": 0.0,
      "total_profit_loss": 1500.0,
      "profit_loss_ratio": 0.75
    }
  ],
  "pagination": {
    "total": 1,
    "limit": null,
    "offset": 0,
    "count": 1
  }
}
```

## 综合财务数据查询

### 获取所有财务数据
```http
GET /api/v1/stocks/{code}/financial?type=summary&start_date={YYYY-MM-DD}&end_date={YYYY-MM-DD}
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/stocks/000001.SZ/financial?type=summary"
```

**响应包含:**
- `valuation_data`: 估值数据
- `indicator_data`: 财务指标数据
- `income_statement`: 利润表数据
- `balance_sheet`: 资产负债表数据
- `cashflow_statement`: 现金流量表数据

## 数据库信息

### 获取数据库统计信息
```http
GET /api/v1/database/info
```

**请求示例:**
```bash
curl "http://127.0.0.1:5005/api/v1/database/info"
```

## 支持状况总结

| 表名 | user_id+日期范围 | 股票代码+日期范围 | 状态 |
|------|------------------|-------------------|------|
| stock_list | N/A | ⚠️ 部分支持 | 缺少日期范围筛选 |
| price_data | N/A | ✅ 完全支持 | 完整实现 |
| valuation_data | N/A | ✅ 完全支持 | 完整实现 |
| indicator_data | N/A | ✅ 完全支持 | 完整实现 |
| user_transactions | ✅ 完全支持 | ✅ 完全支持 | 完整实现 |
| user_positions | ✅ 完全支持 | ✅ 完全支持 | 完整实现 |
| user_account_info | ✅ 完全支持 | N/A | 完整实现 |

## 注意事项

1. **日期格式**: 所有日期参数使用 `YYYY-MM-DD` 格式
2. **分页**: 大部分接口支持 `limit` 和 `offset` 参数进行分页
3. **错误处理**: 所有接口返回统一的错误格式：
   ```json
   {
     "success": false,
     "error": "错误描述"
   }
   ```
4. **数据类型**: JSON响应中的数值类型已正确转换，避免序列化错误 ✅ **已修复**
5. **性能**: 建议在查询大量数据时使用分页参数
6. **服务器端口**: 使用端口5005启动API服务器（修复版本）

## 缺少的功能

1. **stock_list表的日期范围查询**: 需要添加按上市日期、退市日期的筛选功能
2. **批量财务数据查询**: 对valuation_data和indicator_data的批量查询接口

这些功能可在后续版本中添加。