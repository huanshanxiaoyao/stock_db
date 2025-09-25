# Stock Data Platform API Documentation

## 概述

量化数据平台 REST API 提供全面的股票数据查询、分析和管理功能。API 基于 RESTful 架构，支持 JSON 格式的数据交换。

**Base URL**: `http://localhost:5000`
**API Version**: `v1`
**Content-Type**: `application/json`

## 目录

- [系统接口](#系统接口)
- [股票数据接口](#股票数据接口)
- [用户交易接口](#用户交易接口)
- [数据库接口](#数据库接口)
- [响应格式](#响应格式)
- [错误处理](#错误处理)

---

## 系统接口

### 健康检查

**GET** `/health`

检查 API 服务器状态。

**响应示例:**
```json
{
  "status": "healthy",
  "timestamp": "2025-09-25T12:00:00",
  "version": "1.0.0"
}
```

### API 信息

**GET** `/api/v1/info`

获取 API 基本信息和可用端点列表。

**响应示例:**
```json
{
  "name": "量化数据平台API",
  "version": "1.0.0",
  "description": "提供股票数据查询、分析和管理功能",
  "endpoints": {
    "stocks": "/api/v1/stocks",
    "price": "/api/v1/stocks/{code}/price",
    "financial": "/api/v1/stocks/{code}/financial",
    "batch_prices": "/api/v1/stocks/batch/prices",
    "transactions": "/api/v1/transactions",
    "positions": "/api/v1/positions",
    "accounts": "/api/v1/accounts",
    "database_info": "/api/v1/database/info"
  }
}
```

---

## 股票数据接口

### 获取股票列表

**GET** `/api/v1/stocks`

获取股票基本信息列表，支持市场筛选和分页。

**查询参数:**
| 参数 | 类型 | 必需 | 描述 | 默认值 |
|------|------|------|------|---------|
| `market` | string | 否 | 市场类型 | 全部 |
| `exchange` | string | 否 | 交易所代码 | 全部 |
| `active_only` | boolean | 否 | 仅活跃股票 | true |
| `limit` | integer | 否 | 返回数量限制 | 无限制 |
| `offset` | integer | 否 | 偏移量 | 0 |

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "code": "000001.SZ",
      "name": "平安银行",
      "display_name": "平安银行",
      "exchange": "SZ",
      "market": "深交所"
    }
  ],
  "pagination": {
    "total": 5000,
    "limit": 50,
    "offset": 0,
    "count": 50
  }
}
```

### 获取股票基本信息

**GET** `/api/v1/stocks/{code}`

获取指定股票的详细基本信息。

**路径参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `code` | string | 是 | 股票代码 (如: 000001.SZ) |

**响应示例:**
```json
{
  "success": true,
  "data": {
    "code": "000001.SZ",
    "name": "平安银行",
    "display_name": "平安银行",
    "exchange": "SZ",
    "market": "深交所",
    "industry": "银行",
    "list_date": "1991-04-03"
  }
}
```

### 获取股票价格数据

**GET** `/api/v1/stocks/{code}/price`

获取指定股票的历史价格数据。

**路径参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `code` | string | 是 | 股票代码 |

**查询参数:**
| 参数 | 类型 | 必需 | 描述 | 格式 |
|------|------|------|------|------|
| `start_date` | string | 否 | 开始日期 | YYYY-MM-DD |
| `end_date` | string | 否 | 结束日期 | YYYY-MM-DD |
| `fields` | string | 否 | 字段列表 | 逗号分隔 |

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "trade_date": "2025-09-25",
      "code": "000001.SZ",
      "open": 12.50,
      "high": 12.80,
      "low": 12.30,
      "close": 12.65,
      "volume": 1500000,
      "amount": 18975000
    }
  ],
  "count": 250
}
```

### 获取股票财务数据

**GET** `/api/v1/stocks/{code}/financial`

获取指定股票的财务报表数据。

**路径参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `code` | string | 是 | 股票代码 |

**查询参数:**
| 参数 | 类型 | 必需 | 描述 | 可选值 |
|------|------|------|------|---------|
| `type` | string | 否 | 数据类型 | summary, ratios |
| `periods` | integer | 否 | 期数 | 默认4期 |

**响应示例:**
```json
{
  "success": true,
  "data": {
    "income_statement": [
      {
        "pub_date": "2025-03-31",
        "revenue": 50000000000,
        "net_profit": 8000000000
      }
    ],
    "balance_sheet": [...],
    "cashflow_statement": [...]
  }
}
```

### 批量获取价格数据

**POST** `/api/v1/stocks/batch/prices`

批量获取多只股票的价格数据。

**请求体:**
```json
{
  "codes": ["000001.SZ", "000002.SZ"],
  "start_date": "2025-09-01",
  "end_date": "2025-09-25",
  "fields": ["open", "high", "low", "close", "volume"]
}
```

**响应示例:**
```json
{
  "success": true,
  "data": {
    "000001.SZ": [
      {
        "trade_date": "2025-09-25",
        "open": 12.50,
        "close": 12.65
      }
    ],
    "000002.SZ": [...]
  }
}
```


---

## 用户交易接口

### 获取交易记录

**GET** `/api/v1/transactions`

获取用户交易记录。

**查询参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `user_id` | string | 是 | 用户ID |
| `account_id` | string | 否 | 账户ID |
| `start_date` | string | 否 | 开始日期 |
| `end_date` | string | 否 | 结束日期 |
| `stock_code` | string | 否 | 股票代码 |
| `limit` | integer | 否 | 返回数量 |

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "trade_id": "T20250925001",
      "user_id": "123456789",
      "account_id": "ACC001",
      "stock_code": "000001.SZ",
      "trade_date": "2025-09-25",
      "trade_type": "buy",
      "quantity": 1000,
      "price": 12.50,
      "amount": 12500.00,
      "commission": 7.50
    }
  ],
  "count": 50
}
```

### 获取持仓记录

**GET** `/api/v1/positions`

获取用户持仓记录。

**查询参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `user_id` | string | 是 | 用户ID |
| `account_id` | string | 否 | 账户ID |
| `date` | string | 否 | 查询日期 |
| `stock_code` | string | 否 | 股票代码 |

**响应示例:**
```json
{
  "success": true,
  "data": [
    {
      "user_id": "123456789",
      "account_id": "ACC001",
      "stock_code": "000001.SZ",
      "stock_name": "平安银行",
      "position_date": "2025-09-25",
      "quantity": 2000,
      "avg_cost": 12.30,
      "market_value": 25300.00,
      "unrealized_pnl": 700.00
    }
  ],
  "count": 10
}
```

### 持仓汇总

**GET** `/api/v1/positions/summary`

获取用户持仓汇总信息。

**查询参数:**
| 参数 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `user_id` | string | 是 | 用户ID |
| `account_id` | string | 否 | 账户ID |

**响应示例:**
```json
{
  "success": true,
  "data": {
    "total_market_value": 125000.00,
    "total_cost": 120000.00,
    "total_pnl": 5000.00,
    "total_pnl_ratio": 0.0417,
    "position_count": 8,
    "top_holdings": [
      {
        "stock_code": "000001.SZ",
        "weight": 0.35,
        "pnl_ratio": 0.05
      }
    ]
  }
}
```

---

## 数据库接口

### 数据库信息

**GET** `/api/v1/database/info`

获取数据库基本信息和统计数据。

**响应示例:**
```json
{
  "success": true,
  "data": {
    "database_size": "2.5GB",
    "table_count": 15,
    "stock_count": 5000,
    "last_update": "2025-09-25T08:00:00"
  }
}
```


---

## 响应格式

所有 API 响应都遵循统一的 JSON 格式：

### 成功响应
```json
{
  "success": true,
  "data": {}, // 或 []
  "count": 100,     // 可选：数据数量
  "pagination": {}  // 可选：分页信息
}
```

### 错误响应
```json
{
  "success": false,
  "error": "错误描述",
  "code": "ERROR_CODE"  // 可选：错误代码
}
```

## 错误处理

### HTTP 状态码

| 状态码 | 描述 |
|--------|------|
| 200 | 请求成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

### 常见错误示例

**参数错误 (400):**
```json
{
  "success": false,
  "error": "参数错误: start_date 格式不正确"
}
```

**资源不存在 (404):**
```json
{
  "success": false,
  "error": "股票 INVALID.CODE 不存在"
}
```

**服务器错误 (500):**
```json
{
  "success": false,
  "error": "数据库连接失败"
}
```

---

## 使用示例

### Python 客户端示例

```python
import requests

# 基础配置
BASE_URL = "http://localhost:5000"
headers = {"Content-Type": "application/json"}

# 获取股票列表
response = requests.get(f"{BASE_URL}/api/v1/stocks?limit=10")
stocks = response.json()

# 获取价格数据
code = "000001.SZ"
params = {
    "start_date": "2025-09-01",
    "end_date": "2025-09-25"
}
response = requests.get(f"{BASE_URL}/api/v1/stocks/{code}/price", params=params)
price_data = response.json()

# 批量查询
payload = {
    "codes": ["000001.SZ", "000002.SZ"],
    "start_date": "2025-09-20"
}
response = requests.post(f"{BASE_URL}/api/v1/stocks/batch/prices",
                        json=payload, headers=headers)
batch_data = response.json()

# 获取用户交易记录
params = {
    "user_id": "123456789",
    "limit": 50
}
response = requests.get(f"{BASE_URL}/api/v1/transactions", params=params)
transactions = response.json()
```

### JavaScript 客户端示例

```javascript
const BASE_URL = "http://localhost:5000";

// 获取股票信息
async function getStockInfo(code) {
  try {
    const response = await fetch(`${BASE_URL}/api/v1/stocks/${code}`);
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('获取股票信息失败:', error);
  }
}

// 批量获取价格数据
async function getBatchPrices(codes, startDate) {
  try {
    const response = await fetch(`${BASE_URL}/api/v1/stocks/batch/prices`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        codes: codes,
        start_date: startDate
      })
    });
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('批量查询失败:', error);
  }
}
```

---

## 版本信息

**当前版本**: v1.0.0
**最后更新**: 2025-09-25
**维护状态**: 活跃开发中

## 技术支持

如有问题或建议，请联系开发团队或查看项目文档。