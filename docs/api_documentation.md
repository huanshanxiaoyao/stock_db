# Stock Data API 文档

## 概述

Stock Data API 提供了完整的股票数据查询和分析功能，支持价格数据、财务数据、技术指标等多维度数据访问。

## API基础信息

- **基础URL**: `http://localhost:5000/api/v1`
- **数据格式**: JSON
- **字符编码**: UTF-8
- **日期格式**: YYYY-MM-DD

## 核心接口

### 1. 价格数据接口

#### 1.1 获取单个股票价格数据

**接口地址**: `GET /stocks/{code}/price`

**请求参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| code | string | 是 | 股票代码，如"000001.SZ" |
| start_date | string | 否 | 开始日期，格式：YYYY-MM-DD |
| end_date | string | 否 | 结束日期，格式：YYYY-MM-DD |

**响应示例**:
```json
{
  "success": true,
  "data": [
    {
      "trade_date": "2025-09-19",
      "open": 1669.73,
      "high": 1671.16,
      "low": 1631.03,
      "close": 1635.33,
      "volume": 967150.0
    }
  ],
  "count": 1
}
```

#### 1.2 批量获取价格数据

**接口地址**: `POST /stocks/batch/prices`

**请求参数**:
```json
{
  "codes": ["000001.SZ", "000002.SZ"],
  "start_date": "2025-08-01",
  "end_date": "2025-09-19",
  "fields": ["open", "close", "volume"]
}
```

**响应示例**:
```json
{
  "success": true,
  "data": [
    {
      "code": "000001.SZ",
      "date": "2025-09-19",
      "open": 1669.73,
      "close": 1635.33,
      "volume": 967150.0
    }
  ],
  "count": 2
}
```

### 2. 股票信息接口

#### 2.1 获取股票列表

**接口地址**: `GET /stocks`

**请求参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| market | string | 否 | 市场类型: main/gem/star/bse |
| exchange | string | 否 | 交易所: XSHG/XSHE/BSE |
| active_only | boolean | 否 | 是否只返回活跃股票，默认true |
| limit | integer | 否 | 返回数量限制 |
| offset | integer | 否 | 偏移量，用于分页 |

**响应示例**:
```json
{
  "success": true,
  "data": [
    {
      "code": "000001.SZ",
      "name": "平安银行",
      "exchange": "XSHE",
      "market": "main"
    }
  ],
  "pagination": {
    "total": 5000,
    "limit": 10,
    "offset": 0,
    "count": 10
  }
}
```

## Python客户端示例

### 基础使用

```python
from api import create_api

# 创建API实例
api = create_api(db_path="stock_data.duckdb")

# 获取股票列表
stocks = api.get_stock_list(market="main", limit=10)

# 获取价格数据
price_data = api.get_price_data(
    code="000001.SZ",
    start_date="2025-08-01",
    end_date="2025-09-19"
)

# 批量获取价格数据
batch_prices = api.get_batch_price_data(
    codes=["000001.SZ", "000002.SZ"],
    start_date="2025-08-01"
)

# 股票筛选
results = api.screen_stocks({
    "market_cap_min": 1000000000,
    "pe_ratio_max": 30
})
```

### HTTP客户端示例

```python
import requests

# API基础URL
BASE_URL = "http://localhost:5000/api/v1"

# 获取股票价格数据
response = requests.get(f"{BASE_URL}/stocks/000001.SZ/price", params={
    "start_date": "2025-08-01",
    "end_date": "2025-09-19"
})

if response.status_code == 200:
    data = response.json()
    if data["success"]:
        prices = data["data"]
        print(f"获取到 {len(prices)} 条价格数据")

# 批量获取价格数据
response = requests.post(f"{BASE_URL}/stocks/batch/prices", json={
    "codes": ["000001.SZ", "000002.SZ"],
    "start_date": "2025-08-01",
    "end_date": "2025-09-19",
    "fields": ["open", "close", "volume"]
})

if response.status_code == 200:
    data = response.json()
    print(f"获取到 {data['count']} 条数据")
```

## 错误处理

API使用统一的错误响应格式：

```json
{
  "success": false,
  "error": "错误描述信息",
  "details": "详细错误信息（可选）"
}
```

常见HTTP状态码：
- 200: 成功
- 400: 请求参数错误
- 404: 资源未找到
- 500: 服务器内部错误

## 性能优化建议

1. **使用批量接口**: 当需要查询多个股票数据时，使用批量接口而不是循环调用单个接口
2. **合理设置时间范围**: 限制查询的时间范围，避免返回过多数据
3. **使用字段筛选**: 只请求需要的字段，减少数据传输量
4. **实现客户端缓存**: 对频繁访问的数据实现客户端缓存

## 数据更新频率

- 价格数据: 每日收盘后更新
- 财务数据: 每季度更新
- 股票列表: 每周更新