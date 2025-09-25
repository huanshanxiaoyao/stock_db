# 股票数据存储平台 (Stock Data Storage Platform)

一个专注于股票原始数据本地存储和查询的平台，为其他量化模块提供高效的数据服务。

## 📋 目录

- [项目概述](#项目概述)
- [架构设计](#架构设计)
- [技术方案](#技术方案)
- [项目结构](#项目结构)
- [安装配置](#安装配置)
- [使用指南](#使用指南)
- [数据模型](#数据模型)
- [API接口](#api接口)
- [测试体系](#测试体系)
- [常见问题](#常见问题)

## 🎯 项目概述

### 设计目标

本平台专注于第一期核心功能：股票原始数据的本地存储和查询服务，解决以下核心问题：

1. **数据获取**: 从聚宽、Tushare等数据源获取股票原始数据和部分加工后的财务数据
2. **数据存储**: 高效的本地存储，支持大规模数据快速查询
3. **数据查询**: 提供API和模块调用接口，供其他模块使用
4. **数据管理**: 数据更新、备份和基础维护功能

### 核心特性

- ✅ **多数据源支持**: 聚宽、Tushare等主流数据源
- ✅ **高性能存储**: DuckDB列式存储，查询速度快
- ✅ **基础数据模型**: 股票基本信息、价格数据、财务数据等原始数据
- ✅ **数据更新机制**: 支持增量更新和定期更新
- ✅ **简洁API设计**: 统一接口，支持快速查询和批量操作
- ✅ **模块化设计**: 便于其他量化模块集成使用

## 🏗️ 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application Layer)                │
├─────────────────────────────────────────────────────────────┤
│     命令行工具     │     API接口     │     数据管理工具     │
├─────────────────────────────────────────────────────────────┤
│                    服务层 (Service Layer)                   │
├─────────────────────────────────────────────────────────────┤
│      数据服务      │      更新服务      │   股票列表服务   │
├─────────────────────────────────────────────────────────────┤
│                    业务层 (Business Layer)                  │
├─────────────────────────────────────────────────────────────┤
│    API管理器    │   数据源管理器   │   数据库管理器   │
├─────────────────────────────────────────────────────────────┤
│                    数据层 (Data Layer)                      │
├─────────────────────────────────────────────────────────────┤
│  数据模型  │  数据提供商  │  数据库实现  │  配置管理  │
├─────────────────────────────────────────────────────────────┤
│                   基础设施层 (Infrastructure)                │
└─────────────────────────────────────────────────────────────┘
│    DuckDB    │    聚宽API    │    网络通信    │    日志系统    │
└─────────────────────────────────────────────────────────────┘
```

### 设计原则

1. **分层架构**: 清晰的分层设计，职责分离，便于维护和扩展
2. **接口抽象**: 定义标准接口，支持多种数据源实现
3. **配置管理**: 通过配置文件管理数据源和数据库连接
4. **模块化设计**: 支持其他模块方便地集成和使用
5. **错误处理**: 完善的异常处理和错误恢复机制
6. **性能优化**: 批量操作、索引优化、查询缓存

## 🔧 技术方案

### 核心技术栈

| 进一步 | 技术选型 | 版本要求 | 选择理由 |
|---------|---------|---------|----------|
| **编程语言** | Python | 3.8+ | 丰富的数据处理生态，易于开发和维护 |
| **数据库** | DuckDB | 0.8+ | 高性能列式存储，支持快速查询 |
| **数据处理** | Pandas | 1.3+ | 强大的数据处理能力 |
| **配置管理** | PyYAML | 6.0+ | 灵活的配置文件格式 |
| **日志系统** | Python logging | 内置 | 标准化的日志记录和管理 |
| **API框架** | FastAPI | 最新 | 高性能的API框架 |

### 数据存储方案

#### DuckDB选择理由

1. **高性能**: 列式存储，查询速度快
2. **轻量级**: 嵌入式数据库，无需独立服务器
3. **SQL兼容**: 支持标准SQL，学习成本低
4. **数据友好**: 专为数据查询场景设计
5. **Python集成**: 原生Python支持，API简洁易用

#### 数据存储策略

```sql
-- 按数据类型分表存储
CREATE TABLE stock_list (code, name, display_name, start_date, end_date, ...);
CREATE TABLE stock_price (code, date, open, high, low, close, volume, ...);
CREATE TABLE financial_data (code, report_date, revenue, profit, ...);
```

### 数据源集成方案

#### 抽象接口设计

```python
class BaseDataSource(ABC):
    """数据源基类"""
    
    @abstractmethod
    def authenticate(self) -> bool:
        """认证"""
        pass
    
    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def get_price_data(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取价格数据"""
        pass
    
    @abstractmethod
    def get_financial_data(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取财务数据"""
        pass
```

## 📁 项目结构

### 核心文件说明

- **配置文件**: `config.yaml` - 系统配置，`requirements.txt` - 依赖管理
- **核心模块**: `api.py` - API接口，`database.py` - 数据库操作，`data_source.py` - 数据源管理
- **主程序**: `main.py` - 命令行入口，`api_server.py` - API服务器
- **数据更新**: `services/update_service.py` - 数据更新服务，支持每日/历史更新

### 目录结构

- **data/**: 数据文件存储（CSV、DuckDB数据库文件）
- **models/**: 数据模型定义（股票信息、价格数据、财务数据）
- **providers/**: 数据源实现（聚宽、Tushare等）
- **services/**: 业务服务层（数据查询、更新服务）
- **scripts/**: 工具脚本（数据检查、导出、测试等）
- **examples/**: 使用示例和演示代码
- **test/**: 测试代码和测试数据
- **logs/**: 系统日志文件

## 📦 核心模块

### 数据模型层 (`models/`)
- **base.py**: 数据模型基类，提供通用的数据操作方法
- **stock_list.py**: 股票基本信息模型，包含股票代码、名称、上市日期等
- **market.py**: 市场数据模型，包含价格、成交量等交易数据
- **financial.py**: 财务数据模型，包含营收、利润等财务指标

### 数据提供商层 (`providers/`)
- **jqdata.py**: 聚宽数据源实现，提供股票列表、价格数据、财务数据获取
- 支持多数据源扩展，统一的数据接口标准

### 服务层 (`services/`)
- **data_service.py**: 数据查询服务，提供统一的数据访问接口
- **stock_list_service.py**: 股票列表管理服务
- **update_service.py**: 数据更新服务，支持增量和全量更新

### 数据库层
- **database.py**: 数据库抽象接口
- **duckdb_impl.py**: DuckDB具体实现，提供高性能的列式存储


## 🚀 安装配置

### 环境要求

- Python 3.8+
- 内存: 4GB以上推荐
- 存储: 根据数据量，建议预留10GB以上空间
- 网络: 稳定的互联网连接（用于数据获取）

### 安装步骤

1. **克隆项目**
```bash
git clone https://github.com/your-repo/stock_db.git
cd stock_db
```

2. **创建虚拟环境**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

4. **配置文件**
```bash
cp config_example.yaml config.yaml
# 编辑config.yaml，填入数据源配置
```

### 配置说明

```yaml
# config.yaml
database:
  type: "duckdb"
  path: "stock_data.db"
  
data_sources:
  jqdata:
    enabled: true
    username: "your_username"
    password: "your_password"
    
logging:
  level: "INFO"
  file: "stock_data.log"
  
update:
  default_data_types:
    - "financial"
    - "market"
  max_workers: 4
  incremental_update: true
  data_retention_days: 0
  default_history_start_date: "2019-01-01"

```

## 📖 使用指南

### 命令行工具

#### 基本操作
- **初始化数据库**: `python main.py init --db-path stock_data.db`
- **更新股票列表**: `python main.py update-stock-list`
- **数据更新**: `python main.py update` (增量) / `python main.py daily` (每日)
- **数据查询**: `python main.py info` (数据库信息) / `python main.py query "SQL语句"`
- **数据导出**: `python main.py export --type stock_list --output stocks.csv`
- **数据检查**: `python main.py check-data`

### Python API

#### 基本使用流程
1. 导入并创建API实例: `from stock_db import StockDataAPI`
2. 连接数据库: `api = StockDataAPI("stock_data.db")`
3. 获取数据: `stock_list = api.get_stock_list()`
4. 关闭连接: `api.close()`

#### 主要功能
- **股票列表**: `get_stock_list()` - 获取所有股票信息
- **股票信息**: `get_stock_info(code)` - 获取单只股票详细信息
- **价格数据**: `get_price_data(code, start_date, end_date)` - 获取历史价格
- **财务数据**: `get_financial_data(code, start_date, end_date)` - 获取财务报表
- **批量操作**: `get_batch_price_data(codes, start_date, end_date)` - 批量获取数据

详细使用示例请参考 `examples/` 目录下的示例文件。

## 📊 数据模型

### 数据库表结构

#### 用户交易记录表 (`user_transactions`)

**目的**: 记录用户的股票买卖、分红、配股等所有交易行为，提供详细的交易流水。

**设计要点**:
- **唯一性**: 每笔交易应有唯一的标识
- **时间戳**: 精确记录交易发生的时间
- **关联性**: 能够关联到具体的股票和用户
- **交易类型**: 区分买入、卖出、分红、配股等不同交易类型
- **金额与数量**: 记录交易涉及的股票数量、价格和总金额
- **备注**: 允许用户添加自定义备注信息

**表结构**:

```sql
CREATE TABLE user_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,     -- 交易唯一ID
    user_id VARCHAR(64) NOT NULL,              -- 用户ID
    stock_code VARCHAR(16) NOT NULL,           -- 股票代码，例如 '831010.BJ'
    trade_date DATE NOT NULL,                  -- 交易日期（索引字段）
    trade_datetime TIMESTAMP NOT NULL,         -- 交易日期时间
    trade_type INTEGER NOT NULL,               -- 交易类型：23-买入，24-卖出
    strategy_id VARCHAR(64),                   -- 策略ID，标记触发此交易的策略（索引字段）
    volume INTEGER NOT NULL,                   -- 交易数量
    price DECIMAL(10, 4) NOT NULL,             -- 交易价格
    value DECIMAL(12, 4) NOT NULL,             -- 交易总金额
    commission DECIMAL(10, 4) DEFAULT 0.0,     -- 佣金
    tax DECIMAL(10, 4) DEFAULT 0.0,            -- 印花税
    other_fees DECIMAL(10, 4) DEFAULT 0.0,     -- 其他费用
    net_amount DECIMAL(12, 4) NOT NULL,        -- 净交易金额
    order_id VARCHAR(64),                      -- 订单ID
    trade_id VARCHAR(64),                      -- 交易流水号
    remark VARCHAR(255),                       -- 备注信息
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_user_transactions_user_id ON user_transactions(user_id);
CREATE INDEX idx_user_transactions_stock_code ON user_transactions(stock_code);
CREATE INDEX idx_user_transactions_trade_date ON user_transactions(trade_date);
CREATE INDEX idx_user_transactions_strategy_id ON user_transactions(strategy_id);
-- 复合索引：用于高效查询某用户某股票在特定日期范围的交易记录
CREATE INDEX idx_user_transactions_composite ON user_transactions(user_id, stock_code, trade_date);
```

**字段说明**:

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| transaction_id | VARCHAR(64) | 交易唯一标识符 | 'TXN_20240101_001' |
| user_id | VARCHAR(64) | 用户ID（索引字段） | 'user_001' |
| stock_code | VARCHAR(16) | 股票代码（索引字段） | '831010.BJ' |
| trade_date | DATE | 交易日期（索引字段） | '2024-01-15' |
| trade_datetime | TIMESTAMP | 交易日期时间 | '2024-01-15 09:30:15' |
| trade_type | INTEGER | 交易类型：23-买入，24-卖出 | 23 |
| strategy_id | VARCHAR(64) | 策略ID，标记触发此交易的策略（索引字段） | 'MACD_001' |
| volume | INTEGER | 交易数量 | 600 |
| price | DECIMAL(10,4) | 交易价格 | 14.4500 |
| value | DECIMAL(12,4) | 交易总金额 | 8670.0000 |
| commission | DECIMAL(10,4) | 佣金 | 5.0000 |
| tax | DECIMAL(10,4) | 印花税 | 8.6700 |
| other_fees | DECIMAL(10,4) | 其他费用 | 0.0000 |
| net_amount | DECIMAL(12,4) | 净交易金额 | 8683.6700 |
| order_id | VARCHAR(64) | 订单ID | '1478492199' |
| trade_id | VARCHAR(64) | 交易流水号 | '01250350' |
| remark | VARCHAR(255) | 备注信息 | 'str1001_831010.BJ' |
| created_at | TIMESTAMP | 记录创建时间 | '2024-01-15 09:30:15' |

**与样例数据的对应关系**:
- `StockCode` → `stock_code`
- `Volume` → `volume`
- `Price` → `price`
- `Value` → `value`
- `TradeType` → `trade_type` (23-买入，24-卖出)
- `Remark` → `remark`
- `OrderId` → `order_id`
- `TradeId` → `trade_id`
- `TradeTime` → `trade_datetime`

#### 用户持仓记录表 (`user_positions`)

**目的**: 记录用户每日的股票持仓情况，包括持仓数量、市值、成本价等信息，用于跟踪持仓变化和计算收益。

**设计要点**:
- **时间序列**: 按日期记录持仓快照，支持历史持仓查询
- **用户隔离**: 不同用户的持仓数据完全隔离
- **持仓状态**: 区分可用数量、冻结数量、在途股份等不同状态
- **成本跟踪**: 记录开仓价格，便于计算盈亏
- **账户信息**: 同时记录账户总资产、可用资金等汇总信息

**表结构**:

```sql
CREATE TABLE user_positions (
    position_id VARCHAR(64) PRIMARY KEY,       -- 持仓记录唯一ID
    user_id VARCHAR(64) NOT NULL,              -- 用户ID
    position_date DATE NOT NULL,               -- 持仓日期（索引字段）
    stock_code VARCHAR(16) NOT NULL,           -- 股票代码，例如 '000030.SZ'
    position_quantity INTEGER NOT NULL,        -- 持仓数量
    available_quantity INTEGER NOT NULL,       -- 可用数量
    frozen_quantity INTEGER DEFAULT 0,         -- 冻结数量
    transit_shares INTEGER DEFAULT 0,          -- 在途股份
    yesterday_quantity INTEGER DEFAULT 0,      -- 昨夜持股
    open_price DECIMAL(10, 4) NOT NULL,        -- 开仓价格（成本价）
    market_value DECIMAL(12, 4) NOT NULL,      -- 持仓市值
    current_price DECIMAL(10, 4),              -- 当前价格
    unrealized_pnl DECIMAL(12, 4),             -- 未实现盈亏
    unrealized_pnl_ratio DECIMAL(8, 4),        -- 未实现盈亏比例
    remark VARCHAR(255),                       -- 备注信息
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_user_positions_user_id ON user_positions(user_id);
CREATE INDEX idx_user_positions_date ON user_positions(position_date);
CREATE INDEX idx_user_positions_stock_code ON user_positions(stock_code);
-- 复合索引：用于高效查询某用户在特定日期的持仓记录
CREATE INDEX idx_user_positions_composite ON user_positions(user_id, position_date);
-- 唯一约束：确保同一用户同一日期同一股票只有一条记录
CREATE UNIQUE INDEX idx_user_positions_unique ON user_positions(user_id, position_date, stock_code);
```

**字段说明**:

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| position_id | VARCHAR(64) | 持仓记录唯一标识符 | 'POS_20250901_001' |
| user_id | VARCHAR(64) | 用户ID（索引字段） | '6681802461' |
| position_date | DATE | 持仓日期（索引字段） | '2025-09-01' |
| stock_code | VARCHAR(16) | 股票代码（索引字段） | '000030.SZ' |
| position_quantity | INTEGER | 持仓数量 | 3500 |
| available_quantity | INTEGER | 可用数量 | 3500 |
| frozen_quantity | INTEGER | 冻结数量 | 0 |
| transit_shares | INTEGER | 在途股份 | 0 |
| yesterday_quantity | INTEGER | 昨夜持股 | 3500 |
| open_price | DECIMAL(10,4) | 开仓价格（成本价） | 5.6815 |
| market_value | DECIMAL(12,4) | 持仓市值 | 20055.0000 |
| current_price | DECIMAL(10,4) | 当前价格 | 5.7300 |
| unrealized_pnl | DECIMAL(12,4) | 未实现盈亏 | 169.7500 |
| unrealized_pnl_ratio | DECIMAL(8,4) | 未实现盈亏比例 | 0.0085 |
| remark | VARCHAR(255) | 备注信息 | '主板股票' |
| created_at | TIMESTAMP | 记录创建时间 | '2025-09-01 15:20:03' |
| updated_at | TIMESTAMP | 记录更新时间 | '2025-09-01 15:20:03' |

#### 用户账户信息表 (`user_account_info`)

**目的**: 记录用户每日的账户汇总信息，包括总资产、持仓市值、可用资金等。

**表结构**:

```sql
CREATE TABLE user_account_info (
    user_id VARCHAR(64) NOT NULL,              -- 用户ID（主键组成部分）
    info_date DATE NOT NULL,                   -- 信息日期（主键组成部分）
    total_assets DECIMAL(15, 2) NOT NULL,      -- 总资产
    position_market_value DECIMAL(15, 2) NOT NULL, -- 持仓市值
    available_cash DECIMAL(15, 2) NOT NULL,    -- 可用资金
    frozen_cash DECIMAL(15, 2) DEFAULT 0.0,    -- 冻结资金
    total_profit_loss DECIMAL(15, 2),          -- 总盈亏
    total_profit_loss_ratio DECIMAL(8, 4),     -- 总盈亏比例
    timestamp TIMESTAMP,                       -- 数据时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, info_date)           -- 联合主键
);

-- 创建索引（联合主键自动创建索引，无需额外创建）
-- 如需要单独查询某个用户的所有记录，可以创建以下索引：
-- CREATE INDEX idx_user_account_info_user_id ON user_account_info(user_id);
-- 如需要单独查询某个日期的所有记录，可以创建以下索引：
-- CREATE INDEX idx_user_account_info_date ON user_account_info(info_date);
```

**字段说明**:

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| user_id | VARCHAR(64) | 用户ID（联合主键组成部分） | '6681802461' |
| info_date | DATE | 信息日期（联合主键组成部分） | '2025-09-01' |
| total_assets | DECIMAL(15,2) | 总资产 | 490738.31 |
| position_market_value | DECIMAL(15,2) | 持仓市值 | 441540.89 |
| available_cash | DECIMAL(15,2) | 可用资金 | 49197.42 |
| frozen_cash | DECIMAL(15,2) | 冻结资金 | 0.00 |
| total_profit_loss | DECIMAL(15,2) | 总盈亏 | 12345.67 |
| total_profit_loss_ratio | DECIMAL(8,4) | 总盈亏比例 | 0.0258 |
| timestamp | TIMESTAMP | 数据时间戳 | '2025-09-01 15:20:03' |
| created_at | TIMESTAMP | 记录创建时间 | '2025-09-01 15:20:03' |
| updated_at | TIMESTAMP | 记录更新时间 | '2025-09-01 15:20:03' |

**与JSON数据的对应关系**:

**持仓数据 (positions)**:
- `证券代码` → `stock_code`
- `持仓数量` → `position_quantity`
- `可用数量` → `available_quantity`
- `冻结数量` → `frozen_quantity`
- `在途股份` → `transit_shares`
- `昨夜持股` → `yesterday_quantity`
- `开仓价格` → `open_price`
- `持仓市值` → `market_value`

**账户信息 (account_info)**:
- `总资产` → `total_assets`
- `持仓市值` → `position_market_value`
- `可用资金` → `available_cash`
- `冻结资金` → `frozen_cash`
- `timestamp` → `timestamp`

#### 股票列表表 (`stock_list`)

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.XSHE |
| display_name | VARCHAR | 显示名称 | 平安银行 |
| name | VARCHAR | 股票名称 | 平安银行股份有限公司 |
| start_date | DATE | 上市日期 | 1991-04-03 |
| end_date | DATE | 退市日期 | NULL |
| exchange | VARCHAR | 交易所 | XSHE |
| market | VARCHAR | 市场板块 | main |
| industry_name | VARCHAR | 行业名称 | 银行 |
| status | VARCHAR | 状态 | normal |

#### 价格数据表 (`price_data`) - 基于聚宽API get_price接口字段

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.XSHE |
| day | DATE | 交易日期 | 2023-12-29 |
| open | DOUBLE | 开盘价 | 10.50 |
| close | DOUBLE | 收盘价 | 10.75 |
| high | DOUBLE | 最高价 | 10.80 |
| low | DOUBLE | 最低价 | 10.45 |
| pre_close | DOUBLE | 前收盘价 | 10.40 |
| volume | DOUBLE | 成交量 | 12345678.0 |
| money | DOUBLE | 成交额 | 132456789.50 |
| factor | DOUBLE | 复权因子 | 1.0234 |
| high_limit | DOUBLE | 涨停价 | 11.44 |
| low_limit | DOUBLE | 跌停价 | 9.36 |
| avg | DOUBLE | 均价 | 10.62 |
| paused | INTEGER | 停牌状态 | 0 |
| adj_close | DOUBLE | 复权收盘价 | 10.75 |
| adj_factor | DOUBLE | 复权因子(兼容) | 1.0234 |

#### 资产负债表 (`balance_sheet`) - 基于聚宽API资产负债表字段对齐

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.SZ |
| pub_date | DATE | 公布日期 | 2023-04-28 |
| stat_date | DATE | 统计日期 | 2023-03-31 |
| **流动资产** | | | |
| money_cap | DOUBLE | 货币资金 | 123456789012.34 |
| settlement_provi | DOUBLE | 结算备付金 | 12345678901.23 |
| lend_capital | DOUBLE | 拆出资金 | 9876543210.98 |
| trading_assets | DOUBLE | 交易性金融资产 | 56789012345.67 |
| notes_receivable | DOUBLE | 应收票据 | 34567890123.45 |
| accounts_receivable | DOUBLE | 应收账款 | 98765432109.87 |
| advance_payment | DOUBLE | 预付款项 | 23456789012.34 |
| insurance_receivables | DOUBLE | 应收保费 | 12345678901.23 |
| other_receivable | DOUBLE | 其他应收款 | 45678901234.56 |
| inventories | DOUBLE | 存货 | 56789012345.67 |
| total_current_assets | DOUBLE | 流动资产合计 | 567890123456.78 |
| **非流动资产** | | | |
| available_for_sale_assets | DOUBLE | 可供出售金融资产 | 78901234567.89 |
| held_to_maturity_investments | DOUBLE | 持有至到期投资 | 34567890123.45 |
| long_term_equity_invest | DOUBLE | 长期股权投资 | 123456789012.34 |
| investment_real_estate | DOUBLE | 投资性房地产 | 45678901234.56 |
| fixed_assets | DOUBLE | 固定资产 | 345678901234.56 |
| constru_in_process | DOUBLE | 在建工程 | 23456789012.34 |
| intangible_assets | DOUBLE | 无形资产 | 67890123456.78 |
| good_will | DOUBLE | 商誉 | 12345678901.23 |
| deferred_tax_assets | DOUBLE | 递延所得税资产 | 9876543210.98 |
| total_non_current_assets | DOUBLE | 非流动资产合计 | 678901234567.89 |
| total_assets | DOUBLE | 资产总计 | 1234567890123.45 |
| **流动负债** | | | |
| short_term_loan | DOUBLE | 短期借款 | 123456789012.34 |
| notes_payable | DOUBLE | 应付票据 | 45678901234.56 |
| accounts_payable | DOUBLE | 应付账款 | 234567890123.45 |
| salaries_payable | DOUBLE | 应付职工薪酬 | 12345678901.23 |
| tax_payable | DOUBLE | 应交税费 | 9876543210.98 |
| total_current_liability | DOUBLE | 流动负债合计 | 456789012345.67 |
| **非流动负债** | | | |
| long_term_loan | DOUBLE | 长期借款 | 234567890123.45 |
| bonds_payable | DOUBLE | 应付债券 | 123456789012.34 |
| total_non_current_liability | DOUBLE | 非流动负债合计 | 345678901234.56 |
| total_liability | DOUBLE | 负债合计 | 987654321098.76 |
| **所有者权益** | | | |
| paid_capital | DOUBLE | 实收资本(或股本) | 123456789012.34 |
| capital_reserve_fund | DOUBLE | 资本公积 | 45678901234.56 |
| surplus_reserve_fund | DOUBLE | 盈余公积 | 23456789012.34 |
| retained_profit | DOUBLE | 未分配利润 | 56789012345.67 |
| total_owner_equity | DOUBLE | 归属于母公司所有者权益合计 | 234567890123.45 |
| minority_equity | DOUBLE | 少数股东权益 | 12345678901.23 |
| total_equity | DOUBLE | 所有者权益合计 | 246913569024.69 |

#### 现金流量表 (`cashflow_statement`) - 基于聚宽API现金流量表字段对齐

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.SZ |
| pub_date | DATE | 公布日期 | 2023-04-28 |
| stat_date | DATE | 统计日期 | 2023-03-31 |
| **经营活动现金流入** | | | |
| goods_sale_and_service_render_cash | DOUBLE | 销售商品、提供劳务收到的现金 | 45678901234.56 |
| net_deposit_increase | DOUBLE | 客户存款和同业存放款项净增加额 | 12345678901.23 |
| net_original_insurance_cash | DOUBLE | 收到原保险合同保费取得的现金 | 9876543210.98 |
| interest_and_commission_cashin | DOUBLE | 收取利息、手续费及佣金的现金 | 23456789012.34 |
| tax_levy_refund | DOUBLE | 收到的税费返还 | 1234567890.12 |
| other_cashin_related_operate | DOUBLE | 收到其他与经营活动有关的现金 | 5678901234.56 |
| subtotal_operate_cash_inflow | DOUBLE | 经营活动现金流入小计 | 98765432109.87 |
| **经营活动现金流出** | | | |
| goods_and_services_cash_paid | DOUBLE | 购买商品、接受劳务支付的现金 | 34567890123.45 |
| net_loan_and_advance_increase | DOUBLE | 客户贷款及垫款净增加额 | 23456789012.34 |
| original_compensation_paid | DOUBLE | 支付原保险合同赔付款项的现金 | 12345678901.23 |
| handling_charges_and_commission | DOUBLE | 支付利息、手续费及佣金的现金 | 9876543210.98 |
| staff_behalf_paid | DOUBLE | 支付给职工以及为职工支付的现金 | 5678901234.56 |
| tax_payments | DOUBLE | 支付的各项税费 | 3456789012.34 |
| other_cash_paid_related_operate | DOUBLE | 支付其他与经营活动有关的现金 | 7890123456.78 |
| subtotal_operate_cash_outflow | DOUBLE | 经营活动现金流出小计 | 86419753208.64 |
| net_operate_cash_flow | DOUBLE | 经营活动产生的现金流量净额 | 12345678901.23 |
| **投资活动现金流** | | | |
| invest_withdrawal_cash | DOUBLE | 收回投资收到的现金 | 12345678901.23 |
| invest_proceeds | DOUBLE | 取得投资收益收到的现金 | 2345678901.23 |
| fix_intan_other_asset_dispo_cash | DOUBLE | 处置固定资产、无形资产收回的现金净额 | 1234567890.12 |
| subtotal_invest_cash_inflow | DOUBLE | 投资活动现金流入小计 | 15925925692.58 |
| fix_intan_other_asset_acqui_cash | DOUBLE | 购建固定资产、无形资产支付的现金 | 23456789012.34 |
| invest_cash_paid | DOUBLE | 投资支付的现金 | 12345678901.23 |
| subtotal_invest_cash_outflow | DOUBLE | 投资活动现金流出小计 | 35802467913.57 |
| net_invest_cash_flow | DOUBLE | 投资活动产生的现金流量净额 | -9876543210.98 |
| **筹资活动现金流** | | | |
| cash_from_invest | DOUBLE | 吸收投资收到的现金 | 12345678901.23 |
| cash_from_borrowing | DOUBLE | 取得借款收到的现金 | 23456789012.34 |
| cash_from_bonds_issue | DOUBLE | 发行债券收到的现金 | 9876543210.98 |
| subtotal_finance_cash_inflow | DOUBLE | 籌资活动现金流入小计 | 45679011124.55 |
| borrowing_repayment | DOUBLE | 偿还债务支付的现金 | 34567890123.45 |
| dividend_interest_payment | DOUBLE | 分配股利、利润或偿付利息支付的现金 | 5432109876.54 |
| subtotal_finance_cash_outflow | DOUBLE | 籌资活动现金流出小计 | 39999999999.99 |
| net_finance_cash_flow | DOUBLE | 籌资活动产生的现金流量净额 | 5678901234.56 |
| **现金净增加** | | | |
| exchange_rate_change_effect | DOUBLE | 汇率变动对现金及现金等价物的影响 | 123456789.01 |
| cash_equivalent_increase | DOUBLE | 现金及现金等价物净增加额 | 8147036924.81 |
| cash_equivalents_at_beginning | DOUBLE | 期初现金及现金等价物余额 | 45678901234.56 |
| cash_and_equivalents_at_end | DOUBLE | 期末现金及现金等价物余额 | 53825938159.37 |

#### 利润表 (`income_statement`) - 基于聚宽API利润表字段对齐

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.SZ |
| pub_date | DATE | 公布日期 | 2023-04-28 |
| stat_date | DATE | 统计日期 | 2023-03-31 |
| **收入项目** | | | |
| total_operating_revenue | DOUBLE | 营业总收入 | 45678901234.56 |
| operating_revenue | DOUBLE | 营业收入 | 45678901234.56 |
| interest_income | DOUBLE | 利息收入 | 12345678901.23 |
| premiums_earned | DOUBLE | 已赚保费 | 9876543210.98 |
| commission_income | DOUBLE | 手续费及佣金收入 | 2345678901.23 |
| **成本费用项目** | | | |
| total_operating_cost | DOUBLE | 营业总成本 | 34567890123.45 |
| operating_cost | DOUBLE | 营业成本 | 28901234567.89 |
| interest_expense | DOUBLE | 利息支出 | 5678901234.56 |
| commission_expense | DOUBLE | 手续费及佣金支出 | 1234567890.12 |
| refunded_premiums | DOUBLE | 退保金 | 987654321.09 |
| net_pay_insurance_claims | DOUBLE | 赔付支出净额 | 3456789012.34 |
| policy_dividend_payout | DOUBLE | 保单红利支出 | 567890123.45 |
| reinsurance_cost | DOUBLE | 分保费用 | 234567890.12 |
| operating_tax_surcharges | DOUBLE | 营业税金及附加 | 567890123.45 |
| sale_expense | DOUBLE | 销售费用 | 2345678901.23 |
| administration_expense | DOUBLE | 管理费用 | 1789012345.67 |
| finance_expense | DOUBLE | 财务费用 | 890123456.78 |
| asset_impairment_loss | DOUBLE | 资产减值损失 | 456789012.34 |
| **投资收益项目** | | | |
| fair_value_variable_income | DOUBLE | 公允价值变动收益 | 123456789.01 |
| invest_income | DOUBLE | 投资收益 | 1234567890.12 |
| invest_income_associates | DOUBLE | 对联营企业和合营企业的投资收益 | 345678901.23 |
| exchange_income | DOUBLE | 汇兑收益 | 78901234.56 |
| **利润项目** | | | |
| operating_profit | DOUBLE | 营业利润 | 11111111111.11 |
| non_operating_revenue | DOUBLE | 营业外收入 | 234567890.12 |
| non_operating_expense | DOUBLE | 营业外支出 | 123456789.01 |
| total_profit | DOUBLE | 利润总额 | 11222222222.22 |
| income_tax | DOUBLE | 所得税费用 | 1345679012.34 |
| net_profit | DOUBLE | 净利润 | 9876543210.98 |
| np_parent_company_owners | DOUBLE | 归属于母公司所有者的净利润 | 9012345678.90 |
| minority_profit | DOUBLE | 少数股东损益 | 864197532.08 |
| **综合收益项目** | | | |
| other_composite_income | DOUBLE | 其他综合收益 | 123456789.01 |
| total_composite_income | DOUBLE | 综合收益总额 | 9999999999.99 |
| ci_parent_company_owners | DOUBLE | 归属于母公司所有者的综合收益总额 | 9135802469.13 |
| ci_minority_owners | DOUBLE | 归属于少数股东的综合收益总额 | 864197530.86 |
| **每股收益** | | | |
| basic_eps | DOUBLE | 基本每股收益 | 1.23 |
| diluted_eps | DOUBLE | 稀释每股收益 | 1.22 |

#### 财务指标表 (`indicator_data`) - 严格基于聚宽API indicator财务指标表

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码(带后缀) | 000001.XSHE |
| pubDate | DATE | 公司发布财报日期 | 2023-04-28 |
| statDate | DATE | 财报统计的季度的最后一天 | 2023-03-31 |
| eps | DOUBLE | 每股收益EPS(元) | 1.23 |
| adjusted_profit | DOUBLE | 扣除非经常损益后的净利润(元) | 123456789.12 |
| operating_profit | DOUBLE | 经营活动净收益(元) | 234567890.23 |
| value_change_profit | DOUBLE | 价值变动净收益(元) | 12345678.90 |
| roe | DOUBLE | 净资产收益率ROE(%) | 0.1234 |
| inc_return | DOUBLE | 净资产收益率(扣除非经常损益)(%) | 0.0234 |
| roa | DOUBLE | 总资产净利率ROA(%) | 0.0987 |
| net_profit_margin | DOUBLE | 销售净利率(%) | 0.1234 |
| gross_profit_margin | DOUBLE | 销售毛利率(%) | 0.3456 |
| expense_to_total_revenue | DOUBLE | 营业总成本/营业总收入(%) | 0.7654 |
| operation_profit_to_total_revenue | DOUBLE | 营业利润/营业总收入(%) | 0.2345 |
| net_profit_to_total_revenue | DOUBLE | 净利润/营业总收入(%) | 0.1234 |
| operating_expense_to_total_revenue | DOUBLE | 营业费用/营业总收入(%) | 0.0567 |
| ga_expense_to_total_revenue | DOUBLE | 管理费用/营业总收入(%) | 0.0789 |
| financing_expense_to_total_revenue | DOUBLE | 财务费用/营业总收入(%) | 0.0234 |
| operating_profit_to_profit | DOUBLE | 经营活动净收益/利润总额(%) | 0.8901 |
| invesment_profit_to_profit | DOUBLE | 价值变动净收益/利润总额(%) | 0.1099 |
| adjusted_profit_to_profit | DOUBLE | 扣除非经常损益后的净利润/归属于母公司所有者的净利润(%) | 0.9567 |
| goods_sale_and_service_to_revenue | DOUBLE | 销售商品提供劳务收到的现金/营业收入(%) | 1.0234 |
| ocf_to_revenue | DOUBLE | 经营活动产生的现金流量净额/营业收入(%) | 0.1567 |
| ocf_to_operating_profit | DOUBLE | 经营活动产生的现金流量净额/经营活动净收益(%) | 0.8901 |
| inc_total_revenue_year_on_year | DOUBLE | 营业总收入同比增长率(%) | 0.1567 |
| inc_total_revenue_annual | DOUBLE | 营业总收入环比增长率(%) | 0.0456 |
| inc_revenue_year_on_year | DOUBLE | 营业收入同比增长率(%) | 0.1234 |

#### 估值数据表 (`valuation_data`) - 基于聚宽API valuation估值数据表

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.XSHE |
| day | DATE | 数据日期 | 2023-12-29 |
| capitalization | DOUBLE | 总股本(万股) | 1234567.89 |
| circulating_cap | DOUBLE | 流通股本(万股) | 987654.32 |
| market_cap | DOUBLE | 总市值(亿元) | 123456789012.34 |
| circulating_market_cap | DOUBLE | 流通市值(亿元) | 98765432109.87 |
| turnover_ratio | DOUBLE | 换手率(%) | 2.34 |
| pe_ratio | DOUBLE | 市盈率(PE, TTM) | 12.34 |
| pe_ratio_lyr | DOUBLE | 市盈率(PE) | 11.23 |
| pb_ratio | DOUBLE | 市净率(PB) | 1.23 |
| ps_ratio | DOUBLE | 市销率(PS, TTM) | 2.34 |
| pcf_ratio | DOUBLE | 市现率(PCF, 现金净流量TTM) | 8.90 |
| pcf_ratio2 | DOUBLE | 市现率(PCF,经营活动现金流TTM) | 7.65 |
| dividend_ratio | DOUBLE | 股息率(TTM) % | 3.45 |
| free_cap | DOUBLE | 自由流通股本(万股) | 876543.21 |
| free_market_cap | DOUBLE | 自由流通市值(亿元) | 87654321098.76 |
| a_cap | DOUBLE | A股总股本(万股) | 1234567.89 |
| a_market_cap | DOUBLE | A股总市值(亿元) | 123456789012.34 |

### 数据库索引汇总

数据库采用分层索引策略，包括主键索引（自动创建）和业务索引（显式创建），确保查询性能。

#### 主键索引（自动创建）

所有表的主键会自动创建唯一索引：

| 表名 | 主键 | 说明 |
|------|------|------|
| `stock_list` | `code` | 股票代码唯一索引 |
| `price_data` | `code, day` | 股票代码+日期复合索引 |
| `valuation_data` | `code, day` | 股票代码+日期复合索引 |
| `mtss_data` | `code, day` | 股票代码+日期复合索引 |
| `income_statement` | `code, stat_date` | 股票代码+统计日期复合索引 |
| `cashflow_statement` | `code, stat_date` | 股票代码+统计日期复合索引 |
| `balance_sheet` | `code, stat_date` | 股票代码+统计日期复合索引 |
| `indicator_data` | `code, pubDate, statDate` | 股票代码+发布日期+统计日期复合索引 |
| `user_account_info` | `user_id, info_date` | 用户ID+信息日期复合索引 |

#### 业务索引（显式创建）

针对高频查询场景创建的专用索引：

**用户交易记录表索引**：
```sql
-- 单字段索引
CREATE INDEX idx_user_transactions_user_id ON user_transactions(user_id);
CREATE INDEX idx_user_transactions_stock_code ON user_transactions(stock_code);
CREATE INDEX idx_user_transactions_trade_date ON user_transactions(trade_date);
CREATE INDEX idx_user_transactions_strategy_id ON user_transactions(strategy_id);

-- 复合索引（用户+股票+日期组合查询）
CREATE INDEX idx_user_transactions_composite ON user_transactions(user_id, stock_code, trade_date);
```

**用户持仓记录表索引**：
```sql
-- 单字段索引
CREATE INDEX idx_user_positions_user_id ON user_positions(user_id);
CREATE INDEX idx_user_positions_stock_code ON user_positions(stock_code);
CREATE INDEX idx_user_positions_date ON user_positions(position_date);

-- 复合索引
CREATE INDEX idx_user_positions_user_date ON user_positions(user_id, position_date);

-- 唯一约束索引（防止重复持仓记录）
CREATE UNIQUE INDEX idx_user_positions_unique ON user_positions(user_id, stock_code, position_date);
```

**用户账户信息表索引**：
```sql
-- 单字段索引
CREATE INDEX idx_user_account_info_user_id ON user_account_info(user_id);
CREATE INDEX idx_user_account_info_date ON user_account_info(info_date);

-- 唯一约束索引（用户+日期唯一）
CREATE UNIQUE INDEX idx_user_account_info_unique ON user_account_info(user_id, info_date);
```

#### 索引优化说明

1. **查询性能**: 所有常用查询字段都有对应索引，确保毫秒级查询响应
2. **存储效率**: 使用复合索引减少索引数量，节省存储空间
3. **唯一性约束**: 通过唯一索引防止重复数据插入
4. **更新性能**: 平衡查询性能和数据写入性能，避免过多索引影响更新速度

### 数据关系图

```
stock_list (股票列表)
    ├── stock_price (价格数据)
    ├── financial_data (财务数据)
    └── user_transactions (用户交易记录)
```

## 🔌 API接口

### 核心API类 - StockDataAPI

#### 初始化方法
- `__init__(db_path, config_path=None)` - 创建API实例

#### 股票基础信息
- `get_stock_list(market=None)` - 获取股票列表，可按市场筛选
- `get_stock_info(code)` - 获取单只股票的详细信息

#### 数据获取
- `get_price_data(code, start_date, end_date)` - 获取指定时间段的价格数据
- `get_financial_data(code, count=8)` - 获取财务数据，默认最近8期

#### 批量操作
- `get_batch_price_data(codes, start_date, end_date)` - 批量获取多只股票价格数据
- `get_batch_stock_info(codes)` - 批量获取多只股票基本信息

#### 数据管理
- `update_stock_data(code=None)` - 更新股票数据，可指定股票或全量更新
- `query(sql, params=None)` - 执行自定义SQL查询
- `close()` - 关闭数据库连接

### 便捷函数
- `create_api(db_path, **kwargs)` - 工厂函数，快速创建API实例
- `get_stock_data(code, data_type, **kwargs)` - 通用数据获取函数

## 🧪 测试体系

### 测试文件结构
- **test_system.py** - 系统集成测试
- **test_stock_list.py** - 股票列表功能测试
- **test_api.py** - API接口测试
- **test_real_stocks.py** - 真实股票数据测试
- **test_jqdata_stocks.py** - 聚宽数据源测试

### 测试类型
1. **单元测试** - 测试数据模型、基础功能
2. **集成测试** - 测试API接口、数据库操作
3. **性能测试** - 测试查询速度、批量操作性能
4. **数据测试** - 测试真实数据获取和处理

### 运行测试
- **运行所有测试**: `python scripts/run_tests.py`
- **运行单个测试**: `python test/test_system.py`
- **使用pytest**: `pytest test/ -v`
- **生成覆盖率报告**: `pytest test/ --cov=. --cov-report=html`

## 📈 数据产出

### 数据覆盖范围

#### 股票基础数据
- **股票列表**: 全市场4000+只股票
- **基本信息**: 股票代码、名称、上市日期、行业分类
- **市场分类**: 主板、创业板、科创板、北交所
- **状态信息**: 正常交易、停牌、退市等

#### 价格数据
- **日线数据**: 开高低收、成交量、成交额
- **历史数据**: 支持获取完整历史数据

#### 财务数据
- **基础财务**: 营业收入、净利润等核心财务指标
- **报告期数据**: 季报、年报财务数据
- **历史数据**: 支持获取多期财务数据对比

### 数据质量保证

#### 数据验证
- **字段完整性**: 检查必要字段是否存在
- **价格逻辑性**: 验证价格数据的合理性（如最高价不低于最低价）
- **数据连续性**: 检查时间序列数据的连续性
- **异常值检测**: 识别和标记异常数据点

#### 数据修复
- **异常价格修复**: 自动修复明显错误的价格数据
- **缺失数据填补**: 使用合理方法填补缺失值
- **重复数据清理**: 移除重复的数据记录
- **数据标准化**: 统一数据格式和精度

### 数据更新策略

#### 增量更新
- **时间范围确定**: 基于最后更新时间确定需要更新的日期范围
- **活跃股票筛选**: 优先更新正常交易的股票
- **批量处理**: 分批次处理大量股票数据
- **时间戳管理**: 记录和更新数据更新时间戳

#### 定期全量更新
- **数据质量检查**: 定期进行全面的数据质量评估
- **问题修复**: 自动修复发现的数据问题
- **股票列表更新**: 定期更新股票基础信息
- **系统维护**: 数据库优化和清理工作







## 📚 常见问题

### Q1: 数据源认证失败
**问题**: 聚宽数据认证失败，无法获取数据  
**解决方案**: 
- 检查用户名和密码是否正确
- 确认聚宽账户是否有效且未过期
- 检查网络连接是否正常
- 重新设置认证信息并测试连接

### Q2: 查询速度慢
**问题**: 大量数据查询时速度较慢  
**解决方案**: 
- 为常用查询字段创建数据库索引
- 使用分批查询处理大数据集
- 启用查询结果缓存
- 优化查询条件，避免全表扫描

### Q3: 数据更新失败
**问题**: 自动数据更新过程中出现错误  
**解决方案**: 
- 检查数据源连接状态
- 查看错误日志确定具体问题
- 手动重试更新操作
- 检查磁盘空间是否充足

### Q4: 如何扩展新数据类型
**问题**: 需要添加新的数据类型（如期权、期货数据）  
**解决方案**: 
- 定义新的数据模型类
- 创建对应的数据库表
- 实现数据获取和存储方法
- 在API中注册新的数据类型

### Q5: 如何优化存储空间
**问题**: 数据库文件过大，需要优化存储空间  
**解决方案**: 
- 定期清理过期的历史数据
- 压缩数据库文件
- 优化数据类型精度
- 使用数据分区存储

### Q6: 数据质量问题
**问题**: 发现数据中存在异常值或缺失值  
**解决方案**: 
- 定期进行数据质量检查
- 设置数据验证规则
- 自动修复明显的数据错误
- 使用合理方法填补缺失数据


## 🙏 致谢

感谢以下项目和服务:
- [DuckDB](https://duckdb.org/) - 高性能分析数据库
- [聚宽](https://www.joinquant.com/) - 数据源支持
- [Pandas](https://pandas.pydata.org/) - 数据处理
- [NumPy](https://numpy.org/) - 数值计算

---

**量化数据平台** - 让数据驱动投资决策 🚀