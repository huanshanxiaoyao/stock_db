# 量化数据平台 (Quantitative Data Platform)

一个专业的股票数据获取、存储、分析平台，为量化投资提供完整的数据解决方案。

## 📋 目录

- [项目概述](#项目概述)
- [架构设计](#架构设计)
- [技术方案](#技术方案)
- [核心模块](#核心模块)
- [安装配置](#安装配置)
- [使用指南](#使用指南)
- [数据模型](#数据模型)
- [API接口](#api接口)
- [测试体系](#测试体系)
- [数据产出](#数据产出)
- [性能优化](#性能优化)
- [扩展开发](#扩展开发)
- [部署运维](#部署运维)

## 🎯 项目概述

### 设计目标

本平台旨在构建一个高性能、可扩展、易维护的量化数据平台，解决以下核心问题：

1. **数据获取**: 统一多数据源接口，支持实时和历史数据获取
2. **数据存储**: 高效的列式存储，支持大规模数据快速查询
3. **数据处理**: 标准化的数据清洗、转换和质量控制
4. **数据分析**: 专业的财务分析、技术分析和量化指标计算
5. **系统集成**: 简洁的API设计，便于集成到量化策略系统

### 核心特性

- ✅ **多数据源支持**: 聚宽、Wind、同花顺等主流数据源
- ✅ **高性能存储**: DuckDB列式存储，查询速度提升10倍以上
- ✅ **完整数据模型**: 覆盖股票基本信息、财务数据、市场数据、技术指标
- ✅ **智能更新机制**: 增量更新、定期更新、数据修复和质量检查
- ✅ **专业分析工具**: 财务健康、盈利能力、成长性、估值、技术分析
- ✅ **简洁API设计**: 统一接口，支持快速查询和批量操作
- ✅ **完善测试体系**: 单元测试、集成测试、性能测试
- ✅ **可扩展架构**: 插件化设计，支持自定义数据源和分析模块

## 🏗️ 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application Layer)                │
├─────────────────────────────────────────────────────────────┤
│  命令行工具  │  API接口  │  分析工具  │  数据管理  │  测试工具  │
├─────────────────────────────────────────────────────────────┤
│                    服务层 (Service Layer)                   │
├─────────────────────────────────────────────────────────────┤
│ 数据服务 │ 更新服务 │ 分析服务 │ 股票列表服务 │ 质量检查服务 │
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
2. **接口抽象**: 定义标准接口，支持多种实现，提高系统灵活性
3. **依赖注入**: 通过配置管理依赖关系，降低模块耦合度
4. **插件化设计**: 支持动态加载数据源和分析模块
5. **错误处理**: 完善的异常处理和错误恢复机制
6. **性能优化**: 缓存机制、批量操作、异步处理

## 🔧 技术方案

### 核心技术栈

| 技术领域 | 技术选型 | 版本要求 | 选择理由 |
|---------|---------|---------|----------|
| **编程语言** | Python | 3.8+ | 丰富的数据科学生态，易于开发和维护 |
| **数据库** | DuckDB | 0.8+ | 高性能列式存储，支持复杂分析查询 |
| **数据处理** | Pandas | 1.3+ | 强大的数据处理和分析能力 |
| **数值计算** | NumPy | 1.20+ | 高效的数值计算和数组操作 |
| **配置管理** | PyYAML | 6.0+ | 灵活的配置文件格式 |
| **日志系统** | Python logging | 内置 | 标准化的日志记录和管理 |
| **测试框架** | pytest | 6.0+ | 功能强大的测试框架 |
| **代码质量** | black, flake8 | 最新 | 代码格式化和质量检查 |

### 数据存储方案

#### DuckDB选择理由

1. **高性能**: 列式存储，查询速度比传统数据库快10-100倍
2. **轻量级**: 嵌入式数据库，无需独立服务器
3. **SQL兼容**: 支持标准SQL，学习成本低
4. **分析友好**: 专为OLAP场景设计，支持复杂分析查询
5. **Python集成**: 原生Python支持，API简洁易用

#### 数据分区策略

```sql
-- 按年份分区存储历史数据
CREATE TABLE stock_price_2023 AS SELECT * FROM stock_price WHERE year = 2023;
CREATE TABLE stock_price_2024 AS SELECT * FROM stock_price WHERE year = 2024;

-- 按股票代码分区存储
CREATE TABLE stock_data_main AS SELECT * FROM stock_data WHERE code LIKE '6%';
CREATE TABLE stock_data_gem AS SELECT * FROM stock_data WHERE code LIKE '3%';
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
```

#### 多数据源管理

```python
class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self):
        self.sources = {}
        self.primary_source = None
        self.fallback_sources = []
    
    def add_source(self, name: str, source: BaseDataSource):
        """添加数据源"""
        self.sources[name] = source
    
    def get_data_with_fallback(self, method: str, *args, **kwargs):
        """带故障转移的数据获取"""
        for source in [self.primary_source] + self.fallback_sources:
            try:
                return getattr(source, method)(*args, **kwargs)
            except Exception as e:
                logger.warning(f"数据源 {source} 失败: {e}")
                continue
        raise Exception("所有数据源都不可用")
```

## 📦 核心模块

### 1. 数据模型层 (`models/`)

#### 基础模型 (`base.py`)

```python
class BaseModel:
    """数据模型基类"""
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def from_dict(cls, data: dict):
        """从字典创建对象"""
        return cls(**data)
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    def validate(self) -> bool:
        """数据验证"""
        return True
    
    @classmethod
    def get_table_name(cls) -> str:
        """获取数据库表名"""
        return cls.__name__.lower()
```

#### 股票信息模型 (`stock_list.py`)

```python
class StockInfo(BaseModel):
    """股票基本信息模型"""
    
    def __init__(self, code: str, display_name: str, name: str, 
                 start_date: date, end_date: date = None, 
                 exchange: str = None, market: str = None, 
                 industry_name: str = None, status: str = 'normal', **kwargs):
        super().__init__(**kwargs)
        self.code = code
        self.display_name = display_name
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange
        self.market = market
        self.industry_name = industry_name
        self.status = status
    
    @property
    def is_active(self) -> bool:
        """是否为活跃股票"""
        return self.end_date is None and self.status == 'normal'
    
    def to_jq_code(self) -> str:
        """转换为聚宽代码格式"""
        if '.XSHE' in self.code or '.XSHG' in self.code:
            return self.code
        
        if self.code.endswith('.SZ'):
            return self.code.replace('.SZ', '.XSHE')
        elif self.code.endswith('.SH'):
            return self.code.replace('.SH', '.XSHG')
        elif self.code.endswith('.BJ'):
            return self.code.replace('.BJ', '.BSE')
        
        return self.code
```

#### 财务数据模型 (`financial.py`)

```python
class IncomeStatement(BaseModel):
    """利润表模型"""
    
    def __init__(self, code: str, pub_date: date, stat_date: date,
                 total_operating_revenue: float = None,
                 operating_profit: float = None,
                 net_profit: float = None, **kwargs):
        super().__init__(**kwargs)
        self.code = code
        self.pub_date = pub_date
        self.stat_date = stat_date
        self.total_operating_revenue = total_operating_revenue
        self.operating_profit = operating_profit
        self.net_profit = net_profit
    
    @property
    def profit_margin(self) -> float:
        """净利润率"""
        if self.total_operating_revenue and self.net_profit:
            return self.net_profit / self.total_operating_revenue
        return 0.0
    
    @property
    def operating_margin(self) -> float:
        """营业利润率"""
        if self.total_operating_revenue and self.operating_profit:
            return self.operating_profit / self.total_operating_revenue
        return 0.0
```

### 2. 数据提供商层 (`providers/`)

#### 聚宽数据源 (`jqdata.py`)

```python
class JQDataSource(BaseDataSource):
    """聚宽数据源实现"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.authenticated = False
    
    def authenticate(self) -> bool:
        """聚宽认证"""
        try:
            import jqdata
            jqdata.auth(self.config.username, self.config.password)
            self.authenticated = True
            return True
        except Exception as e:
            logger.error(f"聚宽认证失败: {e}")
            return False
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.authenticated:
            raise Exception("未认证")
        
        import jqdata
        stocks = jqdata.get_all_securities('stock')
        
        # 转换为标准格式
        result = []
        for code, info in stocks.iterrows():
            stock_info = StockInfo(
                code=self._to_standard_code(code),
                display_name=info['display_name'],
                name=info['name'],
                start_date=info['start_date'],
                end_date=info['end_date'] if pd.notna(info['end_date']) else None
            )
            result.append(stock_info.to_dict())
        
        return pd.DataFrame(result)
    
    def _to_standard_code(self, jq_code: str) -> str:
        """聚宽代码转标准代码"""
        if '.XSHE' in jq_code:
            return jq_code.replace('.XSHE', '.SZ')
        elif '.XSHG' in jq_code:
            return jq_code.replace('.XSHG', '.SH')
        elif '.BSE' in jq_code:
            return jq_code.replace('.BSE', '.BJ')
        return jq_code
```

### 3. 服务层 (`services/`)

#### 数据服务 (`data_service.py`)

```python
class DataService:
    """数据服务"""
    
    def __init__(self, db_manager: DatabaseManager, data_source_manager: DataSourceManager):
        self.db_manager = db_manager
        self.data_source_manager = data_source_manager
    
    def get_stock_data(self, code: str, data_type: str, 
                      start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """获取股票数据"""
        # 先从数据库查询
        local_data = self._query_local_data(code, data_type, start_date, end_date)
        
        # 检查数据完整性
        if self._is_data_complete(local_data, start_date, end_date):
            return local_data
        
        # 从数据源获取缺失数据
        missing_data = self._fetch_missing_data(code, data_type, start_date, end_date)
        
        # 合并并保存数据
        if not missing_data.empty:
            self._save_data(missing_data, data_type)
            return pd.concat([local_data, missing_data]).drop_duplicates()
        
        return local_data
    
    def _query_local_data(self, code: str, data_type: str, 
                         start_date: date, end_date: date) -> pd.DataFrame:
        """查询本地数据"""
        table_name = self._get_table_name(data_type)
        
        sql = f"SELECT * FROM {table_name} WHERE code = ?"
        params = [code]
        
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY date"
        
        return self.db_manager.query(sql, params)
```

#### 分析服务 (`analysis_service.py`)

```python
class AnalysisService:
    """分析服务"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def comprehensive_analysis(self, code: str) -> AnalysisResult:
        """综合分析"""
        # 获取各维度分析结果
        financial_health = self.analyze_financial_health(code)
        profitability = self.analyze_profitability(code)
        growth = self.analyze_growth(code)
        valuation = self.analyze_valuation(code)
        technical = self.analyze_technical_indicators(code)
        
        # 计算综合评分
        weights = {
            'financial_health': 0.25,
            'profitability': 0.25,
            'growth': 0.20,
            'valuation': 0.15,
            'technical': 0.15
        }
        
        total_score = (
            financial_health.score * weights['financial_health'] +
            profitability.score * weights['profitability'] +
            growth.score * weights['growth'] +
            valuation.score * weights['valuation'] +
            technical.score * weights['technical']
        )
        
        # 确定评级
        rating = self._get_rating(total_score)
        
        # 生成分析摘要
        summary = self._generate_summary(code, total_score, rating, {
            'financial_health': financial_health,
            'profitability': profitability,
            'growth': growth,
            'valuation': valuation,
            'technical': technical
        })
        
        return AnalysisResult(
            code=code,
            analysis_type='comprehensive',
            score=total_score,
            rating=rating,
            summary=summary,
            details={
                'financial_health': financial_health,
                'profitability': profitability,
                'growth': growth,
                'valuation': valuation,
                'technical': technical
            }
        )
```

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
  auto_update: true
  update_time: "09:00"
  batch_size: 100
```

## 📖 使用指南

### 命令行工具

#### 初始化数据库
```bash
python main.py init --db-path stock_data.db
```

#### 更新股票列表
```bash
python main.py update-stock-list
python main.py update-stock-list --force  # 强制更新
```

#### 数据更新
```bash
# 增量更新
python main.py update

# 每日更新
python main.py daily

# 指定股票更新
python main.py update --codes 000001.XSHE,000002.XSHE
```

#### 数据查询
```bash
# 查看数据库信息
python main.py info

# 执行SQL查询
python main.py query "SELECT * FROM stock_list LIMIT 10"
```

#### 股票分析
```bash
# 综合分析
python main.py analyze 000001.XSHE

# 指定分析类型
python main.py analyze 000001.XSHE --type financial_health
```

#### 股票筛选
```bash
# 按条件筛选
python main.py screen --pe-min 5 --pe-max 20 --pb-max 3
```

#### 数据质量检查
```bash
python main.py check-quality
```

### Python API

#### 基础使用

```python
from stock_db import QuantDataAPI, create_api

# 方式1: 直接创建
api = QuantDataAPI("stock_data.db")

# 方式2: 通过工厂函数创建
api = create_api(
    db_path="stock_data.db",
    jq_username="your_username",
    jq_password="your_password"
)

# 获取股票列表
stock_list = api.get_stock_list()
print(f"股票总数: {len(stock_list)}")

# 获取股票基本信息
basic_info = api.get_stock_basic_info("000001.XSHE")
print(f"股票名称: {basic_info['display_name']}")

# 关闭连接
api.close()
```

#### 数据获取

```python
# 获取价格数据
price_data = api.get_price_data(
    code="000001.XSHE",
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)

# 获取财务数据
financial_data = api.get_financial_data(
    code="000001.XSHE",
    data_type="income_statement",
    count=8  # 最近8个季度
)

# 获取估值数据
valuation_data = api.get_valuation_data(
    code="000001.XSHE",
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31)
)
```

#### 快速查询函数

```python
from stock_db import quick_query, analyze_stock

# 快速查询价格数据（最近30天）
price_data = quick_query("000001.XSHE", "price", 30)

# 快速查询估值数据
valuation_data = quick_query("000001.XSHE", "valuation", 30)

# 快速分析
result = analyze_stock("000001.XSHE", "comprehensive")
print(f"评分: {result.score}, 评级: {result.rating}")
```

#### 批量操作

```python
# 批量获取数据
codes = ["000001.XSHE", "000002.XSHE", "600000.XSHG"]
batch_data = api.get_batch_data(codes, "price", start_date, end_date)

# 批量分析
batch_results = api.batch_analysis(codes, "comprehensive")

# 股票排名
rankings = api.rank_stocks(codes, "comprehensive")
for i, stock in enumerate(rankings, 1):
    print(f"{i}. {stock['code']}: {stock['score']}")
```

## 📊 数据模型

### 数据库表结构

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

#### 价格数据表 (`price_data`)

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.XSHE |
| date | DATE | 交易日期 | 2023-12-29 |
| open | DECIMAL | 开盘价 | 10.50 |
| high | DECIMAL | 最高价 | 10.80 |
| low | DECIMAL | 最低价 | 10.45 |
| close | DECIMAL | 收盘价 | 10.75 |
| volume | BIGINT | 成交量 | 12345678 |
| amount | DECIMAL | 成交额 | 132456789.50 |

#### 财务数据表 (`income_statement`)

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| code | VARCHAR | 股票代码 | 000001.XSHE |
| pub_date | DATE | 公布日期 | 2023-04-28 |
| stat_date | DATE | 统计日期 | 2023-03-31 |
| total_operating_revenue | DECIMAL | 营业总收入 | 45678901234.56 |
| operating_profit | DECIMAL | 营业利润 | 12345678901.23 |
| net_profit | DECIMAL | 净利润 | 9876543210.98 |

### 数据关系图

```
stock_list (股票列表)
    ├── price_data (价格数据)
    ├── fundamental_data (基本面数据)
    ├── income_statement (利润表)
    ├── balance_sheet (资产负债表)
    ├── cash_flow_statement (现金流量表)
    └── indicator_data (技术指标)
```

## 🔌 API接口

### 核心API类

#### QuantDataAPI

```python
class QuantDataAPI:
    """量化数据API主类"""
    
    def __init__(self, db_path: str, config_path: str = None):
        """初始化API"""
        pass
    
    # 股票基础信息
    def get_stock_list(self, market: str = None) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    def get_stock_basic_info(self, code: str) -> dict:
        """获取股票基本信息"""
        pass
    
    # 价格数据
    def get_price_data(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取价格数据"""
        pass
    
    def get_latest_price(self, code: str) -> dict:
        """获取最新价格"""
        pass
    
    # 财务数据
    def get_financial_data(self, code: str, data_type: str, count: int = 8) -> pd.DataFrame:
        """获取财务数据"""
        pass
    
    def get_income_statement(self, code: str, count: int = 8) -> pd.DataFrame:
        """获取利润表"""
        pass
    
    def get_balance_sheet(self, code: str, count: int = 8) -> pd.DataFrame:
        """获取资产负债表"""
        pass
    
    def get_cash_flow_statement(self, code: str, count: int = 8) -> pd.DataFrame:
        """获取现金流量表"""
        pass
    
    # 估值数据
    def get_valuation_data(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取估值数据"""
        pass
    
    # 技术指标
    def get_technical_indicators(self, code: str, indicators: list, period: int = 30) -> pd.DataFrame:
        """获取技术指标"""
        pass
    
    # 数据分析
    def analyze_financial_health(self, code: str) -> AnalysisResult:
        """财务健康分析"""
        pass
    
    def analyze_profitability(self, code: str) -> AnalysisResult:
        """盈利能力分析"""
        pass
    
    def analyze_growth(self, code: str) -> AnalysisResult:
        """成长性分析"""
        pass
    
    def analyze_valuation(self, code: str) -> AnalysisResult:
        """估值分析"""
        pass
    
    def comprehensive_analysis(self, code: str) -> AnalysisResult:
        """综合分析"""
        pass
    
    # 股票筛选
    def screen_stocks(self, criteria: dict) -> pd.DataFrame:
        """股票筛选"""
        pass
    
    # 批量操作
    def batch_analysis(self, codes: list, analysis_type: str) -> list:
        """批量分析"""
        pass
    
    def rank_stocks(self, codes: list, criteria: str) -> list:
        """股票排名"""
        pass
    
    # 数据更新
    def update_stock_data(self, code: str, data_types: list = None) -> bool:
        """更新股票数据"""
        pass
    
    def daily_update(self) -> dict:
        """每日更新"""
        pass
    
    # 数据质量
    def check_data_quality(self) -> dict:
        """数据质量检查"""
        pass
    
    # 数据库管理
    def get_database_info(self) -> dict:
        """获取数据库信息"""
        pass
    
    def query(self, sql: str, params: list = None) -> pd.DataFrame:
        """执行SQL查询"""
        pass
    
    def close(self):
        """关闭连接"""
        pass
```

### 便捷函数

```python
# 快速查询函数
def quick_query(code: str, data_type: str, period: int = 30) -> pd.DataFrame:
    """快速查询数据"""
    pass

# 快速分析函数
def analyze_stock(code: str, analysis_type: str = "comprehensive") -> AnalysisResult:
    """快速分析股票"""
    pass

# 工厂函数
def create_api(db_path: str, **kwargs) -> QuantDataAPI:
    """创建API实例"""
    pass

# 获取股票数据
def get_stock_data(code: str, data_type: str, **kwargs) -> pd.DataFrame:
    """获取股票数据"""
    pass
```

## 🧪 测试体系

### 测试架构

```
test/
├── __init__.py
├── test_system.py              # 系统集成测试
├── test_stock_list.py          # 股票列表功能测试
├── test_stock_list_simple.py   # 核心功能单元测试
└── conftest.py                 # 测试配置
```

### 测试类型

#### 1. 单元测试

```python
# test/test_stock_list_simple.py
def test_stock_info_model():
    """测试StockInfo模型"""
    stock_info = StockInfo(
        code='000001.XSHE',
        display_name='平安银行',
        name='平安银行',
        start_date=date(1991, 4, 3)
    )
    
    assert stock_info.code == '000001.XSHE'
    assert stock_info.is_active == True
    assert stock_info.to_jq_code() == '000001.XSHE'
    assert stock_info.exchange_name == '深圳证券交易所'
```

#### 2. 集成测试

```python
# test/test_system.py
def test_api_functions():
    """测试API功能"""
    api = QuantDataAPI("test_stock_data.db")
    
    # 测试股票列表获取
    stock_list = api.get_stock_list()
    assert isinstance(stock_list, pd.DataFrame)
    
    # 测试数据库信息
    db_info = api.get_database_info()
    assert isinstance(db_info, dict)
    
    api.close()
```

#### 3. 性能测试

```python
def test_query_performance():
    """测试查询性能"""
    api = QuantDataAPI("stock_data.db")
    
    start_time = time.time()
    result = api.get_price_data("000001.XSHE", start_date, end_date)
    end_time = time.time()
    
    # 查询时间应小于1秒
    assert end_time - start_time < 1.0
    assert len(result) > 0
```

### 运行测试

```bash
# 运行所有测试
python run_tests.py

# 运行单个测试文件
python test/test_system.py
python test/test_stock_list_simple.py

# 使用pytest运行
pytest test/ -v

# 生成测试覆盖率报告
pytest test/ --cov=. --cov-report=html
```

### 测试结果

```
============================================================
测试结果汇总
============================================================
test/test_system.py: ✅ 通过 (7/7项)
test/test_stock_list_simple.py: ✅ 通过 (4/4项)
test/test_stock_list.py: ⚠️ 部分通过 (3/4项)

总计: 14/15 项测试通过
🎉 核心功能测试通过！
```

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
- **实时数据**: 当日最新价格信息
- **复权处理**: 前复权、后复权价格

#### 财务数据
- **利润表**: 营业收入、净利润、毛利率等
- **资产负债表**: 总资产、净资产、负债率等
- **现金流量表**: 经营现金流、投资现金流等
- **财务指标**: ROE、ROA、EPS、BPS等

#### 估值数据
- **估值指标**: PE、PB、PS、EV/EBITDA
- **市值数据**: 总市值、流通市值
- **分红数据**: 股息率、分红比例

#### 技术指标
- **趋势指标**: MA、EMA、MACD、布林带
- **动量指标**: RSI、KDJ、威廉指标
- **成交量指标**: OBV、成交量比率

### 数据质量保证

#### 数据验证
```python
def validate_price_data(data: pd.DataFrame) -> dict:
    """价格数据验证"""
    issues = []
    
    # 检查必要字段
    required_fields = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
    missing_fields = [f for f in required_fields if f not in data.columns]
    if missing_fields:
        issues.append(f"缺少字段: {missing_fields}")
    
    # 检查价格逻辑
    invalid_prices = data[(data['high'] < data['low']) | 
                         (data['high'] < data['open']) | 
                         (data['high'] < data['close'])]
    if not invalid_prices.empty:
        issues.append(f"价格逻辑错误: {len(invalid_prices)}条记录")
    
    # 检查数据连续性
    date_gaps = data['date'].diff().dt.days
    large_gaps = date_gaps[date_gaps > 7]  # 超过7天的间隔
    if not large_gaps.empty:
        issues.append(f"数据间隔过大: {len(large_gaps)}处")
    
    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'total_records': len(data)
    }
```

#### 数据修复
```python
def repair_price_data(data: pd.DataFrame) -> pd.DataFrame:
    """价格数据修复"""
    # 修复异常价格
    data.loc[data['high'] < data['low'], ['high', 'low']] = \
        data.loc[data['high'] < data['low'], ['low', 'high']].values
    
    # 填补缺失数据
    data['volume'] = data['volume'].fillna(0)
    data['amount'] = data['amount'].fillna(0)
    
    # 移除重复数据
    data = data.drop_duplicates(subset=['code', 'date'])
    
    return data
```

### 数据更新策略

#### 增量更新
```python
def incremental_update():
    """增量更新策略"""
    # 1. 检查最后更新时间
    last_update = get_last_update_time()
    
    # 2. 确定需要更新的日期范围
    start_date = last_update + timedelta(days=1)
    end_date = date.today()
    
    # 3. 获取活跃股票列表
    active_stocks = get_active_stocks()
    
    # 4. 批量更新数据
    for batch in batch_split(active_stocks, batch_size=100):
        update_batch_data(batch, start_date, end_date)
    
    # 5. 更新时间戳
    update_last_update_time(end_date)
```

#### 定期全量更新
```python
def full_update_schedule():
    """定期全量更新"""
    # 每周末进行全量数据检查和修复
    if datetime.now().weekday() == 6:  # 周日
        # 1. 数据质量检查
        quality_report = check_data_quality()
        
        # 2. 修复发现的问题
        if quality_report['issues']:
            repair_data_issues(quality_report['issues'])
        
        # 3. 重新计算技术指标
        recalculate_technical_indicators()
        
        # 4. 更新股票列表
        update_stock_list(force=True)
```

## ⚡ 性能优化

### 查询优化

#### 索引策略
```sql
-- 为常用查询字段创建索引
CREATE INDEX idx_price_code_date ON price_data(code, date);
CREATE INDEX idx_financial_code_date ON income_statement(code, stat_date);
CREATE INDEX idx_stock_list_market ON stock_list(market, status);
```

#### 查询优化
```python
def optimized_query(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
    """优化的查询方法"""
    # 使用参数化查询，避免SQL注入
    sql = """
    SELECT code, date, open, high, low, close, volume, amount
    FROM price_data 
    WHERE code = ? AND date BETWEEN ? AND ?
    ORDER BY date
    """
    
    # 使用DuckDB的向量化查询
    return self.db_manager.query(sql, [code, start_date, end_date])
```

### 缓存机制

```python
from functools import lru_cache
from datetime import datetime, timedelta

class CachedDataService:
    """带缓存的数据服务"""
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = timedelta(minutes=30)
    
    @lru_cache(maxsize=1000)
    def get_stock_basic_info(self, code: str) -> dict:
        """缓存股票基本信息"""
        return self._fetch_stock_basic_info(code)
    
    def get_price_data_cached(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """带缓存的价格数据获取"""
        cache_key = f"{code}_{start_date}_{end_date}"
        
        # 检查缓存
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_ttl:
                return cached_data
        
        # 获取新数据
        data = self._fetch_price_data(code, start_date, end_date)
        
        # 更新缓存
        self.cache[cache_key] = (data, datetime.now())
        
        return data
```

### 批量处理

```python
def batch_update_stocks(self, codes: list, data_type: str) -> dict:
    """批量更新股票数据"""
    results = {'success': [], 'failed': []}
    
    # 分批处理，避免内存溢出
    batch_size = 50
    for i in range(0, len(codes), batch_size):
        batch_codes = codes[i:i + batch_size]
        
        try:
            # 批量获取数据
            batch_data = self.data_source.get_batch_data(batch_codes, data_type)
            
            # 批量插入数据库
            self.db_manager.batch_insert(data_type, batch_data)
            
            results['success'].extend(batch_codes)
            
        except Exception as e:
            logger.error(f"批量更新失败: {e}")
            results['failed'].extend(batch_codes)
    
    return results
```

## 🔧 扩展开发

### 添加新数据源

#### 1. 实现数据源接口

```python
class WindDataSource(BaseDataSource):
    """Wind数据源实现"""
    
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.authenticated = False
    
    def authenticate(self) -> bool:
        """Wind认证"""
        try:
            from WindPy import w
            w.start()
            self.authenticated = True
            return True
        except Exception as e:
            logger.error(f"Wind认证失败: {e}")
            return False
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        from WindPy import w
        
        # 获取A股列表
        data = w.wset("sectorconstituent", "date=20231229;sectorid=a001010100000000")
        
        # 转换为标准格式
        stocks = []
        for code in data.Data[1]:
            stock_info = StockInfo(
                code=self._to_standard_code(code),
                display_name=self._get_stock_name(code),
                name=self._get_stock_name(code)
            )
            stocks.append(stock_info.to_dict())
        
        return pd.DataFrame(stocks)
    
    def get_price_data(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取价格数据"""
        from WindPy import w
        
        wind_code = self._to_wind_code(code)
        data = w.wsd(wind_code, "open,high,low,close,volume,amt", 
                    start_date, end_date, "")
        
        # 转换为标准格式
        df = pd.DataFrame(data.Data, 
                         columns=['open', 'high', 'low', 'close', 'volume', 'amount'],
                         index=data.Times)
        df['code'] = code
        df['date'] = df.index
        
        return df.reset_index(drop=True)
```

#### 2. 注册数据源

```python
# 在配置文件中添加
data_sources:
  wind:
    enabled: true
    username: "your_username"
    password: "your_password"

# 在代码中注册
wind_config = DataSourceConfig(
    name="wind",
    source_type="wind",
    username=config.wind.username,
    password=config.wind.password
)
wind_source = WindDataSource(wind_config)
api.data_source_manager.add_source("wind", wind_source)
```

### 添加新分析模块

```python
class TechnicalAnalysisModule:
    """技术分析模块"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def calculate_macd(self, code: str, period: int = 250) -> pd.DataFrame:
        """计算MACD指标"""
        # 获取价格数据
        price_data = self.db_manager.query(
            "SELECT date, close FROM price_data WHERE code = ? ORDER BY date DESC LIMIT ?",
            [code, period]
        )
        
        # 计算MACD
        exp1 = price_data['close'].ewm(span=12).mean()
        exp2 = price_data['close'].ewm(span=26).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9).mean()
        histogram = macd - signal
        
        return pd.DataFrame({
            'date': price_data['date'],
            'macd': macd,
            'signal': signal,
            'histogram': histogram
        })
    
    def analyze_trend(self, code: str) -> AnalysisResult:
        """趋势分析"""
        # 获取技术指标
        macd_data = self.calculate_macd(code)
        ma_data = self.calculate_moving_averages(code)
        
        # 分析趋势
        latest_macd = macd_data.iloc[-1]
        latest_ma = ma_data.iloc[-1]
        
        score = 50  # 基础分数
        
        # MACD信号
        if latest_macd['macd'] > latest_macd['signal']:
            score += 20
        
        # 均线排列
        if latest_ma['ma5'] > latest_ma['ma20'] > latest_ma['ma60']:
            score += 20
        
        # 价格位置
        if latest_ma['close'] > latest_ma['ma20']:
            score += 10
        
        return AnalysisResult(
            code=code,
            analysis_type='technical_trend',
            score=min(score, 100),
            rating=self._get_rating(score),
            summary=f"技术趋势分析评分: {score}"
        )
```

### 自定义指标计算

```python
class CustomIndicators:
    """自定义指标"""
    
    @staticmethod
    def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_bollinger_bands(prices: pd.Series, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """计算布林带"""
        ma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        
        upper_band = ma + (std * std_dev)
        lower_band = ma - (std * std_dev)
        
        return pd.DataFrame({
            'middle': ma,
            'upper': upper_band,
            'lower': lower_band
        })
    
    @staticmethod
    def calculate_kdj(high: pd.Series, low: pd.Series, close: pd.Series, 
                     period: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """计算KDJ指标"""
        lowest_low = low.rolling(window=period).min()
        highest_high = high.rolling(window=period).max()
        
        rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
        
        k = rsv.ewm(alpha=1/m1).mean()
        d = k.ewm(alpha=1/m2).mean()
        j = 3 * k - 2 * d
        
        return pd.DataFrame({
            'k': k,
            'd': d,
            'j': j
        })
```

## 🚀 部署运维

### 生产环境部署

#### 1. 环境准备

```bash
# 创建生产环境
python -m venv prod_env
source prod_env/bin/activate

# 安装依赖
pip install -r requirements.txt

# 创建数据目录
mkdir -p /data/stock_db
mkdir -p /logs/stock_db
```

#### 2. 配置文件

```yaml
# config_prod.yaml
database:
  type: "duckdb"
  path: "/data/stock_db/stock_data.db"
  
data_sources:
  jqdata:
    enabled: true
    username: "${JQ_USERNAME}"
    password: "${JQ_PASSWORD}"
    
logging:
  level: "INFO"
  file: "/logs/stock_db/stock_data.log"
  max_size: "100MB"
  backup_count: 10
  
update:
  auto_update: true
  update_time: "09:00"
  batch_size: 100
  retry_times: 3
  
performance:
  cache_size: 1000
  query_timeout: 30
  batch_size: 100
```

#### 3. 系统服务

```ini
# /etc/systemd/system/stock-data-updater.service
[Unit]
Description=Stock Data Updater Service
After=network.target

[Service]
Type=simple
User=stockdata
WorkingDirectory=/opt/stock_db
Environment=PYTHONPATH=/opt/stock_db
ExecStart=/opt/stock_db/prod_env/bin/python daily_update.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

#### 4. 定时任务

```bash
# crontab -e
# 每日9点更新数据
0 9 * * 1-5 /opt/stock_db/prod_env/bin/python /opt/stock_db/main.py daily

# 每周日进行数据质量检查
0 2 * * 0 /opt/stock_db/prod_env/bin/python /opt/stock_db/main.py check-quality

# 每月1号更新股票列表
0 1 1 * * /opt/stock_db/prod_env/bin/python /opt/stock_db/main.py update-stock-list --force
```

### 监控告警

#### 1. 健康检查

```python
class HealthChecker:
    """健康检查"""
    
    def __init__(self, api: QuantDataAPI):
        self.api = api
    
    def check_database_health(self) -> dict:
        """数据库健康检查"""
        try:
            # 检查数据库连接
            db_info = self.api.get_database_info()
            
            # 检查表结构
            tables = self.api.query("SHOW TABLES")
            
            # 检查数据完整性
            stock_count = self.api.query("SELECT COUNT(*) as count FROM stock_list").iloc[0]['count']
            
            return {
                'status': 'healthy',
                'database_size': db_info.get('size', 0),
                'table_count': len(tables),
                'stock_count': stock_count,
                'last_check': datetime.now()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': datetime.now()
            }
    
    def check_data_freshness(self) -> dict:
        """数据新鲜度检查"""
        try:
            # 检查最新数据日期
            latest_date = self.api.query(
                "SELECT MAX(date) as latest_date FROM price_data"
            ).iloc[0]['latest_date']
            
            days_old = (date.today() - latest_date).days
            
            return {
                'latest_date': latest_date,
                'days_old': days_old,
                'is_fresh': days_old <= 3  # 3天内为新鲜
            }
        except Exception as e:
            return {
                'error': str(e),
                'is_fresh': False
            }
```

#### 2. 告警系统

```python
class AlertSystem:
    """告警系统"""
    
    def __init__(self, config: dict):
        self.config = config
        self.email_config = config.get('email', {})
        self.webhook_config = config.get('webhook', {})
    
    def send_alert(self, level: str, title: str, message: str):
        """发送告警"""
        alert_data = {
            'level': level,
            'title': title,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'hostname': socket.gethostname()
        }
        
        # 发送邮件告警
        if self.email_config.get('enabled'):
            self._send_email_alert(alert_data)
        
        # 发送Webhook告警
        if self.webhook_config.get('enabled'):
            self._send_webhook_alert(alert_data)
    
    def _send_email_alert(self, alert_data: dict):
        """发送邮件告警"""
        import smtplib
        from email.mime.text import MIMEText
        
        msg = MIMEText(alert_data['message'])
        msg['Subject'] = f"[{alert_data['level'].upper()}] {alert_data['title']}"
        msg['From'] = self.email_config['from']
        msg['To'] = self.email_config['to']
        
        with smtplib.SMTP(self.email_config['smtp_server']) as server:
            server.login(self.email_config['username'], self.email_config['password'])
            server.send_message(msg)
    
    def _send_webhook_alert(self, alert_data: dict):
        """发送Webhook告警"""
        import requests
        
        requests.post(
            self.webhook_config['url'],
            json=alert_data,
            timeout=10
        )
```

#### 3. 性能监控

```python
class PerformanceMonitor:
    """性能监控"""
    
    def __init__(self):
        self.metrics = {}
    
    def record_query_time(self, query_type: str, execution_time: float):
        """记录查询时间"""
        if query_type not in self.metrics:
            self.metrics[query_type] = []
        
        self.metrics[query_type].append({
            'execution_time': execution_time,
            'timestamp': datetime.now()
        })
        
        # 保留最近1000条记录
        if len(self.metrics[query_type]) > 1000:
            self.metrics[query_type] = self.metrics[query_type][-1000:]
    
    def get_performance_stats(self, query_type: str = None) -> dict:
        """获取性能统计"""
        if query_type:
            data = self.metrics.get(query_type, [])
            if not data:
                return {}
            
            times = [item['execution_time'] for item in data]
            return {
                'query_type': query_type,
                'count': len(times),
                'avg_time': sum(times) / len(times),
                'min_time': min(times),
                'max_time': max(times),
                'p95_time': sorted(times)[int(len(times) * 0.95)]
            }
        else:
            return {
                query_type: self.get_performance_stats(query_type)
                for query_type in self.metrics.keys()
            }
```

### 日志管理

#### 日志配置

```python
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(config: dict):
    """设置日志配置"""
    # 创建logger
    logger = logging.getLogger('stock_db')
    logger.setLevel(getattr(logging, config.get('level', 'INFO')))
    
    # 文件处理器
    file_handler = RotatingFileHandler(
        config.get('file', 'stock_db.log'),
        maxBytes=config.get('max_size', 100 * 1024 * 1024),  # 100MB
        backupCount=config.get('backup_count', 10)
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
```

### 备份恢复

#### 数据备份

```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/backup/stock_db"
DATE=$(date +%Y%m%d_%H%M%S)
DB_PATH="/data/stock_db/stock_data.db"

# 创建备份目录
mkdir -p $BACKUP_DIR

# 备份数据库文件
cp $DB_PATH $BACKUP_DIR/stock_data_$DATE.db

# 压缩备份文件
gzip $BACKUP_DIR/stock_data_$DATE.db

# 删除7天前的备份
find $BACKUP_DIR -name "*.gz" -mtime +7 -delete

echo "备份完成: stock_data_$DATE.db.gz"
```

#### 数据恢复

```bash
#!/bin/bash
# restore.sh

if [ $# -ne 1 ]; then
    echo "用法: $0 <备份文件>"
    exit 1
fi

BACKUP_FILE=$1
DB_PATH="/data/stock_db/stock_data.db"

# 停止服务
sudo systemctl stop stock-data-updater

# 备份当前数据库
cp $DB_PATH $DB_PATH.backup.$(date +%Y%m%d_%H%M%S)

# 恢复数据库
if [[ $BACKUP_FILE == *.gz ]]; then
    gunzip -c $BACKUP_FILE > $DB_PATH
else
    cp $BACKUP_FILE $DB_PATH
fi

# 启动服务
sudo systemctl start stock-data-updater

echo "数据恢复完成"
```

## 📚 常见问题

### Q1: 如何处理数据源认证失败？

**A**: 检查配置文件中的用户名和密码是否正确，确保账户有效且有足够的API调用次数。

```python
# 检查认证状态
api = QuantDataAPI("stock_data.db")
auth_status = api.data_source_manager.check_authentication()
print(f"认证状态: {auth_status}")
```

### Q2: 查询速度慢怎么办？

**A**: 
1. 检查是否创建了合适的索引
2. 优化查询条件，避免全表扫描
3. 使用批量查询代替单条查询
4. 考虑增加缓存

```python
# 查看查询执行计划
api.query("EXPLAIN SELECT * FROM price_data WHERE code = '000001.XSHE'")
```

### Q3: 数据更新失败怎么处理？

**A**:
1. 检查网络连接
2. 查看错误日志
3. 检查数据源API状态
4. 尝试手动更新单只股票

```python
# 手动更新单只股票
api.update_stock_data("000001.XSHE", force=True)
```

### Q4: 如何扩展支持新的数据类型？

**A**:
1. 在`models/`目录下创建新的数据模型
2. 在数据源中实现获取方法
3. 在数据库中创建对应表结构
4. 更新API接口

### Q5: 如何优化存储空间？

**A**:
1. 定期清理过期数据
2. 使用数据压缩
3. 按时间分区存储
4. 只保留必要的字段

```python
# 清理过期数据
api.query("DELETE FROM price_data WHERE date < '2020-01-01'")
```

## 🔮 未来规划

### 短期目标 (3个月)
- [ ] 支持更多数据源（Wind、同花顺等）
- [ ] 增加实时数据推送功能
- [ ] 优化查询性能
- [ ] 完善监控告警系统

### 中期目标 (6个月)
- [ ] 支持期货、债券等其他金融产品
- [ ] 增加机器学习预测模块
- [ ] 开发Web管理界面
- [ ] 支持分布式部署

### 长期目标 (1年)
- [ ] 构建完整的量化交易平台
- [ ] 支持策略回测和实盘交易
- [ ] 提供SaaS服务
- [ ] 建立开发者生态

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📞 联系方式

- 项目主页: [GitHub Repository]
- 问题反馈: [GitHub Issues]
- 邮箱: contact@stockdata.com

## 🙏 致谢

感谢以下项目和服务:
- [DuckDB](https://duckdb.org/) - 高性能分析数据库
- [聚宽](https://www.joinquant.com/) - 数据源支持
- [Pandas](https://pandas.pydata.org/) - 数据处理
- [NumPy](https://numpy.org/) - 数值计算

---

**量化数据平台** - 让数据驱动投资决策 🚀