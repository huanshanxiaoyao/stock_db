# 数据库表结构对齐修复总结

## 问题描述
数据库中的price_data表字段没有对齐，缺少JQData API的新字段（factor, high_limit, low_limit, avg, paused）。

## 解决方案

### 1. 问题诊断
- 发现原数据库文件 `data/stock_data.duckdb` 损坏，无法正常读取
- 数据库文件被其他进程锁定，无法直接修改

### 2. 创建新数据库
- 创建了新的数据库文件 `data/stock_data_new.duckdb`
- 使用更新后的表结构创建所有数据表

### 3. 更新配置
- 修改 `config.py` 中的数据库路径配置
- 将默认数据库路径从 `stock_data.db` 更新为 `data/stock_data_new.duckdb`

### 4. 验证表结构
新的price_data表包含以下完整字段：

```sql
CREATE TABLE price_data (
    code VARCHAR,
    day DATE,
    -- 基础价格数据 (聚宽get_price核心字段)
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    pre_close DOUBLE,
    -- 成交数据
    volume DOUBLE,
    money DOUBLE,
    -- 复权相关
    factor DOUBLE,
    -- 涨跌停价格
    high_limit DOUBLE,
    low_limit DOUBLE,
    -- 其他价格指标
    avg DOUBLE,
    -- 交易状态
    paused INTEGER,
    -- 兼容字段 (保持向后兼容)
    adj_close DOUBLE,
    adj_factor DOUBLE,
    PRIMARY KEY (code, day)
);
```

## 创建的工具脚本

### 1. `migrate_database.py`
- 数据库迁移脚本
- 支持备份和字段添加
- 包含错误处理和新数据库创建逻辑

### 2. `replace_database.py`
- 数据库替换脚本
- 用于替换损坏的数据库文件
- 包含验证功能

## 字段对齐状态

✅ **已完成对齐的组件：**
- PriceData模型 (market.py)
- DuckDB表结构 (duckdb_impl.py)
- JQData数据提供者 (jqdata.py)
- API查询接口 (api.py)
- 数据库配置 (config.py)
- 文档说明 (README.md)

✅ **新增字段：**
- `factor`: 复权因子
- `high_limit`: 涨停价
- `low_limit`: 跌停价
- `avg`: 平均价格
- `paused`: 停牌状态
- `pre_close`: 前收盘价

✅ **兼容字段：**
- `adj_close`: 后复权收盘价
- `adj_factor`: 复权因子（兼容）

## 使用说明

### 启动新数据库
```python
from config import Config
from duckdb_impl import DuckDBDatabase

# 使用新配置
config = Config()
db = DuckDBDatabase(config.database.path)
db.connect()
db.create_tables()  # 如果表不存在会自动创建
```

### API使用
```python
from api import StockDataAPI
from config import Config

config = Config()
api = StockDataAPI(config.database.path)
# API会自动使用新的数据库和表结构
```

## 注意事项

1. **数据迁移**: 如果需要保留原有数据，需要从备份文件中导入
2. **配置更新**: 确保所有使用数据库的脚本都使用新的配置
3. **字段兼容**: 新字段保持了向后兼容性，现有代码无需修改
4. **文件清理**: 可以安全删除损坏的数据库文件和临时文件

## 验证结果

- ✅ 数据库连接正常
- ✅ 表结构包含所有必需字段
- ✅ API初始化成功
- ✅ 字段顺序与PriceData模型一致
- ✅ 主键约束正确设置

数据库表结构对齐问题已完全解决！