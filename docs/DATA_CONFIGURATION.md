# 数据目录配置说明

## 概述

为了实现数据与代码分离，本项目支持自定义数据存储目录。所有数据库文件、缓存文件等都可以存储在独立的数据目录中，便于数据管理和备份。

## 默认配置

- **默认数据根目录**: `D:/Data/stock_db`
- **数据库文件**: `D:/Data/stock_db/stock_data.duckdb`
- **测试数据库**: `D:/Data/stock_db/test_stock_data.duckdb`
- **日志文件**: `项目根目录/logs/stock_data.log`

## 配置方式

### 方式1：环境变量配置（推荐）

1. 复制 `.env.example` 为 `.env`
2. 设置数据根目录：
   ```bash
   STOCK_DATA_ROOT=D:/Data/stock_db
   ```
3. 可选：设置具体的数据库路径：
   ```bash
   STOCK_DB_PATH=stock_data.duckdb  # 相对于数据根目录
   # 或
   STOCK_DB_PATH=D:/Data/stock_db/custom_db.duckdb  # 绝对路径
   ```

### 方式2：配置文件

1. 复制 `config.example.yaml` 为 `config.yaml`
2. 修改数据库路径配置：
   ```yaml
   database:
     path: "stock_data.duckdb"  # 相对于数据根目录
   ```

### 方式3：代码中指定

```python
from api import create_api

# 使用默认配置（基于环境变量或默认路径）
api = create_api()

# 或指定具体路径
api = create_api(db_path="D:/Data/stock_db/custom_db.duckdb")
```

## 配置优先级

路径解析的优先级顺序：

1. **代码中直接指定的绝对路径**
2. **环境变量 `STOCK_DB_PATH`**（如果是绝对路径）
3. **环境变量 `STOCK_DB_PATH`**（相对于 `STOCK_DATA_ROOT`）
4. **配置文件中的路径**（相对于 `STOCK_DATA_ROOT`）
5. **默认路径**：`D:/Data/stock_db/stock_data.duckdb`

数据根目录的优先级：

1. **环境变量 `STOCK_DATA_ROOT`**
2. **默认路径**：`D:/Data/stock_db`

## 目录结构示例

```
D:/Data/stock_db/          # 数据根目录
├── stock_data.duckdb      # 主数据库文件
├── test_stock_data.duckdb # 测试数据库文件
├── cache/                 # 缓存目录（如果需要）
└── backups/              # 备份目录（如果需要）

项目根目录/
├── logs/                  # 日志目录
│   └── stock_data.log
├── config.yaml           # 配置文件
├── .env                  # 环境变量文件
└── ...
```

## 迁移现有数据

如果你已经有现有的数据文件，可以按以下步骤迁移：

1. 创建新的数据目录：
   ```bash
   mkdir -p D:/Data/stock_db
   ```

2. 移动现有数据文件：
   ```bash
   # 如果存在 data/stock_data.duckdb
   move data/stock_data.duckdb D:/Data/stock_db/
   ```

3. 设置环境变量或配置文件

4. 验证配置：
   ```python
   from config import get_config
   config = get_config()
   print(f"数据库路径: {config.database.path}")
   ```

## 注意事项

1. **权限**: 确保应用程序对数据目录有读写权限
2. **备份**: 建议定期备份数据目录
3. **路径分隔符**: 在Windows上建议使用正斜杠 `/` 或双反斜杠 `\\`
4. **相对路径**: 相对路径总是相对于数据根目录解析
5. **自动创建**: 如果数据目录不存在，程序会自动创建

## 常见问题

### Q: 如何查看当前使用的数据路径？

A: 可以通过以下代码查看：
```python
from config import get_config
config = get_config()
print(f"数据库路径: {config.database.path}")
print(f"数据根目录: {config.get_data_root()}")
```

### Q: 可以在不同环境使用不同的数据目录吗？

A: 可以，通过设置不同的环境变量 `STOCK_DATA_ROOT` 即可。

### Q: 如何在多个项目间共享数据？

A: 将多个项目的 `STOCK_DATA_ROOT` 设置为同一个目录即可。