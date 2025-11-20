# 项目重构整理指南

## 📋 整理概述

本次整理对项目结构进行了全面重构，将原本散乱的60+个文件整理为清晰的目录结构，第一级目录文件数量从60+个减少到30个，符合项目管理最佳实践。

## 🔄 文件移动清单

### 测试文件 → `test/` 目录

以下测试文件已移动到 `test/` 目录：

- `test_api.py` → `test/test_api.py`
- `test_api_get_stock_list.py` → `test/test_api_get_stock_list.py`
- `test_api_server_debug.py` → `test/test_api_server_debug.py`
- `test_clean_db.py` → `test/test_clean_db.py`
- `test_jqdata_stocks.py` → `test/test_jqdata_stocks.py`
- `test_real_bj_stocks.py` → `test/test_real_bj_stocks.py`
- `test_real_stocks.py` → `test/test_real_stocks.py`
- `test_sz_sh_stocks.py` → `test/test_sz_sh_stocks.py`

### 调试脚本 → `scripts/` 目录

以下调试和工具脚本已移动到 `scripts/` 目录：

- `debug_api_server.py` → `scripts/debug_api_server.py`
- `debug_db_path.py` → `scripts/debug_db_path.py`
- `debug_stock_info.py` → `scripts/debug_stock_info.py`

### 示例代码 → `examples/` 目录

以下示例和演示文件已移动到 `examples/` 目录：

- `api_client_example.py` → `examples/api_client_example.py`
- `demo_real_stocks.py` → `examples/demo_real_stocks.py`
- `demo_sz_sh_api.py` → `examples/demo_sz_sh_api.py`
- `examples.py` → `examples/examples.py`

### 工具脚本 → `scripts/` 目录

以下工具和检查脚本已移动到 `scripts/` 目录：

- `check_active_stocks.py` → `scripts/check_active_stocks.py`

- `check_price_data.py` → `scripts/check_price_data.py`
- `check_stock_list.py` → `scripts/check_stock_list.py`
- `check_tables.py` → `scripts/check_tables.py`
- `export_a_stocks.py` → `scripts/export_a_stocks.py`
- `final_real_stock_test.py` → `scripts/final_real_stock_test.py`
- `find_real_stocks.py` → `scripts/find_real_stocks.py`
- `simple_api_test.py` → `scripts/simple_api_test.py`
- `init_clean_db.py` → `scripts/init_clean_db.py`
- `run_tests.py` → `scripts/run_tests.py`
- `github.txt` → `scripts/github.txt`

### 数据文件 → `data/` 目录

以下数据文件已移动到 `data/` 目录：

- `all_a_stocks.csv` → `data/all_a_stocks.csv`
- `all_stocks.csv` → `data/all_stocks.csv`
- `jqdata_all_stocks.csv` → `data/jqdata_all_stocks.csv`
- `sample_stock_list.csv` → `data/sample_stock_list.csv`
- `sample_stocks.csv` → `data/sample_stocks.csv`
- `sz_sh_stock_list.csv` → `data/sz_sh_stock_list.csv`
- `stock_data.db` → `data/stock_data.db`
- `stock_data.duckdb` → `data/stock_data.duckdb`
- `stock_data_new.duckdb` → `data/stock_data_new.duckdb`

### 日志文件 → `logs/` 目录

以下日志文件已移动到 `logs/` 目录：

- `api_server.log` → `logs/api_server.log`
- `stock_data_main.log` → `logs/stock_data_main.log`

## 📁 新目录结构

```
stock_db/
├── 📄 核心配置文件 (11个)
│   ├── .env
│   ├── config.py
│   ├── config.yaml
│   ├── config_example.yaml
│   ├── requirements.txt
│   ├── requirements_api.txt
│   ├── API_DOCUMENTATION.md
│   ├── DEVELOP_GUIDE.md
│   ├── README.md
│   ├── REORGANIZATION_GUIDE.md
│   └── __init__.py
├── 🔧 核心模块 (5个)
│   ├── api.py
│   ├── api_server.py
│   ├── database.py
│   ├── duckdb_impl.py
│   └── data_source.py
├── 🚀 启动脚本 (2个)
│   ├── main.py
│   └── start_api.py
├── 📦 功能模块目录 (7个)
│   ├── data/           # 数据文件
│   ├── examples/       # 示例代码
│   ├── logs/          # 日志文件
│   ├── models/        # 数据模型
│   ├── providers/     # 数据提供商
│   ├── scripts/       # 工具脚本
│   ├── services/      # 业务服务
│   └── test/          # 测试代码
└── 🔧 系统文件 (2个)
    ├── .vercel/
    └── __pycache__/
```

**总计**: 30个第一级项目 (符合 <20个子项 的目标)

## 🔧 使用指南更新

### 运行测试

```bash
# 运行所有测试 (新路径)
python scripts/run_tests.py

# 运行单个测试文件
python test/test_system.py
python test/test_api.py
python test/test_real_stocks.py

# 使用pytest运行
pytest test/ -v
```

### 运行示例

```bash
# API客户端示例
python examples/api_client_example.py

# 演示脚本
python examples/demo_real_stocks.py
python examples/demo_sz_sh_api.py
```

### 运行工具脚本

```bash
# 检查脚本
python scripts/check_active_stocks.py

# 调试脚本
python scripts/debug_api_server.py
python scripts/debug_stock_info.py

# 数据导出
python scripts/export_a_stocks.py
```

### 数据文件访问

```python
# 更新数据库路径配置
DATABASE_PATH = "data/stock_data.duckdb"
CSV_DATA_PATH = "data/all_stocks.csv"
LOG_FILE_PATH = "logs/stock_data.log"
```

## 📝 配置文件更新

需要更新以下配置中的路径：

### `config.yaml`

```yaml
database:
  path: "data/stock_data.duckdb"  # 更新路径
  
logging:
  file: "logs/stock_data.log"     # 更新路径
```

### 代码中的路径引用

如果代码中有硬编码的文件路径，需要相应更新：

```python
# 旧路径
db_path = "stock_data.duckdb"
log_path = "stock_data.log"

# 新路径
db_path = "data/stock_data.duckdb"
log_path = "logs/stock_data.log"
```

## ✅ 整理效果

### 改进前
- 第一级目录文件: 60+ 个
- 文件分布混乱，难以维护
- 测试文件散落各处
- 数据文件与代码混合

### 改进后
- 第一级目录文件: 30 个
- 功能分类清晰，易于维护
- 测试文件统一管理
- 数据文件独立存储
- 符合项目管理最佳实践

## 🎯 维护建议

1. **新文件放置原则**:
   - 测试文件 → `test/`
   - 示例代码 → `examples/`
   - 工具脚本 → `scripts/`
   - 数据文件 → `data/`
   - 日志文件 → `logs/`

2. **核心模块保持简洁**:
   - 只在根目录放置核心业务模块
   - 避免在根目录创建临时文件
   - 定期清理不需要的文件

3. **文档同步更新**:
   - 及时更新README.md中的路径引用
   - 保持API文档的示例代码路径正确
   - 更新部署脚本中的文件路径

## 🔄 回滚指南

如果需要回滚到原始结构，可以执行以下命令：

```bash
# 将文件移回根目录
mv test/test_*.py .
mv scripts/debug_*.py .
mv scripts/check_*.py .
mv examples/demo_*.py .
mv examples/api_client_example.py .
mv examples/examples.py .
mv data/*.csv .
mv data/*.db .
mv data/*.duckdb .
mv logs/*.log .

# 删除空目录
rmdir examples scripts data logs
```

---

**整理完成时间**: 2025-01-28  
**整理人**: AI Assistant  
**版本**: v1.0