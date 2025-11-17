import duckdb
import pandas as pd
from pathlib import Path
import sys
sys.path.append('..')
from config import get_config

# 获取数据库路径
config = get_config()
db_path = config.database.path
conn = duckdb.connect(db_path)

# 查看股票列表表结构
print("=== 股票列表表结构 ===")
try:
    result = conn.execute("DESCRIBE stock_list").fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")
except Exception as e:
    print(f"查看表结构失败: {e}")

# 查看A股股票总数（不含北交所）
print("\n=== A股股票统计 ===")
try:
    result = conn.execute("SELECT COUNT(*) FROM stock_list WHERE code NOT LIKE '%.BJ'").fetchone()
    print(f"A股股票总数（不含北交所）: {result[0]}")
    
    # 查看沪深股票分布
    sh_count = conn.execute("SELECT COUNT(*) FROM stock_list WHERE code LIKE '%.SH'").fetchone()[0]
    sz_count = conn.execute("SELECT COUNT(*) FROM stock_list WHERE code LIKE '%.SZ'").fetchone()[0]
    bj_count = conn.execute("SELECT COUNT(*) FROM stock_list WHERE code LIKE '%.BJ'").fetchone()[0]
    
    print(f"上海股票数量: {sh_count}")
    print(f"深圳股票数量: {sz_count}")
    print(f"北交所股票数量: {bj_count}")
    
except Exception as e:
    print(f"查询股票数量失败: {e}")

# 查看前10个股票代码示例
print("\n=== 股票代码示例 ===")
try:
    result = conn.execute("SELECT code, name FROM stock_list WHERE code NOT LIKE '%.BJ' LIMIT 10").fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")
except Exception as e:
    print(f"查询股票代码失败: {e}")

conn.close()