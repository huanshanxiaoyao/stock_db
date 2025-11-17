import duckdb
import sys
sys.path.append('..')
from config import get_config

def check_database_tables():
    # 获取数据库路径并连接
    config = get_config()
    db_path = config.database.path
    conn = duckdb.connect(db_path)
    
    # 获取所有表名
    tables = conn.execute("SHOW TABLES").fetchall()
    
    print("数据库中的表:")
    for table in tables:
        table_name = table[0]
        print(f"\n表名: {table_name}")
        
        # 获取表结构
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        print("列信息:")
        for col in columns:
            print(f"  {col[0]} ({col[1]})")
        
        # 获取记录数
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"记录数: {count}")
        
        # 如果是价格相关的表，显示一些样本数据
        if 'price' in table_name.lower() or 'indicator' in table_name.lower() or 'market' in table_name.lower():
            samples = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            print("样本数据:")
            for sample in samples:
                print(f"  {sample}")
    
    conn.close()

if __name__ == "__main__":
    check_database_tables()