"""数据质量检查工具函数"""

def validate_sql_readonly(sql: str) -> bool:
    """验证SQL是否为只读操作"""
    sql_upper = sql.upper().strip()

    # 禁止的关键词
    forbidden_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'TRUNCATE', 'REPLACE', 'MERGE', 'CALL', 'EXEC'
    ]

    for keyword in forbidden_keywords:
        if keyword in sql_upper:
            raise ValueError(f"检测到非只读SQL操作: {keyword}")

    # 只允许SELECT和WITH语句
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        raise ValueError("只允许SELECT和WITH查询语句")

    return True