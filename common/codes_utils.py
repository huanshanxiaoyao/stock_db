import os
import pandas as pd

def load_bj_code_mapping():
    """
    加载北交所旧代码到新代码的映射关系
    优先从D:\Data\shared\BJ_old2new.csv读取，
    """
    mapping = {}
    
    # 尝试从CSV文件读取映射关系
    csv_path = r"D:\Data\shared\BJ_old2new.csv"
    try:
        if os.path.exists(csv_path):
            df_mapping = pd.read_csv(csv_path)
            # 假设CSV文件有两列：old_code, new_code
            if 'old_code' in df_mapping.columns and 'new_code' in df_mapping.columns:
                mapping = dict(zip(df_mapping['old_code'], df_mapping['new_code']))
                #print(f"从CSV文件加载了 {len(mapping)} 个北交所代码映射关系")
            else:
                # 如果列名不同，尝试使用前两列
                if len(df_mapping.columns) >= 2:
                    mapping = dict(zip(df_mapping.iloc[:, 0], df_mapping.iloc[:, 1]))
                    #print(f"从CSV文件加载了 {len(mapping)} 个北交所代码映射关系")
    except Exception as e:
        print(f"无法读取CSV文件 {csv_path}: {e}")
    
    return mapping

def convert2new_bj_code(old_code, mapping):
    """
    将北交所旧代码转换为新代码
    如果旧代码不在映射中，返回旧代码本身
    注意，输入的旧代码格式为835185.BJ，输出的期望是920185.BJ这类格式
    """
    # 如果完整代码映射失败，尝试映射数字部分
    if old_code.endswith('.BJ'):
        code_part = old_code.replace('.BJ', '')
        # 尝试字符串键
        new_code_part = mapping.get(code_part, None)
        if new_code_part is not None:
            return str(new_code_part) + '.BJ'
        
        # 尝试整数键
        try:
            code_int = int(code_part)
            new_code_part = mapping.get(code_int, None)
            if new_code_part is not None:
                return str(new_code_part) + '.BJ'
        except ValueError:
            pass
    else:
        return old_code
    
    # 如果都没有找到映射，返回原代码
    return old_code
    