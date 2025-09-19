from datetime import date, datetime
import pandas as pd


def to_date(val):
    """将各种类型的值转换为 date 对象
    
    Args:
        val: 需要转换的值，可以是 None、date、datetime、字符串等
        
    Returns:
        date: 转换后的日期对象，如果无法转换则返回 None
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if hasattr(val, 'to_pydatetime'):
        try:
            return val.to_pydatetime().date()
        except Exception:
            pass
    if hasattr(val, 'date'):
        try:
            return val.date()
        except Exception:
            pass
    if isinstance(val, str) and val:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.strptime(val, fmt).date()
            except Exception:
                continue
    return None