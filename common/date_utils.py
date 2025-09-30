import jqdatasdk as jq
from jqdatasdk import get_trade_days

from datetime import datetime, timedelta


jq.auth("18701341286", "120502JoinQuant")

#jq的 get_trade_days 接口说明
#get_trade_days(start_date=None, end_date=None, count=None)
#获取“2018-02-10”至”2018-03-01“的交易日
#get_trade_days(start_date="2018-02-10",end_date="2018-03-01")

# 输出为数组
#array([datetime.date(2018, 2, 12), datetime.date(2018, 2, 13),
#       datetime.date(2018, 2, 14), datetime.date(2018, 2, 22),
#       datetime.date(2018, 2, 23), datetime.date(2018, 2, 26),
#       datetime.date(2018, 2, 27), datetime.date(2018, 2, 28),
#       datetime.date(2018, 3, 1)], dtype=object)

#get_all_trade_days() 不传任何参数的时候，返回从2005年至今的所有交易日

def get_trading_days(start_date, end_date):
    """
    获取指定日期范围内的交易日列表
    
    :param start_date: 开始日期，格式为"20240101"
    :param end_date: 结束日期，格式为"20240101"
    :return: 按时间顺序排列的交易日列表
    """
    # 确保输入格式正确
    if not (isinstance(start_date, str) and isinstance(end_date, str)):
        raise TypeError("日期必须是字符串格式")
    
    if len(start_date) != 8 or len(end_date) != 8:
        raise ValueError("日期格式必须为'YYYYMMDD'")
    
    # 格式化日期为聚宽接口需要的格式 (YYYY-MM-DD)
    formatted_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    formatted_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    
    # 使用聚宽接口获取交易日
    trade_days = get_trade_days(start_date=formatted_start, end_date=formatted_end)
    
    # 将datetime.date对象转换为字符串格式 "YYYYMMDD"
    trading_days = [day.strftime("%Y%m%d") for day in trade_days]
    
    return trading_days

def get_last_trading_day(end_date, n=1):
    """
    获取指定日期前n个交易日的日期
    
    :param end_date: 结束日期，格式为"20240101"
    :param n: 向前推n个交易日，n=0 时返回 end_date 自身（如果是交易日）
    :return: 前n个交易日的日期，格式为"20240101"
    """
    # 确保输入格式正确
    if not isinstance(end_date, str):
        raise TypeError("日期必须是字符串格式")
    
    if len(end_date) != 8:
        raise ValueError("日期格式必须为'YYYYMMDD'")
    
    if not isinstance(n, int) or n < 0:
        raise ValueError("n必须是非负整数")
    
    # 格式化日期为聚宽接口需要的格式 (YYYY-MM-DD)
    formatted_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    
    # 为了获取足够的交易日，我们需要向前查找更多的日期
    # 假设每周有5个交易日，我们向前查找n*2个自然日，确保能获取到足够的交易日
    
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    # 向前推算足够多的天数，确保能覆盖到n个交易日
    days_to_lookback = n * 3 + 2 # 假设每周有2-3个交易日
    start_dt = end_dt - timedelta(days=days_to_lookback)
    formatted_start = start_dt.strftime("%Y-%m-%d")
    
    # 使用聚宽接口获取交易日
    trade_days = get_trade_days(start_date=formatted_start, end_date=formatted_end)
    
    # 将datetime.date对象转换为字符串格式 "YYYYMMDD"
    trading_days = [day.strftime("%Y%m%d") for day in trade_days]
    
    # 如果没有获取到交易日，可能需要向前查找更多日期
    if not trading_days:
        raise ValueError(f"在 {end_date} 之前没有找到交易日")
    
    # 找到结束日期在交易日列表中的位置
    end_index = -1
    for i, day in enumerate(trading_days):
        if day == end_date:
            end_index = i
            break
        elif day > end_date:
            end_index = i - 1
            break
    
    # 如果结束日期大于最后一个交易日，则使用最后一个交易日的索引
    if end_index == -1 and trading_days and end_date > trading_days[-1]:
        end_index = len(trading_days) - 1
    
    # 检查是否找到有效的结束日期索引
    if end_index == -1:
        raise ValueError(f"无法确定日期 {end_date} 的交易日位置")
    
    # 特殊情况：n=0 且 end_date 是交易日
    if n == 0:
        if trading_days[end_index] == end_date:
            return end_date
        else:
            if len(trading_days) > 0:
                return trading_days[-1]
    
    # 检查是否有足够的交易日
    if end_index < n:
        # 如果没有足够的交易日，尝试获取更多历史交易日
        start_dt = start_dt - timedelta(days=days_to_lookback * 2)  # 再向前推更多天数
        formatted_start = start_dt.strftime("%Y-%m-%d")
        more_trade_days = get_trade_days(start_date=formatted_start, end_date=formatted_end)
        more_trading_days = [day.strftime("%Y%m%d") for day in more_trade_days]
        
        if len(more_trading_days) <= end_index + 1:
            raise ValueError(f"在 {end_date} 之前没有足够的交易日")
        
        trading_days = more_trading_days
        end_index = trading_days.index(end_date) if end_date in trading_days else -1
        
        if end_index == -1:
            for i, day in enumerate(trading_days):
                if day > end_date:
                    end_index = i - 1
                    break
        
        if end_index < n:
            raise ValueError(f"在 {end_date} 之前没有足够的交易日")
    
    # 返回前n个交易日
    return trading_days[end_index - n]
