# Price Data 字段对比分析

## 问题描述
错误信息：table price_data has 16 columns but 18 values were supplied

## Price_data 表结构 (16列)
从数据库查询结果确认：
1. code
2. day  
3. open
4. close
5. high
6. low
7. pre_close
8. volume
9. money
10. factor
11. high_limit
12. low_limit
13. avg
14. paused
15. adj_close
16. adj_factor

## Tushare DataFrame 结构 (18列)
从tushare.py代码分析：

### 原始tushare字段 (11列)
1. code
2. day
3. open
4. high
5. low
6. close
7. pre_close
8. change
9. pct_change (重命名为pct_change)
10. volume (从vol重命名)
11. money (从amount重命名)

### 添加的None字段 (7列)
12. factor
13. high_limit
14. low_limit
15. avg
16. paused
17. adj_close
18. adj_factor

## 问题分析
**总计：11 + 7 = 18列，但数据库表只有16列**

### 多出的字段
- change (涨跌额)
- pct_change (涨跌幅)

这两个字段在tushare DataFrame中存在，但在price_data表定义中不存在，导致列数不匹配。

## 解决方案
需要在tushare.py中移除change和pct_change字段，或者在数据库表中添加这两个字段。