#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
价格数据服务类
提供高性能的价格数据查询和处理功能
"""

from typing import List, Dict, Optional, Any, Union
from datetime import date, datetime, timedelta
import pandas as pd
import logging
from functools import lru_cache
import numpy as np

from database import DatabaseManager


class PriceDataService:
    """价格数据服务类"""

    def __init__(self, db_manager: DatabaseManager):
        """初始化价格数据服务

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        self._cache = {}

    def get_price_data(self, code: str, start_date: Optional[date] = None,
                      end_date: Optional[date] = None,
                      fields: Optional[List[str]] = None) -> pd.DataFrame:
        """获取单个股票的价格数据

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表，None表示全部字段

        Returns:
            价格数据DataFrame
        """
        # 默认字段
        if fields is None:
            fields = ['day', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'factor', 'change', 'pct_change']

        # 构建SQL
        field_str = ', '.join(fields)
        sql = f"SELECT {field_str} FROM price_data WHERE code = ?"
        params = [code]

        if start_date:
            sql += " AND day >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND day <= ?"
            params.append(end_date)

        sql += " ORDER BY day"

        try:
            df = self.db.query(sql, params)
            return df
        except Exception as e:
            self.logger.error(f"获取{code}价格数据失败: {e}")
            return pd.DataFrame()

    def get_batch_price_data(self, codes: List[str], start_date: Optional[date] = None,
                            end_date: Optional[date] = None,
                            fields: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """批量获取多个股票的价格数据

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表

        Returns:
            {股票代码: 价格数据DataFrame}的字典
        """
        if not codes:
            return {}

        result = {}

        # 批量查询优化：一次性查询所有数据
        if len(codes) <= 100:  # 小批量直接查询
            df_all = self._batch_query_price_data(codes, start_date, end_date, fields)
            if not df_all.empty:
                # 按股票代码分组
                for code in codes:
                    df_code = df_all[df_all['code'] == code].copy()
                    if not df_code.empty:
                        df_code = df_code.drop('code', axis=1)
                        result[code] = df_code
        else:  # 大批量分批查询
            batch_size = 50
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i + batch_size]
                df_batch = self._batch_query_price_data(batch_codes, start_date, end_date, fields)
                if not df_batch.empty:
                    for code in batch_codes:
                        df_code = df_batch[df_batch['code'] == code].copy()
                        if not df_code.empty:
                            df_code = df_code.drop('code', axis=1)
                            result[code] = df_code

        return result

    def _batch_query_price_data(self, codes: List[str], start_date: Optional[date] = None,
                                end_date: Optional[date] = None,
                                fields: Optional[List[str]] = None) -> pd.DataFrame:
        """批量查询价格数据（内部方法）"""
        # 默认字段
        if fields is None:
            fields = ['code', 'day', 'open', 'high', 'low', 'close', 'volume']
        elif 'code' not in fields:
            fields = ['code'] + fields

        # 构建SQL
        field_str = ', '.join(fields)
        codes_str = "', '".join(codes)
        sql = f"SELECT {field_str} FROM price_data WHERE code IN ('{codes_str}')"

        if start_date:
            sql += f" AND day >= '{start_date}'"
        if end_date:
            sql += f" AND day <= '{end_date}'"

        sql += " ORDER BY code, day"

        try:
            return self.db.query(sql)
        except Exception as e:
            self.logger.error(f"批量查询价格数据失败: {e}")
            return pd.DataFrame()

    def get_latest_price(self, codes: Union[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """获取最新价格

        Args:
            codes: 股票代码或代码列表

        Returns:
            {股票代码: 最新价格信息}的字典
        """
        if isinstance(codes, str):
            codes = [codes]

        if not codes:
            return {}

        codes_str = "', '".join(codes)
        sql = f"""
        WITH latest_prices AS (
            SELECT code, MAX(day) as latest_day
            FROM price_data
            WHERE code IN ('{codes_str}')
            GROUP BY code
        )
        SELECT p.*
        FROM price_data p
        JOIN latest_prices l ON p.code = l.code AND p.day = l.latest_day
        """

        try:
            df = self.db.query(sql)
            if df.empty:
                return {}

            result = {}
            for _, row in df.iterrows():
                result[row['code']] = row.to_dict()

            return result
        except Exception as e:
            self.logger.error(f"获取最新价格失败: {e}")
            return {}

    def calculate_returns(self, code: str, start_date: date, end_date: date,
                         period: str = 'daily') -> pd.DataFrame:
        """计算收益率

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            period: 计算周期 ('daily', 'weekly', 'monthly')

        Returns:
            包含收益率的DataFrame
        """
        df = self.get_price_data(code, start_date, end_date, ['day', 'close', 'factor'])

        if df.empty:
            return pd.DataFrame()

        # 计算复权价格
        df['adj_close'] = df['close'] * df['factor']

        # 计算收益率
        if period == 'daily':
            df['return'] = df['adj_close'].pct_change()
        elif period == 'weekly':
            df.set_index('day', inplace=True)
            weekly_df = df.resample('W')['adj_close'].last()
            weekly_df = weekly_df.to_frame()
            weekly_df['return'] = weekly_df['adj_close'].pct_change()
            return weekly_df.reset_index()
        elif period == 'monthly':
            df.set_index('day', inplace=True)
            monthly_df = df.resample('M')['adj_close'].last()
            monthly_df = monthly_df.to_frame()
            monthly_df['return'] = monthly_df['adj_close'].pct_change()
            return monthly_df.reset_index()

        return df[['day', 'close', 'adj_close', 'return']]

    def calculate_volatility(self, code: str, start_date: date, end_date: date,
                           window: int = 20) -> pd.DataFrame:
        """计算波动率

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            window: 计算窗口期

        Returns:
            包含波动率的DataFrame
        """
        df = self.get_price_data(code, start_date, end_date, ['day', 'close', 'factor'])

        if df.empty:
            return pd.DataFrame()

        # 计算复权价格
        df['adj_close'] = df['close'] * df['factor']

        # 计算日收益率
        df['return'] = df['adj_close'].pct_change()

        # 计算滚动波动率（年化）
        df['volatility'] = df['return'].rolling(window=window).std() * np.sqrt(252)

        return df[['day', 'close', 'return', 'volatility']]

    def get_price_range(self, code: str, days: int = 252) -> Dict[str, Any]:
        """获取价格区间统计

        Args:
            code: 股票代码
            days: 统计天数

        Returns:
            价格区间统计信息
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        df = self.get_price_data(code, start_date, end_date, ['day', 'high', 'low', 'close'])

        if df.empty:
            return {}

        return {
            'code': code,
            'period_days': days,
            'start_date': df['day'].min(),
            'end_date': df['day'].max(),
            'highest': df['high'].max(),
            'lowest': df['low'].min(),
            'latest_close': df.iloc[-1]['close'],
            'avg_price': df['close'].mean(),
            'position_pct': (df.iloc[-1]['close'] - df['low'].min()) / (df['high'].max() - df['low'].min()) * 100
        }

    def get_moving_averages(self, code: str, end_date: Optional[date] = None,
                           periods: List[int] = None) -> Dict[str, float]:
        """计算移动平均线

        Args:
            code: 股票代码
            end_date: 结束日期
            periods: MA周期列表，默认[5, 10, 20, 60, 120, 250]

        Returns:
            各周期移动平均值
        """
        if periods is None:
            periods = [5, 10, 20, 60, 120, 250]

        if end_date is None:
            end_date = date.today()

        max_period = max(periods)
        start_date = end_date - timedelta(days=max_period * 2)

        df = self.get_price_data(code, start_date, end_date, ['day', 'close'])

        if df.empty:
            return {}

        result = {'code': code, 'date': end_date}

        for period in periods:
            if len(df) >= period:
                ma_value = df['close'].rolling(window=period).mean().iloc[-1]
                result[f'ma{period}'] = round(ma_value, 2)

        return result

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        self.logger.info("价格数据缓存已清除")