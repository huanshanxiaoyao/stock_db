"""聚宽数据源实现"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any
import logging

# 导入聚宽相关模块
try:
    import jqdatasdk as jq
    from jqdatasdk import (
    auth, get_price, get_all_securities, get_fundamentals, 
    get_concept, get_trade_days, query, finance,
    valuation, indicator, get_mtss
)
except ImportError:
    print("警告: 未安装jqdatasdk，请先安装: pip install jqdatasdk")
    jq = None

from data_source import BaseDataSource, DataType
from models.stock_list import StockInfo


class JQDataSource(BaseDataSource):
    """聚宽数据源实现"""
    
    def __init__(self):
        super().__init__("JointQuant")
        self.logger = logging.getLogger(__name__)
        
        if jq is None:
            raise ImportError("jqdatasdk未安装，请先安装: pip install jqdatasdk")
    
    def authenticate(self, username: str, password: str, **kwargs) -> bool:
        """聚宽API认证"""
        try:
            jq.auth(username, password)
            self._authenticated = True
            self._credentials = {'username': username, 'password': password}
            self.logger.info("聚宽API认证成功")
            return True
        except Exception as e:
            self.logger.error(f"聚宽API认证失败: {e}")
            self._authenticated = False
            return False
    
    def _to_jq_code(self, ts_code: str) -> str:
        """将tushare格式代码转换为聚宽格式"""
        if ts_code.endswith('.SZ'):
            return ts_code.replace('.SZ', '.XSHE')
        elif ts_code.endswith('.SH'):
            return ts_code.replace('.SH', '.XSHG')
        elif ts_code.endswith('.BJ'):
            return ts_code.replace('.BJ', '.BSE')
        return ts_code
    
    def _from_jq_code(self, jq_code: str) -> str:
        """将聚宽格式代码转换为标准格式"""
        if jq_code.endswith('.XSHE'):
            return jq_code.replace('.XSHE', '.SZ')
        elif jq_code.endswith('.XSHG'):
            return jq_code.replace('.XSHG', '.SH')
        elif jq_code.endswith('.BSE'):
            return jq_code.replace('.BSE', '.BJ')
        elif jq_code.endswith('.BJSE'):
            # 北交所股票代码格式：xxxxxx.BJSE -> xxxx.BJ
            code_part = jq_code.replace('.BJSE', '')
            return f"{code_part}.BJ"
        elif jq_code.isdigit() and len(jq_code) == 4:
            # 北交所股票代码：4位数字，添加.BJ后缀
            return f"{jq_code}.BJ"
        elif jq_code.isdigit() and len(jq_code) == 6:
            # 沪深股票代码：6位数字，根据开头判断交易所
            if jq_code.startswith(('60', '68', '90')):
                return f"{jq_code}.SH"
            elif jq_code.startswith(('00', '30', '20')):
                return f"{jq_code}.SZ"
        return jq_code
    
    def get_stock_list(self, market: Optional[str] = None, **kwargs) -> List[str]:
        """获取股票列表"""
        if not self.is_authenticated():
            raise RuntimeError("请先进行认证")
        
        try:
            # 使用get_all_securities获取A股股票列表
            if market == 'sz':
                # 获取深交所股票
                df_stock = get_all_securities(types=['stock'])
                # 筛选深交所股票（代码以.XSHE结尾）
                sz_codes = [code for code in df_stock.index if code.endswith('.XSHE')]
                stocks = sz_codes
            elif market == 'sh':
                # 获取上交所股票
                df_stock = get_all_securities(types=['stock'])
                # 筛选上交所股票（代码以.XSHG结尾）
                sh_codes = [code for code in df_stock.index if code.endswith('.XSHG')]
                stocks = sh_codes
            elif market == 'bj' or market == 'bse':
                # 获取北交所股票
                df_bj = get_all_securities(types=['bjse'])
                stocks = list(df_bj.index)
            else:
                # 获取全部A股（沪深A股 + 北交所）
                df_stock = get_all_securities(types=['stock'])
                df_bj = get_all_securities(types=['bjse'])
                stocks = list(df_stock.index) + list(df_bj.index)
            
            # 转换为标准格式
            return [self._from_jq_code(code) for code in stocks]
            
        except Exception as e:
            self._handle_api_error(e, "获取股票列表")
    
    def get_all_stock_list(self) -> pd.DataFrame:
        """获取完整的中国A股股票列表"""
        if not self.is_authenticated():
            raise RuntimeError("请先进行认证")
        
        try:
            # 获取沪深A股
            df_stock = get_all_securities(types=['stock'])
            # 获取北交所股票
            df_bj = get_all_securities(types=['bjse'])
            
            # 合并数据
            df_all = pd.concat([df_stock, df_bj], ignore_index=False)  # 保持索引
            
            # 重置索引，将股票代码作为列
            df_all = df_all.reset_index()
            df_all.rename(columns={'index': 'code'}, inplace=True)
            
            # 确保代码列为字符串类型
            df_all['code'] = df_all['code'].astype(str)
            
            # 转换为标准格式代码（这里的code已经是聚宽格式的代码了）
            df_all['code'] = df_all['code'].apply(self._from_jq_code)
            
            # 添加交易所和市场信息
            df_all['exchange'] = df_all['code'].apply(self._get_exchange_from_code)
            df_all['market'] = df_all['code'].apply(self._get_market_from_code)
            
            # 重命名列以匹配StockInfo模型
            df_all.rename(columns={
                'display_name': 'display_name',
                'name': 'name',
                'start_date': 'start_date',
                'end_date': 'end_date'
            }, inplace=True)
            
            # 添加状态信息
            df_all['status'] = 'normal'
            df_all['is_st'] = df_all['display_name'].str.contains('ST', na=False)
            df_all['update_date'] = pd.Timestamp.now().date()
            
            self.logger.info(f"获取到 {len(df_all)} 只股票信息")
            return df_all
            
        except Exception as e:
            self._handle_api_error(e, "获取完整股票列表")
    
    def _get_exchange_from_code(self, code: str) -> str:
        """从股票代码获取交易所"""
        if code.endswith('.SH'):
            return 'XSHG'
        elif code.endswith('.SZ'):
            return 'XSHE'
        elif code.endswith('.BJ'):
            return 'BSE'
        return 'UNKNOWN'
    
    def _get_market_from_code(self, code: str) -> str:
        """从股票代码获取市场类型"""
        if code.endswith('.SH'):
            # 上交所：主板、科创板
            if code.startswith('688'):
                return 'star'  # 科创板
            else:
                return 'main'  # 主板
        elif code.endswith('.SZ'):
            # 深交所：主板、创业板
            if code.startswith('300'):
                return 'gem'  # 创业板
            else:
                return 'main'  # 主板
        elif code.endswith('.BJ'):
            return 'bse'  # 北交所
        return 'unknown'
    
    def get_financial_data(self, codes: List[str], start_date: date, end_date: date, 
                          data_type: str) -> pd.DataFrame:
        """获取财务数据"""
        if not self.is_authenticated():
            raise RuntimeError("请先进行认证")
        
        self._validate_codes(codes)
        self._validate_date_range(start_date, end_date)
        
        try:
            if data_type == DataType.INCOME_STATEMENT:
                return self._get_income_statement_data(codes, start_date, end_date)
            elif data_type == DataType.CASHFLOW_STATEMENT:
                return self._get_cashflow_statement_data(codes, start_date, end_date)
            elif data_type == DataType.BALANCE_SHEET:
                return self._get_balance_sheet_data(codes, start_date, end_date)
            else:
                raise ValueError(f"不支持的财务数据类型: {data_type}")
                
        except Exception as e:
            self._handle_api_error(e, f"获取{data_type}数据")
    
    def _get_income_statement_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取利润表数据"""
        dfs = []
        
        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)
                
                q = query(finance.STK_INCOME_STATEMENT).filter(
                    finance.STK_INCOME_STATEMENT.code == jq_code,
                    finance.STK_INCOME_STATEMENT.pub_date >= start_date.strftime('%Y-%m-%d'),
                    finance.STK_INCOME_STATEMENT.pub_date <= end_date.strftime('%Y-%m-%d')
                )
                
                df = finance.run_query(q)
                if not df.empty:
                    # 转换代码格式
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                # 控制请求频率
                if idx % 99 == 0 and idx > 0:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(codes)} 个股票")
                    
            except Exception as e:
                self.logger.error(f"获取 {code} 利润表数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _get_cashflow_statement_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取现金流量表数据"""
        dfs = []
        
        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)
                
                q = query(finance.STK_CASHFLOW_STATEMENT).filter(
                    finance.STK_CASHFLOW_STATEMENT.code == jq_code,
                    finance.STK_CASHFLOW_STATEMENT.pub_date >= start_date.strftime('%Y-%m-%d'),
                    finance.STK_CASHFLOW_STATEMENT.pub_date <= end_date.strftime('%Y-%m-%d')
                )
                
                df = finance.run_query(q)
                if not df.empty:
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                if idx % 99 == 0 and idx > 0:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(codes)} 个股票")
                    
            except Exception as e:
                self.logger.error(f"获取 {code} 现金流量表数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _get_balance_sheet_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取资产负债表数据"""
        dfs = []
        
        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)
                
                q = query(finance.STK_BALANCE_SHEET).filter(
                    finance.STK_BALANCE_SHEET.code == jq_code,
                    finance.STK_BALANCE_SHEET.pub_date >= start_date.strftime('%Y-%m-%d'),
                    finance.STK_BALANCE_SHEET.pub_date <= end_date.strftime('%Y-%m-%d')
                )
                
                df = finance.run_query(q)
                if not df.empty:
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                if idx % 99 == 0 and idx > 0:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(codes)} 个股票")
                    
            except Exception as e:
                self.logger.error(f"获取 {code} 资产负债表数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def get_market_data(self, codes: List[str], start_date: date, end_date: date, 
                       data_type: str) -> pd.DataFrame:
        """获取市场数据"""
        if not self.is_authenticated():
            raise RuntimeError("请先进行认证")
        
        self._validate_date_range(start_date, end_date)
        
        try:
            if data_type == DataType.INDICATOR_DATA:
                return self._get_indicator_data(start_date, end_date)
            elif data_type == DataType.MTSS_DATA:
                return self._get_mtss_data(codes, start_date, end_date)
            elif data_type == DataType.PRICE_DATA:
                return self._get_price_data(codes, start_date, end_date)
            else:
                raise ValueError(f"不支持的市场数据类型: {data_type}")
                
        except Exception as e:
            self._handle_api_error(e, f"获取{data_type}数据")
    
    def get_fundamental_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取基本面数据"""
        if not self.is_authenticated():
            raise RuntimeError("请先进行认证")
        
        self._validate_date_range(start_date, end_date)
        
        try:
            return self._get_valuation_data(start_date, end_date)
        except Exception as e:
            self._handle_api_error(e, "获取基本面数据")
    
    def _get_valuation_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """获取估值数据"""
        trade_days = get_trade_days(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        dfs = []
        for idx, day in enumerate(trade_days):
            try:
                day_str = day.strftime("%Y-%m-%d")
                df = get_fundamentals(query(valuation), date=day_str)
                
                if not df.empty:
                    df['day'] = day_str
                    # 转换代码格式
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                if idx % 100 == 0 and idx > 0:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(trade_days)} 个交易日")
                    
            except Exception as e:
                self.logger.error(f"获取 {day_str} 基本面数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _get_indicator_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """获取技术指标数据"""
        trade_days = get_trade_days(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        dfs = []
        for idx, day in enumerate(trade_days):
            try:
                day_str = day.strftime("%Y-%m-%d")
                df = get_fundamentals(query(indicator), date=day_str)
                
                if not df.empty:
                    df['day'] = day_str
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                if idx % 100 == 0 and idx > 0:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(trade_days)} 个交易日")
                    
            except Exception as e:
                self.logger.error(f"获取 {day_str} 技术指标数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _get_mtss_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取融资融券数据"""
        dfs = []
        
        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)
                
                df = get_mtss([jq_code], start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                if not df.empty:
                    df['code'] = df['code'].apply(self._from_jq_code)
                    dfs.append(df)
                
                if idx % 100 == 0 and idx > 0:
                    time.sleep(0.3)
                    self.logger.info(f"已处理 {idx+1}/{len(codes)} 个股票")
                    
            except Exception as e:
                self.logger.error(f"获取 {code} 融资融券数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _get_price_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取价格数据（分批请求聚宽API并合并结果）"""
        self.logger.info(f"开始获取价格数据，股票数量: {len(codes)}，日期范围: {start_date} 到 {end_date}")
        # 分批处理股票，避免单次请求过多
        batch_size = 400
        all_data = []
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i + batch_size]
            self.logger.info(f"处理第 {i//batch_size + 1} 批，股票数量: {len(batch_codes)}")
            batch_data = self._get_batch_price_data(batch_codes, start_date, end_date)
            if not batch_data.empty:
                all_data.append(batch_data)
            # 批次间休息，避免API限制
            if i + batch_size < len(codes):
                time.sleep(0.1)
        
        # 合并所有批次的数据
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"价格数据获取完成，共 {len(combined_data)} 条记录")
            return combined_data
        else:
            self.logger.warning("没有获取到任何价格数据")
            return pd.DataFrame()
    
    def _get_batch_price_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """批量获取价格数据"""
        dfs = []
        
        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)
                
                # 使用get_price获取价格数据，包含复权因子等字段
                df = get_price(
                    jq_code, 
                    start_date=start_date.strftime('%Y-%m-%d'), 
                    end_date=end_date.strftime('%Y-%m-%d'),
                    frequency='daily',
                    fields=['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close', 'factor', 'high_limit', 'low_limit', 'avg', 'paused'],
                    skip_paused=False,
                    fq='post'
                )
                
                if not df.empty:
                    # 重置索引，将日期作为列（修正：显式设置索引名为 day）
                    df.index.name = 'day'
                    df = df.reset_index()
                    
                    # 添加股票代码
                    df['code'] = self._from_jq_code(jq_code)
                    
                    # 计算复权收盘价和复权因子（保持向后兼容）
                    if 'factor' in df.columns:
                        df['adj_close'] = df['close'] * df['factor']
                        df['adj_factor'] = df['factor']  # 兼容字段
                    else:
                        df['adj_close'] = df['close']
                        df['factor'] = 1.0
                        df['adj_factor'] = 1.0
                    
                    # 确保所有字段都存在，如果缺失则填充默认值
                    required_fields = ['high_limit', 'low_limit', 'avg', 'paused']
                    for field in required_fields:
                        if field not in df.columns:
                            if field == 'paused':
                                df[field] = 0  # 默认未停牌
                            else:
                                df[field] = None  # 其他字段默认为None
                    
                    # 重新排列列顺序，确保与PriceData模型匹配
                    df = df[['code', 'day', 'open', 'close', 'high', 'low', 'pre_close', 'volume', 'money', 
                            'factor', 'high_limit', 'low_limit', 'avg', 'paused', 'adj_close', 'adj_factor']]
                    
                    dfs.append(df)
                
                # 每处理一定数量的股票后休息
                if idx % 50 == 0 and idx > 0:
                    time.sleep(0.05)
                    
            except Exception as e:
                self.logger.error(f"获取 {code} 价格数据失败: {e}")
                continue
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def close(self):
        """关闭连接（JQData API不需要显式关闭连接）"""
        # 注释掉这行，避免意外重置认证状态
        # self._authenticated = False
        self.logger.info("JQData连接已关闭")
    
    # 便捷方法
    def download_all_financial_data(self, codes: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        """下载所有财务数据"""
        result = {}
        
        financial_types = DataType.get_financial_types()
        for data_type in financial_types:
            self.logger.info(f"开始下载 {data_type} 数据")
            df = self.get_financial_data(codes, start_date, end_date, data_type)
            if not df.empty:
                result[data_type] = df
                self.logger.info(f"{data_type} 数据下载完成，共 {len(df)} 条记录")
        
        return result
    
    def download_all_market_data(self, codes: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
        """下载所有市场数据"""
        result = {}
        
        # 基本面数据
        self.logger.info("开始下载基本面数据")
        fundamental_df = self.get_fundamental_data(codes, start_date, end_date)
        if not fundamental_df.empty:
            result[DataType.FUNDAMENTAL_DATA] = fundamental_df
            self.logger.info(f"基本面数据下载完成，共 {len(fundamental_df)} 条记录")
        
        # 价格数据
        self.logger.info("开始下载价格数据")
        price_df = self.get_market_data(codes, start_date, end_date, DataType.PRICE_DATA)
        if not price_df.empty:
            result[DataType.PRICE_DATA] = price_df
            self.logger.info(f"价格数据下载完成，共 {len(price_df)} 条记录")
        
        # 技术指标数据
        self.logger.info("开始下载技术指标数据")
        indicator_df = self.get_market_data(codes, start_date, end_date, DataType.INDICATOR_DATA)
        if not indicator_df.empty:
            result[DataType.INDICATOR_DATA] = indicator_df
            self.logger.info(f"技术指标数据下载完成，共 {len(indicator_df)} 条记录")
        
        # 融资融券数据
        self.logger.info("开始下载融资融券数据")
        mtss_df = self.get_market_data(codes, start_date, end_date, DataType.MTSS_DATA)
        if not mtss_df.empty:
            result[DataType.MTSS_DATA] = mtss_df
            self.logger.info(f"融资融券数据下载完成，共 {len(mtss_df)} 条记录")
        
        return result