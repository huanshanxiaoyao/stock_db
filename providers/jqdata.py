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
    auth, get_price, get_all_securities, get_fundamentals, get_fundamentals_continuously,
    get_concept, get_trade_days, query, finance,
    valuation, indicator, get_mtss, get_money_flow, get_money_flow_pro
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
        
        if data_type == DataType.INDICATOR_DATA:
            return self._get_indicator_data(start_date, end_date)
        elif data_type == DataType.VALUATION_DATA:
            return self._get_valuation_data(start_date, end_date)
        elif data_type == DataType.MTSS_DATA:
            return self._get_mtss_data(codes, start_date, end_date)
        elif data_type == DataType.PRICE_DATA:
            return self._get_price_data(codes, start_date, end_date)
        else:
            raise ValueError(f"不支持的市场数据类型: {data_type}")
            
    def _get_indicator_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self._get_fundamental_data(DataType.INDICATOR_DATA, start_date, end_date)

    def _get_valuation_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self._get_fundamental_data(DataType.VALUATION_DATA, start_date, end_date)

    def _get_fundamental_data(self, data_type:DataType, start_date: date, end_date: date) -> pd.DataFrame:
        """获取财务指标数据 - 按交易日逐日获取"""
        
        start_str = start_date.strftime('%Y-%m-%d')
        #start_str = '2025-09-01' #测试，
        end_str = end_date.strftime('%Y-%m-%d')
        self.logger.info(f"开始逐日获取{data_type}数据，日期范围: {start_str} 到 {end_str}")
        
        dfs = []
        trade_days = get_trade_days(start_str, end_str)
        
        for idx, day in enumerate(trade_days):
            try:
                day_str = day.strftime("%Y-%m-%d")
                
                # 创建查询对象
                if data_type == DataType.INDICATOR_DATA:
                    q = query(indicator)
                elif data_type == DataType.VALUATION_DATA:
                    q = query(valuation)
                else:
                    raise ValueError(f"不支持的市场数据类型: {data_type}")
                
                # 按日获取财务指标数据
                df = get_fundamentals(q, date=day_str)
                
                if not df.empty:
                    # 转换股票代码格式
                    if 'code' in df.columns:
                        df['code'] = df['code'].apply(self._from_jq_code)
                    
                    # 确保数值列的数据类型正确，保留原有的日期字段
                    if data_type == DataType.INDICATOR_DATA:
                        exclude_cols = ['code', 'pubDate', 'statDate']
                    elif data_type == DataType.VALUATION_DATA:
                        exclude_cols = ['code', 'day']
                    else:
                        exclude_cols = ['code']
                    
                    numeric_columns = df.select_dtypes(include=['object']).columns
                    for col in numeric_columns:
                        if col not in exclude_cols:
                            try:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            except Exception:
                                pass  # 保持原始类型
                    
                    dfs.append(df)
                
                # 每处理50个交易日休息一下，并打印进度
                if idx % 50 == 26:
                    time.sleep(0.5)
                    self.logger.info(f"已处理 {idx+1}/{len(trade_days)} 个交易日")
                    
            except Exception as e:
                self.logger.error(f"获取 {day_str} {data_type}数据失败: {e}")
                continue
        
        if dfs:
            result_df = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"成功获取 {len(result_df)} 条{data_type}数据")
            return result_df
        else:
            self.logger.warning(f"未获取到任何{data_type}数据")
            return pd.DataFrame()

    
    def _get_mtss_data(self, codes: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """获取融资融券和资金流向数据 - 合并Source1和Source2"""
        self.logger.info(f"开始获取融资融券和资金流向数据，股票数量: {len(codes)}")
        mtss_dfs = []
        money_flow_dfs = []

        for idx, code in enumerate(codes):
            try:
                jq_code = self._to_jq_code(code)

                # 获取融资融券数据 (Source2)
                try:
                    mtss_df = get_mtss([jq_code], start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                    if not mtss_df.empty:
                        # get_mtss returns 'sec_code' not 'code', need to rename first
                        if 'sec_code' in mtss_df.columns:
                            mtss_df.rename(columns={'sec_code': 'code'}, inplace=True)
                        if 'code' in mtss_df.columns:
                            mtss_df['code'] = mtss_df['code'].apply(self._from_jq_code)
                        if 'date' in mtss_df.columns:
                            mtss_df.rename(columns={'date': 'day'}, inplace=True)  # 统一日期字段名
                        mtss_dfs.append(mtss_df)
                except Exception as e:
                    self.logger.warning(f"获取 {code} 融资融券数据失败: {e}")

                # 获取资金流向数据 (Source1: get_money_flow_pro)
                try:
                    # 计算需要获取的天数
                    days_count = (end_date - start_date).days + 1

                    # Use get_money_flow_pro to get all 12 fields (inflow, outflow, netflow)
                    # Note: date and sec_code are returned automatically, don't include in fields
                    money_flow_df = get_money_flow_pro(
                        jq_code,
                        end_date=end_date.strftime('%Y-%m-%d'),
                        count=days_count,
                        frequency='1d',
                        fields=['inflow_xl', 'inflow_l', 'inflow_m', 'inflow_s',
                                'outflow_xl', 'outflow_l', 'outflow_m', 'outflow_s',
                                'netflow_xl', 'netflow_l', 'netflow_m', 'netflow_s']
                    )

                    if money_flow_df is not None and not money_flow_df.empty:
                        # Reset index if date/time is in the index
                        if 'date' in money_flow_df.index.names or money_flow_df.index.name == 'date':
                            money_flow_df = money_flow_df.reset_index()
                        if 'time' in money_flow_df.index.names or money_flow_df.index.name == 'time':
                            money_flow_df = money_flow_df.reset_index()

                        # Handle sec_code -> code conversion (code column already exists in this case)
                        if 'sec_code' in money_flow_df.columns:
                            if 'code' in money_flow_df.columns:
                                # Both exist, drop sec_code and keep code
                                money_flow_df.drop(columns=['sec_code'], inplace=True)
                            else:
                                # Only sec_code exists, rename and convert
                                money_flow_df['code'] = money_flow_df['sec_code'].apply(self._from_jq_code)
                                money_flow_df.drop(columns=['sec_code'], inplace=True)

                        # Apply code format conversion if code column exists
                        if 'code' in money_flow_df.columns:
                            money_flow_df['code'] = money_flow_df['code'].apply(self._from_jq_code)

                        # Rename date/time to day for consistency
                        # get_money_flow_pro returns 'time' not 'date'!
                        if 'time' in money_flow_df.columns:
                            money_flow_df.rename(columns={'time': 'day'}, inplace=True)
                        elif 'date' in money_flow_df.columns:
                            money_flow_df.rename(columns={'date': 'day'}, inplace=True)

                        # Ensure day column exists
                        if 'day' not in money_flow_df.columns:
                            self.logger.error(f"money_flow_df 缺少 day 列。当前列: {money_flow_df.columns.tolist()}")
                            continue

                        # Select the columns we need (all 12 money flow fields + code + day)
                        required_cols = ['code', 'day']
                        money_flow_cols = ['inflow_xl', 'inflow_l', 'inflow_m', 'inflow_s',
                                          'outflow_xl', 'outflow_l', 'outflow_m', 'outflow_s',
                                          'netflow_xl', 'netflow_l', 'netflow_m', 'netflow_s']
                        available_cols = required_cols + [col for col in money_flow_cols if col in money_flow_df.columns]
                        money_flow_df = money_flow_df[available_cols]

                        money_flow_dfs.append(money_flow_df)
                    else:
                        self.logger.debug(f"未获取到 {code} 的资金流向数据")
                except Exception as e:
                    self.logger.warning(f"获取 {code} 资金流向数据失败: {e}")

                # 优化延迟策略：减少延迟频率和时间
                if idx % 200 == 0 and idx > 0:
                    time.sleep(0.1)
                    self.logger.info(f"已处理 {idx+1}/{len(codes)} 个股票")

            except Exception as e:
                self.logger.error(f"获取 {code} 数据失败: {e}")
                continue

        # 合并融资融券数据
        mtss_result = pd.concat(mtss_dfs, ignore_index=True) if mtss_dfs else pd.DataFrame()
        money_flow_result = pd.concat(money_flow_dfs, ignore_index=True) if money_flow_dfs else pd.DataFrame()

        # 合并两个数据集
        if not mtss_result.empty and not money_flow_result.empty:
            # 确保day字段是date类型
            if 'day' in mtss_result.columns:
                mtss_result['day'] = pd.to_datetime(mtss_result['day']).dt.date
            if 'day' in money_flow_result.columns:
                money_flow_result['day'] = pd.to_datetime(money_flow_result['day']).dt.date

            result = pd.merge(mtss_result, money_flow_result, on=['code', 'day'], how='outer')
            self.logger.info(f"成功合并融资融券和资金流向数据，共 {len(result)} 条记录")
        elif not mtss_result.empty:
            result = mtss_result
            self.logger.info(f"仅获取到融资融券数据，共 {len(result)} 条记录")
        elif not money_flow_result.empty:
            result = money_flow_result
            self.logger.info(f"仅获取到资金流向数据，共 {len(result)} 条记录")
        else:
            result = pd.DataFrame()
            self.logger.warning("未获取到任何融资融券或资金流向数据")

        return result
    
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
                # 使用fq='pre'获取前复权价格数据（中国市场标准）
                # fq='pre': 返回前复权价格，最新日期factor=1.0，历史日期factor<1.0
                # 计算原始价格：raw_price = adjusted_price / factor
                df = get_price(
                    jq_code,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    frequency='daily',
                    fields=['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close', 'factor', 'high_limit', 'low_limit', 'avg', 'paused'],
                    skip_paused=False,
                    fq='pre'
                )
                
                if not df.empty:
                    # 重置索引，将日期作为列（修正：显式设置索引名为 day）
                    df.index.name = 'day'
                    df = df.reset_index()
                    
                    # 添加股票代码
                    df['code'] = self._from_jq_code(jq_code)

                    # 使用fq='pre'时，close已经是前复权价格（已调整）
                    # factor是复权因子：最新日期为1.0，历史日期<1.0
                    # adj_close就是close本身（已经是复权价格）
                    # 如需原始价格：raw_price = close / factor
                    if 'factor' in df.columns:
                        # fq='pre'模式：close已是前复权价格，无需再乘以factor
                        df['adj_close'] = df['close']
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
    