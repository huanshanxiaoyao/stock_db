"""Tushare数据源提供商"""

import logging
import time
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Union
import pandas as pd

try:
    import tushare as ts
except ImportError:
    ts = None

from data_source import DataSourceInterface, BaseDataSource, DataType
from models.stock_list import StockInfo


class TushareDataSource(BaseDataSource):
    """Tushare数据源实现类"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化Tushare数据源
        
        Args:
            config: 配置字典，包含token等认证信息
        """
        super().__init__('tushare')
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.pro = None
        
        # 检查tushare是否安装
        if ts is None:
            raise ImportError("请安装tushare包: pip install tushare")
        
        # 初始化token
        self.token = self._credentials.get('token', '')
        
        # 请求间隔控制
        self.request_interval = config.get('request_interval', 0.1)
        self.last_request_time = 0
        
        # 批处理大小
        self.batch_size = config.get('batch_size', 100)
        
    def authenticate(self, **credentials) -> bool:
        """认证Tushare数据源
        
        Args:
            **credentials: 认证凭据，包含token
            
        Returns:
            bool: 认证是否成功
        """
        try:
            # 更新token
            if 'token' in credentials:
                self.token = credentials['token']
                self._credentials['token'] = self.token
            
            if not self.token:
                self.logger.error("Tushare token未提供")
                return False
            
            # 设置token并初始化pro接口
            ts.set_token(self.token)
            self.pro = ts.pro_api(token=self.token)
            
            # 测试连接
            test_df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
            if test_df is not None and len(test_df) > 0:
                self._authenticated = True
                self.logger.info("Tushare认证成功")
                return True
            else:
                self.logger.error("Tushare认证失败：无法获取股票列表")
                return False
                
        except Exception as e:
            self.logger.error(f"Tushare认证失败: {e}")
            self._authenticated = False
            return False
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return self._authenticated
    
    def _wait_for_rate_limit(self):
        """等待请求间隔"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_interval:
            time.sleep(self.request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def _to_tushare_code(self, code: str) -> str:
        """转换股票代码为Tushare格式
        
        Args:
            code: 标准格式代码 (如: 000001.SZ)
            
        Returns:
            str: Tushare格式代码 (如: 000001.SZ)
        """
        # Tushare使用的格式与标准格式相同
        return code
    
    def _from_tushare_code(self, ts_code: str) -> str:
        """从Tushare格式转换为标准格式
        
        Args:
            ts_code: Tushare格式代码
            
        Returns:
            str: 标准格式代码
        """
        # Tushare使用的格式与标准格式相同
        return ts_code
    
    def get_stock_list(self, market: Optional[str] = None, **kwargs) -> List[str]:
        """获取股票代码列表"""
        if not self.is_authenticated():
            raise RuntimeError("Tushare数据源未认证")
            
        try:
            self._wait_for_rate_limit()
            
            # 获取股票基本信息
            df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code')
            
            if df is None or len(df) == 0:
                self.logger.warning("未获取到股票列表数据")
                return []
            
            # 根据市场筛选
            if market:
                if market.upper() == 'SH':
                    df = df[df['ts_code'].str.endswith('.SH')]
                elif market.upper() == 'SZ':
                    df = df[df['ts_code'].str.endswith('.SZ')]
                elif market.upper() == 'BJ':
                    df = df[df['ts_code'].str.endswith('.BJ')]
            
            codes = df['ts_code'].tolist()
            self.logger.info(f"成功获取股票代码列表，共{len(codes)}只股票")
            return codes
            
        except Exception as e:
            self.logger.error(f"获取股票代码列表失败: {e}")
            return []
    
    def get_all_stock_list(self) -> List[StockInfo]:
        """获取所有股票列表
        
        Returns:
            List[StockInfo]: 股票信息列表
        """
        if not self.is_authenticated():
            raise RuntimeError("Tushare数据源未认证")
        
        try:
            self._wait_for_rate_limit()
            
            # 获取所有上市股票
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date,delist_date'
            )
            
            if df is None or len(df) == 0:
                self.logger.warning("未获取到股票列表数据")
                return []
            
            stock_list = []
            for _, row in df.iterrows():
                # 转换上市日期
                list_date = None
                if pd.notna(row['list_date']):
                    try:
                        list_date = datetime.strptime(str(row['list_date']), '%Y%m%d').date()
                    except:
                        pass
                
                # 转换退市日期
                delist_date = None
                if pd.notna(row['delist_date']):
                    try:
                        delist_date = datetime.strptime(str(row['delist_date']), '%Y%m%d').date()
                    except:
                        pass
                
                # 确定交易所
                ts_code = row['ts_code']
                if ts_code.endswith('.SH'):
                    exchange = 'XSHG'
                elif ts_code.endswith('.SZ'):
                    exchange = 'XSHE'
                elif ts_code.endswith('.BJ'):
                    exchange = 'XBSE'
                else:
                    exchange = 'UNKNOWN'
                
                stock_info = StockInfo(
                    code=ts_code,
                    display_name=row['name'],
                    name=row['name'],
                    start_date=list_date,
                    end_date=delist_date,
                    type='stock',
                    exchange=exchange,
                    market=row.get('market', ''),
                    industry=row.get('industry', ''),
                    area=row.get('area', '')
                )
                stock_list.append(stock_info)
            
            self.logger.info(f"获取到 {len(stock_list)} 只股票")
            return stock_list
            
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")
            return []
    
    
    def get_market_data(self, codes: List[str], start_date: date, end_date: date,
                       data_type: str = DataType.PRICE_DATA) -> Optional[pd.DataFrame]:
        """获取市场数据

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型

        Returns:
            Optional[pd.DataFrame]: 市场数据
        """
        if not self.is_authenticated():
            raise RuntimeError("Tushare数据源未认证")

        if data_type == DataType.PRICE_DATA:
            return self._get_price_data(codes, start_date, end_date)
        elif data_type == DataType.DAILY_BASIC:
            return self._get_daily_basic_data(codes, start_date, end_date)
        else:
            self.logger.warning(f"不支持的市场数据类型: {data_type}")
            return None
    
    def _get_price_data(self, codes: List[str], start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """获取价格数据"""
        try:
            all_data = []
            
            # 批量处理股票代码
            for i in range(0, len(codes), self.batch_size):
                batch_codes = codes[i:i + self.batch_size]
                
                for code in batch_codes:
                    self._wait_for_rate_limit()
                    
                    ts_code = self._to_tushare_code(code)
                    
                    df = self.pro.daily(
                        ts_code=ts_code,
                        start_date=start_date.strftime('%Y%m%d'),
                        end_date=end_date.strftime('%Y%m%d')
                    )
                    
                    if df is not None and len(df) > 0:
                        # 转换日期格式
                        df['day'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                        df['code'] = code  # 使用标准格式代码
                        
                        # 选择和重命名字段 - 只选择数据库表中存在的字段
                        result = df[[
                            'code', 'day', 'open', 'high', 'low', 'close',
                            'pre_close', 'vol', 'amount'
                        ]].copy()
                        
                        # 重命名字段以匹配数据库schema
                        column_mapping = {
                            'vol': 'volume',
                            'amount': 'money'
                        }
                        
                        result = result.rename(columns=column_mapping)
                        
                        # 检查volume和money字段的负数问题并记录日志
                        negative_volume = result[result['volume'] < 0]
                        if not negative_volume.empty:
                            for _, row in negative_volume.iterrows():
                                self.logger.error(f"发现负数成交量数据: 股票代码={row['code']}, 日期={row['day']}, volume={row['volume']}")
                        
                        negative_money = result[result['money'] < 0]
                        if not negative_money.empty:
                            for _, row in negative_money.iterrows():
                                self.logger.error(f"发现负数成交额数据: 股票代码={row['code']}, 日期={row['day']}, money={row['money']}")
                        
                        # 添加tushare未返回的字段，设置为None不做任何假设
                        result['factor'] = None
                        result['high_limit'] = None
                        result['low_limit'] = None
                        result['avg'] = None
                        result['paused'] = None
                        result['adj_close'] = None
                        result['adj_factor'] = None
                        
                        # 重新排列字段顺序以匹配jqdata.py中的price_data表字段列表
                        result = result[['code', 'day', 'open', 'close', 'high', 'low', 'pre_close', 'volume', 'money', 
                                       'factor', 'high_limit', 'low_limit', 'avg', 'paused', 'adj_close', 'adj_factor']]
                        
                        all_data.append(result)
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)

                # 为北交所股票填充复权因子数据
                result = self._fill_bj_adjustment_factors(result, start_date, end_date)

                self.logger.info(f"获取到 {len(result)} 条价格数据")
                return result
            else:
                self.logger.warning("未获取到价格数据")
                return None
                
        except Exception as e:
            self.logger.error(f"获取价格数据失败: {e}")
            return None
    
    def _get_daily_basic_data(self, codes: List[str], start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """获取每日基本指标数据"""
        try:
            all_data = []

            # 批量处理股票代码
            for i in range(0, len(codes), self.batch_size):
                batch_codes = codes[i:i + self.batch_size]

                for code in batch_codes:
                    self._wait_for_rate_limit()

                    ts_code = self._to_tushare_code(code)

                    df = self.pro.daily_basic(
                        ts_code=ts_code,
                        start_date=start_date.strftime('%Y%m%d'),
                        end_date=end_date.strftime('%Y%m%d'),
                        fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,'
                               'pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,'
                               'total_share,float_share,free_share,total_mv,circ_mv'
                    )

                    if df is not None and len(df) > 0:
                        # 转换日期格式
                        df['day'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                        df['code'] = code  # 使用标准格式代码

                        # 删除原始的trade_date和ts_code字段
                        df = df.drop(columns=['trade_date', 'ts_code'], errors='ignore')

                        all_data.append(df)

            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"获取到 {len(result)} 条每日基本指标数据")
                return result
            else:
                self.logger.warning("未获取到每日基本指标数据")
                return None

        except Exception as e:
            self.logger.error(f"获取每日基本指标数据失败: {e}")
            return None


    def _fill_bj_adjustment_factors(self, df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
        """为北交所股票填充复权因子数据"""

        # 筛选出北交所股票
        bj_stocks = df[df['code'].str.endswith('.BJ')]['code'].unique()

        if len(bj_stocks) == 0:
            return df

        self.logger.info(f"开始获取 {len(bj_stocks)} 只北交所股票的复权因子数据")

        # 为每只北交所股票获取复权因子
        for code in bj_stocks:
            self._wait_for_rate_limit()

            # 调用Tushare复权因子接口
            adj_df = self.pro.adj_factor(
                ts_code=code,
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d')
            )

            if adj_df is not None and len(adj_df) > 0:
                # 转换日期格式以便匹配
                adj_df['day'] = pd.to_datetime(adj_df['trade_date'], format='%Y%m%d').dt.date

                # 与主数据框按code和day进行匹配更新
                for _, adj_row in adj_df.iterrows():
                    mask = (df['code'] == code) & (df['day'] == adj_row['day'])
                    if mask.any():
                        # 同时填充adj_factor和factor字段
                        df.loc[mask, 'adj_factor'] = adj_row['adj_factor']
                        df.loc[mask, 'factor'] = adj_row['adj_factor']  # 使用相同值填充factor

                self.logger.debug(f"已更新股票 {code} 的复权因子数据，共 {len(adj_df)} 条记录")


        return df

    def close(self):
        """关闭连接"""
        # 只有在明确需要关闭时才重置认证状态
        if hasattr(self, 'pro') and self.pro is not None:
            self.pro = None
        # 注释掉这行，避免意外重置认证状态
        # self._authenticated = False
        self.logger.info("Tushare连接已关闭")