"""数据更新服务类"""

from typing import List, Dict, Any, Optional, Set
from datetime import date, datetime, timedelta
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from database import DatabaseManager
from data_source import DataSourceManager, DataType
from config import get_config


class UpdateService:
    """数据更新服务类，提供数据更新和同步功能"""
    
    def __init__(self, db_manager: DatabaseManager, data_source_manager: DataSourceManager):
        self.db = db_manager
        self.data_sources = data_source_manager
        self.logger = logging.getLogger(__name__)
        self.max_workers = 3  # 并发线程数
        self.request_delay = 0.1  # API请求间隔（秒）
        # 从配置读取默认历史起始日期
        cfg = get_config()
        cfg_val = getattr(getattr(cfg, 'update', None), 'default_history_start_date', None)
        default_date = date(2019, 1, 1)
        if isinstance(cfg_val, date):
            default_date = cfg_val
        elif isinstance(cfg_val, str) and cfg_val:
            try:
                default_date = datetime.strptime(cfg_val, "%Y-%m-%d").date()
            except Exception:
                self.logger.warning(f"default_history_start_date 格式不正确: {cfg_val}，已回退到 2019-01-01")
        self.default_history_start_date = default_date
        # 缓存股票列表，避免重复请求数据源
        self._cached_stock_list = None
    
    # ==================== 增量更新服务 ====================
    
    def update_stock_data(self, code: str, data_types: Optional[List[str]] = None, 
                         force_full_update: bool = False,
                         end_date: Optional[date] = None) -> Dict[str, Any]:
        """更新单只股票的数据"""
        if data_types is None:
            data_types = ['financial', 'market']
        
        results = {'code': code, 'updated': {}, 'errors': {}}
        
        try:
            self.logger.debug(f"开始更新 {code}: data_types={data_types}, force_full_update={force_full_update}")
            # 获取股票的上市和退市日期
            stock_info = self._get_stock_info(code)
            if not stock_info:
                results['errors']['stock_info'] = '无法获取股票信息'
                return results
            
            # 更新财务数据
            if 'financial' in data_types:
                financial_result = self._update_financial_data(
                    code, stock_info, force_full_update
                )
                results['updated']['financial'] = financial_result
            
            # 计算需要更新的市场子类型
            market_types_to_update = []
            # 如果包含类别 market，则包含全部市场子表
            if 'market' in data_types:
                market_types_to_update = [
                    DataType.FUNDAMENTAL_DATA, DataType.INDICATOR_DATA, DataType.MTSS_DATA, DataType.PRICE_DATA
                ]
            # 如果包含具体的市场表名（如 price_data），也加入
            for t in [DataType.FUNDAMENTAL_DATA, DataType.INDICATOR_DATA, DataType.MTSS_DATA, DataType.PRICE_DATA]:
                if t in data_types and t not in market_types_to_update:
                    market_types_to_update.append(t)
            
            # 更新市场数据（如果需要）
            if market_types_to_update:
                market_result = self._update_market_data(
                    code, stock_info, force_full_update, market_types=market_types_to_update
                )
                results['updated']['market'] = market_result
            
        except Exception as e:
            self.logger.error(f"更新股票 {code} 数据时发生错误: {e}")
            results['errors']['general'] = str(e)
        
        return results
    
    def update_multiple_stocks(self, codes: List[str], data_types: Optional[List[str]] = None,
                              force_full_update: bool = False, 
                              max_workers: Optional[int] = None,
                              end_date: Optional[date] = None) -> Dict[str, Any]:
        """批量更新多只股票的数据"""
        if max_workers is None:
            max_workers = self.max_workers
        
        results = {
            'total_stocks': len(codes),
            'successful': [],
            'failed': [],
            'summary': {}
        }
        
        start_time = time.time()
        total = len(codes)
        self.logger.info(f"准备批量更新 {total} 只股票，data_types={data_types}, force_full_update={force_full_update}, max_workers={max_workers}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self.update_stock_data, code, data_types, force_full_update, end_date
                ): code for code in codes
            }
            
            processed = 0
            # 处理结果
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    stock_result = future.result()
                    if stock_result.get('errors'):
                        results['failed'].append({
                            'code': code,
                            'errors': stock_result['errors']
                        })
                        status = 'fail'
                    else:
                        results['successful'].append({
                            'code': code,
                            'updated': stock_result['updated']
                        })
                        status = 'ok'
                except Exception as e:
                    self.logger.error(f"处理股票 {code} 更新结果时发生错误: {e}")
                    results['failed'].append({
                        'code': code,
                        'errors': {'processing': str(e)}
                    })
                    status = 'exception'
                
                processed += 1
                elapsed = time.time() - start_time
                # 前10个和每满100个打印一次进度，避免刷屏
                if processed <= 10 or processed % 100 == 0 or processed == total:
                    self.logger.info(f"[进度] {processed}/{total} 完成，最近: {code} ({status})，累计 成功 {len(results['successful'])} 失败 {len(results['failed'])}，耗时 {elapsed:.1f}s")
                
                # 控制请求频率
                time.sleep(self.request_delay)  
        
        # 生成摘要
        end_time = time.time()
        results['summary'] = {
            'successful_count': len(results['successful']),
            'failed_count': len(results['failed']),
            'success_rate': len(results['successful']) / len(codes) * 100 if codes else 0.0,
            'total_time': end_time - start_time
        }
        
        self.logger.info(
            f"批量更新完成: 成功 {results['summary']['successful_count']}只, "
            f"失败 {results['summary']['failed_count']}只, "
            f"成功率 {results['summary']['success_rate']:.1f}%, "
            f"耗时 {results['summary']['total_time']:.1f}秒"
        )
        
        return results
    
    def update_all_stocks(self, data_types: Optional[List[str]] = None,
                         force_full_update: bool = False) -> Dict[str, Any]:
        """更新所有股票的数据"""
        # 获取所有股票代码
        all_codes = self._get_all_stock_codes()
        
        if not all_codes:
            return {
                'error': '无法获取股票列表',
                'total_stocks': 0,
                'successful': [],
                'failed': []
            }
        
        self.logger.info(f"开始更新所有股票数据，共 {len(all_codes)} 只股票")
        
        return self.update_multiple_stocks(
            all_codes, data_types, force_full_update
        )
    
    def update_bj_stocks(self, codes: List[str], data_types: Optional[List[str]] = None,
                        force_full_update: bool = False, max_workers: Optional[int] = None,
                        end_date: Optional[date] = None) -> Dict[str, Any]:
        """更新北交所股票数据，使用统一的更新流程"""
        self.logger.info(f"准备更新 {len(codes)} 只北交所股票，data_types={data_types}, force_full_update={force_full_update}")
        
        # 直接使用主流程的update_multiple_stocks方法
        # 数据源选择逻辑已经在_fetch_market_data和_fetch_financial_data中处理
        return self.update_multiple_stocks(
            codes=codes,
            data_types=data_types,
            force_full_update=force_full_update,
            max_workers=max_workers,
            end_date=end_date
        )
    


    # ==================== 定时更新服务 ====================
    
    def daily_update(self) -> Dict[str, Any]:
        """每日数据更新"""
        self.logger.info("开始每日数据更新")
        
        # 更新市场数据（每日）
        market_result = self.update_all_stocks(['market'])
        
        # 检查是否需要更新财务数据（季度数据）
        financial_result = None
        if self._should_update_financial_data():
            self.logger.info("检测到需要更新财务数据")
            financial_result = self.update_all_stocks(['financial'])
        
        return {
            'market_update': market_result,
            'financial_update': financial_result,
            'update_time': datetime.now()
        }

    # ==================== 私有辅助方法 ====================
    
    def _get_stock_info(self, code: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        # 从数据源获取股票信息
        for source_name, source in self.data_sources.sources.items():
            # 优先使用缓存
            stock_list = None
            if hasattr(source, 'get_all_stock_list'):
                if isinstance(getattr(self, '_cached_stock_list', None), pd.DataFrame) and not self._cached_stock_list.empty:
                    stock_list = self._cached_stock_list
                else:
                    stock_list = source.get_all_stock_list()
                    if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                        self._cached_stock_list = stock_list

                if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                    stock_info = stock_list[stock_list['code'] == code]
                    if not stock_info.empty:
                        info = stock_info.iloc[0].to_dict()

                        # 统一把日期字段转为 datetime.date，并修正 end_date>today 的情况
                        today = date.today()

                        def _to_date(val):
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

                        start_d = _to_date(info.get('start_date')) or self.default_history_start_date
                        end_d = _to_date(info.get('end_date'))
                        # 修正 JQData 活跃股票 end_date=2200-01-01 的哨兵值
                        if end_d is None or end_d > today:
                            end_d = today
                        info['start_date'] = start_d
                        info['end_date'] = end_d
                        if 'update_date' in info:
                            upd = _to_date(info.get('update_date'))
                            info['update_date'] = upd or today

                        # 补充必要字段
                        info.setdefault('display_name', code)
                        info.setdefault('name', code)
                        info.setdefault('code', code)

                        return info
            else:
                # 如果没有get_all_stock_list方法，返回基本信息
                return {
                    'code': code,
                    'start_date': self.default_history_start_date,  # 默认开始日期来自配置
                    'end_date': date.today(),
                    'name': code,
                    'display_name': code
                }
        
        return None
    
    def _update_financial_data(self, code: str, stock_info: Dict[str, Any], 
                              force_full_update: bool) -> Dict[str, Any]:
        """更新财务数据"""
        results = {}
        
        # 确定更新日期范围
        if force_full_update:
            start_date = stock_info.get('start_date')
            end_date = stock_info.get('end_date', date.today())
        else:
            # 增量更新：从最后更新日期开始
            start_date = self._get_last_update_date(code, 'financial')
            end_date = date.today()
        
        self.logger.debug(f"{code} 财务数据更新范围: {start_date} -> {end_date}")
        
        # 更新各类财务数据
        financial_types = ['income_statement', 'cashflow_statement', 'balance_sheet']
        
        for data_type in financial_types:
            try:
                # 从数据源获取数据
                data = self._fetch_financial_data(code, data_type, start_date, end_date)
                
                if data is not None and not data.empty:
                    rows = len(data)
                    # 保存到数据库
                    self.db.save_dataframe(data, data_type)
                    results[data_type] = {
                        'records_updated': rows,
                        'date_range': f'{start_date} to {end_date}'
                    }
                    self.logger.info(f"{code} {data_type} 更新完成，写入 {rows} 行")
                else:
                    results[data_type] = {
                        'records_updated': 0,
                        'message': '无新数据'
                    }
                    
            except Exception as e:
                self.logger.error(f"更新 {code} 的 {data_type} 数据时发生错误: {e}")
                results[data_type] = {
                    'error': str(e)
                }
        
        return results
    
    def _update_market_data(self, code: str, stock_info: Dict[str, Any], 
                           force_full_update: bool, market_types: Optional[List[str]] = None,
                           end_date: Optional[date] = None) -> Dict[str, Any]:
        """更新市场数据"""
        results = {}
        
        # 默认市场数据类型（包含价格数据）
        if market_types is None or len(market_types) == 0:
            market_types = [DataType.FUNDAMENTAL_DATA, DataType.INDICATOR_DATA, DataType.MTSS_DATA, DataType.PRICE_DATA]
        
        # 确定更新日期范围
        if force_full_update:
            start_base = stock_info.get('start_date') or self.default_history_start_date
            start_date = max(start_base, self.default_history_start_date)
            end_date_used = end_date or stock_info.get('end_date', date.today())
        else:
            if len(market_types) == 1 and market_types[0] == DataType.PRICE_DATA:
                latest = None
                try:
                    latest = self.db.get_latest_date(DataType.PRICE_DATA, code)
                except Exception as e:
                    self.logger.warning(f"获取 {code} 价格数据最后日期失败: {e}")
                if latest:
                    start_date = latest + timedelta(days=1)
                else:
                    start_base = stock_info.get('start_date') or self.default_history_start_date
                    start_date = max(start_base, self.default_history_start_date)
                end_date_used = end_date or date.today()
            else:
                start_date = self._get_last_update_date(code, 'market')
                end_date_used = end_date or date.today()

        # 统一夹到今天，避免 JQData 报 结束日期 2200-01-01 不能晚于今天
        end_date_used = min(end_date_used, date.today())

        self.logger.debug(f"{code} 市场数据更新范围: {start_date} -> {end_date_used}, types={market_types}")
        
        # 更新各类市场数据
        for data_type in market_types:
            try:
                self.logger.debug(f"{code} 获取 {data_type} 数据: {start_date} -> {end_date_used}")
                # 从数据源获取数据（修正：传入 end_date_used）
                data = self._fetch_market_data(code, data_type, start_date, end_date_used)
                
                if data is not None and not data.empty:
                    rows = len(data)
                    # 保存到数据库
                    self.db.save_dataframe(data, data_type)
                    results[data_type] = {
                        'records_updated': rows,
                        'date_range': f'{start_date} to {end_date_used}'
                    }
                    self.logger.info(f"{code} {data_type} 更新完成，写入 {rows} 行")
                else:
                    results[data_type] = {
                        'records_updated': 0,
                        'message': '无新数据'
                    }
                    self.logger.info(f"{code} {data_type} 无新数据")
                    
            except Exception as e:
                self.logger.error(f"更新 {code} 的 {data_type} 数据时发生错误: {e}")
                results[data_type] = {
                    'error': str(e)
                }
        
        return results
    
    def _get_last_update_date(self, code: str, data_category: str) -> Optional[date]:
        """获取最后更新日期"""
        try:
            if data_category == 'financial':
                tables = DataType.get_financial_types()
            else:  # market
                tables = DataType.get_market_types()  # 包含 price_data
            
            latest_date = None
            for table in tables:
                if self.db.table_exists(table):
                    table_latest = self.db.get_latest_date(table, code)
                    if table_latest:
                        if latest_date is None or table_latest > latest_date:
                            latest_date = table_latest
            
            # 如果有最新日期，从下一天开始更新
            if latest_date:
                return latest_date + timedelta(days=1)
            else:
                # 如果没有数据，从配置的默认历史起始日期开始
                return self.default_history_start_date
                
        except Exception as e:
            self.logger.error(f"获取 {code} 最后更新日期时发生错误: {e}")
            return self.default_history_start_date
    
    def _fetch_financial_data(self, code: str, data_type: str, 
                             start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """从数据源获取财务数据，智能选择数据源"""
        # 优先尝试首选数据源
        preferred_source_name = self.data_sources.get_preferred_source_for_stock(code)
        if preferred_source_name and preferred_source_name in self.data_sources.sources:
            source = self.data_sources.sources[preferred_source_name]
            try:
                if data_type == DataType.INCOME_STATEMENT:
                    result = source.get_income_statement(code, start_date, end_date)
                elif data_type == DataType.CASHFLOW_STATEMENT:
                    result = source.get_cashflow_statement(code, start_date, end_date)
                elif data_type == DataType.BALANCE_SHEET:
                    result = source.get_balance_sheet(code, start_date, end_date)
                else:
                    result = None
                
                if result is not None and not result.empty:
                    return result
            except Exception as e:
                self.logger.warning(f"从首选数据源 {preferred_source_name} 获取 {data_type} 数据失败: {e}")
        
        # 如果首选数据源失败，尝试其他数据源
        for source_name, source in self.data_sources.sources.items():
            if source_name == preferred_source_name:
                continue  # 跳过已经尝试过的首选数据源
            try:
                if data_type == DataType.INCOME_STATEMENT:
                    return source.get_income_statement(code, start_date, end_date)
                elif data_type == DataType.CASHFLOW_STATEMENT:
                    return source.get_cashflow_statement(code, start_date, end_date)
                elif data_type == DataType.BALANCE_SHEET:
                    return source.get_balance_sheet(code, start_date, end_date)
            except Exception as e:
                self.logger.warning(f"从 {source_name} 获取 {data_type} 数据失败: {e}")
                continue
        
        return None
    
    def _fetch_market_data(self, code: str, data_type: str, 
                          start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """从数据源获取市场数据，智能选择数据源"""
        # 优先尝试首选数据源
        preferred_source_name = self.data_sources.get_preferred_source_for_stock(code)
        if preferred_source_name and preferred_source_name in self.data_sources.sources:
            source = self.data_sources.sources[preferred_source_name]
            try:
                if data_type == DataType.FUNDAMENTAL_DATA:
                    result = source.get_fundamental_data([code], start_date, end_date)
                elif data_type == DataType.INDICATOR_DATA:
                    result = source.get_market_data([code], start_date, end_date, DataType.INDICATOR_DATA)
                elif data_type == DataType.MTSS_DATA:
                    result = source.get_market_data([code], start_date, end_date, DataType.MTSS_DATA)
                elif data_type == DataType.PRICE_DATA:
                    result = source.get_market_data([code], start_date, end_date, DataType.PRICE_DATA)
                else:
                    result = None
                
                if result is not None and not result.empty:
                    return result
            except Exception as e:
                self.logger.warning(f"从首选数据源 {preferred_source_name} 获取 {data_type} 数据失败: {e}")
        
        # 如果首选数据源失败，尝试其他数据源
        for source_name, source in self.data_sources.sources.items():
            if source_name == preferred_source_name:
                continue  # 跳过已经尝试过的首选数据源
            try:
                if data_type == DataType.FUNDAMENTAL_DATA:
                    return source.get_fundamental_data([code], start_date, end_date)
                elif data_type == DataType.INDICATOR_DATA:
                    return source.get_market_data([code], start_date, end_date, DataType.INDICATOR_DATA)
                elif data_type == DataType.MTSS_DATA:
                    return source.get_market_data([code], start_date, end_date, DataType.MTSS_DATA)
                elif data_type == DataType.PRICE_DATA:
                    return source.get_market_data([code], start_date, end_date, DataType.PRICE_DATA)
            except Exception as e:
                self.logger.warning(f"从 {source_name} 获取 {data_type} 数据失败: {e}")
                continue
        
        return None
    
    def _get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        try:
            # 从数据源获取股票列表
            for source_name, source in self.data_sources.sources.items():
                try:
                    # 优先使用get_all_stock_list获取完整信息
                    if hasattr(source, 'get_all_stock_list'):
                        stock_list = source.get_all_stock_list()
                        if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                            codes = stock_list['code'].tolist()
                            # 过滤北交所
                            return [c for c in codes if not c.endswith('.BJ')]
                    
                    # 如果没有get_all_stock_list，使用get_stock_list
                    stock_list = source.get_stock_list()
                    if isinstance(stock_list, list) and stock_list:
                        return [c for c in stock_list if not c.endswith('.BJ')]
                    elif hasattr(stock_list, 'empty') and not stock_list.empty:
                        codes = stock_list['code'].tolist()
                        return [c for c in codes if not c.endswith('.BJ')]
                        
                except Exception as e:
                    self.logger.warning(f"从 {source_name} 获取股票列表失败: {e}")
                    continue
            
            # 如果数据源获取失败，从数据库获取已有股票代码
            existing_codes = set()
            tables = ['income_statement', 'fundamental_data', 'indicator_data']
            
            for table in tables:
                if self.db.table_exists(table):
                    codes = self.db.get_existing_codes(table)
                    existing_codes.update(codes)
            
            return [c for c in list(existing_codes) if not c.endswith('.BJ')]
            
        except Exception as e:
            self.logger.error(f"获取股票代码列表时发生错误: {e}")
            return []
    
    def _should_update_financial_data(self) -> bool:
        """判断是否需要更新财务数据"""
        try:
            # 检查是否是季度末或年末
            today = date.today()
            month = today.month
            
            # 财务数据通常在季度结束后1-2个月发布
            # 检查是否是财务数据发布期间
            financial_months = [4, 5, 8, 9, 10, 11]  # 4月(Q1), 8-9月(Q2), 10-11月(Q3), 4-5月(Q4)
            
            if month in financial_months:
                return True
            
            # 检查数据库中最新财务数据的日期
            latest_dates = []
            for table in ['income_statement', 'cashflow_statement', 'balance_sheet']:
                if self.db.table_exists(table):
                    sql = f"SELECT MAX(stat_date) as latest_date FROM {table}"
                    result = self.db.query(sql)
                    if not result.empty and result.iloc[0]['latest_date']:
                        latest_dates.append(result.iloc[0]['latest_date'])
            
            if latest_dates:
                latest_date = max(latest_dates)
                # 如果最新数据超过3个月，需要更新
                if (today - latest_date).days > 90:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"判断是否需要更新财务数据时发生错误: {e}")
            return False