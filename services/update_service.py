"""数据更新服务类"""

from ast import Not
from typing import List, Dict, Any, Optional, Set
from datetime import date, datetime, timedelta
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from database import DatabaseManager
from data_source import DataSourceManager, DataType
from config import get_config
from .utils import to_date


class UpdateService:
    """数据更新服务类，提供数据更新和同步功能"""
    
    def __init__(self, db_manager: DatabaseManager, data_source_manager: DataSourceManager):
        self.db = db_manager
        self.data_sources = data_source_manager
        self.logger = logging.getLogger(__name__)
        self.max_workers = 2  # 并发线程数
        self.request_delay = 0.2  # API请求间隔（秒）
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
    
    def update_stock_data(self, code: str, data_types: List[str],
                         force_full_update: bool = False,
                         end_date: Optional[date] = None,
                         latest_dates_cache: Optional[Dict[str, Dict[str, Optional[date]]]] = None) -> Dict[str, Any]:
        """更新单只股票的数据"""
        if not data_types:
            self.logger.error("update_stock_data 方法必须指定 data_types 参数")
            return {}
        
        results = {'code': code, 'updated': {}, 'errors': {}}
        
        self.logger.debug(f"开始更新 {code}: data_types={data_types}, force_full_update={force_full_update}")
        # 获取股票的上市和退市日期
        stock_info = self._get_stock_info(code)
        if not stock_info:
            results['errors']['stock_info'] = '无法获取股票信息'
            return results
        
        market_result = self._update_market_data(
            code, stock_info, force_full_update, data_types, end_date, latest_dates_cache
        )
        results['updated']['market'] = market_result

        return results
    
    def update_multiple_stocks(self, codes: List[str], data_types: List[str],
                              force_full_update: bool = False, 
                              max_workers: Optional[int] = None,
                              end_date: Optional[date] = None) -> Dict[str, Any]:
        """批量更新多只股票的数据
        
        Args:
            codes: 股票代码列表
            data_types: 数据类型列表，必须指定
            force_full_update: 是否强制全量更新
            max_workers: 最大工作线程数
            end_date: 结束日期
        
        Returns:
            更新结果字典
        
        Raises:
            ValueError: 当data_types为None或空列表时抛出异常
        """
        # 验证data_types参数
        if not data_types:
            raise ValueError("data_types参数不能为空，必须指定要更新的数据类型")
        
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
        
        
        # 分离批量处理和单股票处理的数据类型
        #前者是按天获取所有股票，不能指定股票
        batch_data_types = [dt for dt in data_types if dt in [DataType.INDICATOR_DATA, DataType.VALUATION_DATA]]
        individual_data_types = [dt for dt in data_types if dt not in batch_data_types]
        
        # 处理批量数据类型
        if batch_data_types:
            batch_results = self._update_multiple_stocks_batch(
                codes, batch_data_types, force_full_update, end_date
            )
            # 合并批量处理结果
            results['successful'].extend([{'code': code, 'updated': batch_results['successful'][code]} 
                                        for code in codes if code in batch_results['successful']])
            results['failed'].extend([{'code': code, 'errors': batch_results['failed'][code]} 
                                    for code in codes if code in batch_results['failed']])
        
        # 处理需要单股票遍历的数据类型
        if individual_data_types:
            # 批量查询最新日期以优化性能
            latest_dates_cache = self._batch_query_latest_dates(codes, individual_data_types, force_full_update)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交任务
                future_to_code = {
                    executor.submit(
                        self.update_stock_data, code, individual_data_types, force_full_update, end_date, latest_dates_cache
                    ): code for code in codes
                }
                
                processed = 0
                # 处理结果
                for future in as_completed(future_to_code):
                    code = future_to_code[future]
                    stock_result = future.result()
                    
                    # 查找是否已有该股票的结果（来自批量处理）
                    existing_success = next((item for item in results['successful'] if item['code'] == code), None)
                    existing_failed = next((item for item in results['failed'] if item['code'] == code), None)
                    
                    if stock_result.get('errors'):
                        if existing_failed:
                            # 合并错误信息
                            existing_failed['errors'].extend(stock_result['errors'])
                        elif existing_success:
                            # 从成功列表移到失败列表
                            results['successful'].remove(existing_success)
                            results['failed'].append({
                                'code': code,
                                'errors': stock_result['errors']
                            })
                        else:
                            results['failed'].append({
                                'code': code,
                                'errors': stock_result['errors']
                            })
                        status = 'fail'
                    else:
                        if existing_success:
                            # 合并更新信息
                            existing_success['updated'].update(stock_result['updated'])
                        elif not existing_failed:
                            results['successful'].append({
                                'code': code,
                                'updated': stock_result['updated']
                            })
                        status = 'ok'
                    
                    processed += 1
                    # 前10个和每满100个打印一次进度，避免刷屏
                    if processed <= 10 or processed % 100 == 0 or processed == total:
                        elapsed = time.time() - start_time
                        self.logger.info(f"[进度] {processed}/{total} 完成，最近: {code} ({status})，累计 成功 {len(results['successful'])} 失败 {len(results['failed'])}，耗时 {elapsed:.1f}s")
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
    
    def _update_multiple_stocks_batch(self, codes: List[str], data_types: List[str], 
                                     force_full_update: bool = False,
                                     end_date: Optional[date] = None) -> Dict[str, Any]:
        """批量处理数据类型（如indicator_data）的更新方法"""
        results = {
            'successful': {},
            'failed': {}
        }
        
        self.logger.info(f"开始批量处理数据类型: {data_types}，股票数量: {len(codes)}")
        
        for data_type in data_types:
            if data_type == DataType.INDICATOR_DATA or data_type == DataType.VALUATION_DATA:
                # 批量处理indicator数据
                batch_result = self._update_fundamental_data_batch(
                    codes, data_type, force_full_update, end_date
                )
                
                # 处理批量结果
                for code in codes:
                    if code not in results['successful']:
                        results['successful'][code] = {}
                    if code not in results['failed']:
                        results['failed'][code] = []
                    
                    if code in batch_result.get('successful', {}):
                        results['successful'][code][data_type] = batch_result['successful'][code]
                    else:
                        error_msg = batch_result.get('failed', {}).get(code, f"批量更新{data_type}失败")
                        results['failed'][code].append(error_msg)
                    
        # 清理空的失败记录
        results['failed'] = {k: v for k, v in results['failed'].items() if v}
        
        return results
    
    def _update_fundamental_data_batch(self, codes: List[str], date_type: DataType, force_full_update: bool = False,
                                    end_date: Optional[date] = None) -> Dict[str, Any]:
        """批量更新indicator数据的具体实现"""
        results = {
            'successful': {},
            'failed': {}
        }
        
        # 确定更新日期范围
        if end_date is None:
            end_date = date.today()
        
        if force_full_update:
            # 全量更新：从默认历史起始日期开始（来自配置）
            start_date = self.default_history_start_date
        else:
            # 增量更新：取 indicator_data 表的全局最后日期（不按 code）
            try:
                last_date = self.db.get_latest_date(date_type)
            except Exception as e:
                self.logger.warning(f"获取 {date_type} 全局最新日期失败: {e}")
                last_date = None
            if last_date:
                start_date = last_date + timedelta(days=1)
            else:
                start_date = self.default_history_start_date
        
        self.logger.info(f"批量更新{date_type}数据，日期范围: {start_date} 到 {end_date}")
        
        # 批量获取数据（按首选数据源分组调用）
        data_frames = []
        source_groups: Dict[str, List[str]] = {}
        for code in codes:
            name = self.data_sources.get_preferred_source_for_stock(code)
            if not name or name not in self.data_sources.sources:
                # 退化到默认数据源或任一可用数据源
                name = self.data_sources.get_default_source() or next(iter(self.data_sources.sources.keys()), None)
            if not name:
                self.logger.warning(f"未找到可用数据源: {code}")
                continue
            source_groups.setdefault(name, []).append(code)
        
        for name, group_codes in source_groups.items():
            source = self.data_sources.sources[name]
            try:
                self.logger.info(f"从数据源 {name} 批量获取{len(group_codes)}只 {date_type} ")
                df = source.get_market_data(group_codes, start_date, end_date, date_type)
                if df is not None and not df.empty:
                    data_frames.append(df)
            except Exception as e:
                self.logger.warning(f"从数据源 {name} 批量获取 {date_type} 失败: {e}")
        
        data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        
        if data is not None and not data.empty:
            # 保存数据到数据库
            self.db.save_dataframe(data, date_type)
            # 统计每个股票的更新情况（优化版本）
            if 'code' in data.columns:
                code_stats = data.groupby('code').size().to_dict()
                updated_codes = set(code_stats.keys())
                
                for code in codes:
                    if code in updated_codes:
                        results['successful'][code] = {
                            'records_count': code_stats[code],
                            'date_range': f"{start_date} 到 {end_date}"
                        }
                    else:
                        results['failed'][code] = f"未获取到{code}的{date_type}数据"
            else:
                # 如果没有code列，所有股票都算成功
                total_records = len(data)
                for code in codes:
                    results['successful'][code] = {
                        'records_count': total_records,
                        'date_range': f"{start_date} 到 {end_date}"
                    }
                
            self.logger.info(f"批量更新{date_type}数据完成，成功: {len(results['successful'])}只，失败: {len(results['failed'])}只")
        else:
            # 没有获取到数据
            for code in codes:
                results['failed'][code] = f"未获取到{date_type}数据"
            self.logger.warning(f"批量获取{date_type}数据为空，日期范围: {start_date} 到 {end_date}")
        
        return results

    
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
    
    def daily_update(self, exchange: str, data_types: List[str], stock_codes: Optional[List[str]] = None, 
                        ) -> Dict[str, Any]:
        """每日数据更新
        
        Args:
            exchange: 交易所类型 ('all', 'bj', 'sh', 'sz', 'sh_sz') - 指定要更新的交易所
            stock_codes: 具体股票代码列表（如果提供，忽略exchange参数）
            data_types: 要更新的数据类型列表
        """
        self.logger.info(f"开始每日数据更新，exchange={exchange}, stock_codes={'指定' if stock_codes else '未指定'}")
        
        results = {'update_time': datetime.now()}
        
        # 确定要更新的股票列表
        target_stocks = None
        if stock_codes:
            target_stocks = stock_codes
            self.logger.info(f"使用指定的 {len(stock_codes)} 只股票")
        elif exchange == 'all':
            # 获取所有股票代码
            target_stocks = self._get_all_stock_codes()
            self.logger.info(f"获取所有股票，共 {len(target_stocks) if target_stocks else 0} 只")
        elif exchange in ['bj','sh','sz','sh_sz']:
            target_stocks = self._get_stocks_by_exchange(exchange)
            self.logger.info(f"根据交易所类型 '{exchange}' 获取到 {len(target_stocks) if target_stocks else 0} 只股票")
        
        if not target_stocks:
            self.logger.error(f"没有获取到股票列表，exchange={exchange}")
            return results
        
        market_types = []
        # 检查是否包含具体的市场数据类型
        for dt in ['price_data', 'valuation_data', 'indicator_data', 'mtss_data']:
            if dt in data_types:
                market_types.append(dt)
        
        if market_types:
            market_result = self.update_multiple_stocks(target_stocks, market_types)
            results['market_update'] = market_result
        
        return results

    def _get_stocks_by_exchange(self, exchange: str) -> Optional[List[str]]:
        """根据交易所类型获取股票列表
        
        Args:
            exchange: 交易所类型 ('all', 'bj', 'sh_sz')
            
        Returns:
            股票代码列表，如果是'all'则返回None（表示更新所有股票）
        """
        if exchange not in ['bj','sh','sz','sh_sz']:
            self.logger.error(f"未知的交易所类型:")
            return None  
        # 获取所有股票代码
        all_stocks = self._get_all_stock_codes()
        if not all_stocks:
            self.logger.warning("无法获取股票列表")
            return []
        
        if exchange == 'bj':
            # 筛选北交所股票（以.BJ结尾）
            bj_stocks = [code for code in all_stocks if code.endswith('.BJ')]
            self.logger.info(f"找到 {len(bj_stocks)} 只北交所股票")
            return bj_stocks
        elif exchange == 'sh_sz':
            # 筛选沪深股票（不以.BJ结尾）
            sh_sz_stocks = [code for code in all_stocks if not code.endswith('.BJ')]
            self.logger.info(f"找到 {len(sh_sz_stocks)} 只沪深股票")
            return sh_sz_stocks
        elif exchange == 'sz':
            # 筛选深证股票（以.SZ结尾）
            sz_stocks = [code for code in all_stocks if code.endswith('.SZ')]
            self.logger.info(f"找到 {len(sz_stocks)} 只深证股票")
            return sz_stocks
        elif exchange == 'sh':
            # 筛选上交所股票（以.SH结尾）
            sh_stocks = [code for code in all_stocks if code.endswith('.SH')]
            self.logger.info(f"找到 {len(sh_stocks)} 只上交所股票")
            return sh_stocks
        else:
            self.logger.warning(f"未知的交易所类型: {exchange}，将更新所有股票")
            return None

    # ==================== 私有辅助方法 ====================
    
    def _get_stock_info(self, code: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        # 固定从jq数据源获取股票信息
        jq_source = self.data_sources.sources.get('jqdata')
        if not jq_source or not hasattr(jq_source, 'get_all_stock_list'):
            self.logger.error("jq数据源不存在或不支持获取股票列表")
            return {
                'code': code,
                'start_date': self.default_history_start_date,  # 默认开始日期来自配置
                'end_date': date.today(),
                'name': code,
                'display_name': code
            }

        # 优先使用缓存
        stock_list = None

        if isinstance(getattr(self, '_cached_stock_list', None), pd.DataFrame) and not self._cached_stock_list.empty:
            stock_list = self._cached_stock_list
        else:
            stock_list = jq_source.get_all_stock_list()
            if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                self._cached_stock_list = stock_list

        if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
            stock_info = stock_list[stock_list['code'] == code]
            if not stock_info.empty:
                info = stock_info.iloc[0].to_dict()

                # 统一把日期字段转为 datetime.date，并修正 end_date>today 的情况
                today = date.today()

                start_d = to_date(info.get('start_date')) or self.default_history_start_date
                end_d = to_date(info.get('end_date'))
                # 修正 JQData 活跃股票 end_date=2200-01-01 的哨兵值
                if end_d is None or end_d > today:
                    end_d = today
                info['start_date'] = start_d
                info['end_date'] = end_d
                if 'update_date' in info:
                    upd = to_date(info.get('update_date'))
                    info['update_date'] = upd or today

                # 补充必要字段
                info.setdefault('display_name', code)
                info.setdefault('name', code)
                info.setdefault('code', code)

                return info
 
        return None
    
    def _update_financial_data(self, code: str, stock_info: Dict[str, Any], 
                              force_full_update: bool) -> Dict[str, Any]:
        """更新财务数据
        这个实现，稍后废弃，或重写，不再对财务类一起更新，一个表一个表的更新
        _get_last_update_date 目前是已经被删除了，所有这个实现目前已经无法运行
        """
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

    def _batch_query_latest_dates(self, codes: List[str], data_types: List[str], force_full_update: bool) -> Dict[str, Dict[str, Optional[date]]]:
        """批量查询所有股票的最新日期，避免逐股票查询性能瓶颈

        Args:
            codes: 股票代码列表
            data_types: 数据类型列表
            force_full_update: 是否强制全量更新

        Returns:
            Dict[str, Dict[str, Optional[date]]]: 嵌套字典，结构为 {data_type: {code: latest_date}}
        """
        latest_dates_cache = {}

        if not force_full_update:
            start_time = time.time()
            self.logger.info(f"开始批量查询 {len(codes)} 只股票在 {len(data_types)} 个数据类型的最新日期")

            for data_type in data_types:
                try:
                    # 使用新的批量查询方法
                    dates_dict = self.db.get_latest_dates_batch(data_type, codes)
                    latest_dates_cache[data_type] = dates_dict

                    # 统计有数据的股票数量
                    has_data_count = len([d for d in dates_dict.values() if d is not None])
                    self.logger.debug(f"{data_type}: {has_data_count}/{len(codes)} 只股票有历史数据")

                except Exception as e:
                    self.logger.error(f"批量查询 {data_type} 最新日期失败: {e}")
                    # 如果批量查询失败，初始化为空字典，后续会使用单独查询
                    latest_dates_cache[data_type] = {code: None for code in codes}

            elapsed = time.time() - start_time
            self.logger.info(f"批量查询最新日期完成，耗时 {elapsed:.2f}秒，平均每只股票 {elapsed/len(codes)*1000:.1f}ms")
        else:
            # 强制全量更新时，不需要查询最新日期
            for data_type in data_types:
                latest_dates_cache[data_type] = {code: None for code in codes}
            self.logger.debug("强制全量更新模式，跳过最新日期查询")

        return latest_dates_cache

    def _update_market_data(self, code: str, stock_info: Dict[str, Any],
                           force_full_update: bool, data_types: List[str],
                           end_date: Optional[date] = None,
                           latest_dates_cache: Optional[Dict[str, Dict[str, Optional[date]]]] = None) -> Dict[str, Any]:
        """更新市场数据"""
        results = {}

        
        # 更新各类市场数据
        for data_type in data_types:
            # 确定更新日期范围
            if force_full_update:
                start_base = stock_info.get('start_date') or self.default_history_start_date
                start_date = max(start_base, self.default_history_start_date)
                end_date_used = end_date or stock_info.get('end_date', date.today())
            else:
                latest = None
                # 优先使用缓存的最新日期，避免重复数据库查询
                if latest_dates_cache and data_type in latest_dates_cache:
                    latest = latest_dates_cache[data_type].get(code)
                else:
                    # 如果缓存中没有，才进行数据库查询（兼容旧逻辑）
                    try:
                        latest = self.db.get_latest_date(data_type, code)
                    except Exception as e:
                        self.logger.warning(f"获取 {code} {data_type} 数据最后日期失败: {e}")

                if latest:
                    start_date = latest + timedelta(days=1)
                else:
                    start_base = stock_info.get('start_date') or self.default_history_start_date
                    start_date = max(start_base, self.default_history_start_date)
                end_date_used = end_date or date.today()

            # 统一夹到今天，避免 JQData 报 结束日期 2200-01-01 不能晚于今天
            end_date_used = min(end_date_used, date.today())
            #self.logger.debug(f"{code} 获取 {data_type} 数据: {start_date} -> {end_date_used}")
            
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
                    
        return results
    
    
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
        
        return None
    
    def _fetch_market_data(self, code: str, data_type: str, 
                      start_date: date, end_date: date) -> Optional[pd.DataFrame]:
        """从数据源获取市场数据，仅使用首选数据源"""
        # 仅尝试首选数据源
        preferred_source_name = self.data_sources.get_preferred_source_for_stock(code)
        if preferred_source_name and preferred_source_name in self.data_sources.sources:
            source = self.data_sources.sources[preferred_source_name]
            if data_type == DataType.VALUATION_DATA:
                result = source.get_valuation_data([code], start_date, end_date)
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

            return None
    
    def _get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        # 从数据源获取股票列表
        self.logger.info(f"开始 _get_all_stock_codes, 数据源: {self.data_sources.sources}")
        for source_name, source in self.data_sources.sources.items():
            self.logger.info(f"开始 _get_all_stock_codes，数据源: {source_name}")
            if hasattr(source, 'get_all_stock_list'):
                stock_list = source.get_all_stock_list()
                if isinstance(stock_list, pd.DataFrame) and not stock_list.empty:
                    codes = stock_list['code'].tolist()
                    self.logger.info(f"从 {source_name} 获取股票列表，共 {len(codes)} 只股票")
                    return codes
                
                # 如果没有get_all_stock_list，使用get_stock_list
                stock_list = source.get_stock_list()
                self.logger.info(f"从 {source_name} 获取股票列表，类型: {type(stock_list)}")

                if isinstance(stock_list, list) and stock_list:
                    return stock_list  
                elif hasattr(stock_list, 'empty') and not stock_list.empty:
                    codes = stock_list['code'].tolist()
                    return codes  
            else:
                self.logger.error(f" {source_name} 没有get_all_stock_list")
            
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