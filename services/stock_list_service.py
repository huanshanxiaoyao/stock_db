"""股票列表服务"""

import pandas as pd
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import logging

from database import DatabaseInterface
from data_source import BaseDataSource
from models.stock_list import StockInfo


class StockListService:
    """股票列表服务"""
    
    def __init__(self, database: DatabaseInterface, data_source: BaseDataSource):
        self.database = database
        self.data_source = data_source
        self.logger = logging.getLogger(__name__)
    
    def update_stock_list(self, force_update: bool = False) -> bool:
        """更新股票列表
        
        Args:
            force_update: 是否强制更新，忽略最后更新时间
        
        Returns:
            bool: 更新是否成功
        """
        try:
            # 检查是否需要更新
            if not force_update and not self._need_update():
                self.logger.info("股票列表无需更新")
                return True
            
            self.logger.info("开始更新股票列表...")
            
            # 从数据源获取最新股票列表
            if hasattr(self.data_source, 'get_all_stock_list'):
                df_stocks = self.data_source.get_all_stock_list()
            else:
                self.logger.error("数据源不支持获取完整股票列表")
                return False
            
            if df_stocks.empty:
                self.logger.warning("未获取到股票列表数据")
                return False
            
            # 转换为StockInfo对象列表
            stock_models = []
            for _, row in df_stocks.iterrows():
                try:
                    # 处理日期字段
                    start_date = row['start_date']
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    elif hasattr(start_date, 'date'):
                        start_date = start_date.date()
                    
                    end_date = row.get('end_date')
                    if end_date and isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    elif end_date and hasattr(end_date, 'date'):
                        end_date = end_date.date()
                    
                    stock_info = StockInfo(
                        code=row['code'],
                        display_name=row['display_name'],
                        name=row['name'],
                        start_date=start_date,
                        end_date=end_date,
                        exchange=row.get('exchange'),
                        market=row.get('market'),
                        status=row.get('status', 'normal'),
                        is_st=row.get('is_st', False),
                        update_date=date.today()
                    )
                    
                    if stock_info.validate():
                        stock_models.append(stock_info)
                    else:
                        self.logger.warning(f"股票信息验证失败: {row['code']} - code: {stock_info.code}, display_name: {stock_info.display_name}, start_date: {stock_info.start_date}")
                        
                except Exception as e:
                    self.logger.error(f"处理股票信息失败 {row.get('code', 'unknown')}: {e}")
                    continue
            
            if not stock_models:
                self.logger.error("没有有效的股票信息")
                return False
            
            # 批量插入数据库
            success = self.database.save_models(stock_models)
            
            if success:
                self.logger.info(f"成功更新 {len(stock_models)} 只股票信息")
                return True
            else:
                self.logger.error("股票列表数据库更新失败")
                return False
                
        except Exception as e:
            self.logger.error(f"更新股票列表失败: {e}")
            return False
    
    def _need_update(self) -> bool:
        """
        检查是否需要更新股票列表
        
        Returns:
            是否需要更新
        """
        try:
            # 检查表是否存在数据
            result = self.database.query(
                "SELECT COUNT(*) as count FROM stock_list"
            )
            
            if result.empty or result.iloc[0]['count'] == 0:
                return True
            
            # 检查最后更新时间
            result = self.database.query(
                "SELECT MAX(update_date) as last_update FROM stock_list"
            )
            
            if result.empty or result.iloc[0]['last_update'] is None:
                return True
            
            last_update = pd.to_datetime(result.iloc[0]['last_update'])
            days_since_update = (datetime.now() - last_update).days
            
            # 如果超过7天未更新，则需要更新
            return days_since_update > 7
            
        except Exception as e:
            logging.error(f"检查更新状态失败: {e}")
            return True
    
    def get_active_stocks(self, exchange: Optional[str] = None, 
                         market: Optional[str] = None) -> pd.DataFrame:
        """获取活跃股票列表
        
        Args:
            exchange: 交易所过滤 (XSHG/XSHE/BSE)
            market: 市场过滤 (main/gem/star/bse)
        
        Returns:
            pd.DataFrame: 股票列表
        """
        try:
            sql = "SELECT * FROM stock_list WHERE status = 'normal' AND end_date IS NULL"
            params = {}
            
            if exchange:
                sql += " AND exchange = :exchange"
                params['exchange'] = exchange
            
            if market:
                sql += " AND market = :market"
                params['market'] = market
            
            sql += " ORDER BY code"
            
            if params:
                result = self.database.query(sql, params)
            else:
                result = self.database.query(sql)
            
            return result
            
        except Exception as e:
            self.logger.error(f"获取活跃股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, code: str) -> Optional[StockInfo]:
        """获取单个股票信息
        
        Args:
            code: 股票代码
        
        Returns:
            StockInfo: 股票信息对象，如果不存在返回None
        """
        try:
            sql = "SELECT * FROM stock_list WHERE code = :code"
            result = self.database.query(sql, {'code': code})
            
            if result.empty:
                return None
            
            row = result.iloc[0]
            return StockInfo(
                code=row['code'],
                display_name=row['display_name'],
                name=row['name'],
                start_date=row['start_date'],
                end_date=row.get('end_date'),
                exchange=row.get('exchange'),
                market=row.get('market'),
                industry_code=row.get('industry_code'),
                industry_name=row.get('industry_name'),
                sector_code=row.get('sector_code'),
                sector_name=row.get('sector_name'),
                status=row.get('status', 'normal'),
                is_st=row.get('is_st', False),
                update_date=row.get('update_date')
            )
            
        except Exception as e:
            self.logger.error(f"获取股票信息失败 {code}: {e}")
            return None
    
    def search_stocks(self, keyword: str, limit: int = 50) -> pd.DataFrame:
        """搜索股票
        
        Args:
            keyword: 搜索关键词（股票代码或名称）
            limit: 返回结果数量限制
        
        Returns:
            pd.DataFrame: 搜索结果
        """
        try:
            sql = """
            SELECT * FROM stock_list 
            WHERE (code LIKE :keyword1 OR display_name LIKE :keyword2 OR name LIKE :keyword3) 
            AND status = 'normal' AND end_date IS NULL
            ORDER BY 
                CASE 
                    WHEN code = :exact_code THEN 1
                    WHEN code LIKE :code_pattern THEN 2
                    WHEN display_name LIKE :name_pattern THEN 3
                    ELSE 4
                END,
                code
            LIMIT :limit
            """
            
            keyword_pattern = f"%{keyword}%"
            params = {
                'keyword1': keyword_pattern,
                'keyword2': keyword_pattern, 
                'keyword3': keyword_pattern,
                'exact_code': keyword,
                'code_pattern': f"{keyword}%",
                'name_pattern': f"{keyword}%",
                'limit': limit
            }
            
            result = self.database.query(sql)
            return result
            
        except Exception as e:
            self.logger.error(f"搜索股票失败: {e}")
            return pd.DataFrame()
    
    def get_stock_count_by_market(self) -> Dict[str, int]:
        """获取各市场股票数量统计
        
        Returns:
            Dict[str, int]: 各市场股票数量
        """
        try:
            sql = """
            SELECT 
                exchange,
                market,
                COUNT(*) as count
            FROM stock_list 
            WHERE status = 'normal' AND end_date IS NULL
            GROUP BY exchange, market
            ORDER BY exchange, market
            """
            
            result = self.database.query(sql)
            
            stats = {}
            for _, row in result.iterrows():
                key = f"{row['exchange']}_{row['market']}"
                stats[key] = row['count']
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取股票统计失败: {e}")
            return {}
    
    def update_stock_industry(self, code: str, industry_code: str, 
                            industry_name: str, sector_code: str = None, 
                            sector_name: str = None) -> bool:
        """更新股票行业信息
        
        Args:
            code: 股票代码
            industry_code: 行业代码
            industry_name: 行业名称
            sector_code: 板块代码
            sector_name: 板块名称
        
        Returns:
            bool: 更新是否成功
        """
        try:
            data = {
                'industry_code': industry_code,
                'industry_name': industry_name,
                'update_date': date.today()
            }
            
            if sector_code:
                data['sector_code'] = sector_code
            if sector_name:
                data['sector_name'] = sector_name
            
            conditions = {'code': code}
            
            success = self.database.update_data('stock_list', data, conditions)
            
            if success:
                self.logger.info(f"更新股票行业信息成功: {code}")
            else:
                self.logger.error(f"更新股票行业信息失败: {code}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"更新股票行业信息失败 {code}: {e}")
            return False