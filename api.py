"""量化数据平台主API"""

from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime, timedelta
import pandas as pd
import logging
import os
from pathlib import Path

from database import DatabaseManager
from duckdb_impl import DuckDBDatabase
from replica_database_wrapper import ReplicaDatabaseWrapper
from data_source import DataSourceManager, DataSourceConfig, DataType
from providers.jqdata import JQDataSource
from providers.tushare import TushareDataSource
from services.update_service import UpdateService
from services.stock_list_service import StockListService
from services.trade_import_service import TradeImportService
from config import Config, get_config


def load_credentials():
    """
    从.env文件中读取jq和tushare的认证配置变量
    """
    # 获取项目根目录（api.py所在目录）
    project_dir = Path(__file__).parent.resolve()
    env_file = project_dir / '.env'
    
    if env_file.exists():
        try:
            # 手动读取.env文件
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        except Exception as e:
            logging.warning(f"读取.env文件失败: {e}")
    else:
        logging.warning(f".env文件不存在: {env_file}")
    
    # 获取JQData凭证
    jq_username = os.getenv('JQ_USERNAME')
    jq_password = os.getenv('JQ_PASSWORD')
    
    # 获取Tushare凭证
    tushare_token = os.getenv('TUSHARE_TOKEN')
    
    return {
        'jq_username': jq_username,
        'jq_password': jq_password,
        'tushare_token': tushare_token
    }


class StockDataAPI:
    """股票数据存储平台主API类"""
    
    def __init__(self, db_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None, use_replica: bool = True):
        # 如果没有指定db_path，使用配置中的路径
        if db_path is None:
            config_obj = get_config()
            db_path = config_obj.database.path
        """
        初始化量化数据API
        
        Args:
            db_path: 数据库文件路径
            config: 配置字典，包含数据源配置等
            use_replica: 是否使用副本数据库模式
        """
        self.logger = logging.getLogger(__name__)
        self.use_replica = use_replica
        
        # 初始化数据库
        if use_replica:
            # 使用副本数据库包装器
            self.db = DatabaseManager(ReplicaDatabaseWrapper(db_path))
            self.logger.info(f"使用副本数据库模式，主数据库: {db_path}")
        else:
            # 使用传统的直接连接模式
            self.db = DatabaseManager(DuckDBDatabase(db_path))
            self.logger.info(f"使用直接连接模式，数据库: {db_path}")
        
        # 初始化数据源管理器
        self.data_sources = DataSourceManager()
        
        # 加载配置
        self.config = config or {}
        
        # 初始化服务
        self.stock_list_service = None
        self.update_service = None
        self.trade_import_service = None
        
        # 初始化状态
        self._initialized = False
    
    def initialize(self) -> None:
        """初始化数据平台"""
        try:
            # 连接数据库并创建表
            self.db.initialize()
            
            # 注册数据源（如果配置中有的话）
            self.logger.info(f"配置: {self.config}")
            if 'data_sources' in self.config:
                self._register_data_sources(self.config['data_sources'])
            
            # 初始化股票列表服务
            try:
                default_source = self.data_sources.get_source()
                self.stock_list_service = StockListService(self.db, default_source)
            except ValueError:
                # 没有默认数据源时，股票列表服务为None
                self.stock_list_service = None
            
            # 初始化更新服务
            self.update_service = UpdateService(self.db, self.data_sources)
            
            # 初始化交易导入服务
            self.trade_import_service = TradeImportService(self.db.db)
            
            self._initialized = True
            self.logger.info("量化数据平台初始化完成")
            
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            raise
    
    def _register_data_sources(self, sources_config: List[Dict[str, Any]]) -> None:
        """注册数据源"""
        # 从.env文件获取认证信息
        credentials = load_credentials()
        self.logger.info(f"加载认证信息: {credentials}, {sources_config}")
        for source_config in sources_config:
            source_type = source_config.get('type')
            if source_type == 'jqdata':
                source = JQDataSource()
                self.data_sources.register_source(
                    source_config['name'], 
                    source, 
                    source_config.get('default', False)
                )
                # 使用从.env获取的认证信息
                if credentials['jq_username'] and credentials['jq_password']:
                    try:
                        source.authenticate(
                            username=credentials['jq_username'],
                            password=credentials['jq_password']
                        )
                        self.logger.info(f"JQData数据源认证成功")
                    except Exception as e:
                        self.logger.warning(f"JQData数据源认证失败: {e}")
                else:
                    self.logger.warning("JQData认证信息不完整，跳过认证")
                    
            elif source_type == 'tushare':
                # Tushare需要在初始化时传入token
                tushare_config = {}
                if credentials['tushare_token']:
                    tushare_config['token'] = credentials['tushare_token']
                
                source = TushareDataSource(tushare_config)
                self.data_sources.register_source(
                    source_config['name'], 
                    source, 
                    source_config.get('default', False)
                )
                # 使用从.env获取的认证信息
                if credentials['tushare_token']:
                    try:
                        source.authenticate(token=credentials['tushare_token'])
                        self.logger.info(f"Tushare数据源认证成功")
                    except Exception as e:
                        self.logger.warning(f"Tushare数据源认证失败: {e}")
                else:
                    self.logger.warning("Tushare认证信息不完整，跳过认证")
    
    def _ensure_initialized(self) -> None:
        """确保已初始化"""
        if not self._initialized:
            self.initialize()
    
    # ==================== 数据查询接口 ====================
    
    def get_stock_list(self, market: Optional[str] = None, 
                      exchange: Optional[str] = None,
                      active_only: bool = True) -> List[str]:
        """获取股票列表
        
        Args:
            market: 市场过滤 (main/gem/star/bse)
            exchange: 交易所过滤 (XSHG/XSHE/BSE)
            active_only: 是否只返回活跃股票
        
        Returns:
            List[str]: 股票代码列表
        """
        self._ensure_initialized()
        
        # 是否包含北交所：若显式请求 BSE（exchange='BSE' 或 market='bse'）则包含，否则默认不包含
        include_bj = (
            (exchange and str(exchange).upper() == 'BSE') or 
            (market and str(market).lower() == 'bse')
        )
        
        # 首先尝试从 stock_list 表获取
        if self.db.table_exists('stock_list'):
            sql = "SELECT DISTINCT code FROM stock_list"
            conditions = []
            params = []
            
            if market:
                conditions.append("market = ?")
                params.append(market)
            
            if exchange:
                conditions.append("exchange = ?")
                params.append(exchange)
                
            if active_only:
                conditions.append("(end_date IS NULL OR end_date > CURRENT_DATE)")
            
            # 默认过滤北交所，除非明确请求 BSE
            if not include_bj:
                conditions.append("code NOT LIKE '%.BJ'")
            
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            
            sql += " ORDER BY code"
            
            try:
                df = self.db.query(sql, params)
                if not df.empty:
                    codes = df['code'].tolist()
                    return codes
                else:
                    return []
            except Exception as e:
                self.logger.warning(f"从stock_list表获取股票列表失败: {e}")
        
        # 如果有股票列表服务，尝试使用（不回退到其他业务表）
        if self.stock_list_service:
            try:
                df = self.stock_list_service.get_active_stocks(exchange=exchange, market=market)
                if not df.empty:
                    codes = df['code'].tolist()
                    if not include_bj:
                        codes = [c for c in codes if not c.endswith('.BJ')]
                    return codes
            except Exception as e:
                self.logger.warning(f"股票列表服务获取失败: {e}")
        
        # 不进行其他表回退，直接返回空列表
        return []
    
    def update_stock_list(self, force_update: bool = False) -> bool:
        """更新股票列表
        
        Args:
            force_update: 是否强制更新
        
        Returns:
            bool: 更新是否成功
        """
        self._ensure_initialized()
        
        if not self.stock_list_service:
            self.logger.error("股票列表服务未初始化")
            return False
        
        return self.stock_list_service.update_stock_list(force_update)
    
    def get_stock_info(self, code: str) -> Optional[Dict[str, Any]]:
        """获取股票详细信息
        
        Args:
            code: 股票代码
        
        Returns:
            Dict[str, Any]: 股票信息字典
        """
        self._ensure_initialized()
        
        if not self.stock_list_service:
            return None
        
        stock_info = self.stock_list_service.get_stock_info(code)
        return stock_info.to_dict() if stock_info else None
    
    def search_stocks(self, keyword: str, limit: int = 50) -> pd.DataFrame:
        """搜索股票
        
        Args:
            keyword: 搜索关键词
            limit: 返回结果数量限制
        
        Returns:
            pd.DataFrame: 搜索结果
        """
        self._ensure_initialized()
        
        if not self.stock_list_service:
            return pd.DataFrame()
        
        return self.stock_list_service.search_stocks(keyword, limit)
    
    def get_market_statistics(self) -> Dict[str, Any]:
        """获取市场统计信息
        
        Returns:
            Dict[str, Any]: 市场统计信息
        """
        self._ensure_initialized()
        
        if not self.stock_list_service:
            return {}
        
        return self.stock_list_service.get_stock_count_by_market()
    
    def get_financial_data(self, code: str, start_date: Optional[date] = None,
                          end_date: Optional[date] = None) -> pd.DataFrame:
        """获取财务数据"""
        self._ensure_initialized()

        # 尝试从多个财务表获取数据
        financial_tables = ['income_statement', 'balance_sheet', 'cashflow_statement', 'valuation_data', 'indicator_data']
        result = {}

        for table in financial_tables:
            try:
                sql = f"SELECT * FROM {table} WHERE code = ?"
                params = [code]

                if start_date:
                    sql += " AND day >= ?"
                    params.append(start_date)

                if end_date:
                    sql += " AND day <= ?"
                    params.append(end_date)

                sql += " ORDER BY day DESC LIMIT 10"

                df = self.db.query(sql, params)
                if not df.empty:
                    result[table] = df
            except Exception as e:
                self.logger.debug(f"从{table}表获取数据失败: {e}")
                continue

        # 如果有数据返回字典，否则返回空DataFrame
        return result if result else pd.DataFrame()
    
    def get_price_data(self, code: str, start_date: Optional[date] = None,
                      end_date: Optional[date] = None) -> pd.DataFrame:
        """获取价格数据"""
        self._ensure_initialized()

        sql = "SELECT day as trade_date, open, close, high, low, volume, money, factor FROM price_data WHERE code = ?"
        params = [code]

        if start_date:
            sql += " AND day >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND day <= ?"
            params.append(end_date)

        sql += " ORDER BY day"

        return self.db.query(sql, params)
    
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """执行自定义SQL查询"""
        self._ensure_initialized()
        return self.db.query(sql, params)
    
    # ==================== 批量操作接口 ====================
    
    def get_batch_price_data(self, codes: List[str], start_date: Optional[date] = None, 
                            end_date: Optional[date] = None) -> Dict[str, pd.DataFrame]:
        """批量获取价格数据"""
        self._ensure_initialized()
        
        result = {}
        for code in codes:
            try:
                df = self.get_price_data(code, start_date, end_date)
                if not df.empty:
                    result[code] = df
            except Exception as e:
                self.logger.warning(f"获取{code}价格数据失败: {e}")
        
        return result
    
    def get_batch_stock_info(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取股票信息"""
        self._ensure_initialized()

        result = {}
        for code in codes:
            try:
                info = self.get_stock_info(code)
                if info:
                    result[code] = info
            except Exception as e:
                self.logger.warning(f"获取{code}股票信息失败: {e}")

        return result

    
    # ==================== 数据更新接口 ====================
    
    def update_stock_list_data(self, force_update: bool = False) -> Dict[str, Any]:
        """更新股票列表数据"""
        self._ensure_initialized()
        
        try:
            success = self.update_stock_list(force_update)
            if success:
                return {'success': True, 'message': '股票列表更新成功'}
            else:
                return {'success': False, 'message': '股票列表更新失败'}
        except Exception as e:
            self.logger.error(f"股票列表更新失败: {e}")
            return {'success': False, 'message': f'股票列表更新失败: {str(e)}'}
    
    def update_data(self, codes: List[str], data_types: List[str],
                    force_full_update: bool = False, max_workers: Optional[int] = None,
                    end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        更新指定股票列表的数据
        
        注意：
        - 如果 data_types 包含 DataType.PRICE_DATA，则仅更新价格数据表，并自动过滤掉 .BJ（北交所）股票
        - 如果 data_types 包含 'market' 或 'financial'，则按类别更新
        """
        self._ensure_initialized()
        if not codes:
            return {'success': False, 'message': '股票代码列表为空'}
        
        # 过滤股票代码：包含 A 股主板/创业板/科创板，排除北交所
        filtered = [c for c in codes if c.endswith('.SZ') or c.endswith('.SH')]
        dropped = len(codes) - len(filtered)
        if dropped > 0:
            self.logger.info(f"更新前过滤掉 {dropped} 只北交所或非上/深交易所股票")
        
        if not filtered:
            return {'success': False, 'message': '过滤后无可更新的股票代码（可能全部为北交所 .BJ 或非上/深交易所）'}
        
        # 检查data_types参数
        if not data_types or len(data_types) == 0:
            return {'success': False, 'message': 'data_types参数不能为空，请指定要更新的数据类型'}
        
        # 按类别更新（financial/market）
        result = self.update_service.update_multiple_stocks(
            filtered,
            data_types=data_types,
            force_full_update=force_full_update,
            max_workers=max_workers,
            end_date=end_date
        )
        return {'success': True, 'result': result}
    
    def update_bj_stocks_data(self, codes: List[str], data_types: Optional[List[str]] = None,
                             force_full_update: bool = False, max_workers: Optional[int] = None,
                             end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        更新北交所股票数据，强制使用tushare数据源
        
        Args:
            codes: 北交所股票代码列表
            data_types: 数据类型列表
            force_full_update: 是否强制全量更新
            max_workers: 最大并发数
            end_date: 结束日期
        
        Returns:
            Dict[str, Any]: 更新结果
        """
        self._ensure_initialized()
        if not codes:
            return {'success': False, 'message': '股票代码列表为空'}
        
        # 验证都是北交所股票
        non_bj_codes = [code for code in codes if not code.endswith('.BJ')]
        if non_bj_codes:
            return {'success': False, 'message': f'包含非北交所股票代码: {non_bj_codes}'}
        
        # 检查是否有tushare数据源
        tushare_source = self.data_sources.get_source('tushare')
        if not tushare_source:
            return {'success': False, 'message': 'tushare数据源未注册，无法更新北交所股票'}
        
        # 默认更新类别
        if data_types is None:
            data_types = ['market']  # 北交所主要更新市场数据
        
        # 使用UpdateService的专门方法更新北交所股票
        result = self.update_service.update_bj_stocks(
            codes,
            data_types=data_types,
            force_full_update=force_full_update,
            max_workers=max_workers,
            end_date=end_date
        )
        
        if result.get('successful'):
            success_count = len(result['successful'])
            total_count = result['total_stocks']
            self.logger.info(f"北交所股票数据更新完成: {success_count}/{total_count} 成功")
            return {'success': True, 'message': f'更新完成: {success_count}/{total_count} 成功', 'result': result}
        else:
            return {'success': False, 'message': '更新失败', 'result': result}
    
    def daily_update(self, exchange: str, data_types: List[str]) -> Dict[str, Any]:
        """每日数据更新接口
        
        Args:
            exchange: 交易所类型 ('all', 'bj', 'sh_sz')
            data_types: 要更新的数据类型列表
        
        Returns:
            Dict[str, Any]: 更新结果
        """
        self._ensure_initialized()
        
        if not self.update_service:
            return {'success': False, 'message': '更新服务未初始化'}
        
        result = self.update_service.daily_update(exchange=exchange, data_types=data_types)
        if result:
            return {'success': True, 'result': result}
        else:
            return {'success': False, 'message': '每日更新失败'}
                
    
    # ==================== 系统管理接口 ====================
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        self._ensure_initialized()
        return self.db.get_database_info()
    
    def authenticate_data_source(self, source_name: str, **credentials) -> bool:
        """认证数据源"""
        try:
            source = self.data_sources.get_source(source_name)
            return source.authenticate(**credentials)
        except Exception as e:
            self.logger.error(f"数据源认证失败: {e}")
            return False
    
    def list_data_sources(self) -> List[str]:
        """列出所有数据源"""
        return self.data_sources.list_sources()
    
    def close(self) -> None:
        """关闭连接"""
        try:
            self.data_sources.close_all()
            self.db.close()
            self.logger.info("股票数据存储平台已关闭")
        except Exception as e:
            self.logger.error(f"关闭连接失败: {e}")
    
    # ==================== 交易记录相关API ====================
    
    def get_user_transactions(self, user_id: str = None, stock_code: str = None,
                             trade_date: date = None, strategy_id: str = None,
                             start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """查询用户交易记录
        
        Args:
            user_id: 用户ID（账户ID）
            stock_code: 股票代码
            trade_date: 交易日期
            strategy_id: 策略ID
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易记录DataFrame
        """
        self._ensure_initialized()
        return self.db.database.get_user_transactions(
            user_id=user_id,
            stock_code=stock_code,
            trade_date=trade_date,
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date
        )
    
    def get_user_positions_summary(self, user_id: str, end_date: date = None) -> pd.DataFrame:
        """获取用户持仓汇总
        
        Args:
            user_id: 用户ID（账户ID）
            end_date: 截止日期，默认为所有交易
            
        Returns:
            持仓汇总DataFrame
        """
        self._ensure_initialized()
        return self.db.database.get_user_positions_summary(user_id, end_date)
    
    def import_trade_file(self, file_path: str) -> Dict[str, Any]:
        """导入单个交易文件
        
        Args:
            file_path: 交易文件路径
            
        Returns:
            导入结果统计
        """
        self._ensure_initialized()
        return self.trade_import_service.import_trade_file(file_path)
    
    def import_trade_directory(self, base_path: str) -> Dict[str, Any]:
        """导入指定目录下的所有交易文件
        
        Args:
            base_path: 基础路径，如 D:\\Users\\Jack\\myqmt_admin\\data\\account
            
        Returns:
            导入结果汇总
        """
        self._ensure_initialized()
        return self.trade_import_service.import_directory(base_path)
    
    def delete_user_transactions(self, user_id: str, trade_date: date = None) -> bool:
        """删除用户交易记录
        
        Args:
            user_id: 用户ID（账户ID）
            trade_date: 交易日期，如果不指定则删除该用户所有交易记录
            
        Returns:
            删除是否成功
        """
        self._ensure_initialized()
        return self.db.database.delete_user_transactions(user_id, trade_date)
    
    def reimport_account_date(self, account_id: str, trade_date: date) -> bool:
        """重新导入指定账户和日期的交易数据
        
        Args:
            account_id: 账户ID
            trade_date: 交易日期
            
        Returns:
            重新导入是否成功
        """
        self._ensure_initialized()
        return self.trade_import_service.reimport_account_date(account_id, trade_date)
    
    def __enter__(self):
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ==================== 便捷函数 ====================

def create_api(db_path: Optional[str] = None, config_file: Optional[str] = None, use_replica: bool = False) -> StockDataAPI:
    """创建API实例的便捷函数"""
    # 使用配置系统
    config_obj = get_config(config_file)
    
    # 如果没有指定db_path，使用配置中的路径
    if db_path is None:
        db_path = config_obj.database.path
    
    # 转换配置为API所需的格式
    config_dict = {
        'data_sources': []
    }
    
    # 注册启用的数据源
    for source_name, source_config in config_obj.data_sources.items():
        if source_config.enabled:
            source_dict = {
                'name': source_name,
                'type': source_name,  # 假设类型名与源名相同
                'config': source_config.credentials,
                'default': source_name == 'jqdata'  # jqdata作为默认数据源
            }
            config_dict['data_sources'].append(source_dict)
    
    return StockDataAPI(db_path, config_dict, use_replica=use_replica)


def quick_query(sql: str, db_path: Optional[str] = None) -> pd.DataFrame:
    """快速查询的便捷函数"""
    with create_api(db_path) as api:
        return api.query(sql)


def get_stock_data(code: str, start_date: Optional[date] = None, 
                  end_date: Optional[date] = None, 
                  db_path: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """获取股票所有数据的便捷函数"""
    with create_api(db_path) as api:
        result = {}
        price_data = api.get_price_data(code, start_date, end_date)
        if not price_data.empty:
            result['price_data'] = price_data
        
        financial_data = api.get_financial_data(code, start_date, end_date)
        if not financial_data.empty:
            result['financial_data'] = financial_data
        
        return result