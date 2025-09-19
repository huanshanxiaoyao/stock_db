import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import re

from models.user_transaction import UserTransaction
from duckdb_impl import DuckDBDatabase


class TradeImportService:
    """交易数据导入服务"""
    
    def __init__(self, database: DuckDBDatabase):
        self.database = database
        self.logger = logging.getLogger(__name__)
    
    def parse_trade_file(self, file_path: str) -> tuple[str, date, List[Dict[str, Any]]]:
        """解析交易文件，返回账户ID、交易日期和交易记录列表"""
        path = Path(file_path)
        
        # 从文件路径提取账户ID和交易日期
        # 路径格式: D:/Users/Jack/myqmt_admin/data/account/6681802088/trades_orders/20250821.json
        parts = path.parts
        account_id = None
        trade_date = None
        
        # 查找账户ID（数字目录）
        for i, part in enumerate(parts):
            if part == 'account' and i + 1 < len(parts):
                account_id = parts[i + 1]
                break
        
        # 从文件名提取日期
        filename = path.stem  # 去掉扩展名
        if filename.isdigit() and len(filename) == 8:
            try:
                trade_date = datetime.strptime(filename, '%Y%m%d').date()
            except ValueError:
                raise ValueError(f"无法解析文件名中的日期: {filename}")
        else:
            raise ValueError(f"文件名格式不正确，应为YYYYMMDD.json: {path.name}")
        
        if not account_id:
            raise ValueError(f"无法从路径中提取账户ID: {file_path}")
        
        # 读取JSON文件
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            raise ValueError(f"读取JSON文件失败: {e}")
        
        # 提取trades部分
        if 'trades' not in data:
            raise ValueError(f"JSON文件中缺少trades字段: {file_path}")
        
        trades = data['trades']
        if not isinstance(trades, list):
            raise ValueError(f"trades字段应为列表格式: {file_path}")
        
        return account_id, trade_date, trades
    
    def extract_strategy_id(self, remark: str) -> Optional[str]:
        """从Remark字段提取策略ID
        
        Args:
            remark: 备注字段，格式如 "str1001_600335.SH"
            
        Returns:
            策略ID，如 "str1001"，如果无法提取则返回None
        """
        if not remark:
            return None
        
        # 使用正则表达式提取策略ID（下划线前的部分）
        match = re.match(r'^([^_]+)_', remark)
        if match:
            return match.group(1)
        
        return None
    
    def convert_trade_to_transaction(self, trade_data: Dict[str, Any], 
                                   account_id: str, trade_date: date, file_path: str = '') -> UserTransaction:
        """将交易数据转换为UserTransaction对象"""
        try:
            # 提取策略ID
            remark = trade_data.get('Remark', '')
            strategy_id = self.extract_strategy_id(remark)
            
            # 构建交易时间（合并日期和时间）
            trade_time_str = trade_data.get('TradeTime', '00:00:00')
            if isinstance(trade_time_str, str):
                try:
                    trade_time = datetime.combine(
                        trade_date, 
                        datetime.strptime(trade_time_str, '%H:%M:%S').time()
                    )
                except ValueError:
                    # 如果时间格式不正确，使用默认时间
                    trade_time = datetime.combine(trade_date, datetime.min.time())
            else:
                trade_time = datetime.combine(trade_date, datetime.min.time())
            
            # 添加交易日期到trade_data中
            enhanced_trade_data = trade_data.copy()
            enhanced_trade_data['TradeDate'] = trade_date.strftime('%Y%m%d')
            
            # 创建UserTransaction对象
            transaction = UserTransaction.from_json_trade(
                user_id=account_id,
                trade_data=enhanced_trade_data,
                file_path=file_path
            )
            
            # 设置正确的策略ID
            transaction.strategy_id = strategy_id
            
            return transaction
            
        except Exception as e:
            self.logger.error(f"转换交易数据失败: {e}, 数据: {trade_data}")
            raise
    
    def import_trade_file(self, file_path: str) -> Dict[str, Any]:
        """导入单个交易文件
        
        Returns:
            导入结果统计信息
        """
        try:
            # 解析文件
            account_id, trade_date, trades = self.parse_trade_file(file_path)
            
            if not trades:
                return {
                    'success': True,
                    'file_path': file_path,
                    'account_id': account_id,
                    'trade_date': trade_date.isoformat(),
                    'total_trades': 0,
                    'imported_count': 0,
                    'skipped_count': 0,
                    'error_count': 0,
                    'errors': []
                }
            
            # 转换为UserTransaction对象
            transactions = []
            errors = []
            
            for i, trade in enumerate(trades):
                try:
                    transaction = self.convert_trade_to_transaction(trade, account_id, trade_date, file_path)
                    transactions.append(transaction)
                except Exception as e:
                    error_msg = f"第{i+1}条交易记录转换失败: {e}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
            
            # 批量插入数据库
            imported_count = 0
            skipped_count = 0
            if transactions:
                # 记录插入前的总数
                total_before_insert = len(transactions)
                
                success = self.database.insert_user_transactions(transactions)
                if success:
                    # 查询实际插入的记录数（通过检查数据库中的记录）
                    # 由于数据库层面已经处理了重复检查，这里简化处理
                    imported_count = total_before_insert  # 假设全部成功插入
                    self.logger.info(f"处理完成，文件: {file_path}，总记录数: {total_before_insert}")
                else:
                    errors.append("数据库插入失败")
                    self.logger.error(f"数据库插入失败，文件: {file_path}")
            
            return {
                'success': len(errors) == 0 or imported_count > 0,
                'file_path': file_path,
                'account_id': account_id,
                'trade_date': trade_date.isoformat(),
                'total_trades': len(trades),
                'imported_count': imported_count,
                'skipped_count': skipped_count,
                'error_count': len(errors),
                'errors': errors
            }
            
        except Exception as e:
            error_msg = f"导入文件失败: {e}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'file_path': file_path,
                'account_id': None,
                'trade_date': None,
                'total_trades': 0,
                'imported_count': 0,
                'skipped_count': 0,
                'error_count': 1,
                'errors': [error_msg]
            }
    
    def import_directory(self, base_path: str) -> Dict[str, Any]:
        """导入指定目录下的所有交易文件
        
        Args:
            base_path: 基础路径，如 D:/Users/Jack/myqmt_admin/data/account
            
        Returns:
            导入结果汇总
        """
        base_path = Path(base_path)
        if not base_path.exists():
            raise ValueError(f"目录不存在: {base_path}")
        
        # 查找所有交易文件
        trade_files = []
        for account_dir in base_path.iterdir():
            if account_dir.is_dir() and account_dir.name.isdigit():
                trades_orders_dir = account_dir / 'trades_orders'
                if trades_orders_dir.exists():
                    for json_file in trades_orders_dir.glob('*.json'):
                        trade_files.append(str(json_file))
        
        if not trade_files:
            return {
                'success': True,
                'total_files': 0,
                'processed_files': 0,
                'total_trades': 0,
                'imported_count': 0,
                'error_count': 0,
                'file_results': []
            }
        
        # 逐个处理文件
        file_results = []
        total_trades = 0
        total_imported = 0
        total_errors = 0
        
        for file_path in sorted(trade_files):
            result = self.import_trade_file(file_path)
            file_results.append(result)
            
            total_trades += result['total_trades']
            total_imported += result['imported_count']
            total_errors += result['error_count']
        
        return {
            'success': total_errors == 0 or total_imported > 0,
            'total_files': len(trade_files),
            'processed_files': len(file_results),
            'total_trades': total_trades,
            'imported_count': total_imported,
            'error_count': total_errors,
            'file_results': file_results
        }
    
    def reimport_account_date(self, account_id: str, trade_date: date) -> bool:
        """重新导入指定账户和日期的交易数据（先删除再导入）"""
        try:
            # 先删除现有数据
            self.database.delete_user_transactions(account_id, trade_date)
            
            # 构建文件路径并导入
            date_str = trade_date.strftime('%Y%m%d')
            # 这里需要知道基础路径，可以作为参数传入或配置
            # 暂时硬编码，实际使用时应该配置化
            base_path = Path("D:/Users/Jack/myqmt_admin/data/account")
            file_path = base_path / account_id / "trades_orders" / f"{date_str}.json"
            
            if file_path.exists():
                result = self.import_trade_file(str(file_path))
                return result['success']
            else:
                self.logger.warning(f"交易文件不存在: {file_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"重新导入失败: {e}")
            return False