"""持仓数据服务"""

import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

from models.user_position import UserPosition, UserAccountInfo
from duckdb_impl import DuckDBDatabase


class PositionService:
    """持仓数据服务类"""
    
    def __init__(self, database: DuckDBDatabase):
        self.database = database
        self.logger = logging.getLogger(__name__)
    
    def parse_position_file(self, file_path: str) -> Tuple[str, date, Dict[str, Any]]:
        """解析持仓文件，返回用户ID、持仓日期和持仓数据
        
        Args:
            file_path: 持仓文件路径，格式如 D:/Users/Jack/myqmt_admin/data/account/6681802461/account_positions/20250901.json
            
        Returns:
            (user_id, position_date, position_data)
        """
        path = Path(file_path)
        
        # 从文件路径提取用户ID和持仓日期
        # 路径格式: D:/Users/Jack/myqmt_admin/data/account/6681802461/account_positions/20250901.json
        parts = path.parts
        user_id = None
        position_date = None
        
        # 查找用户ID（account目录后的数字目录）
        for i, part in enumerate(parts):
            if part == 'account' and i + 1 < len(parts):
                user_id = parts[i + 1]
                break
        
        # 从文件名提取日期
        filename = path.stem  # 去掉扩展名
        if filename.isdigit() and len(filename) == 8:
            try:
                position_date = datetime.strptime(filename, '%Y%m%d').date()
            except ValueError:
                raise ValueError(f"无法解析文件名中的日期: {filename}")
        else:
            raise ValueError(f"文件名格式不正确，应为YYYYMMDD.json: {path.name}")
        
        if not user_id:
            raise ValueError(f"无法从路径中提取用户ID: {file_path}")
        
        # 读取JSON文件
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            raise ValueError(f"读取JSON文件失败: {e}")
        
        # 验证JSON结构
        if 'account_info' not in data or 'positions' not in data:
            raise ValueError(f"JSON文件格式不正确，缺少account_info或positions字段: {file_path}")
        
        return user_id, position_date, data
    
    def convert_positions_to_objects(self, user_id: str, position_date: date, 
                                   position_data: Dict[str, Any]) -> Tuple[List[UserPosition], UserAccountInfo]:
        """将持仓数据转换为UserPosition和UserAccountInfo对象
        
        Args:
            user_id: 用户ID
            position_date: 持仓日期
            position_data: 持仓数据字典
            
        Returns:
            (positions_list, account_info)
        """
        positions = []
        timestamp = None
        
        # 解析时间戳
        if 'timestamp' in position_data:
            try:
                timestamp = datetime.strptime(position_data['timestamp'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                self.logger.warning(f"无法解析时间戳: {position_data['timestamp']}")
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # 转换持仓记录
        for position_item in position_data['positions']:
            try:
                # 跳过持仓数量为0的记录（可选）
                #if position_item.get('持仓数量', 0) == 0:
                #    self.logger.debug(f"跳过持仓数量为0的记录: {position_item.get('证券代码')}")
                #    continue
                
                position = UserPosition.from_json_position(
                    user_id=user_id,
                    position_data=position_item,
                    position_date=position_date,
                    timestamp=timestamp
                )
                
                if position.validate():
                    positions.append(position)
                else:
                    self.logger.error(f"持仓记录验证失败: {position_item}")
            
            except Exception as e:
                self.logger.error(f"转换持仓记录失败: {e}, 数据: {position_item}")
                continue
        
        # 转换账户信息
        try:
            account_info = UserAccountInfo.from_json_account_info(
                user_id=user_id,
                account_data=position_data['account_info'],
                info_date=position_date,
                timestamp=timestamp
            )
            
            if not account_info.validate():
                self.logger.error(f"账户信息验证失败: {position_data['account_info']}")
        
        except Exception as e:
            self.logger.error(f"转换账户信息失败: {e}, 数据: {position_data['account_info']}")
            raise
        
        return positions, account_info
    
    def import_position_file(self, file_path: str, overwrite: bool = False) -> Dict[str, Any]:
        """导入单个持仓文件
        
        Args:
            file_path: 持仓文件路径
            overwrite: 是否覆盖已存在的记录
            
        Returns:
            导入结果统计
        """
        try:
            # 解析文件
            user_id, position_date, position_data = self.parse_position_file(file_path)
            
            # 转换为对象
            positions, account_info = self.convert_positions_to_objects(
                user_id, position_date, position_data
            )
            
            # 检查是否已存在记录
            if not overwrite:
                existing_positions = self.get_user_positions(user_id, position_date)
                if existing_positions:
                    raise ValueError(f"用户 {user_id} 在 {position_date} 的持仓记录已存在，使用overwrite=True强制覆盖")
                
                existing_account = self.get_user_account_info(user_id, position_date)
                if existing_account:
                    raise ValueError(f"用户 {user_id} 在 {position_date} 的账户信息已存在，使用overwrite=True强制覆盖")
            
            # 开始事务
            success_positions = 0
            failed_positions = 0
            
            # 如果覆盖模式，先删除已存在的记录
            if overwrite:
                self.delete_user_positions(user_id, position_date)
                self.delete_user_account_info(user_id, position_date)
            
            # 插入持仓记录
            for position in positions:
                try:
                    self.insert_position(position)
                    success_positions += 1
                except Exception as e:
                    self.logger.error(f"插入持仓记录失败: {e}, 记录: {position.position_id}")
                    failed_positions += 1
            
            # 插入账户信息
            try:
                self.insert_account_info(account_info)
            except Exception as e:
                self.logger.error(f"插入账户信息失败: {e}")
                raise
            
            result = {
                'file_path': file_path,
                'user_id': user_id,
                'position_date': position_date.strftime('%Y-%m-%d'),
                'total_positions': len(positions),
                'success_positions': success_positions,
                'failed_positions': failed_positions,
                'account_info_inserted': True
            }
            
            self.logger.info(f"导入持仓文件完成: {result}")
            return result
        
        except Exception as e:
            self.logger.error(f"导入持仓文件失败: {e}, 文件: {file_path}")
            raise
    
    def import_position_directory(self, directory_path: str, user_id: Optional[str] = None, 
                                overwrite: bool = False) -> Dict[str, Any]:
        """导入目录下的所有持仓文件
        
        Args:
            directory_path: 目录路径
            user_id: 指定用户ID，如果为None则导入所有用户
            overwrite: 是否覆盖已存在的记录
            
        Returns:
            导入结果统计
        """
        directory = Path(directory_path)
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"目录不存在或不是有效目录: {directory_path}")
        
        total_files = 0
        success_files = 0
        failed_files = 0
        total_positions = 0
        success_positions = 0
        failed_positions = 0
        
        # 查找所有JSON文件
        json_files = []
        
        if user_id:
            # 指定用户ID，查找该用户的持仓文件
            user_dir = directory / user_id / 'account_positions'
            if user_dir.exists():
                json_files.extend(user_dir.glob('*.json'))
        else:
            # 查找所有用户的持仓文件
            for user_dir in directory.iterdir():
                if user_dir.is_dir() and user_dir.name.isdigit():
                    positions_dir = user_dir / 'account_positions'
                    if positions_dir.exists():
                        json_files.extend(positions_dir.glob('*.json'))
        
        self.logger.info(f"找到 {len(json_files)} 个持仓文件")
        
        # 导入每个文件
        for json_file in json_files:
            total_files += 1
            try:
                result = self.import_position_file(str(json_file), overwrite=overwrite)
                success_files += 1
                total_positions += result['total_positions']
                success_positions += result['success_positions']
                failed_positions += result['failed_positions']
            
            except Exception as e:
                failed_files += 1
                self.logger.error(f"导入文件失败: {e}, 文件: {json_file}")
        
        result = {
            'directory_path': directory_path,
            'total_files': total_files,
            'success_files': success_files,
            'failed_files': failed_files,
            'total_positions': total_positions,
            'success_positions': success_positions,
            'failed_positions': failed_positions
        }
        
        self.logger.info(f"批量导入持仓数据完成: {result}")
        return result
    
    def insert_position(self, position: UserPosition) -> bool:
        """插入持仓记录"""
        try:
            sql = """
            INSERT INTO user_positions (
                position_id, user_id, position_date, stock_code,
                position_quantity, available_quantity, frozen_quantity, 
                transit_shares, yesterday_quantity, open_price, market_value,
                current_price, unrealized_pnl, unrealized_pnl_ratio,
                remark, timestamp, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # 使用created_at作为timestamp，如果没有则使用当前时间
            timestamp = position.created_at or datetime.now()
            
            params = (
                position.position_id, position.user_id, position.position_date, position.stock_code,
                position.position_quantity, position.available_quantity, position.frozen_quantity,
                position.transit_shares, position.yesterday_quantity, float(position.open_price), 
                float(position.market_value), float(position.current_price) if position.current_price else None,
                float(position.unrealized_pnl) if position.unrealized_pnl else None,
                float(position.unrealized_pnl_ratio) if position.unrealized_pnl_ratio else None,
                position.remark, timestamp, position.created_at, position.updated_at
            )
            
            self.database.query_data(sql, params)
            return True
        
        except Exception as e:
            self.logger.error(f"插入持仓记录失败: {e}")
            raise
    
    def insert_account_info(self, account_info: UserAccountInfo) -> bool:
        """插入账户信息"""
        try:
            sql = """
            INSERT INTO user_account_info (
                user_id, info_date, total_assets,
                position_market_value, available_cash, frozen_cash,
                total_profit_loss, total_profit_loss_ratio, timestamp,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                account_info.user_id, account_info.info_date,
                float(account_info.total_assets), float(account_info.position_market_value),
                float(account_info.available_cash), float(account_info.frozen_cash),
                float(account_info.total_profit_loss) if account_info.total_profit_loss else None,
                float(account_info.total_profit_loss_ratio) if account_info.total_profit_loss_ratio else None,
                account_info.timestamp, account_info.created_at, account_info.updated_at
            )
            
            self.database.query_data(sql, params)
            return True
        
        except Exception as e:
            self.logger.error(f"插入账户信息失败: {e}")
            raise
    
    def get_user_positions(self, user_id: str, position_date: Optional[date] = None, 
                          stock_code: Optional[str] = None) -> List[UserPosition]:
        """查询用户持仓记录
        
        Args:
            user_id: 用户ID
            position_date: 持仓日期，如果为None则查询所有日期
            stock_code: 股票代码，如果为None则查询所有股票
            
        Returns:
            持仓记录列表
        """
        try:
            sql = "SELECT * FROM user_positions WHERE user_id = ?"
            params = [user_id]
            
            if position_date:
                sql += " AND position_date = ?"
                params.append(position_date)
            
            if stock_code:
                sql += " AND stock_code = ?"
                params.append(stock_code)
            
            sql += " ORDER BY position_date DESC, stock_code"
            
            df = self.database.query_data(sql, params)
            
            positions = []
            for _, row in df.iterrows():
                # 将DataFrame行转换为字典
                position_dict = row.to_dict()
                position = UserPosition.from_dict(position_dict)
                positions.append(position)
            
            return positions
        
        except Exception as e:
            self.logger.error(f"查询用户持仓记录失败: {e}")
            raise
    
    def get_user_account_info(self, user_id: str, info_date: Optional[date] = None) -> Optional[UserAccountInfo]:
        """查询用户账户信息
        
        Args:
            user_id: 用户ID
            info_date: 信息日期，如果为None则查询最新记录
            
        Returns:
            账户信息对象，如果不存在则返回None
        """
        try:
            if info_date:
                sql = "SELECT * FROM user_account_info WHERE user_id = ? AND info_date = ?"
                params = [user_id, info_date]
            else:
                sql = "SELECT * FROM user_account_info WHERE user_id = ? ORDER BY info_date DESC LIMIT 1"
                params = [user_id]
            
            df = self.database.query_data(sql, params)
            
            if not df.empty:
                # 将DataFrame第一行转换为字典
                account_dict = df.iloc[0].to_dict()
                return UserAccountInfo.from_dict(account_dict)
            
            return None
        
        except Exception as e:
            self.logger.error(f"查询用户账户信息失败: {e}")
            raise
    
    def delete_user_positions(self, user_id: str, position_date: date) -> int:
        """删除用户指定日期的持仓记录
        
        Args:
            user_id: 用户ID
            position_date: 持仓日期
            
        Returns:
            删除的记录数
        """
        try:
            sql = "DELETE FROM user_positions WHERE user_id = ? AND position_date = ?"
            params = [user_id, position_date]
            
            self.database.query_data(sql, params)
            # 由于DuckDB的query_data不直接返回影响行数，我们通过查询来获取
            count_sql = "SELECT COUNT(*) as count FROM user_positions WHERE user_id = ? AND position_date = ?"
            count_df = self.database.query_data(count_sql, params)
            deleted_count = 0 if count_df.empty else count_df.iloc[0]['count']
            
            self.logger.info(f"删除用户 {user_id} 在 {position_date} 的 {deleted_count} 条持仓记录")
            return deleted_count
        
        except Exception as e:
            self.logger.error(f"删除用户持仓记录失败: {e}")
            raise
    
    def delete_user_account_info(self, user_id: str, info_date: date) -> int:
        """删除用户指定日期的账户信息
        
        Args:
            user_id: 用户ID
            info_date: 信息日期
            
        Returns:
            删除的记录数
        """
        try:
            sql = "DELETE FROM user_account_info WHERE user_id = ? AND info_date = ?"
            params = [user_id, info_date]
            
            self.database.query_data(sql, params)
            # 由于DuckDB的query_data不直接返回影响行数，我们通过查询来获取
            count_sql = "SELECT COUNT(*) as count FROM user_account_info WHERE user_id = ? AND info_date = ?"
            count_df = self.database.query_data(count_sql, params)
            deleted_count = 0 if count_df.empty else count_df.iloc[0]['count']
            
            self.logger.info(f"删除用户 {user_id} 在 {info_date} 的账户信息")
            return deleted_count
        
        except Exception as e:
            self.logger.error(f"删除用户账户信息失败: {e}")
            raise
    
    def get_position_summary(self, user_id: str, start_date: Optional[date] = None, 
                           end_date: Optional[date] = None) -> Dict[str, Any]:
        """获取持仓汇总信息
        
        Args:
            user_id: 用户ID
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            持仓汇总信息
        """
        try:
            # 查询持仓记录统计
            sql = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT position_date) as total_days,
                COUNT(DISTINCT stock_code) as total_stocks,
                MIN(position_date) as earliest_date,
                MAX(position_date) as latest_date
            FROM user_positions 
            WHERE user_id = ?
            """
            params = [user_id]
            
            if start_date:
                sql += " AND position_date >= ?"
                params.append(start_date)
            
            if end_date:
                sql += " AND position_date <= ?"
                params.append(end_date)
            
            df = self.database.query_data(sql, params)
            
            if not df.empty:
                summary = df.iloc[0].to_dict()
                summary['user_id'] = user_id
                return summary
            
            return {'user_id': user_id, 'total_records': 0}
        
        except Exception as e:
            self.logger.error(f"获取持仓汇总信息失败: {e}")
            raise