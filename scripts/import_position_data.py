#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户持仓数据导入脚本

支持从JSON文件导入用户持仓记录和账户信息：
- 单个文件导入
- 批量目录导入
- 指定用户导入
- 覆盖模式导入

使用方法：
    # 导入单个文件
    python scripts/import_position_data.py --file D:/Users/Jack/myqmt_admin/data/account/6681802461/account_positions/20250901.json
    
    # 导入整个目录
    python scripts/import_position_data.py --directory D:/Users/Jack/myqmt_admin/data/account
    
    # 导入指定用户的所有数据
    python scripts/import_position_data.py --directory D:/Users/Jack/myqmt_admin/data/account --user-id 6681802461
    
    # 覆盖已存在的记录
    python scripts/import_position_data.py --file path/to/file.json --overwrite
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from duckdb_impl import DuckDBDatabase
from services.position_service import PositionService
from config import get_config


class PositionDataImporter:
    """持仓数据导入器"""
    
    def __init__(self):
        """初始化数据库连接和服务"""
        config = get_config()
        self.db = DuckDBDatabase(config.database.path)
        self.db.connect()
        self.position_service = PositionService(self.db)
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db') and self.db:
            self.db.close()
    
    def import_single_file(self, file_path: str, overwrite: bool = False) -> bool:
        """
        导入单个持仓文件
        
        Args:
            file_path: 持仓文件路径
            overwrite: 是否覆盖已存在的记录
            
        Returns:
            导入是否成功
        """
        try:
            self.logger.info(f"开始导入文件: {file_path}")
            
            # 检查文件是否存在
            if not os.path.exists(file_path):
                self.logger.error(f"文件不存在: {file_path}")
                return False
            
            # 导入文件
            result = self.position_service.import_position_file(file_path, overwrite=overwrite)
            
            # 输出结果
            self.logger.info(f"导入完成: {result}")
            print(f"\n=== 导入结果 ===")
            print(f"文件路径: {result['file_path']}")
            print(f"用户ID: {result['user_id']}")
            print(f"持仓日期: {result['position_date']}")
            print(f"总持仓记录: {result['total_positions']}")
            print(f"成功导入: {result['success_positions']}")
            print(f"导入失败: {result['failed_positions']}")
            print(f"账户信息: {'已导入' if result['account_info_inserted'] else '导入失败'}")
            
            return result['failed_positions'] == 0
        
        except Exception as e:
            self.logger.error(f"导入文件失败: {e}")
            print(f"导入失败: {e}")
            return False
    
    def import_directory(self, directory_path: str, user_id: Optional[str] = None, 
                        overwrite: bool = False) -> bool:
        """
        导入目录下的所有持仓文件
        
        Args:
            directory_path: 目录路径
            user_id: 指定用户ID，如果为None则导入所有用户
            overwrite: 是否覆盖已存在的记录
            
        Returns:
            导入是否成功
        """
        try:
            self.logger.info(f"开始批量导入目录: {directory_path}")
            if user_id:
                self.logger.info(f"指定用户ID: {user_id}")
            
            # 检查目录是否存在
            if not os.path.exists(directory_path):
                self.logger.error(f"目录不存在: {directory_path}")
                return False
            
            # 导入目录
            result = self.position_service.import_position_directory(
                directory_path, user_id=user_id, overwrite=overwrite
            )
            
            # 输出结果
            self.logger.info(f"批量导入完成: {result}")
            print(f"\n=== 批量导入结果 ===")
            print(f"目录路径: {result['directory_path']}")
            print(f"总文件数: {result['total_files']}")
            print(f"成功文件: {result['success_files']}")
            print(f"失败文件: {result['failed_files']}")
            print(f"总持仓记录: {result['total_positions']}")
            print(f"成功记录: {result['success_positions']}")
            print(f"失败记录: {result['failed_positions']}")
            
            success_rate = result['success_files'] / result['total_files'] * 100 if result['total_files'] > 0 else 0
            print(f"成功率: {success_rate:.1f}%")
            
            return result['failed_files'] == 0
        
        except Exception as e:
            self.logger.error(f"批量导入失败: {e}")
            print(f"批量导入失败: {e}")
            return False
    
    def validate_file_format(self, file_path: str) -> bool:
        """
        验证文件格式是否正确
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件格式是否正确
        """
        try:
            # 解析文件路径和数据
            user_id, position_date, position_data = self.position_service.parse_position_file(file_path)
            
            print(f"\n=== 文件格式验证 ===")
            print(f"文件路径: {file_path}")
            print(f"用户ID: {user_id}")
            print(f"持仓日期: {position_date}")
            print(f"持仓记录数: {len(position_data.get('positions', []))}")
            print(f"账户信息: {'存在' if 'account_info' in position_data else '缺失'}")
            print(f"时间戳: {position_data.get('timestamp', '无')}")
            
            # 验证数据转换
            positions, account_info = self.position_service.convert_positions_to_objects(
                user_id, position_date, position_data
            )
            
            print(f"\n=== 数据转换验证 ===")
            print(f"有效持仓记录: {len(positions)}")
            print(f"账户信息转换: {'成功' if account_info else '失败'}")
            
            if positions:
                print(f"\n=== 示例持仓记录 ===")
                sample_position = positions[0]
                print(f"股票代码: {sample_position.stock_code}")
                print(f"持仓数量: {sample_position.position_quantity}")
                print(f"可用数量: {sample_position.available_quantity}")
                print(f"开仓价格: {sample_position.open_price}")
                print(f"市值: {sample_position.market_value}")
            
            if account_info:
                print(f"\n=== 账户信息 ===")
                print(f"总资产: {account_info.total_assets}")
                print(f"持仓市值: {account_info.position_market_value}")
                print(f"可用资金: {account_info.available_cash}")
                print(f"冻结资金: {account_info.frozen_cash}")
            
            return True
        
        except Exception as e:
            print(f"文件格式验证失败: {e}")
            return False
    
    def show_import_statistics(self, user_id: Optional[str] = None):
        """
        显示导入统计信息
        
        Args:
            user_id: 指定用户ID，如果为None则显示所有用户统计
        """
        try:
            if user_id:
                # 显示指定用户的统计信息
                summary = self.position_service.get_position_summary(user_id)
                print(f"\n=== 用户 {user_id} 持仓统计 ===")
                print(f"总记录数: {summary.get('total_records', 0)}")
                print(f"总天数: {summary.get('total_days', 0)}")
                print(f"总股票数: {summary.get('total_stocks', 0)}")
                print(f"最早日期: {summary.get('earliest_date', '无')}")
                print(f"最新日期: {summary.get('latest_date', '无')}")
            else:
                # 显示所有用户的统计信息
                sql = """
                SELECT 
                    user_id,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT position_date) as total_days,
                    COUNT(DISTINCT stock_code) as total_stocks,
                    MIN(position_date) as earliest_date,
                    MAX(position_date) as latest_date
                FROM user_positions 
                GROUP BY user_id
                ORDER BY user_id
                """
                
                result = self.db.query_data(sql)
                
                if not result.empty:
                    print(f"\n=== 所有用户持仓统计 ===")
                    print(result.to_string(index=False))
                    
                    print(f"\n=== 汇总统计 ===")
                    print(f"总用户数: {len(result)}")
                    print(f"总记录数: {result['total_records'].sum()}")
                    print(f"平均每用户记录数: {result['total_records'].mean():.1f}")
                else:
                    print("\n暂无持仓数据")
        
        except Exception as e:
            print(f"获取统计信息失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='用户持仓数据导入脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 导入单个文件
  python scripts/import_position_data.py --file D:/path/to/20250901.json
  
  # 导入整个目录
  python scripts/import_position_data.py --directory D:/path/to/account
  
  # 导入指定用户
  python scripts/import_position_data.py --directory D:/path/to/account --user-id 6681802461
  
  # 覆盖已存在记录
  python scripts/import_position_data.py --file D:/path/to/20250901.json --overwrite
  
  # 验证文件格式
  python scripts/import_position_data.py --validate D:/path/to/20250901.json
  
  # 查看统计信息
  python scripts/import_position_data.py --stats
        """
    )
    
    # 互斥参数组：文件、目录、验证、统计
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', '-f', help='导入单个持仓文件')
    group.add_argument('--directory', '-d', help='导入目录下的所有持仓文件')
    group.add_argument('--validate', '-v', help='验证文件格式（不导入数据）')
    group.add_argument('--stats', '-s', action='store_true', help='显示导入统计信息')
    
    # 可选参数
    parser.add_argument('--user-id', '-u', help='指定用户ID（仅用于目录导入和统计查询）')
    parser.add_argument('--overwrite', '-o', action='store_true', help='覆盖已存在的记录')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 创建导入器
    importer = PositionDataImporter()
    
    try:
        success = True
        
        if args.file:
            # 导入单个文件
            success = importer.import_single_file(args.file, overwrite=args.overwrite)
        
        elif args.directory:
            # 导入目录
            success = importer.import_directory(args.directory, user_id=args.user_id, overwrite=args.overwrite)
        
        elif args.validate:
            # 验证文件格式
            success = importer.validate_file_format(args.validate)
        
        elif args.stats:
            # 显示统计信息
            importer.show_import_statistics(user_id=args.user_id)
        
        # 返回适当的退出码
        sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print("\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()