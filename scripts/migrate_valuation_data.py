#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
估值数据表字段对齐脚本

根据聚宽API文档中的valuation估值数据表字段，对齐valuation_data表结构
参考文档: https://www.joinquant.com/help/api/doc?name=JQDatadoc&id=9884
"""

import duckdb
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValuationDataMigrator:
    """估值数据表迁移器"""
    
    def __init__(self, db_path: Optional[str] = None):
        # 如果没有指定db_path，使用配置中的路径
        if db_path is None:
            import sys
            sys.path.append('..')
            from config import get_config
            config = get_config()
            db_path = config.database.path
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """连接数据库"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"已连接到数据库: {self.db_path}")
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def get_current_schema(self) -> List[Tuple[str, str]]:
        """获取当前valuation_data表的字段结构"""
        try:
            result = self.conn.execute("PRAGMA table_info(valuation_data)").fetchall()
            return [(row[1], row[2]) for row in result]  # (column_name, data_type)
        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return []
    
    def get_target_schema(self) -> Dict[str, str]:
        """获取目标字段结构（基于聚宽API文档）"""
        return {
            # 基础字段
            'code': 'VARCHAR',
            'day': 'DATE',
            
            # 聚宽valuation表核心字段
            'capitalization': 'DOUBLE',           # 总股本(万股)
            'circulating_cap': 'DOUBLE',          # 流通股本(万股)
            'market_cap': 'DOUBLE',               # 总市值(亿元)
            'circulating_market_cap': 'DOUBLE',   # 流通市值(亿元)
            'turnover_ratio': 'DOUBLE',           # 换手率(%)
            'pe_ratio': 'DOUBLE',                 # 市盈率(PE, TTM)
            'pe_ratio_lyr': 'DOUBLE',             # 市盈率(PE)
            'pb_ratio': 'DOUBLE',                 # 市净率(PB)
            'ps_ratio': 'DOUBLE',                 # 市销率(PS, TTM)
            'pcf_ratio': 'DOUBLE',                # 市现率(PCF, 现金净流量TTM)
            'pcf_ratio2': 'DOUBLE',               # 市现率(PCF,经营活动现金流TTM)
            'dividend_ratio': 'DOUBLE',           # 股息率(TTM) %
            'free_cap': 'DOUBLE',                 # 自由流通股本(万股)
            'free_market_cap': 'DOUBLE',          # 自由流通市值(亿元)
            'a_cap': 'DOUBLE',                    # A股总股本(万股)
            'a_market_cap': 'DOUBLE',             # A股总市值(亿元)
        }
    
    def analyze_schema_diff(self) -> Tuple[List[str], List[str], List[str]]:
        """分析字段差异"""
        current_schema = dict(self.get_current_schema())
        target_schema = self.get_target_schema()
        
        current_fields = set(current_schema.keys())
        target_fields = set(target_schema.keys())
        
        # 需要添加的字段
        missing_fields = list(target_fields - current_fields)
        
        # 需要删除的字段（当前有但目标没有的）
        extra_fields = list(current_fields - target_fields)
        
        # 共同字段（可能需要类型检查）
        common_fields = list(current_fields & target_fields)
        
        return missing_fields, extra_fields, common_fields
    
    def add_missing_columns(self, missing_fields: List[str]):
        """添加缺失的字段"""
        target_schema = self.get_target_schema()
        
        for field in missing_fields:
            if field in target_schema:
                field_type = target_schema[field]
                try:
                    sql = f"ALTER TABLE valuation_data ADD COLUMN {field} {field_type}"
                    self.conn.execute(sql)
                    logger.info(f"已添加字段: {field} ({field_type})")
                except Exception as e:
                    logger.error(f"添加字段 {field} 失败: {e}")
    
    def create_new_table_if_not_exists(self):
        """如果表不存在，创建新表"""
        target_schema = self.get_target_schema()
        
        # 构建CREATE TABLE语句
        columns = []
        for field, field_type in target_schema.items():
            columns.append(f"{field} {field_type}")
        
        # 添加主键约束
        columns.append("PRIMARY KEY (code, day)")
        
        columns_str = ',\n            '.join(columns)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS valuation_data (
            {columns_str}
        )
        """
        
        try:
            self.conn.execute(create_sql)
            logger.info("valuation_data表创建成功或已存在")
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            raise
    
    def backup_table(self) -> str:
        """备份当前表"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_table_name = f"valuation_data_backup_{timestamp}"
        
        try:
            # 检查表是否存在
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            if 'valuation_data' in table_names:
                sql = f"CREATE TABLE {backup_table_name} AS SELECT * FROM valuation_data"
                self.conn.execute(sql)
                logger.info(f"已备份表为: {backup_table_name}")
                return backup_table_name
            else:
                logger.info("valuation_data表不存在，无需备份")
                return ""
        except Exception as e:
            logger.error(f"备份表失败: {e}")
            raise
    
    def migrate(self, backup: bool = True):
        """执行迁移"""
        logger.info("开始迁移valuation_data表结构")
        
        try:
            # 连接数据库
            self.connect()
            
            # 备份表（如果需要）
            if backup:
                backup_name = self.backup_table()
                if backup_name:
                    logger.info(f"数据已备份到: {backup_name}")
            
            # 创建表（如果不存在）
            self.create_new_table_if_not_exists()
            
            # 分析字段差异
            missing_fields, extra_fields, common_fields = self.analyze_schema_diff()
            
            logger.info(f"字段分析结果:")
            logger.info(f"  缺失字段: {missing_fields}")
            logger.info(f"  多余字段: {extra_fields}")
            logger.info(f"  共同字段: {common_fields}")
            
            # 添加缺失字段
            if missing_fields:
                logger.info("开始添加缺失字段...")
                self.add_missing_columns(missing_fields)
                logger.info("缺失字段添加完成")
            else:
                logger.info("没有缺失字段需要添加")
            
            # 显示最终表结构
            final_schema = self.get_current_schema()
            logger.info("\n最终表结构:")
            for field_name, field_type in final_schema:
                logger.info(f"  {field_name}: {field_type}")
            
            logger.info("valuation_data表结构迁移完成")
            
        except Exception as e:
            logger.error(f"迁移失败: {e}")
            raise
        finally:
            self.close()
    
    def verify_migration(self):
        """验证迁移结果"""
        logger.info("验证迁移结果...")
        
        try:
            self.connect()
            
            # 获取当前字段
            current_schema = dict(self.get_current_schema())
            target_schema = self.get_target_schema()
            
            # 检查所有目标字段是否存在
            missing_fields = []
            for field in target_schema.keys():
                if field not in current_schema:
                    missing_fields.append(field)
            
            if missing_fields:
                logger.error(f"验证失败，仍缺失字段: {missing_fields}")
                return False
            else:
                logger.info("✅ 验证通过，所有字段都已正确添加")
                
                # 显示字段对照表
                logger.info("\n字段对照表（聚宽API -> 数据库字段）:")
                field_mapping = {
                    'capitalization': '总股本(万股)',
                    'circulating_cap': '流通股本(万股)',
                    'market_cap': '总市值(亿元)',
                    'circulating_market_cap': '流通市值(亿元)',
                    'turnover_ratio': '换手率(%)',
                    'pe_ratio': '市盈率(PE, TTM)',
                    'pe_ratio_lyr': '市盈率(PE)',
                    'pb_ratio': '市净率(PB)',
                    'ps_ratio': '市销率(PS, TTM)',
                    'pcf_ratio': '市现率(PCF, 现金净流量TTM)',
                    'pcf_ratio2': '市现率(PCF,经营活动现金流TTM)',
                    'dividend_ratio': '股息率(TTM) %',
                    'free_cap': '自由流通股本(万股)',
                    'free_market_cap': '自由流通市值(亿元)',
                    'a_cap': 'A股总股本(万股)',
                    'a_market_cap': 'A股总市值(亿元)',
                }
                
                for field, description in field_mapping.items():
                    status = "✅" if field in current_schema else "❌"
                    logger.info(f"  {status} {field}: {description}")
                
                return True
                
        except Exception as e:
            logger.error(f"验证过程出错: {e}")
            return False
        finally:
            self.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='估值数据表字段对齐工具')
    # 获取默认数据库路径
    import sys
    sys.path.append('..')
    from config import get_config
    config = get_config()
    default_db_path = config.database.path
    
    parser.add_argument('--db-path', default=default_db_path, help='数据库文件路径')
    parser.add_argument('--no-backup', action='store_true', help='不备份原表')
    parser.add_argument('--verify-only', action='store_true', help='仅验证，不执行迁移')
    
    args = parser.parse_args()
    
    migrator = ValuationDataMigrator(args.db_path)
    
    if args.verify_only:
        migrator.verify_migration()
    else:
        migrator.migrate(backup=not args.no_backup)
        migrator.verify_migration()


if __name__ == "__main__":
    main()