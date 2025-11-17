#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财务指标数据表(indicator_data)字段对齐脚本
参考聚宽API财务指标表字段，对indicator_data表进行字段对齐
"""

import duckdb
import pandas as pd
from typing import Dict, List, Set
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IndicatorDataMigrator:
    """财务指标数据表迁移器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """连接数据库"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"成功连接到数据库: {self.db_path}")
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise
    
    def get_current_schema(self) -> Dict[str, str]:
        """获取当前indicator_data表的字段结构"""
        try:
            result = self.conn.execute("PRAGMA table_info('indicator_data')").fetchall()
            schema = {}
            for row in result:
                column_name = row[1]
                column_type = row[2]
                schema[column_name] = column_type
            logger.info(f"当前indicator_data表有 {len(schema)} 个字段")
            return schema
        except Exception as e:
            logger.error(f"获取当前表结构失败: {e}")
            return {}
    
    def get_target_schema(self) -> Dict[str, str]:
        """定义目标字段结构 - 严格基于聚宽API indicator财务指标表"""
        # 严格按照聚宽官方API文档 indicator表字段定义
        # 参考: https://www.joinquant.com/help/api/doc?name=JQDatadoc&id=9885
        target_schema = {
            # 基础字段
            'code': 'VARCHAR',  # 股票代码 带后缀.XSHE/.XSHG
            'pubDate': 'DATE',  # 公司发布财报日期
            'statDate': 'DATE',  # 财报统计的季度的最后一天
            
            # 盈利能力指标
            'eps': 'DOUBLE',  # 每股收益EPS(元)
            'adjusted_profit': 'DOUBLE',  # 扣除非经常损益后的净利润(元)
            'operating_profit': 'DOUBLE',  # 经营活动净收益(元)
            'value_change_profit': 'DOUBLE',  # 价值变动净收益(元)
            'roe': 'DOUBLE',  # 净资产收益率ROE(%)
            'inc_return': 'DOUBLE',  # 净资产收益率(扣除非经常损益)(%)
            'roa': 'DOUBLE',  # 总资产净利率ROA(%)
            'net_profit_margin': 'DOUBLE',  # 销售净利率(%)
            'gross_profit_margin': 'DOUBLE',  # 销售毛利率(%)
            
            # 成本费用指标
            'expense_to_total_revenue': 'DOUBLE',  # 营业总成本/营业总收入(%)
            'operation_profit_to_total_revenue': 'DOUBLE',  # 营业利润/营业总收入(%)
            'net_profit_to_total_revenue': 'DOUBLE',  # 净利润/营业总收入(%)
            'operating_expense_to_total_revenue': 'DOUBLE',  # 营业费用/营业总收入(%)
            'ga_expense_to_total_revenue': 'DOUBLE',  # 管理费用/营业总收入(%)
            'financing_expense_to_total_revenue': 'DOUBLE',  # 财务费用/营业总收入(%)
            
            # 盈利质量指标
            'operating_profit_to_profit': 'DOUBLE',  # 经营活动净收益/利润总额(%)
            'invesment_profit_to_profit': 'DOUBLE',  # 价值变动净收益/利润总额(%)
            'adjusted_profit_to_profit': 'DOUBLE',  # 扣除非经常损益后的净利润/归属于母公司所有者的净利润(%)
            
            # 现金流指标
            'goods_sale_and_service_to_revenue': 'DOUBLE',  # 销售商品提供劳务收到的现金/营业收入(%)
            'ocf_to_revenue': 'DOUBLE',  # 经营活动产生的现金流量净额/营业收入(%)
            'ocf_to_operating_profit': 'DOUBLE',  # 经营活动产生的现金流量净额/经营活动净收益(%)
            
            # 成长能力指标
            'inc_total_revenue_year_on_year': 'DOUBLE',  # 营业总收入同比增长率(%)
            'inc_total_revenue_annual': 'DOUBLE',  # 营业总收入环比增长率(%)
            'inc_revenue_year_on_year': 'DOUBLE',  # 营业收入同比增长率(%)
        }
        
        logger.info(f"目标schema定义了 {len(target_schema)} 个字段")
        return target_schema
    
    def analyze_schema_differences(self, current: Dict[str, str], target: Dict[str, str]) -> Dict[str, List[str]]:
        """分析当前schema与目标schema的差异"""
        current_fields = set(current.keys())
        target_fields = set(target.keys())
        
        missing_fields = target_fields - current_fields
        redundant_fields = current_fields - target_fields
        common_fields = current_fields & target_fields
        
        differences = {
            'missing': list(missing_fields),
            'redundant': list(redundant_fields),
            'common': list(common_fields)
        }
        
        logger.info(f"字段分析结果:")
        logger.info(f"  缺失字段: {len(missing_fields)} 个")
        logger.info(f"  冗余字段: {len(redundant_fields)} 个")
        logger.info(f"  共同字段: {len(common_fields)} 个")
        
        if missing_fields:
            logger.info(f"  缺失字段列表: {sorted(missing_fields)}")
        if redundant_fields:
            logger.info(f"  冗余字段列表: {sorted(redundant_fields)}")
            
        return differences
    
    def add_missing_columns(self, missing_fields: List[str], target_schema: Dict[str, str]):
        """添加缺失的字段"""
        if not missing_fields:
            logger.info("没有需要添加的字段")
            return
            
        logger.info(f"开始添加 {len(missing_fields)} 个缺失字段...")
        
        for field in missing_fields:
            try:
                field_type = target_schema[field]
                sql = f"ALTER TABLE indicator_data ADD COLUMN {field} {field_type}"
                self.conn.execute(sql)
                logger.info(f"  ✓ 添加字段: {field} ({field_type})")
            except Exception as e:
                logger.error(f"  ✗ 添加字段 {field} 失败: {e}")
    
    def create_table_if_not_exists(self, target_schema: Dict[str, str]):
        """如果表不存在则创建"""
        try:
            # 检查表是否存在
            result = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='indicator_data'"
            ).fetchall()
            
            if result:
                logger.info("indicator_data表已存在")
                return
                
            logger.info("indicator_data表不存在，开始创建...")
            
            # 构建CREATE TABLE语句
            columns_def = []
            for field, field_type in target_schema.items():
                columns_def.append(f"{field} {field_type}")
            
            columns_str = ',\n    '.join(columns_def)
            
            create_sql = f"""
            CREATE TABLE indicator_data (
                {columns_str},
                PRIMARY KEY (code, day)
            )
            """
            
            self.conn.execute(create_sql)
            logger.info("✓ indicator_data表创建成功")
            
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            raise
    
    def backup_table(self):
        """备份原表"""
        try:
            backup_name = f"indicator_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            sql = f"CREATE TABLE {backup_name} AS SELECT * FROM indicator_data"
            self.conn.execute(sql)
            
            # 获取备份表记录数
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {backup_name}").fetchone()
            record_count = count_result[0] if count_result else 0
            
            logger.info(f"✓ 表备份完成: {backup_name} (共 {record_count} 条记录)")
            return backup_name
        except Exception as e:
            logger.error(f"备份表失败: {e}")
            raise
    
    def verify_migration(self, target_schema: Dict[str, str]) -> bool:
        """验证迁移结果"""
        try:
            current_schema = self.get_current_schema()
            target_fields = set(target_schema.keys())
            current_fields = set(current_schema.keys())
            
            missing_fields = target_fields - current_fields
            
            if missing_fields:
                logger.error(f"验证失败: 仍有 {len(missing_fields)} 个字段缺失: {sorted(missing_fields)}")
                return False
            else:
                logger.info("✓ 验证成功: 所有目标字段都已存在")
                return True
                
        except Exception as e:
            logger.error(f"验证迁移结果失败: {e}")
            return False
    
    def migrate(self):
        """执行完整的迁移流程"""
        try:
            logger.info("开始indicator_data表字段对齐迁移...")
            
            # 连接数据库
            self.connect()
            
            # 获取目标schema
            target_schema = self.get_target_schema()
            
            # 创建表(如果不存在)
            self.create_table_if_not_exists(target_schema)
            
            # 备份原表
            backup_name = self.backup_table()
            
            # 获取当前schema
            current_schema = self.get_current_schema()
            
            # 分析差异
            differences = self.analyze_schema_differences(current_schema, target_schema)
            
            # 添加缺失字段
            self.add_missing_columns(differences['missing'], target_schema)
            
            # 验证迁移结果
            if self.verify_migration(target_schema):
                logger.info("🎉 indicator_data表字段对齐迁移完成!")
                logger.info(f"📊 迁移统计:")
                logger.info(f"  - 目标字段总数: {len(target_schema)}")
                logger.info(f"  - 新增字段数: {len(differences['missing'])}")
                logger.info(f"  - 保留字段数: {len(differences['common'])}")
                logger.info(f"  - 备份表名: {backup_name}")
            else:
                logger.error("❌ 迁移验证失败")
                
        except Exception as e:
            logger.error(f"迁移过程中发生错误: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("数据库连接已关闭")

def main():
    """主函数"""
    # 数据库路径
    db_path = "data/stock_data_new.duckdb"
    
    # 创建迁移器并执行迁移
    migrator = IndicatorDataMigrator(db_path)
    migrator.migrate()

if __name__ == "__main__":
    main()