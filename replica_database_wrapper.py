#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
副本数据库包装器

提供数据库连接的包装，支持副本更新时的自动重连功能。

作者: Stock Data Platform Team
版本: 1.0.0
"""

import threading
import logging
from typing import Optional, Any
from pathlib import Path

from database import DatabaseInterface
from duckdb_impl import DuckDBDatabase
from database_replica_manager import DatabaseReplicaManager


class ReplicaDatabaseWrapper(DatabaseInterface):
    """副本数据库包装器
    
    包装原始数据库接口，提供副本管理和自动重连功能。
    """
    
    def __init__(self,
                 master_db_path: str,
                 replica_db_path: Optional[str] = None,
                 check_interval: float = 30.0):
        """
        初始化副本数据库包装器
        
        Args:
            master_db_path: 主数据库文件路径
            replica_db_path: 副本数据库文件路径
            check_interval: 检查间隔（秒）
        """
        self.master_db_path = master_db_path
        self.check_interval = check_interval
        
        # 数据库实例
        self._db_instance = None
        
        # 副本管理器
        self._replica_manager = None
        
        # 连接状态
        self._connected = False
        
        # 线程锁
        self._lock = threading.RLock()
        
        # 日志
        self.logger = logging.getLogger(__name__)
        
        # 初始化副本管理器
        self._init_replica_manager(replica_db_path)
    
    def _init_replica_manager(self, replica_db_path: Optional[str]):
        """初始化副本管理器"""
        self._replica_manager = DatabaseReplicaManager(
            master_db_path=self.master_db_path,
            replica_db_path=replica_db_path,
            check_interval=self.check_interval,
            on_replica_updated=self._on_replica_updated,
            on_before_replica_update=self._on_before_replica_update
        )

    def _on_before_replica_update(self):
        """副本更新前的回调函数 - 释放文件锁"""
        with self._lock:
            self.logger.debug("副本即将更新，释放旧数据库连接")

            if self._db_instance:
                try:
                    self._db_instance.close()
                    self._connected = False
                    self.logger.debug("已释放旧数据库连接")
                except Exception as e:
                    self.logger.warning(f"释放旧数据库连接失败: {e}")

    def _on_replica_updated(self):
        """副本更新后的回调函数 - 重新连接到新副本"""
        with self._lock:
            self.logger.info("副本已更新，重新连接到新副本数据库...")

            try:
                # 创建新的数据库实例并连接到新副本
                replica_path = self._replica_manager.get_replica_path()
                new_db_instance = DuckDBDatabase(replica_path, is_replica=True)
                new_db_instance.connect()

                # 设置新的数据库实例
                self._db_instance = new_db_instance
                self._connected = True

                self.logger.info(f"已成功重连到新副本数据库: {replica_path}")

            except Exception as e:
                self.logger.error(f"重连到新副本数据库失败: {e}")
                self._connected = False

    def _safe_close_old_connection(self, old_db_instance):
        """安全关闭旧数据库连接"""
        try:
            old_db_instance.close()
            self.logger.debug("已安全关闭旧数据库连接")
        except Exception as e:
            self.logger.warning(f"关闭旧数据库连接失败: {e}")
    
    def _ensure_connected(self):
        """确保数据库已连接"""
        with self._lock:
            if not self._connected or not self._db_instance:
                raise RuntimeError("数据库未连接，请先调用 connect() 方法")
    
    def connect(self) -> None:
        """连接数据库"""
        with self._lock:
            try:
                # 启动副本管理器
                replica_path = self._replica_manager.start()

                # 创建数据库实例并连接到副本（标记为副本模式，禁用备份）
                self._db_instance = DuckDBDatabase(replica_path, is_replica=True)
                self._db_instance.connect()
                self._connected = True
                
                self.logger.info(f"已连接到副本数据库: {replica_path}")
                
            except Exception as e:
                self.logger.error(f"连接数据库失败: {e}")
                self._connected = False
                raise
    
    def close(self) -> None:
        """关闭数据库连接"""
        with self._lock:
            try:
                # 关闭数据库连接
                if self._db_instance:
                    self._db_instance.close()
                    self._db_instance = None
                
                # 停止副本管理器
                if self._replica_manager:
                    self._replica_manager.stop()
                
                self._connected = False
                self.logger.info("数据库连接已关闭")
                
            except Exception as e:
                self.logger.error(f"关闭数据库连接失败: {e}")
    
    # 以下方法都是对底层数据库实例的代理调用
    
    def create_tables(self) -> None:
        """创建所有数据表 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持创建表操作")
    
    def insert_data(self, model) -> bool:
        """插入单条数据 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持插入操作")

    def insert_batch(self, models) -> bool:
        """批量插入数据 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持批量插入操作")

    def insert_dataframe(self, df, table_name: str) -> bool:
        """插入DataFrame数据 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持DataFrame插入操作")
    
    def query_data(self, sql: str, params=None):
        """执行SQL查询 - 只读模式仅允许SELECT"""
        self._ensure_connected()

        # 安全检查：只允许SELECT查询
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT'):
            raise RuntimeError("副本数据库为只读模式，只允许SELECT查询")

        # 检查是否包含危险的SQL关键词
        dangerous_keywords = ['DELETE', 'UPDATE', 'INSERT', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'REPLACE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                raise RuntimeError(f"副本数据库为只读模式，不允许包含{keyword}操作的查询")

        return self._db_instance.query_data(sql, params)
    
    def get_latest_date(self, table_name: str, code=None):
        """获取最新数据日期"""
        self._ensure_connected()
        return self._db_instance.get_latest_date(table_name, code)
    
    def get_latest_dates_batch(self, table_name: str, codes):
        """批量获取多只股票的最新数据日期"""
        self._ensure_connected()
        return self._db_instance.get_latest_dates_batch(table_name, codes)
    
    def get_existing_codes(self, table_name: str):
        """获取已存在的股票代码"""
        self._ensure_connected()
        return self._db_instance.get_existing_codes(table_name)
    
    def delete_data(self, table_name: str, conditions) -> bool:
        """删除数据 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持删除操作")

    def update_data(self, table_name: str, data, conditions) -> bool:
        """更新数据 - 只读模式禁用"""
        raise RuntimeError("副本数据库为只读模式，不支持更新操作")
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        self._ensure_connected()
        return self._db_instance.table_exists(table_name)
    
    def get_table_info(self, table_name: str):
        """获取表信息"""
        self._ensure_connected()
        return self._db_instance.get_table_info(table_name)
    
    # 如果底层数据库有其他方法，也需要代理
    def __getattr__(self, name):
        """代理其他方法调用"""
        self._ensure_connected()
        if hasattr(self._db_instance, name):
            return getattr(self._db_instance, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()