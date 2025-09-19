#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库副本管理器

实现数据库文件的副本管理，包括：
1. 启动时创建主数据库的副本
2. 监控主数据库文件的变化
3. 自动重建副本并重连数据库

作者: Stock Data Platform Team
版本: 1.0.0
"""

import os
import shutil
import threading
import time
import logging
from pathlib import Path
from typing import Optional, Callable
from datetime import datetime


class DatabaseReplicaManager:
    """数据库副本管理器"""
    
    def __init__(self, 
                 master_db_path: str,
                 replica_db_path: Optional[str] = None,
                 check_interval: float = 5.0,
                 on_replica_updated: Optional[Callable] = None):
        """
        初始化数据库副本管理器
        
        Args:
            master_db_path: 主数据库文件路径
            replica_db_path: 副本数据库文件路径，如果为None则自动生成
            check_interval: 检查间隔（秒）
            on_replica_updated: 副本更新时的回调函数
        """
        self.master_db_path = Path(master_db_path)
        
        # 如果没有指定副本路径，则在同目录下创建
        if replica_db_path is None:
            replica_name = f"replica_{self.master_db_path.name}"
            self.replica_db_path = self.master_db_path.parent / replica_name
        else:
            self.replica_db_path = Path(replica_db_path)
            
        self.check_interval = check_interval
        self.on_replica_updated = on_replica_updated
        
        # 监控相关
        self._monitor_thread = None
        self._stop_monitoring = threading.Event()
        self._last_master_mtime = None
        
        # 日志
        self.logger = logging.getLogger(__name__)
        
        # 锁，确保线程安全
        self._lock = threading.RLock()
        
    def start(self) -> str:
        """
        启动副本管理器
        
        Returns:
            副本数据库文件路径
        """
        with self._lock:
            # 创建初始副本
            self._create_replica()
            
            # 启动监控线程
            self._start_monitoring()
            
            self.logger.info(f"数据库副本管理器已启动，副本路径: {self.replica_db_path}")
            return str(self.replica_db_path)
    
    def stop(self):
        """停止副本管理器"""
        with self._lock:
            # 停止监控线程
            self._stop_monitoring.set()
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=10)
                
            self.logger.info("数据库副本管理器已停止")
    
    def get_replica_path(self) -> str:
        """获取副本数据库路径"""
        return str(self.replica_db_path)
    
    def force_update_replica(self) -> bool:
        """
        强制更新副本
        
        Returns:
            是否更新成功
        """
        with self._lock:
            return self._create_replica()
    
    def _create_replica(self) -> bool:
        """
        创建或更新副本
        
        Returns:
            是否创建成功
        """
        try:
            # 检查主数据库是否存在
            if not self.master_db_path.exists():
                self.logger.error(f"主数据库文件不存在: {self.master_db_path}")
                return False
            
            # 确保副本目录存在
            self.replica_db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 复制文件
            shutil.copy2(self.master_db_path, self.replica_db_path)
            
            # 更新最后修改时间记录
            self._last_master_mtime = self.master_db_path.stat().st_mtime
            
            self.logger.info(f"副本已创建/更新: {self.replica_db_path}")
            
            # 调用回调函数
            if self.on_replica_updated:
                try:
                    self.on_replica_updated()
                except Exception as e:
                    self.logger.error(f"副本更新回调函数执行失败: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"创建副本失败: {e}")
            return False
    
    def _start_monitoring(self):
        """启动监控线程"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
            
        self._stop_monitoring.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_master_db,
            name="DatabaseReplicaMonitor",
            daemon=True
        )
        self._monitor_thread.start()
        
        self.logger.info("数据库监控线程已启动")
    
    def _monitor_master_db(self):
        """监控主数据库文件变化"""
        self.logger.info(f"开始监控主数据库: {self.master_db_path}")
        
        while not self._stop_monitoring.is_set():
            try:
                # 检查主数据库是否存在
                if not self.master_db_path.exists():
                    self.logger.warning(f"主数据库文件不存在: {self.master_db_path}")
                    time.sleep(self.check_interval)
                    continue
                
                # 获取当前修改时间
                current_mtime = self.master_db_path.stat().st_mtime
                
                # 检查是否有变化
                if (self._last_master_mtime is None or 
                    current_mtime > self._last_master_mtime):
                    
                    self.logger.info(f"检测到主数据库更新，重建副本...")
                    
                    # 等待一小段时间，确保文件写入完成
                    time.sleep(1)
                    
                    # 重建副本
                    with self._lock:
                        if self._create_replica():
                            self.logger.info("副本重建成功")
                        else:
                            self.logger.error("副本重建失败")
                
                # 等待下次检查
                time.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"监控过程中发生错误: {e}")
                time.sleep(self.check_interval)
        
        self.logger.info("数据库监控线程已停止")
    
    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.stop()
        
        # 清理副本文件（可选）
        try:
            if self.replica_db_path.exists():
                self.replica_db_path.unlink()
                self.logger.info(f"已清理副本文件: {self.replica_db_path}")
        except Exception as e:
            self.logger.warning(f"清理副本文件失败: {e}")