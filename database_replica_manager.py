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
                 check_interval: float = 30.0,  # 将检查间隔从5秒调整为30秒，减少频繁检查
                 on_replica_updated: Optional[Callable] = None,
                 on_before_replica_update: Optional[Callable] = None):
        """
        初始化数据库副本管理器
        
        Args:
            master_db_path: 主数据库文件路径
            replica_db_path: 副本数据库文件路径，如果为None则自动生成
            check_interval: 检查间隔（秒）
            on_replica_updated: 副本更新后的回调函数
            on_before_replica_update: 副本更新前的回调函数（用于释放文件锁）
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
        self.on_before_replica_update = on_before_replica_update

        # 监控相关
        self._monitor_thread = None
        self._stop_monitoring = threading.Event()
        self._last_master_mtime = None
        self._replica_updating = False  # 标记副本是否正在更新，避免并发更新
        
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
        创建或更新副本 - 优化版本，避免频繁更新和竞态条件

        Returns:
            是否创建成功
        """
        # 避免并发更新
        if self._replica_updating:
            self.logger.debug("副本正在更新中，跳过本次更新")
            return False

        try:
            self._replica_updating = True

            # 检查主数据库是否存在
            if not self.master_db_path.exists():
                self.logger.error(f"主数据库文件不存在: {self.master_db_path}")
                return False

            # 确保副本目录存在
            self.replica_db_path.parent.mkdir(parents=True, exist_ok=True)

            # 创建临时副本文件，避免直接覆盖正在使用的副本
            temp_replica_path = self.replica_db_path.with_suffix('.tmp')

            try:
                # 等待主数据库文件稳定
                if not self._wait_for_file_stable():
                    self.logger.warning("主数据库文件未稳定，跳过本次复制")
                    return False

                # 调用更新前回调函数，释放文件锁
                if self.on_before_replica_update:
                    try:
                        self.logger.debug("调用更新前回调函数")
                        self.on_before_replica_update()
                        # 给一点时间让文件锁释放
                        time.sleep(0.5)
                    except Exception as e:
                        self.logger.error(f"更新前回调函数执行失败: {e}")

                # 复制到临时文件
                shutil.copy2(self.master_db_path, temp_replica_path)

                # 原子性替换副本文件
                if temp_replica_path.exists():
                    # 使用安全的替换策略，避免删除正在使用的文件
                    if self.replica_db_path.exists():
                        # 创建备份文件名，然后原子重命名
                        backup_path = self.replica_db_path.with_suffix('.backup')
                        try:
                            # 先将当前副本重命名为备份
                            self.replica_db_path.rename(backup_path)
                            # 将新副本移动到正确位置
                            temp_replica_path.rename(self.replica_db_path)
                            # 删除备份文件
                            if backup_path.exists():
                                backup_path.unlink()
                        except OSError as e:
                            # 如果重命名失败（文件被占用），尝试使用Windows特定的方法
                            self.logger.warning(f"文件重命名失败，可能被占用: {e}")
                            if backup_path.exists():
                                try:
                                    backup_path.unlink()
                                except:
                                    pass
                            # 尝试直接覆盖（Windows上有时可以成功）
                            shutil.move(str(temp_replica_path), str(self.replica_db_path))
                    else:
                        # 如果副本文件不存在，直接重命名
                        temp_replica_path.rename(self.replica_db_path)

                # 更新最后修改时间记录
                self._last_master_mtime = self.master_db_path.stat().st_mtime

                self.logger.info(f"副本已安全更新: {self.replica_db_path}")

                # 调用回调函数
                if self.on_replica_updated:
                    try:
                        self.on_replica_updated()
                    except Exception as e:
                        self.logger.error(f"副本更新回调函数执行失败: {e}")

                return True

            except Exception as e:
                # 清理临时文件
                if temp_replica_path.exists():
                    try:
                        temp_replica_path.unlink()
                    except Exception:
                        pass
                raise e

        except Exception as e:
            self.logger.error(f"创建副本失败: {e}")
            return False
        finally:
            self._replica_updating = False

    def _wait_for_file_stable(self, max_wait_time: float = 30.0, check_interval: float = 1.0) -> bool:
        """
        等待文件稳定（大小和修改时间不再变化）

        Args:
            max_wait_time: 最大等待时间（秒）
            check_interval: 检查间隔（秒）

        Returns:
            是否文件已稳定
        """
        start_time = time.time()
        last_size = None
        last_mtime = None
        stable_count = 0

        self.logger.debug("开始等待主数据库文件稳定...")

        while time.time() - start_time < max_wait_time:
            try:
                stat = self.master_db_path.stat()
                current_size = stat.st_size
                current_mtime = stat.st_mtime

                if last_size == current_size and last_mtime is not None and abs(last_mtime - current_mtime) < 0.1:
                    stable_count += 1
                    if stable_count >= 2:  # 连续2次检查都稳定
                        self.logger.debug(f"文件已稳定 (大小: {current_size}, 用时: {time.time() - start_time:.1f}秒)")
                        return True
                else:
                    if last_size is not None:
                        self.logger.debug(f"文件仍在变化 (大小: {last_size}->{current_size}, 时间: {last_mtime:.1f}->{current_mtime:.1f})")
                    stable_count = 0  # 重置计数器

                last_size = current_size
                last_mtime = current_mtime
                time.sleep(check_interval)

            except Exception as e:
                self.logger.warning(f"检查文件稳定性时出错: {e}")
                return False

        self.logger.warning(f"等待文件稳定超时({max_wait_time}秒)")
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
                
                # 检查是否有变化 - 需要显著的时间差异来避免误触发
                time_diff = current_mtime - (self._last_master_mtime or 0)
                if (self._last_master_mtime is None or time_diff > 1.0):

                    self.logger.info(f"检测到主数据库更新 (时间差: {time_diff:.1f}秒)，准备重建副本...")

                    # 等待一小段时间，让可能的写入操作完成
                    time.sleep(2)

                    # 重建副本
                    with self._lock:
                        if self._create_replica():
                            self.logger.info("副本重建成功")
                        else:
                            self.logger.error("副本重建失败")
                else:
                    # 微小的时间变化可能是文件系统元数据更新，不需要重建副本
                    if time_diff > 0:
                        self.logger.debug(f"检测到微小时间变化 ({time_diff:.3f}秒)，忽略")
                
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