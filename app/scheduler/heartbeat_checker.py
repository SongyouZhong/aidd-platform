"""
心跳检查器
定期检查 Worker 心跳超时，自动标记离线 Worker
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2

from app.config import get_settings
from app.scheduler.resource_manager import ResourceManager
from app.models import WorkerStatus

logger = logging.getLogger(__name__)


class HeartbeatChecker:
    """
    心跳检查器
    
    职责：
    1. 定期检查内存中 Worker 的心跳超时
    2. 同步更新数据库中超时 Worker 的状态为 offline
    3. 启动时从数据库恢复 Worker 状态
    """
    
    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager = resource_manager
        self._settings = get_settings()
        self._running = False
        self._check_interval = self._settings.heartbeat_check_interval  # 默认 10 秒
        self._heartbeat_timeout = self._settings.heartbeat_timeout  # 默认 30 秒
        
        logger.info(
            f"HeartbeatChecker 初始化: 检查间隔={self._check_interval}s, "
            f"超时时间={self._heartbeat_timeout}s"
        )
    
    async def start(self) -> None:
        """启动心跳检查循环"""
        if self._running:
            logger.warning("HeartbeatChecker 已在运行")
            return
        
        self._running = True
        logger.info("HeartbeatChecker 已启动")
        
        # 启动时从数据库加载 Worker 状态
        await self._load_workers_from_db()
        
        while self._running:
            try:
                await self._check_heartbeats()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                logger.info("HeartbeatChecker 收到取消信号")
                break
            except Exception as e:
                logger.error(f"HeartbeatChecker 检查异常: {e}")
                await asyncio.sleep(self._check_interval)
        
        logger.info("HeartbeatChecker 已停止")
    
    async def stop(self) -> None:
        """停止心跳检查"""
        self._running = False
    
    async def _check_heartbeats(self) -> None:
        """检查所有 Worker 心跳"""
        # 检查内存中的 Worker
        timed_out_workers = self._resource_manager.check_heartbeat_timeout()
        
        for worker_id in timed_out_workers:
            logger.warning(f"Worker 心跳超时，标记为 offline: {worker_id}")
            # 同步更新数据库
            await self._set_worker_offline_db(worker_id)
        
        # 额外：直接检查数据库中长时间没有心跳的 Worker
        await self._check_db_heartbeats()
    
    async def _check_db_heartbeats(self) -> None:
        """
        直接检查数据库中 Worker 的心跳
        
        处理场景：Worker 在 platform 重启后还未重新注册到内存，
        但数据库中仍有记录且状态为 online
        """
        try:
            settings = self._settings
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            cur = conn.cursor()
            
            # 查找心跳超时的 Worker
            timeout_threshold = datetime.utcnow() - timedelta(seconds=self._heartbeat_timeout)
            
            sql = """
            UPDATE workers 
            SET status = 'offline', updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('online', 'busy') 
              AND last_heartbeat IS NOT NULL 
              AND last_heartbeat < %s
            RETURNING id, hostname
            """
            cur.execute(sql, (timeout_threshold,))
            updated_workers = cur.fetchall()
            
            if updated_workers:
                conn.commit()
                for worker_id, hostname in updated_workers:
                    logger.warning(f"数据库 Worker 心跳超时，标记为 offline: {worker_id} ({hostname})")
                    
                    # 同步更新内存中的状态
                    worker = self._resource_manager.get_worker(worker_id)
                    if worker:
                        worker.status = WorkerStatus.OFFLINE
            
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"检查数据库 Worker 心跳失败: {e}")
    
    async def _set_worker_offline_db(self, worker_id: str) -> bool:
        """将 Worker 在数据库中标记为 offline"""
        try:
            settings = self._settings
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            cur = conn.cursor()
            
            sql = """
            UPDATE workers 
            SET status = 'offline', updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s
            """
            cur.execute(sql, (worker_id,))
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"数据库标记 Worker offline 失败: {e}")
            return False
    
    async def _load_workers_from_db(self) -> None:
        """
        从数据库加载 Worker 状态到内存
        
        用于 platform 重启后恢复状态
        """
        try:
            settings = self._settings
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            cur = conn.cursor()
            
            # 查询所有非 offline 的 Worker
            sql = """
            SELECT id, hostname, ip_address, port, status,
                   total_cpu_cores, total_memory_gb, total_gpu_count, total_gpu_memory_gb,
                   used_cpu_cores, used_memory_gb, used_gpu_count, used_gpu_memory_gb,
                   supported_services, max_concurrent_tasks, labels,
                   registered_at, last_heartbeat
            FROM workers
            WHERE status != 'offline'
            """
            cur.execute(sql)
            rows = cur.fetchall()
            
            logger.info(f"从数据库加载 {len(rows)} 个活跃 Worker")
            
            # 注意：这里不直接恢复到内存，因为 Worker 需要主动发送心跳重新注册
            # 但我们可以检查超时的并标记为 offline
            timeout_threshold = datetime.utcnow() - timedelta(seconds=self._heartbeat_timeout)
            
            for row in rows:
                worker_id = row[0]
                hostname = row[1]
                last_heartbeat = row[17]
                
                if last_heartbeat and last_heartbeat < timeout_threshold:
                    # 心跳超时，标记为 offline
                    cur.execute(
                        "UPDATE workers SET status = 'offline', updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (worker_id,)
                    )
                    logger.warning(f"启动时发现 Worker 心跳超时: {worker_id} ({hostname})")
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"从数据库加载 Worker 状态失败: {e}")


# 全局单例
_heartbeat_checker: Optional[HeartbeatChecker] = None


def get_heartbeat_checker(resource_manager: ResourceManager) -> HeartbeatChecker:
    """获取心跳检查器单例"""
    global _heartbeat_checker
    if _heartbeat_checker is None:
        _heartbeat_checker = HeartbeatChecker(resource_manager)
    return _heartbeat_checker
