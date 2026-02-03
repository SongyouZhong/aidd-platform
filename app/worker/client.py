"""
Worker 客户端
供 aidd-toolkit 的服务使用，用于与 aidd-platform 通信
"""

import asyncio
import logging
import httpx
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Worker 配置"""
    platform_url: str = "http://localhost:8000"
    hostname: str = "worker-001"
    ip_address: Optional[str] = None
    port: int = 8080
    total_cpu: int = 4
    total_memory_gb: float = 16.0
    total_gpu: int = 0
    total_gpu_memory_gb: float = 0.0
    supported_services: List[str] = field(default_factory=list)
    max_concurrent_tasks: int = 4
    labels: Dict[str, str] = field(default_factory=dict)
    heartbeat_interval: int = 30  # 秒


class WorkerClient:
    """
    Worker 客户端
    
    用法:
        client = WorkerClient(config)
        await client.register()
        
        # 启动心跳
        await client.start_heartbeat()
        
        # 处理任务
        while True:
            task = await client.get_task()
            if task:
                result = await process_task(task)
                await client.report_task_completed(task['id'], result)
    """
    
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.worker_id: Optional[str] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self._current_tasks: List[str] = []
        self._used_cpu = 0
        self._used_memory_gb = 0.0
        self._used_gpu = 0
    
    @property
    def api_url(self) -> str:
        """API 基础 URL"""
        return f"{self.config.platform_url}/api/v1"
    
    async def _get_client(self) -> httpx.AsyncClient:
        """获取 HTTP 客户端"""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client
    
    async def close(self) -> None:
        """关闭客户端"""
        self._running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
    
    # =========================================================================
    # 注册与心跳
    # =========================================================================
    
    async def register(self) -> bool:
        """
        注册 Worker
        
        Returns:
            是否注册成功
        """
        client = await self._get_client()
        
        payload = {
            "hostname": self.config.hostname,
            "ip_address": self.config.ip_address,
            "port": self.config.port,
            "total_cpu": self.config.total_cpu,
            "total_memory_gb": self.config.total_memory_gb,
            "total_gpu": self.config.total_gpu,
            "total_gpu_memory_gb": self.config.total_gpu_memory_gb,
            "supported_services": self.config.supported_services,
            "max_concurrent_tasks": self.config.max_concurrent_tasks,
            "labels": self.config.labels
        }
        
        try:
            response = await client.post(
                f"{self.api_url}/workers/register",
                json=payload
            )
            response.raise_for_status()
            
            data = response.json()
            self.worker_id = data.get("id")
            logger.info(f"Worker registered: {self.worker_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            return False
    
    async def unregister(self) -> bool:
        """注销 Worker"""
        if not self.worker_id:
            return False
        
        client = await self._get_client()
        
        try:
            response = await client.delete(
                f"{self.api_url}/workers/{self.worker_id}"
            )
            response.raise_for_status()
            logger.info(f"Worker unregistered: {self.worker_id}")
            self.worker_id = None
            return True
            
        except Exception as e:
            logger.error(f"Failed to unregister worker: {e}")
            return False
    
    async def heartbeat(self) -> bool:
        """发送心跳"""
        if not self.worker_id:
            return False
        
        client = await self._get_client()
        
        payload = {
            "used_cpu": self._used_cpu,
            "used_memory_gb": self._used_memory_gb,
            "used_gpu": self._used_gpu,
            "current_tasks": self._current_tasks
        }
        
        try:
            response = await client.post(
                f"{self.api_url}/workers/{self.worker_id}/heartbeat",
                json=payload
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}")
            return False
    
    async def start_heartbeat(self) -> None:
        """启动心跳循环"""
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def _heartbeat_loop(self) -> None:
        """心跳循环"""
        while self._running:
            try:
                await self.heartbeat()
                await asyncio.sleep(self.config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(5)
    
    # =========================================================================
    # 任务管理
    # =========================================================================
    
    def start_task(
        self,
        task_id: str,
        cpu_cores: int = 1,
        memory_gb: float = 1.0,
        gpu_count: int = 0
    ) -> None:
        """记录任务开始"""
        self._current_tasks.append(task_id)
        self._used_cpu += cpu_cores
        self._used_memory_gb += memory_gb
        self._used_gpu += gpu_count
    
    def finish_task(
        self,
        task_id: str,
        cpu_cores: int = 1,
        memory_gb: float = 1.0,
        gpu_count: int = 0
    ) -> None:
        """记录任务完成"""
        if task_id in self._current_tasks:
            self._current_tasks.remove(task_id)
        self._used_cpu = max(0, self._used_cpu - cpu_cores)
        self._used_memory_gb = max(0, self._used_memory_gb - memory_gb)
        self._used_gpu = max(0, self._used_gpu - gpu_count)
    
    async def report_task_completed(
        self,
        task_id: str,
        result: Optional[Dict[str, Any]] = None
    ) -> bool:
        """报告任务完成"""
        client = await self._get_client()
        
        # 注意：这需要 platform 提供相应的 API
        # 这里假设有一个内部 API 用于 Worker 报告任务状态
        payload = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "status": "completed",
            "result": result
        }
        
        try:
            response = await client.post(
                f"{self.api_url}/internal/tasks/report",
                json=payload
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Failed to report task completion: {e}")
            return False
    
    async def report_task_failed(
        self,
        task_id: str,
        error: str
    ) -> bool:
        """报告任务失败"""
        client = await self._get_client()
        
        payload = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "status": "failed",
            "error": error
        }
        
        try:
            response = await client.post(
                f"{self.api_url}/internal/tasks/report",
                json=payload
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Failed to report task failure: {e}")
            return False


# 便捷函数
def create_worker_client(
    platform_url: str,
    hostname: str,
    supported_services: List[str],
    **kwargs
) -> WorkerClient:
    """
    创建 Worker 客户端
    
    Args:
        platform_url: 平台 URL
        hostname: 主机名
        supported_services: 支持的服务列表
        **kwargs: 其他配置参数
    
    Returns:
        WorkerClient 实例
    """
    config = WorkerConfig(
        platform_url=platform_url,
        hostname=hostname,
        supported_services=supported_services,
        **kwargs
    )
    return WorkerClient(config)
