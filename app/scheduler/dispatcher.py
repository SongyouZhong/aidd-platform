"""
任务调度器
负责任务的分发和生命周期管理
"""

import asyncio
import logging
from typing import Dict, List, Optional, Callable, Awaitable
from datetime import datetime
from enum import Enum

from app.models import Task, TaskStatus, TaskPriority, Worker, ResourceRequirement
from app.scheduler.resource_manager import ResourceManager
from app.scheduler.priority_queue import PriorityTaskQueue
from app.config import get_settings

logger = logging.getLogger(__name__)


class DispatchResult(Enum):
    """调度结果"""
    SUCCESS = "success"
    NO_WORKER = "no_available_worker"
    NO_TASK = "no_pending_task"
    RESOURCE_INSUFFICIENT = "resource_insufficient"
    ERROR = "error"


class TaskDispatcher:
    """
    任务调度器
    
    职责：
    1. 从队列中获取待处理任务
    2. 为任务分配合适的 Worker
    3. 处理任务状态变更
    4. 支持任务重试和失败恢复
    """
    
    def __init__(
        self,
        resource_manager: ResourceManager,
        task_queue: PriorityTaskQueue
    ):
        self._resource_manager = resource_manager
        self._task_queue = task_queue
        self._settings = get_settings()
        
        # 任务追踪
        self._running_tasks: Dict[str, Task] = {}
        self._task_workers: Dict[str, str] = {}  # task_id -> worker_id
        
        # 回调函数
        self._on_task_dispatched: Optional[Callable[[Task, Worker], Awaitable[None]]] = None
        self._on_task_completed: Optional[Callable[[Task], Awaitable[None]]] = None
        self._on_task_failed: Optional[Callable[[Task, str], Awaitable[None]]] = None
        
        # 调度循环控制
        self._running = False
        self._dispatch_interval = 1.0  # 秒
    
    # =========================================================================
    # 回调注册
    # =========================================================================
    
    def on_task_dispatched(
        self,
        callback: Callable[[Task, Worker], Awaitable[None]]
    ) -> None:
        """注册任务分发回调"""
        self._on_task_dispatched = callback
    
    def on_task_completed(
        self,
        callback: Callable[[Task], Awaitable[None]]
    ) -> None:
        """注册任务完成回调"""
        self._on_task_completed = callback
    
    def on_task_failed(
        self,
        callback: Callable[[Task, str], Awaitable[None]]
    ) -> None:
        """注册任务失败回调"""
        self._on_task_failed = callback
    
    # =========================================================================
    # 任务提交
    # =========================================================================
    
    async def submit_task(self, task: Task) -> bool:
        """
        提交任务
        
        Args:
            task: 任务对象
        
        Returns:
            是否提交成功
        """
        if not task.id:
            logger.error("Task ID is required")
            return False
        
        task.status = TaskStatus.PENDING
        task.created_at = task.created_at or datetime.utcnow()
        
        if self._task_queue.push(task):
            logger.info(f"Task submitted: {task.id} (service={task.service})")
            return True
        return False
    
    async def submit_tasks(self, tasks: List[Task]) -> int:
        """批量提交任务"""
        count = 0
        for task in tasks:
            if await self.submit_task(task):
                count += 1
        return count
    
    # =========================================================================
    # 任务调度
    # =========================================================================
    
    async def dispatch_one(self, service: Optional[str] = None) -> DispatchResult:
        """
        调度一个任务
        
        Args:
            service: 指定服务类型
        
        Returns:
            调度结果
        """
        # 从队列获取任务
        task = self._task_queue.pop(service)
        if not task:
            return DispatchResult.NO_TASK
        
        # 获取资源需求
        requirement = task.resource_requirement or ResourceRequirement()
        
        # 查找可用 Worker
        worker = self._resource_manager.find_available_worker(
            requirement=requirement,
            service=task.service,
            preferred_worker_id=self._task_workers.get(task.id)
        )
        
        if not worker:
            # 没有可用 Worker，将任务放回队列
            self._task_queue.push(task)
            return DispatchResult.NO_WORKER
        
        # 分配资源
        if not self._resource_manager.allocate_resources(
            worker_id=worker.id,
            task_id=task.id,
            requirement=requirement
        ):
            self._task_queue.push(task)
            return DispatchResult.RESOURCE_INSUFFICIENT
        
        # 更新任务状态
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        task.worker_id = worker.id
        
        # 记录运行中的任务
        self._running_tasks[task.id] = task
        self._task_workers[task.id] = worker.id
        
        logger.info(f"Task dispatched: {task.id} -> {worker.id}")
        
        # 触发回调
        if self._on_task_dispatched:
            try:
                await self._on_task_dispatched(task, worker)
            except Exception as e:
                logger.error(f"Error in dispatch callback: {e}")
        
        return DispatchResult.SUCCESS
    
    async def dispatch_batch(
        self,
        max_count: int = 10,
        service: Optional[str] = None
    ) -> Dict[DispatchResult, int]:
        """
        批量调度任务
        
        Args:
            max_count: 最大调度数量
            service: 指定服务类型
        
        Returns:
            各调度结果的统计
        """
        results = {r: 0 for r in DispatchResult}
        
        for _ in range(max_count):
            result = await self.dispatch_one(service)
            results[result] += 1
            
            if result in (DispatchResult.NO_TASK, DispatchResult.NO_WORKER):
                break
        
        return results
    
    # =========================================================================
    # 任务状态处理
    # =========================================================================
    
    async def task_completed(
        self,
        task_id: str,
        result: Optional[Dict] = None
    ) -> bool:
        """
        标记任务完成
        
        Args:
            task_id: 任务ID
            result: 任务结果
        
        Returns:
            是否处理成功
        """
        task = self._running_tasks.pop(task_id, None)
        if not task:
            logger.warning(f"Task not found in running tasks: {task_id}")
            return False
        
        # 更新任务状态
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.utcnow()
        task.result = result
        
        # 释放资源
        worker_id = self._task_workers.pop(task_id, None)
        if worker_id and task.resource_requirement:
            self._resource_manager.release_resources(
                worker_id=worker_id,
                task_id=task_id,
                requirement=task.resource_requirement
            )
        
        logger.info(f"Task completed: {task_id}")
        
        # 触发回调
        if self._on_task_completed:
            try:
                await self._on_task_completed(task)
            except Exception as e:
                logger.error(f"Error in completion callback: {e}")
        
        return True
    
    async def task_failed(
        self,
        task_id: str,
        error: str,
        retry: bool = True
    ) -> bool:
        """
        标记任务失败
        
        Args:
            task_id: 任务ID
            error: 错误信息
            retry: 是否重试
        
        Returns:
            是否处理成功
        """
        task = self._running_tasks.pop(task_id, None)
        if not task:
            logger.warning(f"Task not found in running tasks: {task_id}")
            return False
        
        # 释放资源
        worker_id = self._task_workers.pop(task_id, None)
        if worker_id and task.resource_requirement:
            self._resource_manager.release_resources(
                worker_id=worker_id,
                task_id=task_id,
                requirement=task.resource_requirement
            )
        
        # 检查是否可以重试
        task.retry_count = (task.retry_count or 0) + 1
        max_retries = task.max_retries or self._settings.max_task_retries
        
        if retry and task.retry_count < max_retries:
            # 重新入队
            task.status = TaskStatus.PENDING
            task.error_message = error
            self._task_queue.push(task)
            logger.info(f"Task requeued for retry: {task_id} (attempt {task.retry_count})")
        else:
            # 标记失败
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.utcnow()
            task.error_message = error
            logger.error(f"Task failed: {task_id} - {error}")
            
            # 触发回调
            if self._on_task_failed:
                try:
                    await self._on_task_failed(task, error)
                except Exception as e:
                    logger.error(f"Error in failure callback: {e}")
        
        return True
    
    async def cancel_task(self, task_id: str, reason: str = "") -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            reason: 取消原因
        
        Returns:
            是否取消成功
        """
        # 检查是否在队列中
        if self._task_queue.remove(task_id):
            logger.info(f"Task cancelled from queue: {task_id}")
            return True
        
        # 检查是否在运行中
        task = self._running_tasks.get(task_id)
        if task:
            task.status = TaskStatus.CANCELLED
            task.error_message = reason or "Cancelled by user"
            
            # 释放资源
            worker_id = self._task_workers.pop(task_id, None)
            if worker_id and task.resource_requirement:
                self._resource_manager.release_resources(
                    worker_id=worker_id,
                    task_id=task_id,
                    requirement=task.resource_requirement
                )
            
            self._running_tasks.pop(task_id, None)
            logger.info(f"Task cancelled: {task_id}")
            return True
        
        return False
    
    # =========================================================================
    # 调度循环
    # =========================================================================
    
    async def start(self) -> None:
        """启动调度循环"""
        if self._running:
            return
        
        self._running = True
        logger.info("Task dispatcher started")
        
        while self._running:
            try:
                # 检查心跳超时
                timed_out = self._resource_manager.check_heartbeat_timeout()
                for worker_id in timed_out:
                    await self._handle_worker_timeout(worker_id)
                
                # 调度任务
                await self.dispatch_batch()
                
                # 等待下一轮
                await asyncio.sleep(self._dispatch_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dispatch loop: {e}")
                await asyncio.sleep(self._dispatch_interval)
        
        logger.info("Task dispatcher stopped")
    
    async def stop(self) -> None:
        """停止调度循环"""
        self._running = False
    
    async def _handle_worker_timeout(self, worker_id: str) -> None:
        """处理 Worker 超时"""
        # 找到该 Worker 上的所有任务
        affected_tasks = [
            task_id for task_id, wid in self._task_workers.items()
            if wid == worker_id
        ]
        
        for task_id in affected_tasks:
            await self.task_failed(
                task_id=task_id,
                error=f"Worker {worker_id} timeout",
                retry=True
            )
    
    # =========================================================================
    # 状态查询
    # =========================================================================
    
    def get_running_tasks(self) -> List[Task]:
        """获取运行中的任务"""
        return list(self._running_tasks.values())
    
    def get_task_worker(self, task_id: str) -> Optional[str]:
        """获取任务所在的 Worker"""
        return self._task_workers.get(task_id)
    
    def get_stats(self) -> Dict:
        """获取调度器统计信息"""
        return {
            "running_tasks": len(self._running_tasks),
            "pending_tasks": self._task_queue.size(),
            "queue_stats": self._task_queue.get_stats(),
            "cluster_stats": self._resource_manager.get_cluster_stats(),
            "is_running": self._running,
        }
