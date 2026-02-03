"""
任务消费者

供 aidd-toolkit Worker 使用，从 Redis 队列消费任务
"""

import asyncio
import logging
from typing import Callable, Awaitable, Optional, Dict, Any, List
from dataclasses import dataclass, field

from app.models import Task
from app.mq.redis_client import RedisClient
from app.mq.task_queue import RedisTaskQueue

logger = logging.getLogger(__name__)


# 任务处理器类型
TaskHandler = Callable[[Task], Awaitable[Dict[str, Any]]]


@dataclass
class ConsumerConfig:
    """消费者配置"""
    redis_url: str = "redis://localhost:6379/0"
    services: List[str] = field(default_factory=list)  # 订阅的服务类型
    concurrency: int = 4  # 并发处理数
    poll_interval: float = 1.0  # 轮询间隔（秒）
    poll_timeout: int = 5  # 阻塞等待超时（秒）
    worker_id: Optional[str] = None


class TaskConsumer:
    """
    任务消费者
    
    从 Redis 队列中消费任务并执行
    
    用法:
        consumer = TaskConsumer(config)
        
        # 注册处理器
        consumer.register_handler("admet", handle_admet_task)
        consumer.register_handler("docking", handle_docking_task)
        
        # 启动消费
        await consumer.start()
    """
    
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self._redis_client = RedisClient(config.redis_url)
        self._task_queue = RedisTaskQueue(self._redis_client)
        self._handlers: Dict[str, TaskHandler] = {}
        self._running = False
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._tasks: List[asyncio.Task] = []
    
    # =========================================================================
    # 处理器注册
    # =========================================================================
    
    def register_handler(self, service: str, handler: TaskHandler) -> None:
        """
        注册任务处理器
        
        Args:
            service: 服务类型
            handler: 异步处理函数，接收 Task，返回结果 Dict
        """
        self._handlers[service] = handler
        if service not in self.config.services:
            self.config.services.append(service)
        logger.info(f"Handler registered for service: {service}")
    
    def handler(self, service: str):
        """
        装饰器方式注册处理器
        
        用法:
            @consumer.handler("admet")
            async def handle_admet(task: Task) -> Dict:
                ...
        """
        def decorator(func: TaskHandler) -> TaskHandler:
            self.register_handler(service, func)
            return func
        return decorator
    
    # =========================================================================
    # 消费循环
    # =========================================================================
    
    async def start(self) -> None:
        """启动消费者"""
        if self._running:
            logger.warning("Consumer already running")
            return
        
        await self._redis_client.connect()
        self._running = True
        self._semaphore = asyncio.Semaphore(self.config.concurrency)
        
        logger.info(f"Task consumer started (services={self.config.services}, concurrency={self.config.concurrency})")
        
        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled")
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """停止消费者"""
        self._running = False
        
        # 等待正在处理的任务完成
        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        await self._redis_client.disconnect()
        logger.info("Task consumer stopped")
    
    async def _consume_loop(self) -> None:
        """消费循环"""
        while self._running:
            try:
                # 轮询各服务队列
                for service in self.config.services:
                    if not self._running:
                        break
                    
                    # 检查是否有空闲槽位
                    if self._semaphore.locked():
                        continue
                    
                    # 尝试获取任务
                    task = await self._task_queue.pop(
                        service=service,
                        timeout=0  # 非阻塞
                    )
                    
                    if task:
                        # 异步处理任务
                        asyncio_task = asyncio.create_task(
                            self._process_task(task)
                        )
                        self._tasks.append(asyncio_task)
                        asyncio_task.add_done_callback(
                            lambda t: self._tasks.remove(t) if t in self._tasks else None
                        )
                
                # 清理已完成的任务
                self._tasks = [t for t in self._tasks if not t.done()]
                
                # 等待一段时间再继续
                await asyncio.sleep(self.config.poll_interval)
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                await asyncio.sleep(self.config.poll_interval)
    
    async def _process_task(self, task: Task) -> None:
        """处理单个任务"""
        async with self._semaphore:
            handler = self._handlers.get(task.service)
            
            if not handler:
                logger.error(f"No handler for service: {task.service}")
                await self._task_queue.fail(
                    task.id,
                    f"No handler registered for service: {task.service}",
                    retry=False
                )
                return
            
            logger.info(f"Processing task: {task.id} (service={task.service})")
            
            try:
                # 执行处理器
                result = await asyncio.wait_for(
                    handler(task),
                    timeout=task.timeout_seconds if task.timeout_seconds else None
                )
                
                # 标记完成
                await self._task_queue.complete(task.id, result)
                logger.info(f"Task completed: {task.id}")
                
            except asyncio.TimeoutError:
                await self._task_queue.fail(
                    task.id,
                    f"Task timeout after {task.timeout_seconds}s",
                    retry=True
                )
                logger.error(f"Task timeout: {task.id}")
                
            except Exception as e:
                await self._task_queue.fail(task.id, str(e), retry=True)
                logger.error(f"Task failed: {task.id} - {e}")
    
    # =========================================================================
    # 状态查询
    # =========================================================================
    
    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running
    
    @property
    def active_tasks(self) -> int:
        """当前处理中的任务数"""
        return len([t for t in self._tasks if not t.done()])
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计"""
        return await self._task_queue.get_stats()


# =========================================================================
# 便捷函数
# =========================================================================

def create_consumer(
    redis_url: str,
    services: List[str],
    concurrency: int = 4,
    **kwargs
) -> TaskConsumer:
    """
    创建任务消费者
    
    Args:
        redis_url: Redis URL
        services: 订阅的服务列表
        concurrency: 并发数
    
    Returns:
        TaskConsumer 实例
    """
    config = ConsumerConfig(
        redis_url=redis_url,
        services=services,
        concurrency=concurrency,
        **kwargs
    )
    return TaskConsumer(config)
