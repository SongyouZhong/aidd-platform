"""
API 依赖注入
"""

from typing import Generator, Optional
from functools import lru_cache

from sqlalchemy.orm import Session

from app.config import get_settings, Settings
from app.db.session import SessionLocal
from app.scheduler import TaskDispatcher, ResourceManager, PriorityTaskQueue
from app.mq import RedisTaskQueue, RedisClient


def get_db() -> Generator[Session, None, None]:
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================================================================
# 调度器单例
# =========================================================================

_resource_manager: Optional[ResourceManager] = None
_task_queue: Optional[PriorityTaskQueue] = None
_dispatcher: Optional[TaskDispatcher] = None

# Redis MQ 单例
_redis_client: Optional[RedisClient] = None
_redis_task_queue: Optional[RedisTaskQueue] = None


def get_resource_manager() -> ResourceManager:
    """获取资源管理器实例"""
    global _resource_manager
    if _resource_manager is None:
        _resource_manager = ResourceManager()
    return _resource_manager


def get_task_queue() -> PriorityTaskQueue:
    """获取内存任务队列实例（用于本地调度）"""
    global _task_queue
    if _task_queue is None:
        settings = get_settings()
        _task_queue = PriorityTaskQueue(
            starvation_threshold=getattr(settings, 'starvation_threshold', 100)
        )
    return _task_queue


def get_dispatcher() -> TaskDispatcher:
    """获取任务调度器实例"""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = TaskDispatcher(
            resource_manager=get_resource_manager(),
            task_queue=get_task_queue()
        )
    return _dispatcher


async def get_redis_client() -> RedisClient:
    """获取 Redis 客户端实例"""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = RedisClient(settings.redis_url)
        await _redis_client.connect()
    return _redis_client


async def get_redis_task_queue() -> RedisTaskQueue:
    """获取 Redis 任务队列实例"""
    global _redis_task_queue
    if _redis_task_queue is None:
        redis_client = await get_redis_client()
        _redis_task_queue = RedisTaskQueue(redis_client)
    return _redis_task_queue


def reset_scheduler() -> None:
    """重置调度器（仅用于测试）"""
    global _resource_manager, _task_queue, _dispatcher
    _resource_manager = None
    _task_queue = None
    _dispatcher = None


async def close_connections() -> None:
    """关闭所有连接"""
    global _redis_client, _redis_task_queue
    if _redis_client:
        await _redis_client.disconnect()
        _redis_client = None
    _redis_task_queue = None
