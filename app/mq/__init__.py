"""
消息队列模块

基于 Redis 实现分布式任务队列
"""

from app.mq.redis_client import RedisClient, get_redis_client
from app.mq.task_queue import RedisTaskQueue
from app.mq.task_consumer import TaskConsumer

__all__ = [
    "RedisClient",
    "get_redis_client",
    "RedisTaskQueue",
    "TaskConsumer",
]
