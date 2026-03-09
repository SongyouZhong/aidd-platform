"""
Redis 工具函数
封装与 aidd-platform RedisTaskQueue 兼容的队列操作，
Worker 端 (aidd-toolkit) 无需任何改动即可消费此任务。
"""

import json
import logging
import redis
from datetime import datetime
from typing import Dict, List, Optional

from .config import (
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB,
    TASK_KEY_PREFIX, PENDING_QUEUE, STATS_KEY,
    DOCKING_PROCESSED_SET, COMPLETED_LIST, FAILED_LIST,
)

logger = logging.getLogger(__name__)


def get_redis() -> redis.Redis:
    """获取 Redis 同步客户端（Airflow DAG 运行在同步上下文中）"""
    kwargs = dict(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=False,  # 保持与原 RedisTaskQueue 一致
    )
    if REDIS_PASSWORD:
        kwargs['password'] = REDIS_PASSWORD
    return redis.Redis(**kwargs)


def push_task_to_redis(
    task_id: str,
    service: str,
    task_type: str,
    priority: int,
    input_params: dict,
    name: str = "",
    input_files: Optional[List[str]] = None,
    resource_cpu_cores: int = 1,
    resource_memory_gb: float = 1.0,
    resource_gpu_count: int = 0,
    resource_gpu_memory_gb: float = 0.0,
    timeout_seconds: int = 3600,
    max_retries: int = 3,
) -> None:
    """
    提交任务到 Redis，保持与 aidd-platform RedisTaskQueue 兼容的数据结构。
    Worker 端 (aidd-toolkit) 无需任何改动即可消费此任务。

    此函数等价于原 AdmetSyncChecker._push_to_redis / DockingSyncChecker._push_to_redis
    """
    r = get_redis()
    now = datetime.now()

    task_data = {
        'id': task_id,
        'service': service,
        'task_type': task_type,
        'name': name,
        'status': 'pending',
        'priority': str(priority),
        'input_params': json.dumps(input_params),
        'input_files': json.dumps(input_files or []),
        'output_files': json.dumps([]),
        'result': '',
        'worker_id': '',
        'retry_count': '0',
        'max_retries': str(max_retries),
        'timeout_seconds': str(timeout_seconds),
        'error_message': '',
        'resource_cpu_cores': str(resource_cpu_cores),
        'resource_memory_gb': str(resource_memory_gb),
        'resource_gpu_count': str(resource_gpu_count),
        'resource_gpu_memory_gb': str(resource_gpu_memory_gb),
        'created_at': now.isoformat(),
        'started_at': '',
        'completed_at': '',
    }

    # 优先级分数计算（与原 RedisTaskQueue 保持一致）
    priority_base = {0: 0, 1: 100, 2: 200, 3: 300, 4: 400}
    score = priority_base.get(priority, 400) + (now.timestamp() / 10000000000)

    task_key = f"{TASK_KEY_PREFIX}:{task_id}"
    service_queue = f"aidd:queue:service:{service}"

    # 原子 Pipeline 操作
    pipe = r.pipeline(transaction=True)
    pipe.hset(task_key, mapping=task_data)
    pipe.expire(task_key, 7 * 24 * 3600)  # 7 天过期
    pipe.zadd(PENDING_QUEUE, {task_id: score})
    pipe.lpush(service_queue, task_id)
    pipe.hincrby(STATS_KEY, 'total_submitted', 1)
    pipe.hincrby(STATS_KEY, f'submitted:{service}', 1)
    pipe.execute()

    logger.info(f"任务已推送到 Redis: task_id={task_id}, service={service}")


def get_task_data(task_id: str) -> Optional[Dict[str, str]]:
    """
    获取 Redis 中任务的 Hash 数据

    Returns:
        解码后的字典，如任务不存在则返回 None
    """
    r = get_redis()
    raw = r.hgetall(f"{TASK_KEY_PREFIX}:{task_id}")
    if not raw:
        return None
    return {
        (k.decode('utf-8') if isinstance(k, bytes) else k):
        (v.decode('utf-8') if isinstance(v, bytes) else v)
        for k, v in raw.items()
    }


def get_completed_task_ids(max_count: int = 1000) -> List[str]:
    """获取 completed 列表中的任务 ID"""
    r = get_redis()
    raw_ids = r.lrange(COMPLETED_LIST, 0, max_count - 1)
    return [
        tid.decode('utf-8') if isinstance(tid, bytes) else tid
        for tid in raw_ids
    ]


def get_failed_task_ids(max_count: int = 1000) -> List[str]:
    """获取 failed 列表中的任务 ID"""
    r = get_redis()
    raw_ids = r.lrange(FAILED_LIST, 0, max_count - 1)
    return [
        tid.decode('utf-8') if isinstance(tid, bytes) else tid
        for tid in raw_ids
    ]


def is_task_processed(task_id: str) -> bool:
    """检查 docking 任务是否已被结果处理器处理过"""
    r = get_redis()
    return bool(r.sismember(DOCKING_PROCESSED_SET, task_id))


def mark_task_processed(task_id: str) -> None:
    """标记 docking 任务已处理"""
    r = get_redis()
    r.sadd(DOCKING_PROCESSED_SET, task_id)


def wait_for_task_completion(task_id: str, timeout: int = 3600, poll_interval: int = 10) -> str:
    """
    轮询等待任务完成（用于手动触发的 DAG）

    Returns:
        任务最终状态: 'success', 'completed', 'failed', 'timeout'
    """
    import time
    r = get_redis()
    task_key = f"{TASK_KEY_PREFIX}:{task_id}"
    start_time = time.time()

    while time.time() - start_time < timeout:
        raw_status = r.hget(task_key, 'status')
        if raw_status:
            status = raw_status.decode('utf-8') if isinstance(raw_status, bytes) else raw_status
            if status in ('success', 'completed', 'failed'):
                return status
        time.sleep(poll_interval)

    return 'timeout'
