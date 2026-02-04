"""
基于 Redis 的分布式任务队列

使用 Redis Sorted Set 实现优先级队列
使用 Redis Hash 存储任务详情
使用 Redis List 作为服务专用队列
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import asdict

from app.models import Task, TaskStatus, TaskPriority, ResourceRequirement
from app.mq.redis_client import RedisClient, get_redis_client

logger = logging.getLogger(__name__)


class RedisTaskQueue:
    """
    基于 Redis 的分布式任务队列
    
    数据结构设计:
    - aidd:tasks:{task_id}        Hash    任务详情
    - aidd:queue:pending          ZSet    待处理任务（按优先级排序）
    - aidd:queue:service:{name}   List    服务专用队列（FIFO within priority）
    - aidd:queue:running          Set     运行中任务
    - aidd:queue:completed        List    已完成任务（最近 1000 条）
    - aidd:queue:failed           List    失败任务
    - aidd:stats:*                Hash    统计信息
    """
    
    # Redis Key 前缀
    PREFIX = "aidd"
    TASK_KEY = f"{PREFIX}:tasks"
    PENDING_KEY = f"{PREFIX}:queue:pending"
    SERVICE_QUEUE_KEY = f"{PREFIX}:queue:service"
    RUNNING_KEY = f"{PREFIX}:queue:running"
    COMPLETED_KEY = f"{PREFIX}:queue:completed"
    FAILED_KEY = f"{PREFIX}:queue:failed"
    STATS_KEY = f"{PREFIX}:stats"
    
    # 优先级分数映射（分数越小优先级越高，直接使用枚举值）
    PRIORITY_SCORES = {
        TaskPriority.CRITICAL: 0,
        TaskPriority.HIGH: 100,
        TaskPriority.NORMAL: 200,
        TaskPriority.LOW: 300,
        TaskPriority.BATCH: 400,
    }
    
    def __init__(self, redis_client: Optional[RedisClient] = None):
        self._redis_client = redis_client
        self._max_completed_history = 1000
    
    async def _get_redis(self):
        """获取 Redis 客户端"""
        if self._redis_client is None:
            self._redis_client = await get_redis_client()
        return await self._redis_client.get_client()
    
    # =========================================================================
    # 任务入队
    # =========================================================================
    
    async def push(self, task: Task) -> bool:
        """
        将任务加入队列
        
        Args:
            task: 任务对象
        
        Returns:
            是否成功
        """
        redis = await self._get_redis()
        task_key = f"{self.TASK_KEY}:{task.id}"
        
        # 检查是否已存在
        if await redis.exists(task_key):
            logger.warning(f"Task already exists: {task.id}")
            return False
        
        # 序列化任务
        task_data = self._serialize_task(task)
        
        # 计算优先级分数
        # 使用 优先级基础分 + 时间戳 实现同优先级 FIFO
        priority_score = self.PRIORITY_SCORES.get(task.priority, 200)
        timestamp = (task.created_at or datetime.utcnow()).timestamp()
        score = priority_score + (timestamp / 10000000000)  # 保证时间戳影响较小
        
        # 使用 Pipeline 原子操作
        async with redis.pipeline(transaction=True) as pipe:
            # 1. 存储任务详情
            pipe.hset(task_key, mapping=task_data)
            # 设置过期时间（7天）
            pipe.expire(task_key, 7 * 24 * 3600)
            
            # 2. 加入全局优先级队列
            pipe.zadd(self.PENDING_KEY, {task.id: score})
            
            # 3. 加入服务专用队列
            service_queue = f"{self.SERVICE_QUEUE_KEY}:{task.service}"
            pipe.lpush(service_queue, task.id)
            
            # 4. 更新统计
            pipe.hincrby(self.STATS_KEY, "total_submitted", 1)
            pipe.hincrby(self.STATS_KEY, f"submitted:{task.service}", 1)
            
            await pipe.execute()
        
        logger.debug(f"Task enqueued: {task.id} (service={task.service}, priority={task.priority})")
        return True
    
    async def push_batch(self, tasks: List[Task]) -> int:
        """批量入队"""
        count = 0
        for task in tasks:
            if await self.push(task):
                count += 1
        return count
    
    # =========================================================================
    # 任务出队
    # =========================================================================
    
    async def pop(self, service: Optional[str] = None, timeout: int = 0) -> Optional[Task]:
        """
        从队列中取出任务
        
        Args:
            service: 指定服务类型，如果为 None 则从全局队列取
            timeout: 阻塞等待秒数，0 表示不阻塞
        
        Returns:
            任务对象，如果队列为空则返回 None
        """
        redis = await self._get_redis()
        
        if service:
            return await self._pop_from_service(redis, service, timeout)
        else:
            return await self._pop_from_global(redis, timeout)
    
    async def _pop_from_global(self, redis, timeout: int) -> Optional[Task]:
        """从全局队列取出优先级最高的任务"""
        if timeout > 0:
            # 阻塞等待 - 使用 BZPOPMIN
            result = await redis.bzpopmin(self.PENDING_KEY, timeout)
            if not result:
                return None
            _, task_id, _ = result
        else:
            # 非阻塞 - 使用 ZPOPMIN
            result = await redis.zpopmin(self.PENDING_KEY, count=1)
            if not result:
                return None
            task_id, _ = result[0]
        
        return await self._fetch_and_mark_running(redis, task_id)
    
    async def _pop_from_service(self, redis, service: str, timeout: int) -> Optional[Task]:
        """从服务专用队列取任务"""
        service_queue = f"{self.SERVICE_QUEUE_KEY}:{service}"
        
        if timeout > 0:
            # 阻塞等待
            result = await redis.brpop(service_queue, timeout)
            if not result:
                return None
            _, task_id = result
        else:
            # 非阻塞
            task_id = await redis.rpop(service_queue)
            if not task_id:
                return None
        
        # 从全局队列移除
        await redis.zrem(self.PENDING_KEY, task_id)
        
        return await self._fetch_and_mark_running(redis, task_id)
    
    async def _fetch_and_mark_running(self, redis, task_id: str) -> Optional[Task]:
        """获取任务详情并标记为运行中"""
        task_key = f"{self.TASK_KEY}:{task_id}"
        
        # 获取任务详情
        task_data = await redis.hgetall(task_key)
        if not task_data:
            logger.warning(f"Task data not found: {task_id}")
            return None
        
        # 标记为运行中
        async with redis.pipeline(transaction=True) as pipe:
            pipe.sadd(self.RUNNING_KEY, task_id)
            pipe.hset(task_key, mapping={
                "status": TaskStatus.RUNNING.value,
                "started_at": datetime.utcnow().isoformat()
            })
            pipe.hincrby(self.STATS_KEY, "running", 1)
            await pipe.execute()
        
        task_data["status"] = TaskStatus.RUNNING.value
        task_data["started_at"] = datetime.utcnow().isoformat()
        
        return self._deserialize_task(task_data)
    
    # =========================================================================
    # 任务状态更新
    # =========================================================================
    
    async def complete(
        self,
        task_id: str,
        result: Optional[Dict[str, Any]] = None
    ) -> bool:
        """标记任务完成"""
        redis = await self._get_redis()
        task_key = f"{self.TASK_KEY}:{task_id}"
        
        # 检查任务是否存在且在运行中
        if not await redis.sismember(self.RUNNING_KEY, task_id):
            logger.warning(f"Task not in running state: {task_id}")
            return False
        
        completed_at = datetime.utcnow().isoformat()
        
        async with redis.pipeline(transaction=True) as pipe:
            # 更新任务状态
            updates = {
                "status": TaskStatus.COMPLETED.value,
                "completed_at": completed_at,
            }
            if result:
                updates["result"] = json.dumps(result)
            
            pipe.hset(task_key, mapping=updates)
            
            # 从运行中移除
            pipe.srem(self.RUNNING_KEY, task_id)
            
            # 加入已完成列表
            pipe.lpush(self.COMPLETED_KEY, task_id)
            pipe.ltrim(self.COMPLETED_KEY, 0, self._max_completed_history - 1)
            
            # 更新统计
            pipe.hincrby(self.STATS_KEY, "running", -1)
            pipe.hincrby(self.STATS_KEY, "completed", 1)
            
            await pipe.execute()
        
        logger.info(f"Task completed: {task_id}")
        return True
    
    async def fail(
        self,
        task_id: str,
        error: str,
        retry: bool = True
    ) -> bool:
        """标记任务失败"""
        redis = await self._get_redis()
        task_key = f"{self.TASK_KEY}:{task_id}"
        
        # 获取任务信息
        task_data = await redis.hgetall(task_key)
        if not task_data:
            return False
        
        retry_count = int(task_data.get("retry_count", 0)) + 1
        max_retries = int(task_data.get("max_retries", 3))
        
        async with redis.pipeline(transaction=True) as pipe:
            # 从运行中移除
            pipe.srem(self.RUNNING_KEY, task_id)
            pipe.hincrby(self.STATS_KEY, "running", -1)
            
            if retry and retry_count < max_retries:
                # 重新入队
                pipe.hset(task_key, mapping={
                    "status": TaskStatus.PENDING.value,
                    "retry_count": retry_count,
                    "error_message": error
                })
                
                # 重新加入队列（优先级不变）
                priority_str = task_data.get("priority", "normal")
                priority = TaskPriority(priority_str)
                score = self.PRIORITY_SCORES.get(priority, 200)
                pipe.zadd(self.PENDING_KEY, {task_id: score})
                
                service = task_data.get("service", "default")
                service_queue = f"{self.SERVICE_QUEUE_KEY}:{service}"
                pipe.lpush(service_queue, task_id)
                
                pipe.hincrby(self.STATS_KEY, "retries", 1)
                
                logger.info(f"Task requeued for retry: {task_id} (attempt {retry_count})")
            else:
                # 标记失败
                pipe.hset(task_key, mapping={
                    "status": TaskStatus.FAILED.value,
                    "completed_at": datetime.utcnow().isoformat(),
                    "retry_count": retry_count,
                    "error_message": error
                })
                pipe.lpush(self.FAILED_KEY, task_id)
                pipe.hincrby(self.STATS_KEY, "failed", 1)
                
                logger.error(f"Task failed: {task_id} - {error}")
            
            await pipe.execute()
        
        return True
    
    async def cancel(self, task_id: str, reason: str = "") -> bool:
        """取消任务"""
        redis = await self._get_redis()
        task_key = f"{self.TASK_KEY}:{task_id}"
        
        # 从各队列移除
        async with redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.PENDING_KEY, task_id)
            pipe.srem(self.RUNNING_KEY, task_id)
            
            # 获取服务类型以便从服务队列移除
            task_data = await redis.hgetall(task_key)
            if task_data:
                service = task_data.get("service")
                if service:
                    service_queue = f"{self.SERVICE_QUEUE_KEY}:{service}"
                    pipe.lrem(service_queue, 0, task_id)
                
                pipe.hset(task_key, mapping={
                    "status": TaskStatus.CANCELLED.value,
                    "completed_at": datetime.utcnow().isoformat(),
                    "error_message": reason or "Cancelled"
                })
            
            pipe.hincrby(self.STATS_KEY, "cancelled", 1)
            await pipe.execute()
        
        logger.info(f"Task cancelled: {task_id}")
        return True
    
    # =========================================================================
    # 查询方法
    # =========================================================================
    
    async def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务详情"""
        redis = await self._get_redis()
        task_key = f"{self.TASK_KEY}:{task_id}"
        
        task_data = await redis.hgetall(task_key)
        if not task_data:
            return None
        
        return self._deserialize_task(task_data)
    
    async def get_pending_count(self, service: Optional[str] = None) -> int:
        """获取待处理任务数量"""
        redis = await self._get_redis()
        
        if service:
            service_queue = f"{self.SERVICE_QUEUE_KEY}:{service}"
            return await redis.llen(service_queue)
        else:
            return await redis.zcard(self.PENDING_KEY)
    
    async def get_running_count(self) -> int:
        """获取运行中任务数量"""
        redis = await self._get_redis()
        return await redis.scard(self.RUNNING_KEY)
    
    async def get_running_tasks(self) -> List[str]:
        """获取运行中的任务ID列表"""
        redis = await self._get_redis()
        return list(await redis.smembers(self.RUNNING_KEY))
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        redis = await self._get_redis()
        
        stats = await redis.hgetall(self.STATS_KEY)
        
        return {
            "total_submitted": int(stats.get("total_submitted", 0)),
            "pending": await self.get_pending_count(),
            "running": int(stats.get("running", 0)),
            "completed": int(stats.get("completed", 0)),
            "failed": int(stats.get("failed", 0)),
            "cancelled": int(stats.get("cancelled", 0)),
            "retries": int(stats.get("retries", 0)),
        }
    
    # =========================================================================
    # 序列化/反序列化
    # =========================================================================
    
    def _serialize_task(self, task: Task) -> Dict[str, str]:
        """将 Task 序列化为 Redis Hash（与 aidd-toolkit 保持一致）"""
        data = {
            "id": task.id,
            "service": task.service,
            "task_type": task.task_type or "",
            "name": task.name or "",
            "status": task.status.value if task.status else TaskStatus.PENDING.value,
            "priority": str(task.priority.value) if task.priority else str(TaskPriority.NORMAL.value),
            "input_params": json.dumps(task.input_params) if task.input_params else "{}",
            "input_files": json.dumps(task.input_files) if task.input_files else "[]",
            "output_files": json.dumps(task.output_files) if task.output_files else "[]",
            "retry_count": str(task.retry_count or 0),
            "max_retries": str(task.max_retries or 3),
            "timeout_seconds": str(task.timeout_seconds or 0),
            "job_id": task.job_id or "",
            "worker_id": task.worker_id or "",
            "error_message": task.error_message or "",
            "progress": str(task.progress or 0),
            "created_at": task.created_at.isoformat() if task.created_at else datetime.utcnow().isoformat(),
        }
        
        if task.resources:
            data["resource_cpu_cores"] = str(task.resources.cpu_cores)
            data["resource_memory_gb"] = str(task.resources.memory_gb)
            data["resource_gpu_count"] = str(task.resources.gpu_count)
            data["resource_gpu_memory_gb"] = str(task.resources.gpu_memory_gb)
        
        if task.result:
            data["result"] = json.dumps(task.result)
        
        return data
    
    def _deserialize_task(self, data: Dict[str, str]) -> Task:
        """从 Redis Hash 反序列化为 Task（与 aidd-toolkit 保持一致）"""
        # 解析 priority（可能是数字字符串或枚举名称）
        priority_val = data.get("priority", "2")
        try:
            priority = TaskPriority(int(priority_val))
        except ValueError:
            priority = TaskPriority.NORMAL
        
        task = Task(
            id=data.get("id", ""),
            service=data.get("service", ""),
            task_type=data.get("task_type", ""),
            name=data.get("name") or None,
            status=TaskStatus(data.get("status", "pending")),
            priority=priority,
            input_params=json.loads(data.get("input_params", "{}")),
            input_files=json.loads(data.get("input_files", "[]")),
            output_files=json.loads(data.get("output_files", "[]")),
            retry_count=int(data.get("retry_count", 0)),
            max_retries=int(data.get("max_retries", 3)),
            timeout_seconds=int(data.get("timeout_seconds", 0)) or None,
            progress=float(data.get("progress", 0)),
            job_id=data.get("job_id") or None,
            worker_id=data.get("worker_id") or None,
            error_message=data.get("error_message") or None,
        )
        
        # 时间戳
        if data.get("created_at"):
            task.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("started_at"):
            task.started_at = datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            task.completed_at = datetime.fromisoformat(data["completed_at"])
        
        # 资源需求
        if data.get("resource_cpu_cores"):
            task.resources = ResourceRequirement(
                cpu_cores=int(data.get("resource_cpu_cores", 1)),
                memory_gb=float(data.get("resource_memory_gb", 1.0)),
                gpu_count=int(data.get("resource_gpu_count", 0)),
                gpu_memory_gb=float(data.get("resource_gpu_memory_gb", 0.0)),
            )
        
        # 结果
        if data.get("result"):
            task.result = json.loads(data["result"])
        
        return task
