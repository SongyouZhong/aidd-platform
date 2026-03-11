"""
Redis 任务提交 Operator

封装将任务推送到 Redis 和 PostgreSQL 的公共逻辑，
可在多个 DAG 中复用。
"""

import json
import uuid
import logging
from datetime import datetime
from typing import List, Optional

from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class RedisTaskOperator(BaseOperator):
    """
    将计算任务提交到 Redis 队列的 Operator

    同时保存到 PostgreSQL tasks 表，保持与 aidd-platform API 提交任务一致的行为。

    参数:
        service: 服务类型 ('admet' / 'docking')
        task_type: 任务类型 ('qikprop' / 'glide')
        input_params: 任务输入参数
        priority: 优先级 (0-4)
        input_files: 输入文件列表
        cpu_cores: CPU 核数
        memory_gb: 内存 GB
        gpu_count: GPU 数量
        timeout_seconds: 超时时间
        task_name: 任务名称（可选）
    """

    template_fields = ['input_params', 'input_files', 'task_name']

    def __init__(
        self,
        service: str,
        task_type: str,
        input_params: dict,
        priority: int = 2,
        input_files: Optional[List[str]] = None,
        cpu_cores: int = 1,
        memory_gb: float = 1.0,
        gpu_count: int = 0,
        gpu_memory_gb: float = 0.0,
        timeout_seconds: int = 3600,
        max_retries: int = 3,
        task_name: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.service = service
        self.task_type = task_type
        self.input_params = input_params
        self.priority = priority
        self.input_files = input_files or []
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.gpu_count = gpu_count
        self.gpu_memory_gb = gpu_memory_gb
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.task_name = task_name

    def execute(self, context):
        from common.redis_utils import push_task_to_redis
        from common.db_utils import get_sync_connection

        task_id = str(uuid.uuid4())
        now = datetime.now()
        date_str = now.strftime('%Y%m%d')
        name = self.task_name or f"{self.service}_{self.task_type}_{date_str}_{task_id[:8]}"

        # 1. 推送到 Redis 队列
        push_task_to_redis(
            task_id=task_id,
            service=self.service,
            task_type=self.task_type,
            priority=self.priority,
            input_params=self.input_params,
            name=name,
            input_files=self.input_files,
            resource_cpu_cores=self.cpu_cores,
            resource_memory_gb=self.memory_gb,
            resource_gpu_count=self.gpu_count,
            resource_gpu_memory_gb=self.gpu_memory_gb,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
        )

        # 2. 保存到 PostgreSQL
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO tasks (
                    id, service, task_type, name, status, priority,
                    input_params, input_files, max_retries, timeout_seconds,
                    resource_cpu_cores, resource_memory_gb, resource_gpu_count, resource_gpu_memory_gb,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id, self.service, self.task_type, name,
                    'pending', self.priority,
                    json.dumps(self.input_params),
                    json.dumps(self.input_files),
                    self.max_retries, self.timeout_seconds,
                    self.cpu_cores, self.memory_gb, self.gpu_count, self.gpu_memory_gb,
                    now
                )
            )
            conn.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"任务保存到数据库失败（不影响队列分发）: {e}")
        finally:
            conn.close()

        logger.info(f"任务已提交: task_id={task_id}, service={self.service}, type={self.task_type}")
        return task_id
