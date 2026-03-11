"""
Redis 任务完成 Sensor

轮询 Redis 等待指定任务完成，用于手动触发的计算流水线 DAG。
"""

import logging
import time
from typing import Optional

from airflow.sensors.base import BaseSensorOperator

logger = logging.getLogger(__name__)


class RedisTaskSensor(BaseSensorOperator):
    """
    等待 Redis 中任务完成的 Sensor

    监听 aidd:tasks:{task_id} 的 status 字段，
    当 status 变为 success/completed/failed 时触发。

    参数:
        task_id_xcom_task: 从哪个 task 的 XCom 获取 task_id
        task_id_xcom_key: XCom key（默认 'return_value'）
        allowed_statuses: 视为成功的状态列表
        failed_statuses: 视为失败的状态列表
    """

    template_fields = ['task_id_xcom_task', 'task_id_xcom_key']

    def __init__(
        self,
        task_id_xcom_task: str,
        task_id_xcom_key: str = 'return_value',
        allowed_statuses: Optional[list] = None,
        failed_statuses: Optional[list] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.task_id_xcom_task = task_id_xcom_task
        self.task_id_xcom_key = task_id_xcom_key
        self.allowed_statuses = allowed_statuses or ['success', 'completed']
        self.failed_statuses = failed_statuses or ['failed']
        self._task_id = None

    def poke(self, context):
        """检查任务是否完成"""
        from common.redis_utils import get_task_data

        # 首次调用时从 XCom 获取 task_id
        if self._task_id is None:
            self._task_id = context['ti'].xcom_pull(
                task_ids=self.task_id_xcom_task,
                key=self.task_id_xcom_key,
            )
            if not self._task_id:
                raise ValueError(
                    f"无法从 XCom 获取 task_id "
                    f"(task={self.task_id_xcom_task}, key={self.task_id_xcom_key})"
                )
            logger.info(f"开始监听任务状态: task_id={self._task_id}")

        task_data = get_task_data(self._task_id)
        if not task_data:
            logger.warning(f"任务 {self._task_id} 在 Redis 中不存在")
            return False

        status = task_data.get('status', '')

        if status in self.failed_statuses:
            error_msg = task_data.get('error_message', '未知错误')
            raise RuntimeError(f"任务 {self._task_id} 失败: {error_msg}")

        if status in self.allowed_statuses:
            logger.info(f"任务 {self._task_id} 已完成，状态: {status}")
            return True

        logger.debug(f"任务 {self._task_id} 当前状态: {status}，继续等待...")
        return False
