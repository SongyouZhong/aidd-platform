"""
Docking 单次计算流水线 DAG

用途: 用户手动触发（通过 Airflow UI "Trigger DAG w/ config"）
调度: 无自动调度（schedule_interval=None）
参数: 通过 dag_run.conf 传入

流程:
  [validate_and_submit] → [wait_for_completion] → [trigger_result_process]

dag_run.conf 示例:
{
    "input_file": "minio://tasks/input/xxx.csv",
    "grid_file": "minio://receptors/xxx.zip",
    "receptor_id": "uuid",
    "project_id": "uuid",
    "target": "target_name",
    "precision": "SP",
    "n_poses": 3,
    "timeout": 7200
}
"""

import json
import uuid
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='docking_compute',
    default_args=default_args,
    description='手动触发 Docking (Glide) 计算流水线',
    schedule=None,  # 仅手动触发
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['docking', 'compute', 'manual'],
    params={
        'input_file': Param(type='string', description='输入文件路径 (minio://...)'),
        'grid_file': Param(type='string', description='Grid 文件路径 (minio://...)'),
        'receptor_id': Param(type='string', description='受体 ID'),
        'project_id': Param(type='string', description='项目 ID'),
        'target': Param(type='string', default='', description='靶点名称（可选）'),
        'precision': Param(type='string', default='SP', description='对接精度: SP/XP/HTVS'),
        'n_poses': Param(type='integer', default=3, description='每个分子保留的 pose 数'),
        'priority': Param(type='integer', default=2, description='任务优先级'),
        'timeout': Param(type='integer', default=7200, description='超时时间（秒）'),
    },
    render_template_as_native_obj=True,
) as dag:

    def validate_and_submit(**context):
        """验证参数并提交 Docking 任务到 Redis"""
        from common.redis_utils import push_task_to_redis
        from common.db_utils import get_sync_connection

        params = context['params']
        input_file = params.get('input_file', '')
        grid_file = params.get('grid_file', '')
        receptor_id = params.get('receptor_id', '')
        project_id = params.get('project_id', '')
        target = params.get('target', '')
        precision = params.get('precision', 'SP')
        n_poses = params.get('n_poses', 3)
        priority = params.get('priority', 2)
        timeout = params.get('timeout', 7200)

        # 参数校验
        if not input_file:
            raise ValueError("input_file 参数不能为空")
        if not grid_file:
            raise ValueError("grid_file 参数不能为空")
        if not receptor_id:
            raise ValueError("receptor_id 参数不能为空")
        if not project_id:
            raise ValueError("project_id 参数不能为空")

        task_id = str(uuid.uuid4())
        now = datetime.now()
        date_str = now.strftime('%Y%m%d')

        input_params = {
            "input_file": input_file,
            "grid_file": grid_file,
            "receptor_id": receptor_id,
            "project_id": project_id,
            "target": target,
            "precision": precision,
            "n_poses": n_poses,
            "source": "manual_airflow",
        }

        # 推送到 Redis 队列
        push_task_to_redis(
            task_id=task_id,
            service="docking",
            task_type="glide",
            priority=priority,
            input_params=input_params,
            name=f"docking_manual_{date_str}_{task_id[:8]}",
            input_files=[input_file, grid_file],
            resource_cpu_cores=4,
            resource_memory_gb=8.0,
            resource_gpu_count=0,
            timeout_seconds=timeout,
        )

        # 保存到 PostgreSQL tasks 表
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
                    task_id, "docking", "glide",
                    f"docking_manual_{date_str}_{task_id[:8]}",
                    'pending', priority,
                    json.dumps(input_params),
                    json.dumps([input_file, grid_file]),
                    3, timeout,
                    4, 8.0, 0, 0.0,
                    now
                )
            )
            conn.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"任务保存到数据库失败: {e}")
        finally:
            conn.close()

        logger.info(f"Docking 手动任务已提交: task_id={task_id}")
        context['ti'].xcom_push(key='task_id', value=task_id)
        context['ti'].xcom_push(key='timeout', value=timeout)
        return task_id

    def wait_for_completion(**context):
        """轮询 Redis 等待 Docking 任务完成"""
        from common.redis_utils import wait_for_task_completion

        task_id = context['ti'].xcom_pull(task_ids='validate_and_submit', key='task_id')
        timeout = context['ti'].xcom_pull(task_ids='validate_and_submit', key='timeout')
        timeout = int(timeout) if timeout else 7200

        logger.info(f"等待 Docking 任务完成: task_id={task_id}, timeout={timeout}s")

        status = wait_for_task_completion(task_id, timeout=timeout, poll_interval=15)

        if status == 'timeout':
            raise TimeoutError(f"任务 {task_id} 超时 ({timeout}s)")
        elif status == 'failed':
            raise RuntimeError(f"任务 {task_id} 执行失败")

        logger.info(f"Docking 任务完成: task_id={task_id}, status={status}")
        return status

    # =========================================================================
    # DAG 任务编排
    # =========================================================================

    t_submit = PythonOperator(
        task_id='validate_and_submit',
        python_callable=validate_and_submit,
    )
    t_wait = PythonOperator(
        task_id='wait_for_completion',
        python_callable=wait_for_completion,
        execution_timeout=timedelta(hours=4),
    )
    # 完成后自动触发 docking_result_process DAG 立即处理结果
    t_trigger_result = TriggerDagRunOperator(
        task_id='trigger_result_process',
        trigger_dag_id='docking_result_process',
        wait_for_completion=False,
    )

    t_submit >> t_wait >> t_trigger_result
