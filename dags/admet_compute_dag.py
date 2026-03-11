"""
ADMET 单次计算流水线 DAG

用途: 用户手动触发（通过 Airflow UI "Trigger DAG w/ config"）
调度: 无自动调度（schedule_interval=None）
参数: 通过 dag_run.conf 传入

流程:
  [validate_and_submit] → [wait_for_completion]

dag_run.conf 示例:
{
    "smiles": ["CCO", "c1ccccc1"],
    "project_id": "xxx",
    "timeout": 3600
}
"""

import json
import uuid
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='admet_compute',
    default_args=default_args,
    description='手动触发 ADMET (QikProp) 计算流水线',
    schedule=None,  # 仅手动触发
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['admet', 'compute', 'manual'],
    params={
        'smiles': Param(type='array', description='SMILES 列表'),
        'project_id': Param(type='string', default='', description='项目 ID（可选）'),
        'priority': Param(type='integer', default=2, description='任务优先级 (0=CRITICAL, 1=HIGH, 2=NORMAL, 3=LOW, 4=BATCH)'),
        'timeout': Param(type='integer', default=3600, description='超时时间（秒）'),
    },
    render_template_as_native_obj=True,
) as dag:

    def validate_and_submit(**context):
        """验证参数并提交任务到 Redis"""
        from common.redis_utils import push_task_to_redis
        from common.db_utils import get_sync_connection

        params = context['params']
        smiles_list = params.get('smiles', [])
        project_id = params.get('project_id', '')
        priority = params.get('priority', 2)
        timeout = params.get('timeout', 3600)

        if not smiles_list:
            raise ValueError("smiles 参数不能为空")

        task_id = str(uuid.uuid4())
        now = datetime.now()
        date_str = now.strftime('%Y%m%d')

        input_params = {
            "smiles": smiles_list,
            "source": "manual_airflow",
            "project_id": project_id,
        }

        # 推送到 Redis 队列
        push_task_to_redis(
            task_id=task_id,
            service="admet",
            task_type="qikprop",
            priority=priority,
            input_params=input_params,
            name=f"admet_manual_{date_str}_{task_id[:8]}",
            resource_cpu_cores=1,
            resource_memory_gb=2.0,
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
                    task_id, "admet", "qikprop",
                    f"admet_manual_{date_str}_{task_id[:8]}",
                    'pending', priority,
                    json.dumps(input_params), json.dumps([]),
                    3, timeout,
                    1, 2.0, 0, 0.0,
                    now
                )
            )
            conn.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"任务保存到数据库失败: {e}")
        finally:
            conn.close()

        logger.info(f"ADMET 手动任务已提交: task_id={task_id}, smiles_count={len(smiles_list)}")
        context['ti'].xcom_push(key='task_id', value=task_id)
        context['ti'].xcom_push(key='timeout', value=timeout)
        return task_id

    def wait_for_completion(**context):
        """轮询 Redis 等待任务完成"""
        from common.redis_utils import wait_for_task_completion

        task_id = context['ti'].xcom_pull(task_ids='validate_and_submit', key='task_id')
        timeout = context['ti'].xcom_pull(task_ids='validate_and_submit', key='timeout')
        timeout = int(timeout) if timeout else 3600

        logger.info(f"等待 ADMET 任务完成: task_id={task_id}, timeout={timeout}s")

        status = wait_for_task_completion(task_id, timeout=timeout, poll_interval=10)

        if status == 'timeout':
            raise TimeoutError(f"任务 {task_id} 超时 ({timeout}s)")
        elif status == 'failed':
            raise RuntimeError(f"任务 {task_id} 执行失败")

        logger.info(f"ADMET 任务完成: task_id={task_id}, status={status}")
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
        execution_timeout=timedelta(hours=2),
    )

    t_submit >> t_wait
