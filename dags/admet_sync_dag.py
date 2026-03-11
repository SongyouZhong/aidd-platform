"""
ADMET 自动同步 DAG

替代: AdmetSyncChecker
调度: @daily（每天凌晨 2:00）
重试: 2 次，间隔 10 分钟

流程:
  [scan_missing_compounds] → [split_batches] → [submit_batch_tasks]

功能:
  1. 扫描 project_compounds LEFT JOIN admet_compute_result，找出缺少 ADMET 结果的化合物
  2. 按批次分组
  3. 提交到 Redis 队列（保持与 aidd-toolkit Worker 兼容）
"""

import json
import uuid
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='admet_sync',
    default_args=default_args,
    description='扫描缺少 ADMET 结果的化合物，自动提交 QikProp 计算任务',
    schedule='0 2 * * *',  # 每天凌晨2点
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['admet', 'sync', 'batch'],
) as dag:

    def scan_missing_compounds(**context):
        """
        查询 project_compounds LEFT JOIN admet_compute_result，
        找出缺少 ADMET 结果的化合物。
        逻辑复用自 AdmetSyncChecker._query_missing_compounds()
        """
        from common.db_utils import get_sync_connection

        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            sql = """
            SELECT pc.id, pc.smiles, pc.name, pc.project_id
            FROM project_compounds pc
            LEFT JOIN admet_compute_result acr ON pc.id = acr.id
            WHERE pc.deleted = false
              AND acr.id IS NULL
              AND pc.smiles IS NOT NULL
              AND pc.smiles != ''
            """
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()

            missing = [
                {"id": str(row[0]), "smiles": row[1], "name": row[2], "project_id": str(row[3])}
                for row in rows
            ]
            logger.info(f"ADMET 同步扫描: 发现 {len(missing)} 个化合物缺少 ADMET 结果")
        finally:
            conn.close()

        # 通过 XCom 传递结果（大数据量场景下考虑写临时表或 MinIO，此处直接传）
        context['ti'].xcom_push(key='missing_compounds', value=missing)
        return len(missing)

    def split_batches(**context):
        """将缺失化合物按批次分组"""
        from common.config import ADMET_BATCH_SIZE

        missing = context['ti'].xcom_pull(
            task_ids='scan_missing_compounds', key='missing_compounds'
        )

        if not missing:
            logger.info("无缺失化合物，跳过分批")
            context['ti'].xcom_push(key='batches', value=[])
            return 0

        batches = [
            missing[i:i + ADMET_BATCH_SIZE]
            for i in range(0, len(missing), ADMET_BATCH_SIZE)
        ]

        logger.info(f"将 {len(missing)} 个化合物分为 {len(batches)} 个批次")
        context['ti'].xcom_push(key='batches', value=batches)
        return len(batches)

    def submit_batch_tasks(**context):
        """
        逐批提交 ADMET 计算任务到 Redis 和 PostgreSQL
        保持与原 AdmetSyncChecker._submit_admet_task 相同的数据结构
        """
        from common.redis_utils import push_task_to_redis
        from common.db_utils import get_sync_connection
        from common.config import ADMET_PRIORITY

        batches = context['ti'].xcom_pull(
            task_ids='split_batches', key='batches'
        )

        if not batches:
            logger.info("无批次需要提交")
            return 0

        now = datetime.now()
        date_str = now.strftime('%Y%m%d')
        task_ids = []

        for batch_idx, batch in enumerate(batches):
            task_id = str(uuid.uuid4())
            smiles_list = [c["smiles"] for c in batch]

            input_params = {
                "smiles": smiles_list,
                "compounds": batch,
                "source": "admet_sync",
                "batch_index": batch_idx,
            }

            # 1. 推送到 Redis 队列（Worker 从此消费）
            push_task_to_redis(
                task_id=task_id,
                service="admet",
                task_type="qikprop",
                priority=ADMET_PRIORITY,
                input_params=input_params,
                name=f"admet_sync_{date_str}_batch_{batch_idx}",
                resource_cpu_cores=1,
                resource_memory_gb=2.0,
                resource_gpu_count=0,
                timeout_seconds=3600,
            )

            # 2. 保存到 PostgreSQL tasks 表（与原行为一致）
            _save_task_to_db(
                task_id=task_id,
                service="admet",
                task_type="qikprop",
                name=f"admet_sync_{date_str}_batch_{batch_idx}",
                priority=ADMET_PRIORITY,
                input_params=input_params,
                cpu_cores=1,
                memory_gb=2.0,
            )

            task_ids.append(task_id)
            logger.info(
                f"已提交 ADMET 同步任务 {batch_idx + 1}/{len(batches)}: "
                f"task_id={task_id}, 化合物数量={len(batch)}"
            )

        context['ti'].xcom_push(key='task_ids', value=task_ids)
        logger.info(f"ADMET 同步完成: 共提交 {len(task_ids)} 个任务")
        return len(task_ids)

    # =========================================================================
    # 辅助函数
    # =========================================================================

    def _save_task_to_db(
        task_id: str,
        service: str,
        task_type: str,
        name: str,
        priority: int,
        input_params: dict,
        cpu_cores: int = 1,
        memory_gb: float = 1.0,
        gpu_count: int = 0,
        gpu_memory_gb: float = 0.0,
        timeout_seconds: int = 3600,
        max_retries: int = 3,
        input_files: list = None,
    ) -> None:
        """将任务保存到 PostgreSQL tasks 表（与原 AdmetSyncChecker._save_task_to_db 一致）"""
        from common.db_utils import get_sync_connection

        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            sql = """
            INSERT INTO tasks (
                id, service, task_type, name, status, priority,
                input_params, input_files, max_retries, timeout_seconds,
                resource_cpu_cores, resource_memory_gb, resource_gpu_count, resource_gpu_memory_gb,
                created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cur.execute(sql, (
                task_id, service, task_type, name,
                'pending', priority,
                json.dumps(input_params),
                json.dumps(input_files or []),
                max_retries, timeout_seconds,
                cpu_cores, memory_gb, gpu_count, gpu_memory_gb,
                datetime.now()
            ))
            conn.commit()
            cur.close()
            logger.debug(f"任务已保存到数据库: {task_id}")
        except Exception as e:
            logger.warning(f"任务保存到数据库失败（不影响队列分发）: {e}")
        finally:
            conn.close()

    # =========================================================================
    # DAG 任务编排
    # =========================================================================

    t_scan = PythonOperator(
        task_id='scan_missing_compounds',
        python_callable=scan_missing_compounds,
    )
    t_split = PythonOperator(
        task_id='split_batches',
        python_callable=split_batches,
    )
    t_submit = PythonOperator(
        task_id='submit_batch_tasks',
        python_callable=submit_batch_tasks,
    )

    t_scan >> t_split >> t_submit
