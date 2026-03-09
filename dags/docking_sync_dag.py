"""
Docking 自动同步 DAG

替代: DockingSyncChecker
调度: 0 */1 * * *（每小时）
重试: 2 次，间隔 5 分钟

流程:
  [scan_and_submit]
    → 查询活跃受体
    → 对每个受体查找缺失 docking 结果的化合物
    → 预插入 pending 记录（防止重复提交）
    → 按批次生成 CSV 并上传 MinIO
    → 提交 docking 任务到 Redis 队列

功能复用自 DockingSyncChecker 的全部逻辑，
保持 Redis 数据结构与 aidd-toolkit Worker 兼容。
"""

import csv
import io
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='docking_sync',
    default_args=default_args,
    description='扫描缺少 docking 结果的化合物，自动提交 Glide docking 任务',
    schedule_interval='0 */1 * * *',  # 每小时
    start_date=days_ago(1),
    catchup=False,
    tags=['docking', 'sync', 'batch'],
) as dag:

    def scan_and_submit(**context):
        """
        核心扫描逻辑（复用自 DockingSyncChecker._scan_and_submit）：
        1. 查询所有活跃的受体文件
        2. 对每个受体，找出缺少 docking 结果的化合物 SMILES
        3. 预插入 pending 记录
        4. 按批次生成 CSV、上传 MinIO、提交 docking 任务
        """
        from common.db_utils import get_sync_connection
        from common.redis_utils import push_task_to_redis
        from common.minio_utils import upload_csv_string
        from common.config import DOCKING_BATCH_SIZE, DOCKING_PRIORITY

        # Step 1: 获取所有活跃受体
        active_receptors = _query_active_receptors()
        if not active_receptors:
            logger.info("Docking 同步扫描完成: 无活跃受体文件，跳过")
            return 0

        logger.info(f"发现 {len(active_receptors)} 个活跃受体文件")

        total_submitted = 0
        all_task_ids = []

        for receptor in active_receptors:
            receptor_id = receptor["id"]
            project_id = receptor["project_id"]
            minio_path = receptor["minio_path"]
            receptor_name = receptor["name"]

            # Step 2: 查询该受体缺失 docking 结果的化合物
            missing_smiles = _query_missing_smiles(project_id, receptor_id)
            if not missing_smiles:
                logger.debug(f"受体 {receptor_name}({receptor_id}): 所有化合物均已有 docking 结果")
                continue

            logger.info(f"受体 {receptor_name}({receptor_id}): 发现 {len(missing_smiles)} 个缺失")

            # Step 3: 预插入 pending 记录
            _insert_pending_records(project_id, receptor_id, missing_smiles)

            # Step 4: 按批次提交任务
            batches = [
                missing_smiles[i:i + DOCKING_BATCH_SIZE]
                for i in range(0, len(missing_smiles), DOCKING_BATCH_SIZE)
            ]

            for batch_idx, batch in enumerate(batches):
                try:
                    now = datetime.now()
                    task_id = str(uuid.uuid4())
                    date_str = now.strftime("%Y%m%d")

                    # 生成 CSV 并上传 MinIO
                    input_file = _generate_csv_and_upload(batch, task_id, date_str)

                    # 构建 grid_file 路径
                    grid_file = (
                        minio_path if minio_path.startswith("minio://")
                        else f"minio://{minio_path}"
                    )

                    input_params = {
                        "input_file": input_file,
                        "grid_file": grid_file,
                        "receptor_id": receptor_id,
                        "project_id": project_id,
                        "source": "docking_sync",
                        "batch_index": batch_idx,
                    }

                    # 推送到 Redis 队列
                    push_task_to_redis(
                        task_id=task_id,
                        service="docking",
                        task_type="glide",
                        priority=DOCKING_PRIORITY,
                        input_params=input_params,
                        name=f"docking_sync_{date_str}_receptor_{receptor_id[:8]}_batch_{batch_idx}",
                        input_files=[input_file, grid_file],
                        resource_cpu_cores=4,
                        resource_memory_gb=8.0,
                        resource_gpu_count=0,
                        timeout_seconds=7200,
                    )

                    # 保存到 PostgreSQL tasks 表
                    _save_task_to_db(
                        task_id=task_id,
                        service="docking",
                        task_type="glide",
                        name=f"docking_sync_{date_str}_receptor_{receptor_id[:8]}_batch_{batch_idx}",
                        priority=DOCKING_PRIORITY,
                        input_params=input_params,
                        input_files=[input_file, grid_file],
                        cpu_cores=4,
                        memory_gb=8.0,
                        timeout_seconds=7200,
                    )

                    # 更新 pending 记录中的 task_id
                    _update_pending_task_id(receptor_id, batch, task_id)

                    total_submitted += 1
                    all_task_ids.append(task_id)
                    logger.info(
                        f"已提交 Docking 任务 {batch_idx + 1}/{len(batches)}: "
                        f"task_id={task_id}, 受体={receptor_name}, SMILES={len(batch)}"
                    )
                except Exception as e:
                    logger.error(
                        f"提交 Docking 任务失败 (受体={receptor_name}, batch={batch_idx}): {e}"
                    )
                    # 回滚孤儿 pending 记录
                    _rollback_pending_records(receptor_id, batch)

        context['ti'].xcom_push(key='task_ids', value=all_task_ids)
        logger.info(f"Docking 同步扫描完成: 共提交 {total_submitted} 个任务")
        return total_submitted

    # =========================================================================
    # 数据库查询辅助函数（复用自 DockingSyncChecker）
    # =========================================================================

    def _query_active_receptors() -> List[Dict]:
        """查询所有活跃的受体文件"""
        from common.db_utils import get_sync_connection
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, project_id, name, minio_path
                FROM project_receptors
                WHERE is_active = true
            """)
            rows = cur.fetchall()
            cur.close()
            return [
                {"id": str(row[0]), "project_id": str(row[1]), "name": row[2], "minio_path": row[3]}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"查询活跃受体文件失败: {e}")
            return []
        finally:
            conn.close()

    def _query_missing_smiles(project_id: str, receptor_id: str) -> List[str]:
        """查询指定项目+受体组合中缺少 docking 结果的化合物 SMILES"""
        from common.db_utils import get_sync_connection
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            sql = """
            SELECT DISTINCT pc.smiles
            FROM project_compounds pc
            LEFT JOIN docking_result dr
                ON pc.smiles = dr.smiles
                AND dr.receptor_id = %s
                AND dr.status IN ('pending', 'computing', 'success')
            WHERE pc.project_id = %s
              AND pc.deleted = false
              AND dr.smiles IS NULL
              AND pc.smiles IS NOT NULL
              AND pc.smiles != ''
            """
            cur.execute(sql, (receptor_id, project_id))
            rows = cur.fetchall()
            cur.close()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"查询缺失 docking 结果失败 (project={project_id}, receptor={receptor_id}): {e}")
            return []
        finally:
            conn.close()

    def _insert_pending_records(project_id: str, receptor_id: str, smiles_list: List[str]) -> None:
        """预插入 status='pending' 的 docking_result 记录（ON CONFLICT DO NOTHING）"""
        from common.db_utils import get_sync_connection
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            sql = """
            INSERT INTO docking_result (id, project_id, receptor_id, smiles, status, created_at)
            VALUES (gen_random_uuid(), %s, %s, %s, 'pending', NOW())
            ON CONFLICT (receptor_id, smiles) DO NOTHING
            """
            for smiles in smiles_list:
                cur.execute(sql, (project_id, receptor_id, smiles))
            conn.commit()
            cur.close()
            logger.debug(f"已预插入 {len(smiles_list)} 条 pending docking_result 记录")
        except Exception as e:
            logger.error(f"预插入 pending 记录失败: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _update_pending_task_id(receptor_id: str, smiles_list: List[str], task_id: str) -> None:
        """更新 pending 记录的 task_id"""
        from common.db_utils import get_sync_connection
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            placeholders = ",".join(["%s"] * len(smiles_list))
            sql = f"""
            UPDATE docking_result
            SET task_id = %s, status = 'pending', updated_at = NOW()
            WHERE receptor_id = %s
              AND smiles IN ({placeholders})
              AND status = 'pending'
            """
            cur.execute(sql, [task_id, receptor_id] + smiles_list)
            conn.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"更新 pending 记录的 task_id 失败: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _rollback_pending_records(receptor_id: str, smiles_list: List[str]) -> None:
        """回滚孤儿 pending 记录"""
        from common.db_utils import get_sync_connection
        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            placeholders = ",".join(["%s"] * len(smiles_list))
            sql = f"""
            DELETE FROM docking_result
            WHERE receptor_id = %s
              AND smiles IN ({placeholders})
              AND status = 'pending'
              AND task_id IS NULL
            """
            cur.execute(sql, [receptor_id] + smiles_list)
            deleted = cur.rowcount
            conn.commit()
            cur.close()
            logger.info(f"已回滚 {deleted} 条孤儿 pending 记录 (receptor={receptor_id})")
        except Exception as e:
            logger.warning(f"回滚 pending 记录失败: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _generate_csv_and_upload(smiles_list: List[str], task_id: str, date_str: str) -> str:
        """将 SMILES 列表生成 CSV 并上传到 MinIO"""
        from common.minio_utils import upload_csv_string

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["smiles", "title"])
        for idx, smi in enumerate(smiles_list):
            writer.writerow([smi, f"mol_{idx + 1}"])

        object_name = f"tasks/input/docking_sync/{date_str}/{task_id}_input.csv"
        return upload_csv_string(buf.getvalue(), object_name)

    def _save_task_to_db(
        task_id: str, service: str, task_type: str, name: str,
        priority: int, input_params: dict, input_files: list = None,
        cpu_cores: int = 4, memory_gb: float = 8.0,
        gpu_count: int = 0, gpu_memory_gb: float = 0.0,
        timeout_seconds: int = 7200, max_retries: int = 3,
    ) -> None:
        """将任务保存到 PostgreSQL tasks 表"""
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
        except Exception as e:
            logger.warning(f"任务保存到数据库失败（不影响队列分发）: {e}")
        finally:
            conn.close()

    # =========================================================================
    # DAG 任务编排
    # =========================================================================

    PythonOperator(
        task_id='scan_and_submit',
        python_callable=scan_and_submit,
    )
