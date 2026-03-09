"""
Worker 心跳检查 DAG

替代: HeartbeatChecker
调度: */1 * * * *（每分钟）
重试: 0 次（心跳检查不需要重试）

流程:
  [check_heartbeats]

功能:
  1. 从 PostgreSQL workers 表检查 Worker 最后心跳时间
  2. 标记超时的节点为 OFFLINE
  3. Worker 继续向 PostgreSQL 写心跳，此 DAG 仅做超时检测
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 0,
}

with DAG(
    dag_id='worker_heartbeat_check',
    default_args=default_args,
    description='检查 Worker 心跳，标记超时节点为 offline',
    schedule_interval='*/1 * * * *',  # 每分钟
    start_date=days_ago(1),
    catchup=False,
    tags=['worker', 'heartbeat', 'monitor'],
) as dag:

    def check_heartbeats(**context):
        """
        检查 Worker 心跳，标记超时节点为 offline
        逻辑复用自 HeartbeatChecker._check_db_heartbeats
        """
        from common.db_utils import get_sync_connection
        from common.config import HEARTBEAT_TIMEOUT

        timeout_threshold = datetime.now() - timedelta(seconds=HEARTBEAT_TIMEOUT)

        conn = get_sync_connection()
        try:
            cur = conn.cursor()

            # 查找并更新心跳超时的 Worker
            sql = """
            UPDATE workers
            SET status = 'offline', updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('online', 'busy')
              AND last_heartbeat IS NOT NULL
              AND last_heartbeat < %s
            RETURNING id, hostname
            """
            cur.execute(sql, (timeout_threshold,))
            updated_workers = cur.fetchall()

            if updated_workers:
                conn.commit()
                for worker_id, hostname in updated_workers:
                    logger.warning(f"Worker 心跳超时，标记为 offline: {worker_id} ({hostname})")
                logger.info(f"本轮标记 {len(updated_workers)} 个 Worker 为 offline")
            else:
                logger.debug("所有 Worker 心跳正常")

            cur.close()
            return len(updated_workers) if updated_workers else 0

        except Exception as e:
            logger.error(f"检查 Worker 心跳失败: {e}")
            return -1
        finally:
            conn.close()

    # =========================================================================
    # DAG 任务
    # =========================================================================

    PythonOperator(
        task_id='check_heartbeats',
        python_callable=check_heartbeats,
    )
