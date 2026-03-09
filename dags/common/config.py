"""
Airflow DAG 共享配置
从 Airflow Variables 或环境变量读取，保持与原 aidd-platform 配置一致
"""

import os
from airflow.models import Variable


def _get_var(key: str, default: str) -> str:
    """优先从 Airflow Variable 获取，回退到环境变量，再回退到默认值"""
    try:
        return Variable.get(key, default_var=os.environ.get(key, default))
    except Exception:
        return os.environ.get(key, default)


# =============================================================================
# 数据库连接（与原 aidd-platform config.py 保持一致）
# =============================================================================
POSTGRES_HOST = _get_var('POSTGRES_HOST', '10.18.85.10')
POSTGRES_PORT = int(_get_var('POSTGRES_PORT', '30684'))
POSTGRES_USER = _get_var('POSTGRES_USER', 'appuser')
POSTGRES_PASSWORD = _get_var('POSTGRES_PASSWORD', 'strongpassword')
POSTGRES_DB = _get_var('POSTGRES_DB', 'aichemol')

# =============================================================================
# Redis（业务数据使用 db0，Airflow Celery Broker 使用 db1 避免冲突）
# =============================================================================
REDIS_HOST = _get_var('REDIS_HOST', '10.18.85.10')
REDIS_PORT = int(_get_var('REDIS_PORT', '6379'))
REDIS_PASSWORD = _get_var('REDIS_PASSWORD', '')
REDIS_DB = int(_get_var('REDIS_DB', '0'))

# =============================================================================
# MinIO 对象存储
# =============================================================================
MINIO_ENDPOINT = _get_var('MINIO_ENDPOINT', '172.19.80.100:9090')
MINIO_ACCESS_KEY = _get_var('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = _get_var('MINIO_SECRET_KEY', 'minio_test_password_2025')
MINIO_BUCKET = _get_var('MINIO_BUCKET', 'aidd-files')
MINIO_SECURE = _get_var('MINIO_SECURE', 'false').lower() == 'true'

# =============================================================================
# 业务参数（与原 config.yml 中的 admet_sync / docking_sync 配置对齐）
# =============================================================================
ADMET_BATCH_SIZE = int(_get_var('ADMET_SYNC_BATCH_SIZE', '50'))
ADMET_PRIORITY = int(_get_var('ADMET_SYNC_PRIORITY', '4'))   # BATCH 优先级

DOCKING_BATCH_SIZE = int(_get_var('DOCKING_SYNC_BATCH_SIZE', '10'))
DOCKING_PRIORITY = int(_get_var('DOCKING_SYNC_PRIORITY', '3'))  # LOW 优先级

# Worker 心跳超时（秒）
HEARTBEAT_TIMEOUT = int(_get_var('HEARTBEAT_TIMEOUT', '30'))

# Redis Key 前缀（与原 RedisTaskQueue 保持一致）
REDIS_PREFIX = "aidd"
TASK_KEY_PREFIX = f"{REDIS_PREFIX}:tasks"
PENDING_QUEUE = f"{REDIS_PREFIX}:queue:pending"
RUNNING_SET = f"{REDIS_PREFIX}:queue:running"
COMPLETED_LIST = f"{REDIS_PREFIX}:queue:completed"
FAILED_LIST = f"{REDIS_PREFIX}:queue:failed"
STATS_KEY = f"{REDIS_PREFIX}:stats"
DOCKING_PROCESSED_SET = f"{REDIS_PREFIX}:docking:processed"
