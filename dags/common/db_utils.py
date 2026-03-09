"""
PostgreSQL 同步连接工具
保持与原 aidd-platform 数据库操作一致
"""

import psycopg2
from .config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB


def get_sync_connection():
    """
    获取 PostgreSQL 同步连接
    调用方负责关闭连接（建议使用 try-finally）
    """
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    )
