"""
MinIO 文件操作工具
保持与原 aidd-platform storage 模块兼容
"""

import io
import logging
from typing import Optional

from minio import Minio

from .config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, MINIO_SECURE

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """获取 MinIO 客户端"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def upload_bytes(content: bytes, object_name: str, content_type: str = 'application/octet-stream') -> str:
    """
    上传字节数据到 MinIO

    Returns:
        minio:// 前缀的对象路径
    """
    client = get_minio_client()
    # 确保 bucket 存在
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    client.put_object(
        MINIO_BUCKET,
        object_name,
        io.BytesIO(content),
        length=len(content),
        content_type=content_type,
    )
    logger.info(f"文件已上传到 MinIO: {object_name}")
    return f'minio://{object_name}'


def upload_csv_string(content: str, object_name: str) -> str:
    """上传 CSV 字符串到 MinIO"""
    return upload_bytes(content.encode('utf-8'), object_name, content_type='text/csv')


def download_file_content(object_name: str) -> bytes:
    """
    从 MinIO 下载文件内容

    Args:
        object_name: 对象名称（不含 minio:// 前缀）
    """
    # 去掉 minio:// 前缀
    if object_name.startswith('minio://'):
        object_name = object_name[len('minio://'):]

    client = get_minio_client()
    response = client.get_object(MINIO_BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def download_file_as_text(object_name: str) -> str:
    """从 MinIO 下载文件并解码为字符串"""
    return download_file_content(object_name).decode('utf-8')
